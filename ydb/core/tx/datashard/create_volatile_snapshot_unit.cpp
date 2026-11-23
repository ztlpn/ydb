#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

#include <ydb/core/protos/change_exchange.pb.h>

#include <ydb/library/aclib/user_context.h>

namespace NKikimr {
namespace NDataShard {

class TCreateVolatileSnapshotUnit : public TExecutionUnit {
public:
    TCreateVolatileSnapshotUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CreateVolatileSnapshot, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ENSURE(op->IsSnapshotTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        const auto& record = tx->GetSnapshotTx();
        if (record.HasPublishChecksum()) {
            return PublishChecksum(op, record.GetPublishChecksum(), txc, ctx);
        }
        if (!record.HasCreateVolatileSnapshot()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = record.GetCreateVolatileSnapshot();

        ui64 ownerId = params.GetOwnerId();
        ui64 pathId = params.GetPathId();
        ui64 step = tx->GetStep();
        ui64 txId = tx->GetTxId();
        Y_ENSURE(step != 0);

        const TSnapshotKey key(ownerId, pathId, step, txId);

        ui64 flags = 0;
        TDuration timeout;
        if (params.HasTimeoutMs()) {
            flags |= TSnapshot::FlagTimeout;
            timeout = TDuration::MilliSeconds(params.GetTimeoutMs());
        }

        bool added = DataShard.GetSnapshotManager().AddSnapshot(
                txc.DB, key, params.GetName(), flags, timeout);

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        if (added) {
            DataShard.GetSnapshotManager().InitSnapshotExpireTime(key, ctx.Now());
            DataShard.PlanCleanup(ctx);

            return EExecutionStatus::ExecutedNoMoreRestarts;
        } else {
            return EExecutionStatus::Executed;
        }
    }

    EExecutionStatus PublishChecksum(
        TOperation::TPtr op, NKikimrChangeExchange::TCdcChecksum checksum, TTransactionContext& txc, const TActorContext& ctx) {
        const auto pathId = TPathId::FromProto(checksum.GetTableId());
        const auto* current = DataShard.GetUserTables().FindPtr(pathId.LocalPathId);
        const bool failure = checksum.GetResult() && checksum.GetStatus() != Ydb::StatusIds::SUCCESS;
        if (!current || (!failure && (*current)->GetTableSchemaVersion() != checksum.GetTableSchemaVersion())) {
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED)->AddError(NKikimrTxDataShard::TError::SCHEME_ERROR,
                "Checksum table changed since prepare");
            return EExecutionStatus::Executed;
        }
        const auto& table = **current;
        if (!failure && AnyOf(checksum.GetStreams(), [&](const auto& stream) {
                const auto* feed = table.CdcStreams.FindPtr(TPathId::FromProto(stream.GetPathId()));
                return !feed || feed->State != NKikimrSchemeOp::ECdcStreamStateReady || feed->Format != NKikimrSchemeOp::ECdcStreamFormatJson;
            })) {
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED)->AddError(NKikimrTxDataShard::TError::SCHEME_ERROR,
                "Selected changefeed changed since prepare");
            return EExecutionStatus::Executed;
        }
        if (!checksum.GetResult()) {
            checksum.SetSnapshotStep(op->GetStep());
            checksum.SetSnapshotTxId(op->GetTxId());
            const TSnapshotKey key(pathId, op->GetStep(), op->GetTxId());
            const auto now = ctx.Now();
            const auto timeout = Max(TInstant::MicroSeconds(checksum.GetDeadlineUs()), now) - now;
            if (DataShard.GetSnapshotManager().AddSnapshot(
                    txc.DB, key, "cdc-checksum", TSnapshot::FlagTimeout, Max(timeout, TDuration::MilliSeconds(1)))) {
                DataShard.GetSnapshotManager().InitSnapshotExpireTime(key, ctx.Now());
                DataShard.PlanCleanup(ctx);
            }
        }
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& stream : checksum.GetStreams()) {
            const auto* feed = table.CdcStreams.FindPtr(TPathId::FromProto(stream.GetPathId()));
            if (!feed || feed->State != NKikimrSchemeOp::ECdcStreamStateReady) {
                continue;
            }
            auto control = checksum;
            control.ClearStreams();
            *control.AddStreams() = stream;
            *control.MutableExpectedPartitions() = stream.GetPartitionIds();
            auto record = TChangeRecordBuilder(
                checksum.GetResult() ? TChangeRecord::EKind::CdcChecksumResult : TChangeRecord::EKind::CdcChecksumCheckpoint)
                              .WithOrder(DataShard.AllocateChangeRecordOrder(db))
                              .WithGroup(0)
                              .WithStep(op->GetStep())
                              .WithTxId(op->GetTxId())
                              .WithPathId(TPathId::FromProto(stream.GetPathId()))
                              .WithTableId(pathId)
                              .WithSchemaVersion(table.GetTableSchemaVersion())
                              .WithBody(control.SerializeAsString())
                              .WithUserCtx(NACLib::TUserContextBuilder().WithUserSID(BUILTIN_ACL_CDC_WITHOUT_USER_SID).Build()).Build();
            DataShard.PersistChangeRecord(db, *record);
            op->ChangeRecords().push_back(IDataShardChangeCollector::TChange{
                .Order = record->GetOrder(),
                .Group = record->GetGroup(),
                .Step = record->GetStep(),
                .TxId = record->GetTxId(),
                .PathId = record->GetPathId(),
                .BodySize = record->GetBodySize(),
                .TableId = record->GetTableId(),
                .SchemaVersion = record->GetSchemaVersion(),
            });
        }
        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());
        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext&) override {
        if (!op->ChangeRecords().empty()) {
            DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
            DataShard.EmitHeartbeats();
        }
    }
};

THolder<TExecutionUnit> CreateCreateVolatileSnapshotUnit(
        TDataShard& dataShard,
        TPipeline& pipeline)
{
    return THolder(new TCreateVolatileSnapshotUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
