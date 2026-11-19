#include "datashard_impl.h"
#include <ydb/library/aclib/user_context.h>
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TAlterCdcStreamUnit : public TExecutionUnit {
public:
    TAlterCdcStreamUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::AlterCdcStream, false, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ENSURE(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasAlterCdcStreamNotice()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetAlterCdcStreamNotice();
        if (params.HasChecksum()) {
            return ExecuteChecksum(op, params, txc, ctx);
        }
        const auto& streamDesc = params.GetStreamDescription();
        const auto streamPathId = TPathId::FromProto(streamDesc.GetPathId());
        const auto state = streamDesc.GetState();

        const auto pathId = TPathId::FromProto(params.GetPathId());
        Y_ENSURE(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto version = params.GetTableSchemaVersion();
        Y_ENSURE(version);

        TUserTable::TPtr tableInfo;
        switch (state) {
        case NKikimrSchemeOp::ECdcStreamStateDisabled:
            tableInfo = DataShard.AlterTableSwitchCdcStreamState(ctx, txc, pathId, version, streamPathId, state);
            DataShard.GetCdcStreamHeartbeatManager().DropCdcStream(txc.DB, pathId, streamPathId);
            break;

        case NKikimrSchemeOp::ECdcStreamStateReady:
            tableInfo = DataShard.AlterTableSwitchCdcStreamState(ctx, txc, pathId, version, streamPathId, state);

            if (params.HasDropSnapshot()) {
                const auto& snapshot = params.GetDropSnapshot();
                Y_ENSURE(snapshot.GetStep() != 0);

                const TSnapshotKey key(pathId, snapshot.GetStep(), snapshot.GetTxId());
                DataShard.GetSnapshotManager().RemoveSnapshot(txc.DB, key);
            } else {
                Y_DEBUG_ABORT("Absent snapshot");
            }

            if (const auto heartbeatInterval = TDuration::MilliSeconds(streamDesc.GetResolvedTimestampsIntervalMs())) {
                DataShard.GetCdcStreamHeartbeatManager().AddCdcStream(txc.DB, pathId, streamPathId, heartbeatInterval);
            }
            break;

        default:
            Y_ENSURE(false, "Unexpected alter cdc stream"
                << ": params# " << params.ShortDebugString());
        }

        Y_ENSURE(tableInfo);
        TDataShardLocksDb locksDb(DataShard, txc);
        DataShard.ReplaceUserTable(pathId, tableInfo, locksDb);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        auto& scanManager = DataShard.GetCdcStreamScanManager();
        scanManager.Forget(txc.DB, pathId, streamPathId);
        if (const auto* info = scanManager.Get(streamPathId)) {
            DataShard.CancelScan(tableInfo->LocalTid, info->ScanId);
            scanManager.Complete(streamPathId);
        }

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    EExecutionStatus ExecuteChecksum(TOperation::TPtr op,
            const NKikimrTxDataShard::TAlterCdcStreamNotice& params,
            TTransactionContext& txc, const TActorContext& ctx) {
        auto checksum = params.GetChecksum();
        const auto pathId = TPathId::FromProto(params.GetPathId());
        const auto& table = *DataShard.GetUserTables().at(pathId.LocalPathId);
        if (!checksum.GetResult()) {
            checksum.SetSnapshotStep(op->GetStep());
            checksum.SetSnapshotTxId(op->GetTxId());
            const TSnapshotKey key(pathId, op->GetStep(), op->GetTxId());
            const auto deadline = TInstant::MicroSeconds(checksum.GetDeadlineUs());
            const auto timeout = deadline > ctx.Now() ? deadline - ctx.Now() : TDuration::MilliSeconds(1);
            if (DataShard.GetSnapshotManager().AddSnapshot(txc.DB, key,
                checksum.GetCheckpointId(), TSnapshot::FlagTimeout, timeout))
            {
                DataShard.GetSnapshotManager().InitSnapshotExpireTime(key, ctx.Now());
                DataShard.PlanCleanup(ctx);
            }
        } else {
            DataShard.GetSnapshotManager().RemoveSnapshot(txc.DB,
                TSnapshotKey(pathId, checksum.GetSnapshotStep(), checksum.GetSnapshotTxId()));
        }
        if (!checksum.GetResult() || DataShard.TabletID() == params.GetChecksumEmitterTabletId()) {
            NIceDb::TNiceDb db(txc.DB);
            for (const auto& stream : checksum.GetStreams()) {
                auto record = TChangeRecordBuilder(checksum.GetResult()
                        ? TChangeRecord::EKind::CdcChecksumResult : TChangeRecord::EKind::CdcChecksumCheckpoint)
                    .WithOrder(DataShard.AllocateChangeRecordOrder(db))
                    .WithGroup(0)
                    .WithStep(op->GetStep())
                    .WithTxId(op->GetTxId())
                    .WithPathId(TPathId::FromProto(stream.GetPathId()))
                    .WithTableId(pathId)
                    .WithSchemaVersion(table.GetTableSchemaVersion())
                    .WithBody(checksum.SerializeAsString())
                    .WithUserCtx(NACLib::TUserContextBuilder().WithUserSID(BUILTIN_ACL_CDC_WITHOUT_USER_SID).Build())
                    .Build();
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
        }
        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());
        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext&) override {
        DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
        DataShard.EmitHeartbeats();
    }
};

THolder<TExecutionUnit> CreateAlterCdcStreamUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TAlterCdcStreamUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
