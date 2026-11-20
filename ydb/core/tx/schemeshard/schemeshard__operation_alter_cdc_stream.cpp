#include "schemeshard__operation_alter_cdc_stream.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_cdc_stream_common.h"

#include <ydb/core/scheme/scheme_type_info.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <numeric>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

namespace NCdc {

namespace {

class TPropose: public TSubOperationState {
    virtual const char* Name() const override final { return "TPropose"; }

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages({});
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterCdcStream);

        // TODO(KIKIMR-12278): shards

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        YDB_LOG_INFO_CTX(context.Ctx, "",
            {"step", step},
        );

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterCdcStream);
        const auto& pathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(pathId));
        auto stream = context.SS->CdcStreams.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        context.SS->PersistCdcStream(db, pathId);
        context.SS->CdcStreams.at(pathId)->FinishAlter();

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

private:
    const TOperationId OperationId;

}; // TPropose

class TAlterCdcStream: public TSubOperation {
    virtual const char* Name() const override final { return "TAlterCdcStream"; }

    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TProposeContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetAlterCdcStream();
        const auto& streamName = op.GetStreamName();

        YDB_LOG_NOTICE_CTX(context.Ctx, "",
            {"stream", workingDir + "/" + streamName},
        );

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        const auto streamPath = TPath::Resolve(workingDir, context.SS).Dive(streamName);
        {
            const auto checks = streamPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsCdcStream()
                .NotUnderDeleting();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto tablePath = streamPath.Parent();
        {
            const auto checks = tablePath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotAsyncReplicaTable()
                .NotUnderDeleting();

            // Allow CDC operations on tables that are under incremental backup/restore
            if (checks && tablePath.IsUnderOperation() &&
                !tablePath.IsUnderOutgoingIncrementalRestore()) {
                checks.NotUnderOperation();
            }

            if (checks && !tablePath.IsInsideTableIndexPath()) {
                checks.IsCommonSensePath();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(streamPath.Base()->PathId));
        auto stream = context.SS->CdcStreams.at(streamPath.Base()->PathId);

        TCdcStreamInfo::EState requiredState = TCdcStreamInfo::EState::ECdcStreamStateInvalid;
        TCdcStreamInfo::EState newState = TCdcStreamInfo::EState::ECdcStreamStateInvalid;

        switch (op.GetActionCase()) {
        case NKikimrSchemeOp::TAlterCdcStream::kDisable:
            requiredState = TCdcStreamInfo::EState::ECdcStreamStateDisabled;
            if (stream->State == TCdcStreamInfo::EState::ECdcStreamStateReady) {
                newState = requiredState;
            }
            break;
        case NKikimrSchemeOp::TAlterCdcStream::kGetReady:
            requiredState = TCdcStreamInfo::EState::ECdcStreamStateReady;
            if (stream->State == TCdcStreamInfo::EState::ECdcStreamStateScan) {
                newState = requiredState;
            }
            break;
        default:
            result->SetError(NKikimrScheme::StatusInvalidParameter, TStringBuilder()
                << "Unknown action: " << static_cast<ui32>(op.GetActionCase()));
            return result;
        }

        if (newState == TCdcStreamInfo::EState::ECdcStreamStateInvalid) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, TStringBuilder()
                << "Cannot switch state"
                << ": from# " << stream->State
                << ", to# " << requiredState);
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, streamPath.Base()->PathId);
        context.MemChanges.GrabCdcStream(context.SS, streamPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(streamPath.Base()->PathId);
        context.DbChanges.PersistAlterCdcStream(streamPath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        auto streamAlter = stream->CreateNextVersion();
        Y_ABORT_UNLESS(streamAlter);
        streamAlter->State = newState;

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterCdcStream, streamPath.Base()->PathId);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);

        streamPath.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
        streamPath.Base()->LastTxId = OperationId.GetTxId();

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TProposeContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "");
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TAlterCdcStream AbortUnsafe",
            {"opId", OperationId},
            {"txId", txId},
            {"schemeshard", context.SS->TabletID()},
        );
        context.OnComplete.DoneOperation(OperationId);
    }

}; // TAlterCdcStream

class TConfigurePartsAtTable: public NCdcStreamState::TConfigurePartsAtTable {
protected:
    void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const override {
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        auto& notice = *tx.MutableAlterCdcStreamNotice();
        pathId.ToProto(notice.MutablePathId());

        const auto* txState = context.SS->FindTx(OperationId);
        if (txState->CdcChecksum.HasCheckpointId()) {
            *notice.MutableChecksum() = txState->CdcChecksum;
            notice.SetTableSchemaVersion(table->AlterVersion);
            // One producer broadcasts the result; checkpoints require every producer.
            ui64 emitter = Max<ui64>();
            for (const auto& shard : txState->Shards) {
                emitter = Min(emitter, ui64(context.SS->ShardInfos.at(shard.Idx).TabletID));
            }
            notice.SetChecksumEmitterTabletId(emitter);
            return;
        }

        table->InitAlterData(OperationId);
        notice.SetTableSchemaVersion(*table->AlterData->CoordinatedSchemaVersion);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistAddAlterTable(db, pathId, table->AlterData);

        bool found = false;
        for (const auto& [childName, childPathId] : path->GetChildren()) {
            Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
            auto childPath = context.SS->PathsById.at(childPathId);

            if (!childPath->IsCdcStream() || childPath->Dropped() || childPath->NormalState()) {
                continue;
            }

            Y_ABORT_UNLESS(context.SS->CdcStreams.contains(childPathId));
            auto stream = context.SS->CdcStreams.at(childPathId);

            Y_VERIFY_S(!found, "Too many cdc streams are planned to alter"
                << ": found# " << TPathId::FromProto(notice.GetStreamDescription().GetPathId())
                << ", another# " << childPathId);
            found = true;

            Y_ABORT_UNLESS(stream->AlterData);
            context.SS->DescribeCdcStream(childPathId, childName, stream->AlterData, *notice.MutableStreamDescription());
        }
    }

public:
    using NCdcStreamState::TConfigurePartsAtTable::TConfigurePartsAtTable;

}; // TConfigurePartsAtTable

class TConfigurePartsAtTableDropSnapshot: public TConfigurePartsAtTable {
protected:
    void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const override {
        TConfigurePartsAtTable::FillNotice(pathId, tx, context);

        const auto snapshotTxId = context.SS->GetSingleTableSnapshot(pathId);
        Y_ABORT_UNLESS(context.SS->SnapshotsStepIds.contains(snapshotTxId));
        const auto snapshotStep = context.SS->SnapshotsStepIds.at(snapshotTxId);

        Y_ABORT_UNLESS(tx.HasAlterCdcStreamNotice());
        auto& notice = *tx.MutableAlterCdcStreamNotice();

        notice.MutableDropSnapshot()->SetStep(ui64(snapshotStep));
        notice.MutableDropSnapshot()->SetTxId(ui64(snapshotTxId));
    }

public:
    using TConfigurePartsAtTable::TConfigurePartsAtTable;

}; // TConfigurePartsAtTableDropSnapshot

class TAlterCdcStreamAtTable: public TSubOperation {
    virtual const char* Name() const override final { return "TAlterCdcStreamAtTable"; }

    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::ProposedWaitParts;
        case TTxState::ProposedWaitParts:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }

        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            if (DropSnapshot) {
                return MakeHolder<TConfigurePartsAtTableDropSnapshot>(OperationId);
            } else {
                return MakeHolder<TConfigurePartsAtTable>(OperationId);
            }
        case TTxState::Propose:
            if (DropSnapshot) {
                return MakeHolder<NCdcStreamState::TProposeAtTableDropSnapshot>(OperationId);
            } else {
                return MakeHolder<NCdcStreamState::TProposeAtTable>(OperationId);
            }
        case TTxState::ProposedWaitParts:
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    explicit TAlterCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool dropSnapshot)
        : TSubOperation(id, tx)
        , DropSnapshot(dropSnapshot)
    {
    }

    explicit TAlterCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool dropSnapshot)
        : TSubOperation(id, state)
        , DropSnapshot(dropSnapshot)
    {
    }

    THolder<TProposeResponse> ProposeChecksum(TProposeContext& context) {
        auto response = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
            ui64(OperationId.GetTxId()), context.SS->TabletID());
        auto reject = [&response](NKikimrScheme::EStatus status, const TString& message) {
            response->SetError(status, message);
            return std::move(response);
        };
        const auto& request = Transaction.GetAlterCdcStream();
        const auto path = TPath::Resolve(Transaction.GetWorkingDir(), context.SS)
            .Child(request.GetTableName(), TPath::TSplitChildTag{});
        const auto checks = path.Check();
        checks.NotEmpty().NotUnderDomainUpgrade().IsAtLocalSchemeShard().IsResolved()
            .NotDeleted().IsTable().NotAsyncReplicaTable().NotUnderDeleting().NotUnderOperation();
        if (!checks) {
            return reject(checks.GetStatus(), checks.GetError());
        }
        if (path.IsBackupTable() || path.IsInsideTableIndexPath()) {
            return reject(NKikimrScheme::StatusPreconditionFailed, "Checksums require a user row table");
        }
        auto table = context.SS->Tables.at(path.Base()->PathId);
        auto checksum = request.GetChecksum();
        const auto now = context.Ctx.Now();
        const bool supportedVersion = checksum.GetProtocolVersion() == 1 || (checksum.GetResult() && checksum.GetProtocolVersion() == 2);
        if (checksum.GetCheckpointId().empty() || !supportedVersion || checksum.GetAlgorithm() != "CityHash64" ||
            checksum.GetAlgorithmVersion() != 1 || TPathId::FromProto(checksum.GetTableId()) != path.Base()->PathId)
        {
            return reject(NKikimrScheme::StatusInvalidParameter, "Invalid checksum request");
        }
        if (checksum.GetResult()) {
            if (table->CdcChecksum.GetCheckpointId() != checksum.GetCheckpointId() ||
                table->CdcChecksum.GetResult())
            {
                return reject(NKikimrScheme::StatusPreconditionFailed, "Checksum attempt is no longer active");
            }
            auto saved = table->CdcChecksum;
            saved.SetResult(true);
            saved.SetStatus(checksum.GetStatus());
            *saved.MutableIssues() = checksum.GetIssues();
            *saved.MutableChecksum() = checksum.GetChecksum();
            checksum = std::move(saved);
        } else {
            if (!AppData()->FeatureFlags.GetEnableAsyncReplicationChecksumCheck()) {
                return reject(NKikimrScheme::StatusPreconditionFailed, "CDC checksums are disabled");
            }
            if (table->CdcChecksum.HasCheckpointId() && !table->CdcChecksum.GetResult() &&
                table->CdcChecksum.GetDeadlineUs() > now.MicroSeconds())
            {
                return reject(NKikimrScheme::StatusMultipleModifications, "Another checksum attempt is active");
            }
            if (checksum.GetDeadlineUs() <= now.MicroSeconds() ||
                checksum.GetDeadlineUs() > (now + TDuration::Hours(1)).MicroSeconds())
            {
                return reject(NKikimrScheme::StatusInvalidParameter, "Invalid checksum deadline");
            }
            checksum.ClearStreams();
            checksum.SetLastReservedBoundaryStep(
                Max(table->CdcChecksum.GetLastReservedBoundaryStep(), table->CdcChecksum.GetCommitBoundaryStep()));
            ui64 period = 0;
            for (const auto& [name, id] : path.Base()->GetChildren()) {
                auto* streamPtr = context.SS->CdcStreams.FindPtr(id);
                if (!streamPtr || context.SS->PathsById.at(id)->Dropped()) {
                    continue;
                }
                const auto& stream = **streamPtr;
                if (stream.State == NKikimrSchemeOp::ECdcStreamStateDisabled ||
                    stream.Format == NKikimrSchemeOp::ECdcStreamFormatProto)
                {
                    continue;
                }
                if (stream.State != NKikimrSchemeOp::ECdcStreamStateReady ||
                    stream.Format != NKikimrSchemeOp::ECdcStreamFormatJson ||
                    !context.SS->PathsById.at(id)->NormalState())
                {
                    return reject(NKikimrScheme::StatusPreconditionFailed,
                        "Every active user feed must be ready and use native JSON");
                }
                const auto interval = stream.ResolvedTimestamps.MilliSeconds();
                if (interval) {
                    const auto factor = interval / std::gcd(period, interval);
                    if (period && period > Max<ui64>() / factor) {
                        return reject(NKikimrScheme::StatusInvalidParameter, "CDC heartbeat grid overflows");
                    }
                    period = period ? period * factor : interval;
                }
                auto* selected = checksum.AddStreams();
                id.ToProto(selected->MutablePathId());
                selected->SetName(name);
            }
            if (period) {
                const auto floor = Max(now.MilliSeconds(), checksum.GetLastReservedBoundaryStep());
                if (floor / period >= Max<ui64>() / period) {
                    return reject(NKikimrScheme::StatusInvalidParameter, "CDC checksum boundary overflows");
                }
                checksum.SetProtocolVersion(2);
                checksum.SetAlignmentPeriodMs(period);
                // This is the last reserved boundary until the plan step is known.
                checksum.SetCommitBoundaryStep(checksum.GetLastReservedBoundaryStep());
            }
            if (checksum.StreamsSize() == 0) {
                return reject(NKikimrScheme::StatusPreconditionFailed, "No eligible CDC feeds");
            }
        }
        const bool terminalFailure = checksum.GetResult() && checksum.GetStatus() != Ydb::StatusIds::SUCCESS;
        if (!checksum.GetResult() && checksum.GetTableSchemaVersion() != table->AlterVersion) {
            return reject(NKikimrScheme::StatusPreconditionFailed, "Table schema changed during checksum");
        }
        if (checksum.GetResult() && !terminalFailure) {
            // Feed/index metadata also advances AlterVersion. Compare the
            // logical columns, using the same encoding as the scan helper,
            // so unrelated feed creation does not invalidate the attempt.
            TMap<TString, TString> columns;
            for (const auto& [id, column] : table->Columns) {
                if (column.IsDropped()) {
                    continue;
                }
                if (column.IsBuildInProgress || column.SetNotNullInProgress ||
                    column.CreateVersion > checksum.GetTableSchemaVersion()) {
                    return reject(NKikimrScheme::StatusPreconditionFailed, "Table schema is changing");
                }
                columns[column.Name] = TStringBuilder() << NScheme::TypeName(column.PType, column.PTypeMod)
                    << ":" << column.NotNull;
            }
            TStringBuilder schema;
            for (const auto& [name, type] : columns) {
                schema << name.size() << ":" << name << type.size() << ":" << type;
            }
            if (schema != checksum.GetSchema()) {
                return reject(NKikimrScheme::StatusPreconditionFailed, "Table schema changed during checksum");
            }
        }
        if (terminalFailure) {
            for (int i = checksum.StreamsSize() - 1; i >= 0; --i) {
                const auto id = TPathId::FromProto(checksum.GetStreams(i).GetPathId());
                const auto* stream = context.SS->CdcStreams.FindPtr(id);
                if (!stream || context.SS->PathsById.at(id)->Dropped() ||
                    (*stream)->State != NKikimrSchemeOp::ECdcStreamStateReady)
                {
                    checksum.MutableStreams()->DeleteSubrange(i, 1);
                }
            }
        }
        for (const auto& selected : checksum.GetStreams()) {
            const auto id = TPathId::FromProto(selected.GetPathId());
            const auto* stream = context.SS->CdcStreams.FindPtr(id);
            if (!stream || context.SS->PathsById.at(id)->Dropped() ||
                (*stream)->State != NKikimrSchemeOp::ECdcStreamStateReady)
            {
                return reject(NKikimrScheme::StatusPreconditionFailed, "Selected feed changed during checksum");
            }
            for (const auto& [name, childId] : context.SS->PathsById.at(id)->GetChildren()) {
                const auto* topic = context.SS->Topics.FindPtr(childId);
                if (!topic || !context.SS->PathsById.at(childId)->NormalState()) {
                    return reject(NKikimrScheme::StatusPreconditionFailed, "Selected topic is being modified");
                }
                const auto config = (*topic)->GetTabletConfig();
                if (config.HasPartitionStrategy() && config.GetPartitionStrategy().GetPartitionStrategyType() !=
                    NKikimrPQ::TPQTabletConfig::DISABLED)
                {
                    return reject(NKikimrScheme::StatusPreconditionFailed, "Checksum requires fixed topic partitions");
                }
            }
        }
        TString error;
        if (!context.SS->CheckApplyIf(Transaction, error) ||
            !context.SS->CheckLocks(path.Base()->PathId, Transaction, error))
        {
            return reject(NKikimrScheme::StatusPreconditionFailed, error);
        }
        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, path.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.DbChanges.PersistPath(path.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterCdcStreamAtTable, path.Base()->PathId);
        txState.State = TTxState::ConfigureParts;
        txState.CdcChecksum = std::move(checksum);
        path.Base()->PathState = NKikimrSchemeOp::EPathStateAlter;
        path.Base()->LastTxId = OperationId.GetTxId();
        for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
        }
        context.OnComplete.ActivateTx(OperationId);
        SetState(NextState());
        return response;
    }

    THolder<TProposeResponse> Propose(const TString&, TProposeContext& context) override {
        if (Transaction.GetAlterCdcStream().HasChecksum()) {
            return ProposeChecksum(context);
        }
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetAlterCdcStream();
        const auto& tableName = op.GetTableName();
        const auto& streamName = op.GetStreamName();

        YDB_LOG_NOTICE_CTX(context.Ctx, "",
            {"stream", workingDir + "/" + tableName + "/" + streamName},
        );

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        const auto tablePath = TPath::Resolve(workingDir, context.SS).Child(tableName, TPath::TSplitChildTag{});
        {
            const auto checks = tablePath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotAsyncReplicaTable()
                .NotUnderDeleting();

            // Allow CDC operations on tables that are under incremental backup/restore
            if (checks && tablePath.IsUnderOperation() &&
                !tablePath.IsUnderOutgoingIncrementalRestore()) {
                checks.NotUnderOperation();
            }

            if (checks && !tablePath.IsInsideTableIndexPath()) {
                checks.IsCommonSensePath();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto streamPath = tablePath.Child(streamName);
        {
            const auto checks = streamPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsCdcStream()
                .IsUnderOperation()
                .IsUnderTheSameOperation(OperationId.GetTxId());

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        if (DropSnapshot && !context.SS->HasTableSnapshot(tablePath.Base()->PathId)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, TStringBuilder() << "Table has no snapshots"
                << ": pathId# " << tablePath.Base()->PathId);
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(tablePath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
        auto table = context.SS->Tables.at(tablePath.Base()->PathId);

        Y_ABORT_UNLESS(table->AlterVersion != 0);

        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(streamPath.Base()->PathId));
        auto stream = context.SS->CdcStreams.at(streamPath.Base()->PathId);

        Y_ABORT_UNLESS(stream->AlterVersion != 0);
        Y_ABORT_UNLESS(stream->AlterData);

        const auto txType = DropSnapshot
            ? TTxState::TxAlterCdcStreamAtTableDropSnapshot
            : TTxState::TxAlterCdcStreamAtTable;

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, txType, tablePath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;
        txState.CdcPathId = streamPath.Base()->PathId;  // Store CDC stream PathId for later use

        tablePath.Base()->PathState = NKikimrSchemeOp::EPathStateAlter;
        tablePath.Base()->LastTxId = OperationId.GetTxId();

        for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
        }

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TProposeContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "");
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TAlterCdcStreamAtTable AbortUnsafe",
            {"opId", OperationId},
            {"txId", txId},
            {"schemeshard", context.SS->TabletID()},
        );
        context.OnComplete.DoneOperation(OperationId);
    }

private:
    const bool DropSnapshot;

}; // TAlterCdcStreamAtTable

} // anonymous

std::variant<TStreamPaths, ISubOperation::TPtr> DoAlterStreamPathChecks(
        const TOperationId& opId,
        const TPath& workingDirPath,
        const TString& tableName,
        const TString& streamName)
{
    const auto tablePath = workingDirPath.Child(tableName, TPath::TSplitChildTag{});
    {
        const auto checks = tablePath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotAsyncReplicaTable();

        // Allow CDC operations on tables that are under incremental backup/restore
        if (checks && tablePath.IsUnderOperation() &&
            !tablePath.IsUnderOutgoingIncrementalRestore()) {
            checks.NotUnderOperation();
        }

        if (checks && !tablePath.IsInsideTableIndexPath()) {
            checks.IsCommonSensePath();
        }

        if (!checks) {
            return CreateReject(opId, checks.GetStatus(), checks.GetError());
        }
    }

    const auto streamPath = tablePath.Child(streamName);
    {
        const auto checks = streamPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsCdcStream()
            .NotUnderOperation();

        if (!checks) {
            return CreateReject(opId, checks.GetStatus(), checks.GetError());
        }
    }

    return TStreamPaths{tablePath, streamPath};
}

void DoAlterStream(
        TVector<ISubOperation::TPtr>& result,
        const NKikimrSchemeOp::TAlterCdcStream& op,
        const TOperationId& opId,
        const TPath& workingDirPath,
        const TPath& tablePath)
{
    {
        auto outTx = TransactionTemplate(tablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamImpl);
        outTx.MutableAlterCdcStream()->CopyFrom(op);
        if (op.HasGetReady()) {
            outTx.MutableLockGuard()->SetOwnerTxId(op.GetGetReady().GetLockTxId());
        }

        result.push_back(CreateAlterCdcStreamImpl(NextPartId(opId, result), outTx));
    }
    {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamAtTable);
        outTx.MutableAlterCdcStream()->CopyFrom(op);
        if (op.HasGetReady()) {
            outTx.MutableLockGuard()->SetOwnerTxId(op.GetGetReady().GetLockTxId());
        }

        result.push_back(CreateAlterCdcStreamAtTable(NextPartId(opId, result), outTx, op.HasGetReady()));
    }
}

} // namespace NCdc

using namespace NCdc;

ISubOperation::TPtr CreateAlterCdcStreamImpl(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterCdcStream>(id, tx);
}

ISubOperation::TPtr CreateAlterCdcStreamImpl(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TAlterCdcStream>(id, state);
}

ISubOperation::TPtr CreateAlterCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool dropSnapshot) {
    return MakeSubOperation<TAlterCdcStreamAtTable>(id, tx, dropSnapshot);
}

ISubOperation::TPtr CreateAlterCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool dropSnapshot) {
    return MakeSubOperation<TAlterCdcStreamAtTable>(id, state, dropSnapshot);
}

TVector<ISubOperation::TPtr> CreateAlterCdcStream(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStream);

    YDB_LOG_DEBUG_CTX(context.Ctx, "CreateAlterCdcStream",
        {"opId", opId},
        {"tx", tx.ShortDebugString()},
        {"schemeshard", context.SS->TabletID()},
    );

    const auto& op = tx.GetAlterCdcStream();
    if (op.HasChecksum()) {
        return {CreateAlterCdcStreamAtTable(opId, tx, false)};
    }
    const auto& tableName = op.GetTableName();
    const auto& streamName = op.GetStreamName();

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);

    const auto checksResult = DoAlterStreamPathChecks(opId, workingDirPath, tableName, streamName);
    if (std::holds_alternative<ISubOperation::TPtr>(checksResult)) {
        return {std::get<ISubOperation::TPtr>(checksResult)};
    }

    const auto [tablePath, streamPath] = std::get<TStreamPaths>(checksResult);

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (!context.SS->CheckLocks(tablePath.Base()->PathId, tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
    }

    TVector<ISubOperation::TPtr> result;

    DoAlterStream(result, op, opId, workingDirPath, tablePath);

    if (op.HasGetReady()) {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropLock);
        outTx.SetFailOnExist(true);
        outTx.SetInternal(true);
        outTx.MutableLockConfig()->SetName(tablePath.LeafName());
        outTx.MutableLockGuard()->SetOwnerTxId(op.GetGetReady().GetLockTxId());

        result.push_back(DropLock(NextPartId(opId, result), outTx));
    }

    if (workingDirPath.IsTableIndex() && !streamName.EndsWith("_continuousBackupImpl")) {
        auto outTx = TransactionTemplate(workingDirPath.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex);
        outTx.MutableAlterTableIndex()->SetName(workingDirPath.LeafName());
        outTx.MutableAlterTableIndex()->SetState(NKikimrSchemeOp::EIndexState::EIndexStateReady);
        NCdcStreamAtTable::MaybeGuardIndexWithBaseCdcScanLock(outTx, workingDirPath, context);

        result.push_back(CreateAlterTableIndex(NextPartId(opId, result), outTx));
    }

    return result;
}

}

#undef YDB_LOG_THIS_FILE_COMPONENT
