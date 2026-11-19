#include "rpc_scheme_base.h"
#include "service_replication.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/replication/checksum/checksum.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/ydb_convert/tx_proxy_status.h>
#include <util/generic/guid.h>

namespace NKikimr::NGRpcService {

using TEvPublishTableChecksum = TGrpcRequestOperationCall<Ydb::Replication::PublishTableChecksumRequest,
    Ydb::Replication::PublishTableChecksumResponse>;

class TPublishTableChecksumRPC: public TRpcSchemeRequestActor<TPublishTableChecksumRPC, TEvPublishTableChecksum> {
    using TBase = TRpcSchemeRequestActor<TPublishTableChecksumRPC, TEvPublishTableChecksum>;
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TChecksumResult = NReplication::NChecksum::TEvResult;
    TString Path;
    TString Database;
    TInstant Deadline;
    NKikimrChangeExchange::TCdcChecksum Checksum;
    TTableId TableId;
    TActorId Query;
    bool CheckpointPublished = false;
    bool PublishingResult = false;
    bool AwaitingOperation = false;
    ui64 ActiveTxId = 0;
    NMonitoring::TDynamicCounters::TCounterPtr ActivePublications;
    NMonitoring::TDynamicCounters::TCounterPtr PublicationFailures;
    bool Succeeded = false;

    void Fail(Ydb::StatusIds::StatusCode status, const TString& error) {
        NYql::TIssues issues;
        issues.AddIssue(error);
        Reply(status, issues, TActivationContext::AsActorContext());
    }

    void Propose() {
        auto request = CreateProposeTransaction();
        const auto now = TActivationContext::Now();
        request->Record.SetExecTimeoutPeriod((Deadline > now ? Deadline - now : TDuration::Seconds(1)).MilliSeconds());
        auto* modify = request->Record.MutableTransaction()->MutableModifyScheme();
        modify->SetWorkingDir(TString(ExtractParent(Path)));
        modify->SetOperationType(NKikimrSchemeOp::ESchemeOpAlterCdcStream);
        auto* alter = modify->MutableAlterCdcStream();
        alter->SetTableName(NKikimr::SplitPath(Path).back());
        *alter->MutableChecksum() = Checksum;
        AwaitingOperation = true;
        ActiveTxId = 0;
        Send(MakeTxProxyID(), request.release());
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& entries = ev->Get()->Request->ResultSet;
        if (entries.size() != 1 || entries.front().Status != TNavigate::EStatus::Ok) {
            return Fail(entries.size() == 1 && entries.front().Status == TNavigate::EStatus::AccessDenied
                ? Ydb::StatusIds::UNAUTHORIZED : Ydb::StatusIds::SCHEME_ERROR, "Cannot resolve checksum table");
        }
        const auto& entry = entries.front();
        if (entry.Kind != TNavigate::KindTable) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Checksum publication requires a row table");
        }
        TString error;
        Checksum.SetSchema(NReplication::NChecksum::DescribeSchema(entry, error));
        if (!error.empty()) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, error);
        }
        TableId = entry.TableId;
        Checksum.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        Checksum.MutableTableId()->SetLocalId(TableId.PathId.LocalPathId);
        Checksum.SetTableSchemaVersion(TableId.SchemaVersion);
        Propose();
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        if (!AwaitingOperation) {
            return;
        }
        ActiveTxId = ev->Get()->Record.GetTxId();
        const auto status = YdbStatusFromProxyStatus(ev->Get());
        if (status == Ydb::StatusIds::SUCCESS) {
            return OperationCompleted();
        }
        if (!IsTxProxyInProgress(status)) {
            if (PublishingResult && Checksum.GetStatus() == Ydb::StatusIds::SUCCESS) {
                // Revalidation may reject a successful scan after a feed or
                // schema change. Publish a failure to surviving original feeds
                // and release the reservation instead of waiting for expiry.
                Checksum.SetStatus(status);
                Checksum.ClearChecksum();
                *Checksum.MutableIssues() = ev->Get()->Record.GetIssues();
                return Propose();
            }
            return Reply(status, ev->Get()->Record.GetIssues(), TActivationContext::AsActorContext());
        }
        SetSchemeShardId(ev->Get()->Record.GetSchemeShardTabletId());
        ForwardToSchemeShard(TActivationContext::AsActorContext(),
            std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(ev->Get()->Record.GetTxId()));
    }

    void OnNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev,
            const TActorContext&) override {
        if (AwaitingOperation && ev->Get()->Record.GetTxId() == ActiveTxId) {
            OperationCompleted();
        }
    }

    void OperationCompleted() {
        AwaitingOperation = false;
        if (PublishingResult) {
            CheckpointPublished = false;
            Succeeded = Checksum.GetStatus() == Ydb::StatusIds::SUCCESS;
            Ydb::Replication::PublishTableChecksumResult result;
            result.set_checkpoint_id(Checksum.GetCheckpointId());
            result.mutable_snapshot()->set_plan_step(Checksum.GetSnapshotStep());
            result.mutable_snapshot()->set_tx_id(Checksum.GetSnapshotTxId());
            *result.mutable_checksum() = Checksum.GetChecksum();
            for (const auto& stream : Checksum.GetStreams()) {
                result.add_changefeed_names(stream.GetName());
            }
            NYql::TIssues issues;
            NYql::IssuesFromMessage(Checksum.GetIssues(), issues);
            Request_->RaiseIssues(issues);
            return ReplyWithResult(Checksum.GetStatus(), result, TActivationContext::AsActorContext());
        }
        // Read the coordinator's chosen version and the frozen feed set.
        CheckpointPublished = true;
        auto request = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(request, *Request_);
        SetDatabase(request.get(), *Request_);
        request->Record.MutableDescribePath()->SetPath(Path);
        Send(MakeTxProxyID(), request.release());
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        const auto& table = record.GetPathDescription().GetTable();
        if (record.GetStatus() != NKikimrScheme::StatusSuccess ||
            table.GetCdcChecksum().GetCheckpointId() != Checksum.GetCheckpointId() ||
            !table.GetCdcChecksum().GetSnapshotStep())
        {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Source checksum checkpoint is no longer available");
        }
        Checksum = table.GetCdcChecksum();
        NReplication::NChecksum::TSettings settings;
        settings.Database = Database;
        settings.Path = Path;
        settings.TableId = TableId;
        settings.Schema = Checksum.GetSchema();
        settings.Snapshot = TRowVersion(Checksum.GetSnapshotStep(), Checksum.GetSnapshotTxId());
        settings.Deadline = Deadline;
        if (auto token = Request_->GetSerializedToken(); !token.empty()) {
            settings.UserToken = std::move(token);
        }
        Query = Register(NReplication::NChecksum::CreateChecksum(SelfId(), std::move(settings)));
    }

    void Handle(TChecksumResult::TPtr& ev) {
        if (ev->Sender != Query) {
            return;
        }
        Query = {};
        Checksum.SetResult(true);
        Checksum.SetStatus(ev->Get()->Status);
        *Checksum.MutableChecksum() = ev->Get()->Checksum;
        NYql::IssuesToMessage(ev->Get()->Issues, Checksum.MutableIssues());
        PublishingResult = true;
        Propose();
    }

    void PassAway() override {
        if (ActivePublications) {
            ActivePublications->Dec();
            if (!Succeeded) {
                PublicationFailures->Inc();
            }
            ActivePublications = nullptr;
        }
        if (Query) {
            Send(Query, new TEvents::TEvPoison());
        }
        if (CheckpointPublished && !PublishingResult) {
            // Best effort terminal notification; snapshots and the reservation
            // still expire if this actor or its node disappears first.
            Checksum.SetResult(true);
            Checksum.SetStatus(Ydb::StatusIds::CANCELLED);
            Checksum.AddIssues()->set_message("Source checksum request ended before publication completed");
            Propose();
        }
        TBase::PassAway();
    }

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        const auto counters = GetServiceCounters(AppData()->Counters, "replication")
            ->GetSubgroup("subsystem", "checksum_publication");
        ActivePublications = counters->GetCounter("Active", false);
        PublicationFailures = counters->GetCounter("Failures", true);
        ActivePublications->Inc();
        if (!AppData()->FeatureFlags.GetEnableAsyncReplicationChecksumCheck()) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "CDC checksums are disabled");
        }
        if (GetProtoRequest()->operation_params().operation_mode() == Ydb::Operations::OperationParams::ASYNC ||
            GetProtoRequest()->path().empty())
        {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Expected one table path and synchronous operation mode");
        }
        auto timeout = TDuration::Hours(1);
        if (GetOperationTimeout()) {
            timeout = Min(timeout, GetOperationTimeout());
        }
        if (GetCancelAfter()) {
            timeout = Min(timeout, GetCancelAfter());
        }
        Deadline = ctx.Now() + timeout;
        Path = Request_->NormalizePath(GetProtoRequest()->path());
        Database = Request_->GetDatabaseName().GetOrElse("");
        Checksum.SetCheckpointId(CreateGuidAsString());
        Checksum.SetDeadlineUs(Deadline.MicroSeconds());
        Checksum.SetProtocolVersion(1);
        Checksum.SetAlgorithm("CityHash64");
        Checksum.SetAlgorithmVersion(1);
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = Database;
        if (auto token = Request_->GetSerializedToken(); !token.empty()) {
            request->UserToken = MakeIntrusive<NACLib::TUserToken>(token);
        }
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = NKikimr::SplitPath(Path);
        entry.Operation = TNavigate::OpTable;
        entry.Access = NACLib::DescribeSchema | NACLib::SelectRow | NACLib::AlterSchema;
        entry.SyncVersion = true;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Schedule(TDuration::Hours(1), new TEvents::TEvWakeup());
        Become(&TThis::StateWork);
    }

    bool HasCancelOperation() {
        return true;
    }

    void OnCancelOperation(const TActorContext&) {
        Fail(Ydb::StatusIds::CANCELLED, "Checksum publication cancelled");
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TChecksumResult, Handle);
            case TEvents::TEvWakeup::EventType:
                if (ev->Get<TEvents::TEvWakeup>()->Tag == 0) {
                    return Timeout();
                }
                return TBase::StateWork(ev);
        default:
            TBase::StateWork(ev);
        }
    }

    void Timeout() {
        Fail(Ydb::StatusIds::TIMEOUT, "Checksum publication deadline expired");
    }
};

void DoPublishTableChecksum(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider& facility) {
    facility.RegisterActor(new TPublishTableChecksumRPC(request.release()));
}

}
