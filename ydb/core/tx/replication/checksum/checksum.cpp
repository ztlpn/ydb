#include "checksum_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/request_types.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/table_digest/aggregate.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/string/escape.h>
#include <util/string/subst.h>

namespace NKikimr::NReplication::NChecksum {
namespace {

class TChecksumQueryActor: public TQueryBase {
public:
    TChecksumQueryActor(const TActorId& parent, const TSettings& settings)
        : TQueryBase(NKikimrServices::REPLICATION_CONTROLLER, {}, settings.Database, !settings.UserToken, false, settings.UserToken)
        , Parent(parent)
        , Path(settings.Path)
        , Algorithm(NYql::NDigest::TryParseAlgorithm(settings.Algorithm))
        , Deadline(settings.Deadline)
    {
        RequestType = TString(NRequestTypes::ChecksumTable);
        MvccVersion = settings.Snapshot;
    }

private:
    void OnRunQuery() override {
        if (Cancelled) {
            return Finish(Ydb::StatusIds::CANCELLED, "Checksum cancelled");
        }
        const auto now = TActivationContext::Now();
        if (now >= Deadline) {
            return Finish(Ydb::StatusIds::TIMEOUT, "Checksum deadline expired");
        }
        if (!Algorithm) {
            return Finish(Ydb::StatusIds::UNSUPPORTED, "Unsupported checksum algorithm");
        }
        auto path = EscapeC(Path);
        SubstGlobal(path, "`", "\\`");
        const auto name = NYql::NDigest::GetAlgorithmName(*Algorithm);
        RunDataQuery(TStringBuilder() << "CHECKSUM TABLE `" << path << "` WITH (ALGORITHM = \"" << name << "\");",
            nullptr, TTxControl::BeginAndCommitTx(true), Deadline - now);
    }

    void OnQueryResult() override {
        if (Finished) {
            return;
        }
        if (ResultSets.size() != 1 || ResultSets[0].RowsCount() != 1 || ResultSets[0].ColumnsCount() != 5) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected checksum result shape");
        }
        NYdb::TResultSetParser parser(ResultSets[0]);
        parser.TryNextRow();
        if (parser.ColumnParser("version").GetUint32() != 1
            || NYql::NDigest::GetAlgorithmName(*Algorithm) != parser.ColumnParser("algorithm").GetUtf8())
        {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unsupported checksum result format");
        }
        auto& timestamp = parser.ColumnParser("mvcc_timestamp");
        timestamp.OpenOptional();
        if (timestamp.IsNull()) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing checksum snapshot");
        }
        TRowVersion actual{};
        timestamp.OpenStruct();
        while (timestamp.TryNextMember()) {
            if (timestamp.GetMemberName() == "plan_step") {
                actual.Step = timestamp.GetUint64();
            } else if (timestamp.GetMemberName() == "tx_id") {
                actual.TxId = timestamp.GetUint64();
            }
        }
        timestamp.CloseStruct();
        timestamp.CloseOptional();
        if (actual != *MvccVersion) {
            return Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Checksum was calculated at a different snapshot");
        }
        const auto count = parser.ColumnParser("count").GetUint64();
        const auto hash = parser.ColumnParser("hash").GetString();
        if (!NYql::NDigest::IsValidHexDigest(*Algorithm, hash)) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected checksum hash format");
        }
        Checksum.set_row_count(count);
        Checksum.set_hash(hash);
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (Reported) {
            return;
        }
        Reported = true;
        auto result = MakeHolder<TEvResult>();
        result->Status = status;
        result->Issues = std::move(issues);
        result->Checksum = std::move(Checksum);
        Send(Parent, result.Release());
    }

    void StateFunc(TAutoPtr<IEventHandle>& ev) override {
        if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType) {
            Cancelled = true;
            // If CreateSession is still in flight, OnRunQuery will close it as
            // soon as it arrives instead of leaving an orphan KQP session.
            if (SessionId && !Finished) {
                Finish(Ydb::StatusIds::CANCELLED, "Checksum cancelled", false);
            }
        } else {
            TQueryBase::StateFunc(ev);
        }
    }

    const TActorId Parent;
    const TString Path;
    const TMaybe<NYql::NDigest::EAlgorithm> Algorithm;
    const TInstant Deadline;
    Ydb::Replication::TableChecksum Checksum;
    bool Cancelled = false;
    bool Reported = false;
};


// Verify identity and schema before and after the read. Snapshots retain data,
// not a promise that a path still denotes the same table or column layout.
class TVerifiedChecksum: public TActorBootstrapped<TVerifiedChecksum> {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    const TActorId Parent;
    const TSettings Settings;
    TMaybe<TString> Schema;
    THashMap<ui32, TString> ColumnNames;
    TActorId Query;
    THolder<TEvResult> Result;

    void Navigate() {
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = Settings.Database;
        if (Settings.UserToken) {
            request->UserToken = MakeIntrusive<NACLib::TUserToken>(*Settings.UserToken);
        }
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(Settings.Path);
        entry.Operation = TNavigate::OpTable;
        entry.Access = NACLib::DescribeSchema | NACLib::SelectRow;
        entry.SyncVersion = true;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Fail(Ydb::StatusIds::StatusCode status, const TString& error) {
        Result = MakeHolder<TEvResult>();
        Result->Status = status;
        Result->Issues.AddIssue(error);
        Send(Parent, Result.Release());
        PassAway();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ResultSet.size() != 1) {
            return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Invalid checksum schema response");
        }
        const auto& entry = ev->Get()->Request->ResultSet.front();
        if (entry.Status != TNavigate::EStatus::Ok) {
            return Fail(entry.Status == TNavigate::EStatus::AccessDenied ? Ydb::StatusIds::UNAUTHORIZED
                : Ydb::StatusIds::SCHEME_ERROR, "Cannot resolve checksum table");
        }
        if (entry.Kind != TNavigate::KindTable || !Settings.TableId.HasSamePath(entry.TableId)) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Checksum table was replaced");
        }
        TString error;
        const auto schema = DescribeSchema(entry, error);
        THashMap<ui32, TString> columnNames;
        for (const auto& [tag, column] : entry.Columns) {
            columnNames.emplace(tag, column.Name);
        }
        if (!error.empty() || (Schema && (*Schema != schema || ColumnNames != columnNames)) ||
            (!Settings.Schema.empty() && Settings.Schema != schema)) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Checksum table schema changed");
        }
        // The destination uses its own schema; ordered CDC schema barriers keep
        // later replicated DDL parked while this snapshot is being checked.
        Schema = schema;
        ColumnNames = std::move(columnNames);
        if (Result) {
            Send(Parent, Result.Release());
            return PassAway();
        }
        Query = Register(new TChecksumQueryActor(SelfId(), Settings));
    }

    void Handle(TEvResult::TPtr& ev) {
        Query = {};
        Result.Reset(ev->Release().Release());
        if (Result->Status != Ydb::StatusIds::SUCCESS) {
            Send(Parent, Result.Release());
            return PassAway();
        }
        Navigate();
    }

    void PassAway() override {
        if (Query) {
            Send(Query, new TEvents::TEvPoison());
        }
        TActorBootstrapped::PassAway();
    }

public:
    TVerifiedChecksum(const TActorId& parent, TSettings settings)
        : Parent(parent), Settings(std::move(settings)) {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        const auto now = TActivationContext::Now();
        if (now >= Settings.Deadline) {
            return Fail(Ydb::StatusIds::TIMEOUT, "Checksum deadline expired");
        }
        Schedule(Settings.Deadline - now, new TEvents::TEvWakeup());
        Navigate();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvResult, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            cFunc(TEvents::TEvWakeup::EventType, Timeout);
        }
    }

    void Timeout() {
        Fail(Ydb::StatusIds::TIMEOUT, "Checksum deadline expired");
    }
};

class TSnapshotActor: public TActorBootstrapped<TSnapshotActor> {
    const TActorId Parent;
    const TString Database;
    const TString Path;
    const TInstant Deadline;
    const TMaybe<TString> UserToken;
    const TMaybe<NKikimrChangeExchange::TCdcChecksum> Publication;

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        using EStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus;
        const auto& record = ev->Get()->Record;
        const auto status = static_cast<EStatus>(record.GetStatus());
        if (status == EStatus::ExecComplete) {
            auto result = MakeHolder<TEvSnapshot>();
            result->Status = record.GetStatusCode() == NKikimrIssues::TStatusIds::SUCCESS
                ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::PRECONDITION_FAILED;
            result->Snapshot = TRowVersion(record.GetStep(), record.GetTxId());
            Send(Parent, result.Release());
            PassAway();
        } else if (status != EStatus::ExecInProgress && status != EStatus::ProxyAccepted &&
                   status != EStatus::ProxyResolved && status != EStatus::ProxyPrepared &&
                   status != EStatus::CoordinatorPlanned) {
            auto result = MakeHolder<TEvSnapshot>();
            if (record.GetStatusCode() == NKikimrIssues::TStatusIds::TIMEOUT) {
                result->Status = Ydb::StatusIds::TIMEOUT;
            } else {
                switch (status) {
                    case EStatus::WrongRequest:
                        result->Status = Ydb::StatusIds::PRECONDITION_FAILED;
                        break;
                    case EStatus::AccessDenied:
                        result->Status = Ydb::StatusIds::UNAUTHORIZED;
                        break;
                    case EStatus::ResolveError:
                        result->Status = Ydb::StatusIds::SCHEME_ERROR;
                        break;
                    case EStatus::ProxyShardOverloaded:
                        result->Status = Ydb::StatusIds::OVERLOADED;
                        break;
                    case EStatus::ProxyShardNotAvailable:
                    case EStatus::ProxyShardUnknown:
                    case EStatus::ProxyShardTryLater:
                        result->Status = Ydb::StatusIds::UNAVAILABLE;
                        break;
                    case EStatus::ExecAborted:
                        result->Status = Ydb::StatusIds::ABORTED;
                        break;
                    case EStatus::ExecCancelled:
                        result->Status = Ydb::StatusIds::CANCELLED;
                        break;
                    default:
                        result->Status = Ydb::StatusIds::GENERIC_ERROR;
                        break;
                }
            }
            Send(Parent, result.Release());
            PassAway();
        }
    }

    void Timeout() {
        auto result = MakeHolder<TEvSnapshot>();
        result->Status = Ydb::StatusIds::TIMEOUT;
        Send(Parent, result.Release());
        PassAway();
    }

public:
    TSnapshotActor(const TActorId& parent, TString database, TString path, TInstant deadline, TMaybe<TString> token = {},
        TMaybe<NKikimrChangeExchange::TCdcChecksum> publication = {})
        : Parent(parent)
        , Database(std::move(database))
        , Path(std::move(path))
        , Deadline(deadline)
        , UserToken(std::move(token))
        , Publication(std::move(publication))
    {
    }

    void Bootstrap() {
        const auto now = TActivationContext::Now();
        if (now >= Deadline) {
            return Timeout();
        }
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetDatabaseName(Database);
        request->Record.SetExecTimeoutPeriod((Deadline - now).MilliSeconds());
        auto* snapshot = request->Record.MutableTransaction()->MutableCreateVolatileSnapshot();
        if (Publication) {
            *snapshot->MutablePublishChecksum() = *Publication;
        }
        if (UserToken) {
            request->Record.SetUserToken(*UserToken);
        }
        snapshot->AddTables()->SetTablePath(Path);
        snapshot->SetTimeoutMs((Deadline - now).MilliSeconds());
        Send(MakeTxProxyID(), request.Release());
        Schedule(Deadline - now, new TEvents::TEvWakeup());
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            cFunc(TEvents::TEvWakeup::EventType, Timeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }
};

} // anonymous namespace

TString DescribeSchema(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, TString& error) {
    TMap<TString, TString> columns;
    for (const auto& [_, column] : entry.Columns) {
        if (column.IsBuildInProgress || column.SetNotNullInProgress) {
            error = "Table schema is changing";
            return {};
        }
        columns[column.Name] = TStringBuilder() << NScheme::TypeName(column.PType, column.PTypeMod)
            << ":" << column.IsNotNullColumn;
    }
    TStringBuilder schema;
    for (const auto& [name, type] : columns) {
        schema << name.size() << ":" << name << type.size() << ":" << type;
    }
    return schema;
}

IActor* CreateChecksum(const TActorId& parent, TSettings settings) {
    return new TVerifiedChecksum(parent, std::move(settings));
}

IActor* ProposeChecksum(const TActorId& parent, const TSettings& settings, NKikimrChangeExchange::TCdcChecksum checksum) {
    return new TSnapshotActor(parent, settings.Database, settings.Path, settings.Deadline, settings.UserToken, std::move(checksum));
}

IActor* CreateSnapshot(const TActorId& parent, TString database, TString path, TInstant deadline) {
    return new TSnapshotActor(parent, std::move(database), std::move(path), deadline);
}

void DiscardSnapshot(const TActorContext& ctx, const TString& database, const TString& path, TRowVersion version) {
    auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetDatabaseName(database);
    auto* snapshot = request->Record.MutableTransaction()->MutableDiscardVolatileSnapshot();
    snapshot->AddTables()->SetTablePath(path);
    snapshot->SetSnapshotStep(version.Step);
    snapshot->SetSnapshotTxId(version.TxId);
    // Acknowledgment is unnecessary: the snapshot also has a bounded lifetime.
    ctx.Send(new IEventHandle(MakeTxProxyID(), TActorId(), request.Release()));
}

} // namespace NKikimr::NReplication::NChecksum
