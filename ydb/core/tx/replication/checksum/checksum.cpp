#include "checksum.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/request_types.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <util/generic/map.h>
#include <util/string/escape.h>
#include <util/string/hex.h>
#include <util/string/subst.h>

namespace NKikimr::NReplication::NChecksum {
namespace {

class TChecksumQueryActor: public TQueryBase {
public:
    TChecksumQueryActor(const TActorId& parent, const TSettings& settings, TString path, TRowVersion snapshot)
        : TQueryBase(NKikimrServices::REPLICATION_CONTROLLER, {}, settings.Database, !settings.UserToken, false, settings.UserToken)
        , Parent(parent)
        , Path(std::move(path))
        , Deadline(settings.Deadline)
        , MaxShardsInFlight(settings.MaxShardsInFlight)
    {
        RequestType = TString(NRequestTypes::ChecksumTable);
        MvccVersion = snapshot;
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
        auto path = EscapeC(Path);
        SubstGlobal(path, "`", "\\`");
        TStringBuilder query;
        if (MaxShardsInFlight) {
            query << "PRAGMA ydb.MaxSequentialReadsInFlight = \"" << MaxShardsInFlight << "\";\n";
        }
        RunDataQuery(query << "CHECKSUM TABLE `" << path << "` WITH (ALGORITHM = \"CityHash64\");",
            nullptr, TTxControl::BeginAndCommitTx(true), Deadline - now);
    }

    void OnQueryResult() override {
        if (Finished) {
            return;
        }
        if (ResultSets.size() != 1 || ResultSets[0].RowsCount() != 1 || ResultSets[0].ColumnsCount() != 4) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected checksum result shape");
        }
        NYdb::TResultSetParser parser(ResultSets[0]);
        parser.TryNextRow();
        if (parser.ColumnParser("version").GetUint32() != 1
            || parser.ColumnParser("algorithm").GetUtf8() != "CityHash64")
        {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unsupported checksum result format");
        }
        const auto count = parser.ColumnParser("count").GetUint64();
        const auto hash = parser.ColumnParser("hash").GetString();
        if (hash.size() != sizeof(ui64)) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected checksum hash width");
        }
        Checksum.set_row_count(count);
        Checksum.set_hash(HexEncode(hash.data(), hash.size()));
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
    const TInstant Deadline;
    const ui32 MaxShardsInFlight;
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
        if (!error.empty() || schema != Settings.Schema) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Checksum table schema changed");
        }
        if (Result) {
            Send(Parent, Result.Release());
            return PassAway();
        }
        Query = Register(new TChecksumQueryActor(SelfId(), Settings, Settings.Path, Settings.Snapshot));
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
                case EStatus::AccessDenied: result->Status = Ydb::StatusIds::UNAUTHORIZED; break;
                case EStatus::ResolveError: result->Status = Ydb::StatusIds::SCHEME_ERROR; break;
                case EStatus::ProxyShardOverloaded: result->Status = Ydb::StatusIds::OVERLOADED; break;
                case EStatus::ProxyShardNotAvailable:
                case EStatus::ProxyShardUnknown:
                case EStatus::ProxyShardTryLater: result->Status = Ydb::StatusIds::UNAVAILABLE; break;
                case EStatus::ExecAborted: result->Status = Ydb::StatusIds::ABORTED; break;
                case EStatus::ExecCancelled: result->Status = Ydb::StatusIds::CANCELLED; break;
                default: result->Status = Ydb::StatusIds::GENERIC_ERROR; break;
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
    TSnapshotActor(const TActorId& parent, TString database, TString path, TInstant deadline)
        : Parent(parent), Database(std::move(database)), Path(std::move(path)), Deadline(deadline) {}

    void Bootstrap() {
        const auto now = TActivationContext::Now();
        if (now >= Deadline) {
            return Timeout();
        }
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetDatabaseName(Database);
        request->Record.SetExecTimeoutPeriod((Deadline - now).MilliSeconds());
        auto* snapshot = request->Record.MutableTransaction()->MutableCreateVolatileSnapshot();
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
