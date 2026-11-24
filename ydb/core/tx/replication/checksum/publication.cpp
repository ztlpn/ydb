#include "checksum_impl.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr::NReplication::NChecksum {
namespace {

class TPublication: public TActorBootstrapped<TPublication> {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    enum class EPhase {
        Table,
        Topics,
        Checkpoint,
        Scan,
        Result
    };

    const TActorId Parent;
    TSettings Settings;
    THashSet<TString> RequestedFeeds;
    NKikimrChangeExchange::TCdcChecksum Control;
    EPhase Phase = EPhase::Table;
    TActorId Child;
    // Column tags identify the physical fields read at S, including a drop and re-add under the same name.
    THashMap<ui32, TString> ColumnNames;
    bool Cancelled = false;
    NMonitoring::TDynamicCounters::TCounterPtr ActivePublications;
    NMonitoring::TDynamicCounters::TCounterPtr Failures;

    void NavigateTable() {
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = Settings.Database;
        if (Settings.UserToken) {
            request->UserToken = MakeIntrusive<NACLib::TUserToken>(*Settings.UserToken);
        }
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(Settings.Path);
        entry.Operation = TNavigate::OpTable;
        entry.Access = NACLib::SelectRow | NACLib::AlterSchema | NACLib::DescribeSchema;
        entry.SyncVersion = true;
        Phase = EPhase::Table;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void NavigateTopics() {
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = Settings.Database;
        for (const auto& stream : Control.GetStreams()) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Path = SplitPath(Settings.Path);
            entry.Path.push_back(stream.GetName());
            entry.Path.emplace_back("streamImpl");
            entry.Operation = TNavigate::OpTopic;
            entry.SyncVersion = entry.ShowPrivatePath = true;
        }
        Phase = EPhase::Topics;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Propose() {
        Phase = Control.GetResult() ? EPhase::Result : EPhase::Checkpoint;
        Child = Register(ProposeChecksum(SelfId(), Settings, Control));
    }

    void Fail(Ydb::StatusIds::StatusCode status, const TString& reason) {
        Control.ClearChecksum();
        Control.SetStatus(status);
        Control.AddIssues()->set_message(reason);
        if (Settings.Snapshot && Phase != EPhase::Result) {
            Control.SetResult(true);
            return Propose();
        }
        Finish();
    }

    void Finish() {
        auto result = MakeHolder<TEvResult>();
        result->Status = Control.GetStatus();
        NYql::IssuesFromMessage(Control.GetIssues(), result->Issues);
        result->Checksum = Control.GetChecksum();
        result->Snapshot = Settings.Snapshot;
        Send(Parent, result.Release());
        PassAway();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& entries = ev->Get()->Request->ResultSet;
        if (Phase == EPhase::Topics) {
            if (entries.size() != static_cast<size_t>(Control.StreamsSize())) {
                return Fail(Ydb::StatusIds::SCHEME_ERROR, "Missing changefeed topic metadata");
            }
            for (size_t i = 0; i < entries.size(); ++i) {
                const auto& entry = entries[i];
                auto& stream = *Control.MutableStreams(i);
                if (entry.Status != TNavigate::EStatus::Ok || !entry.PQGroupInfo) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Selected changefeed topic disappeared");
                }
                const auto& desc = entry.PQGroupInfo->Description;
                TVector<ui32> ids;
                for (const auto& partition : desc.GetPartitions()) {
                    if (partition.GetStatus() == NKikimrPQ::ETopicPartitionStatus::Active) {
                        ids.push_back(partition.GetPartitionId());
                    }
                }
                Sort(ids);
                if (ids.empty() || (Control.GetResult() && TPathId::FromProto(stream.GetTopicId()) != entry.TableId.PathId)) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Selected changefeed topic changed");
                }
                if (!Control.GetResult()) {
                    entry.TableId.PathId.ToProto(stream.MutableTopicId());
                    stream.MutablePartitionIds()->Assign(ids.begin(), ids.end());
                }
            }
            return Propose();
        }
        if (Phase != EPhase::Table) {
            return;
        }
        if (entries.size() != 1 || entries.front().Status != TNavigate::EStatus::Ok) {
            return Fail(entries.size() == 1 && entries.front().Status == TNavigate::EStatus::AccessDenied
                            ? Ydb::StatusIds::UNAUTHORIZED
                            : Ydb::StatusIds::SCHEME_ERROR, "Cannot resolve checksum table");
        }
        const auto& table = entries.front();
        if (table.Kind != TNavigate::KindTable) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Checksum publication requires a user row table");
        }
        TString error;
        auto schema = DescribeSchema(table, error);
        if (error) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, error);
        }
        THashMap<ui32, TString> columnNames;
        for (const auto& [tag, column] : table.Columns) {
            columnNames.emplace(tag, column.Name);
        }
        if (Control.GetResult()) {
            if (!Settings.TableId.HasSamePath(table.TableId) || Settings.Schema != schema || ColumnNames != columnNames) {
                return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Checksum table changed");
            }
            for (const auto& selected : Control.GetStreams()) {
                if (!AnyOf(table.CdcStreams, [&](const auto& stream) {
                        return TPathId::FromProto(stream.GetPathId()) == TPathId::FromProto(selected.GetPathId()) &&
                               stream.GetState() == NKikimrSchemeOp::ECdcStreamStateReady;
                    })) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Selected changefeed changed");
                }
            }
        } else {
            ColumnNames = std::move(columnNames);
            Settings.Schema = schema;
            Settings.TableId = table.TableId;
            table.TableId.PathId.ToProto(Control.MutableTableId());
            for (const auto& stream : table.CdcStreams) {
                if (!RequestedFeeds.erase(stream.GetName())) {
                    continue;
                }
                if (stream.GetState() != NKikimrSchemeOp::ECdcStreamStateReady || stream.GetFormat() != NKikimrSchemeOp::ECdcStreamFormatJson) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED,
                        TStringBuilder() << "Selected changefeed must be ready and use native JSON: " << stream.GetName());
                }
                auto& selected = *Control.AddStreams();
                *selected.MutablePathId() = stream.GetPathId();
                selected.SetName(stream.GetName());
            }
            if (!RequestedFeeds.empty()) {
                return Fail(
                    Ydb::StatusIds::SCHEME_ERROR, TStringBuilder() << "Unknown changefeed on checksum table: " << *RequestedFeeds.begin());
            }
        }
        Control.SetTableSchemaVersion(table.TableId.SchemaVersion);
        NavigateTopics();
    }

    void Handle(TEvSnapshot::TPtr& ev) {
        if (ev->Sender != Child) {
            return;
        }
        Child = {};
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            if (Phase == EPhase::Result && Control.GetStatus() == Ydb::StatusIds::SUCCESS &&
                (ev->Get()->Status == Ydb::StatusIds::PRECONDITION_FAILED || ev->Get()->Status == Ydb::StatusIds::ABORTED)) {
                Control.ClearChecksum();
                Control.SetStatus(ev->Get()->Status);
                Control.AddIssues()->set_message("Checksum target changed before result publication");
                return Propose();
            }
            return Fail(ev->Get()->Status, "Cannot enqueue checksum control record");
        }
        if (Phase == EPhase::Result) {
            return Finish();
        }
        Settings.Snapshot = ev->Get()->Snapshot;
        Control.SetSnapshotStep(Settings.Snapshot.Step);
        Control.SetSnapshotTxId(Settings.Snapshot.TxId);
        if (Cancelled) {
            return Fail(Ydb::StatusIds::CANCELLED, "Checksum publication cancelled");
        }
        Phase = EPhase::Scan;
        Child = Register(CreateChecksum(SelfId(), Settings));
    }

    void Handle(TEvResult::TPtr& ev) {
        if (ev->Sender != Child) {
            return;
        }
        Child = {};
        Control.SetResult(true);
        Control.SetStatus(ev->Get()->Status);
        NYql::IssuesToMessage(ev->Get()->Issues, Control.MutableIssues());
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return Propose();
        }
        *Control.MutableChecksum() = ev->Get()->Checksum;
        NavigateTable();
    }

    void PassAway() override {
        ActivePublications->Dec();
        if (Control.GetStatus() != Ydb::StatusIds::SUCCESS) {
            Failures->Inc();
        }
        if (Child && Phase == EPhase::Scan) {
            Send(Child, new TEvents::TEvPoison());
        }
        if (Settings.Snapshot) {
            DiscardSnapshot(TActivationContext::AsActorContext(), Settings.Database, Settings.Path, Settings.Snapshot);
        }
        TActorBootstrapped::PassAway();
    }

    void Cancel() {
        Cancelled = true;
        if (Phase == EPhase::Result || Phase == EPhase::Checkpoint) {
            return;
        }
        if (Phase == EPhase::Scan && Child) {
            Send(Child, new TEvents::TEvPoison());
        }
        Fail(Ydb::StatusIds::CANCELLED, "Checksum publication cancelled");
    }

public:
    TPublication(TActorId parent, TSettings settings, THashSet<TString> changefeeds)
        : Parent(parent)
        , Settings(std::move(settings))
        , RequestedFeeds(std::move(changefeeds))
    {
    }

    void Bootstrap() {
        const auto counters = GetServiceCounters(AppData()->Counters, "replication")->GetSubgroup("subsystem", "checksum_publication");
        ActivePublications = counters->GetCounter("Active", false);
        Failures = counters->GetCounter("Failures", true);
        ActivePublications->Inc();
        Become(&TThis::StateWork);
        if (RequestedFeeds.empty()) {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Checksum publication requires at least one named changefeed");
        }
        const auto now = TActivationContext::Now();
        if (Settings.Deadline <= now) {
            return Fail(Ydb::StatusIds::TIMEOUT, "Checksum deadline expired");
        }
        Control.SetProtocolVersion(1);
        Control.SetAlgorithm(Settings.Algorithm);
        Control.SetDeadlineUs(Settings.Deadline.MicroSeconds());
        Schedule(Settings.Deadline - now, new TEvents::TEvWakeup());
        NavigateTable();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvSnapshot, Handle);
            hFunc(TEvResult, Handle);
            cFunc(TEvents::TEvPoison::EventType, Cancel);
            case TEvents::TEvWakeup::EventType:
                return Fail(Ydb::StatusIds::TIMEOUT, "Checksum publication deadline expired");
        }
    }
};
}   // namespace

IActor* PublishChecksum(const TActorId& parent, TSettings settings, THashSet<TString> changefeeds) {
    return new TPublication(parent, std::move(settings), std::move(changefeeds));
}
}   // namespace NKikimr::NReplication::NChecksum
