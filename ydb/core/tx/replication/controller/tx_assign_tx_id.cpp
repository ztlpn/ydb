#include "controller_impl.h"

#include <variant>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxAssignTxId: public TTxBase {
    THashMap<ui32, THolder<TEvService::TEvTxIdResult>> Result;
    bool TxIdsExhausted = false;
    bool WaitingForCommit = false;

    struct TTxId {
        ui64 Value;

        TTxId() = default;
        TTxId(ui64 value)
            : Value(value)
        {}
    };

    enum class EError {
        TooManyOpenTxIds,
        TxIdsExhausted,
    };

    using TAssignResult = std::variant<TTxId, EError>;

    template <typename TAssignFunc>
    TAssignResult Process(TAssignFunc assignFunc) {
        TAssignResult result;

        for (auto it = Self->PendingTxId.begin(); it != Self->PendingTxId.end();) {
            result = assignFunc(it->first);
            ui64 txId = 0;

            if (const auto* x = std::get_if<TTxId>(&result)) {
                txId = x->Value;
            } else {
                return result;
            }

            for (const auto nodeId : it->second) {
                auto& ev = Result[nodeId];
                if (!ev) {
                    ev.Reset(new TEvService::TEvTxIdResult(Self->TabletID(), Self->Executor()->Generation()));
                }

                auto& item = *ev->Record.AddVersionTxIds();
                it->first.ToProto(item.MutableVersion());
                item.SetTxId(txId);
            }

            Self->PendingTxId.erase(it++);
        }

        return result;
    }

    void AssignGrid(NIceDb::TNiceDb& db) {
        const auto window = Self->GetWriteWindow();
        const auto begin = TRowVersion::FromProto(window.GetBegin());
        const auto end = TRowVersion::FromProto(window.GetEnd());
        const auto replication = Self->GetSingle();
        const auto interval = replication->GetConfig().GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds();
        for (auto it = Self->PendingTxId.begin(); it != Self->PendingTxId.end();) {
            const auto version = it->first;
            for (const auto nodeId : it->second) {
                auto& reply = Result[nodeId];
                if (!reply) {
                    reply = MakeHolder<TEvService::TEvTxIdResult>(Self->TabletID(), Self->Executor()->Generation());
                }
                *reply->Record.MutableWriteWindow() = window;
            }
            if (version <= begin || version > end) {
                // A bounded reply lets workers scan for resolved progress and
                // replay the deferred records, instead of waiting for an ID.
                it = Self->PendingTxId.erase(it);
                continue;
            }
            auto assigned = window.GetLegacy() ? Self->AssignedTxIds.lower_bound(version) : Self->AssignedTxIds.find(version);
            if (assigned == Self->AssignedTxIds.end()) {
                if (window.GetLegacy() || Self->AssignedTxIds.size() >= MaxOpenTxIds) {
                    WaitingForCommit = true;
                    ++it;
                    continue;
                }
                if (Self->AllocatedTxIds.empty()) {
                    TxIdsExhausted = true;
                    ++it;
                    continue;
                }
                const auto txId = Self->AllocatedTxIds.front();
                Self->AllocatedTxIds.pop_front();
                db.Table<Schema::TxIds>().Key(version.Step, version.TxId).Update(NIceDb::TUpdate<Schema::TxIds::WriteTxId>(txId),
                    NIceDb::TUpdate<Schema::TxIds::GridInterval>(true));
                assigned = Self->AssignedTxIds.emplace(version, txId).first;
            }
            for (const auto nodeId : it->second) {
                auto* item = Result.at(nodeId)->Record.AddVersionTxIds();
                assigned->first.ToProto(item->MutableVersion());
                item->SetTxId(assigned->second);
                if (!window.GetLegacy()) {
                    TRowVersion(assigned->first.Step - interval, 0).ToProto(item->MutableBegin());
                }
            }
            it = Self->PendingTxId.erase(it);
        }
    }

public:
    explicit TTxAssignTxId(TController* self)
        : TTxBase("TxAssignTxId", self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_ASSIGN_TX_ID;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"pending", Self->PendingTxId.size()},
            {"assigned", Self->AssignedTxIds.size()},
            {"allocated", Self->AllocatedTxIds.size()});

        const auto replication = Self->GetSingle();
        if (replication && Self->ReplicationsWithSourceOffsetMismatch.contains(replication->GetId())) {
            Self->PendingTxId.clear();
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->EnableGridAssignments(db);
        if (Self->SysParams.HasGridAssignments()) {
            Self->AdvanceGridFrontier(db);
            AssignGrid(db);
            return true;
        }

        auto result = Process([&](const TRowVersion& version) -> TAssignResult {
            auto it = Self->IndexBuilds.empty()
                ? Self->AssignedTxIds.find(version) : Self->AssignedTxIds.lower_bound(version);
            if (it != Self->AssignedTxIds.end()) {
                return it->second;
            }

            if (Self->AssignedTxIds.size() >= MaxOpenTxIds) {
                Self->TabletCounters->Cumulative()[COUNTER_TOO_MANY_OPEN_TX_IDS] += 1;
                return EError::TooManyOpenTxIds;
            }

            if (Self->AllocatedTxIds.empty()) {
                Self->TabletCounters->Cumulative()[COUNTER_TX_IDS_EXHAUSTED] += 1;
                return EError::TxIdsExhausted;
            }

            const ui64 txId = Self->AllocatedTxIds.front();
            Self->AllocatedTxIds.pop_front();

            db.Table<Schema::TxIds>().Key(version.Step, version.TxId).Update(
                NIceDb::TUpdate<Schema::TxIds::WriteTxId>(txId)
            );
            it = Self->AssignedTxIds.emplace(version, txId).first;

            return txId;
        });

        if (const auto* x = std::get_if<EError>(&result); x && *x == EError::TxIdsExhausted) {
            TxIdsExhausted = true;
            return true;
        }

        if (auto it = Self->PendingTxId.rbegin(); it != Self->PendingTxId.rend()
                && Self->AssignedTxIds.rbegin()->first < it->first) {
            Y_ABORT_UNLESS(std::holds_alternative<EError>(result));
            Y_ABORT_UNLESS(std::get<EError>(result) == EError::TooManyOpenTxIds);

            const auto previousEnd = Self->AssignedTxIds.rbegin()->first;
            bool crossesJoin = false;
            for (const auto& [_, build] : Self->IndexBuilds) {
                if (build.HasJoinVersion()) {
                    const auto join = TRowVersion::FromProto(build.GetJoinVersion());
                    crossesJoin |= previousEnd <= join && join < it->first;
                }
            }
            if (crossesJoin) {
                // C is a fixed boundary. At the open-transaction limit a
                // separate suffix is necessary instead of extending across C.
                if (Self->AllocatedTxIds.empty()) {
                    TxIdsExhausted = true;
                    return true;
                }

                const auto txId = Self->AllocatedTxIds.front();
                Self->AllocatedTxIds.pop_front();
                Self->AssignedTxIds.emplace(it->first, txId);
                db.Table<Schema::TxIds>().Key(it->first.Step, it->first.TxId).Update(
                    NIceDb::TUpdate<Schema::TxIds::WriteTxId>(txId)
                );
            } else {
                auto nh = Self->AssignedTxIds.extract(Self->AssignedTxIds.rbegin()->first);
                db.Table<Schema::TxIds>().Key(nh.key().Step, nh.key().TxId).Delete();

                nh.key() = it->first;
                db.Table<Schema::TxIds>().Key(nh.key().Step, nh.key().TxId).Update(
                    NIceDb::TUpdate<Schema::TxIds::WriteTxId>(nh.mapped())
                );
                Self->AssignedTxIds.insert(std::move(nh));
            }
        }

        result = Process([&](const TRowVersion& version) -> TAssignResult {
            auto it = Self->AssignedTxIds.lower_bound(version);
            Y_ABORT_UNLESS(it != Self->AssignedTxIds.end());
            return it->second;
        });

        Y_ABORT_UNLESS(std::holds_alternative<TTxId>(result));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete",
            {"pending", Self->PendingTxId.size()},
            {"assigned", Self->AssignedTxIds.size()},
            {"allocated", Self->AllocatedTxIds.size()},
            {"exhausted", TxIdsExhausted});

        Self->TabletCounters->Simple()[COUNTER_PENDING_VERSIONS] = Self->PendingTxId.size();
        Self->TabletCounters->Simple()[COUNTER_ALLOCATED_TX_IDS] = Self->AllocatedTxIds.size();
        Self->TabletCounters->Simple()[COUNTER_ASSIGNED_TX_IDS] = Self->AssignedTxIds.size();

        if (Self->ReplicationsWithSourceOffsetMismatch.empty()) {
            for (auto& [nodeId, ev] : Result) {
                ctx.Send(MakeReplicationServiceId(nodeId), std::move(ev));
            }
        }

        Self->BroadcastWriteWindow(ctx);
        Self->AllocateTxIds(ctx);

        if (!TxIdsExhausted && !WaitingForCommit && Self->PendingTxId) {
            Self->Execute(new TTxAssignTxId(Self), ctx);
        } else {
            Self->AssignTxIdInFlight = false;
        }
    }

}; // TTxAssignTxId

void TController::EnableGridAssignments(NIceDb::TNiceDb& db) {
    if (SysParams.HasGridAssignments() || !AppData()->FeatureFlags.GetEnableAsyncReplicationChecksumCheck()) {
        return;
    }
    const auto replication = GetSingle();
    if (!replication || !replication->GetConfig().GetConsistencySettings().HasGlobal()) {
        return;
    }
    LegacyTxIds.clear();
    for (const auto& [end, _] : AssignedTxIds) {
        LegacyTxIds.insert(end);
    }
    SysParams.EnableGridAssignments(db);
}

NKikimrReplication::TGlobalWriteWindow TController::GetWriteWindow() const {
    NKikimrReplication::TGlobalWriteWindow result;
    const auto replication = GetSingle();
    if (!replication) {
        return result;
    }
    const auto interval = replication->GetConfig().GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds();
    const auto it = CommittedVersions.find(replication->GetId());
    const auto begin = it == CommittedVersions.end() ? TRowVersion::Min() : it->second;
    begin.ToProto(result.MutableBegin());
    if (!LegacyTxIds.empty()) {
        LegacyTxIds.rbegin()->ToProto(result.MutableEnd());
        result.SetLegacy(true);
    } else {
        const auto available = (Max<ui64>() - begin.Step) / interval;
        TRowVersion(begin.Step + Min<ui64>(MaxOpenTxIds, available) * interval, 0).ToProto(result.MutableEnd());
    }
    return result;
}

void TController::AdvanceGridFrontier(NIceDb::TNiceDb& db) {
    if (!SysParams.HasGridAssignments() || !LegacyTxIds.empty() || CommittingTxId || WorkersByHeartbeat.empty() ||
        !CountHeartbeatParticipants() || CountHeartbeatParticipants() != WorkersWithHeartbeat.size()) {
        return;
    }
    const auto replication = GetSingle();
    if (!replication) {
        return;
    }
    const auto interval = replication->GetConfig().GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds();
    auto next = TRowVersion(WorkersByHeartbeat.begin()->first.Step / interval * interval, 0);
    if (!AssignedTxIds.empty()) {
        next = Min(next, TRowVersion(AssignedTxIds.begin()->first.Step - interval, 0));
    }
    if (!CanCommitIndexBuilds(next)) {
        return;
    }
    auto& frontier = CommittedVersions[replication->GetId()];
    if (next > frontier) {
        frontier = next;
        db.Table<Schema::Replications>().Key(replication->GetId()).Update(NIceDb::TUpdate<Schema::Replications::CommittedStep>(frontier.Step),
            NIceDb::TUpdate<Schema::Replications::CommittedTxId>(frontier.TxId));
    }
}

void TController::BroadcastWriteWindow(const TActorContext& ctx) {
    if (!SysParams.HasGridAssignments()) {
        return;
    }
    const auto window = GetWriteWindow();
    const auto encoded = window.SerializeAsString();
    if (encoded == LastBroadcastWriteWindow) {
        return;
    }
    LastBroadcastWriteWindow = encoded;
    for (const auto& [nodeId, _] : Sessions) {
        auto reply = MakeHolder<TEvService::TEvTxIdResult>(TabletID(), Executor()->Generation());
        *reply->Record.MutableWriteWindow() = window;
        ctx.Send(MakeReplicationServiceId(nodeId), std::move(reply));
    }
}

void TController::AllocateTxIds(const TActorContext& ctx) {
    const auto replication = GetSingle();
    if (replication && ReplicationsWithSourceOffsetMismatch.contains(replication->GetId())) {
        return;
    }
    if (AllocateTxIdInFlight || AllocatedTxIds.size() >= MinAllocatedTxIds) {
        return;
    }

    AllocateTxIdInFlight = true;
    ctx.Send(TxAllocatorClient, new TEvTxAllocatorClient::TEvAllocate(MaxOpenTxIds));
}

void TController::RunTxAssignTxId(const TActorContext& ctx) {
    if (!AssignTxIdInFlight) {
        AssignTxIdInFlight = true;
        Execute(new TTxAssignTxId(this), ctx);
    }
}

void TController::Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE_CTX(ctx, "Handle",
        {"ev", ev->Get()->ToString()});

    std::copy(ev->Get()->TxIds.begin(), ev->Get()->TxIds.end(), std::back_inserter(AllocatedTxIds));
    AllocateTxIdInFlight = false;

    TabletCounters->Simple()[COUNTER_ALLOCATED_TX_IDS] = AllocatedTxIds.size();
    RunTxAssignTxId(ctx);
    RunTxIndexBuild(ctx);
}

}
