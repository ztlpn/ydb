#include <ydb/core/protos/change_exchange.pb.h>
#include "controller_impl.h"

namespace NKikimr::NReplication::NController {
namespace {

using TAttempt = NKikimrReplication::TCdcChecksumAttempt;
using TKey = std::pair<ui64, ui64>;

TRowVersion Version(const NKikimrChangeExchange::TCdcChecksum& record) {
    return {record.GetSnapshotStep(), record.GetSnapshotTxId()};
}

TAttempt* FindAttempt(NKikimrReplication::TCdcChecksumState& state, const TString& id) {
    for (auto& attempt : *state.MutableAttempts()) {
        if (attempt.GetCheckpoint().GetCheckpointId() == id) {
            return &attempt;
        }
    }
    return nullptr;
}

bool SameCheckpoint(const NKikimrChangeExchange::TCdcChecksum& lhs,
        const NKikimrChangeExchange::TCdcChecksum& rhs) {
    return lhs.GetCheckpointId() == rhs.GetCheckpointId() && Version(lhs) == Version(rhs) &&
           TPathId::FromProto(lhs.GetTableId()) == TPathId::FromProto(rhs.GetTableId()) && lhs.GetSchema() == rhs.GetSchema() &&
           lhs.GetAlgorithm() == rhs.GetAlgorithm() && lhs.GetAlgorithmVersion() == rhs.GetAlgorithmVersion() &&
           lhs.GetProtocolVersion() == rhs.GetProtocolVersion() && lhs.GetCommitBoundaryStep() == rhs.GetCommitBoundaryStep();
}

void Fail(TAttempt& attempt, Ydb::StatusIds::StatusCode status, const TString& error) {
    attempt.SetLocalStatus(status);
    attempt.AddIssues()->set_message(error);
}

void SetTime(google::protobuf::Timestamp* timestamp, ui64 micros) {
    timestamp->set_seconds(micros / 1000000);
    timestamp->set_nanos((micros % 1000000) * 1000);
}

}

void TController::PersistChecksum(const TKey& key, NIceDb::TNiceDb& db, TInstant now) {
    auto& state = ChecksumStates.at(key);
    for (auto& attempt : *state.MutableAttempts()) {
        if (attempt.GetFinished()) {
            continue;
        }
        const auto& source = attempt.GetSourceResult();
        if (attempt.HasSourceResult() && source.GetStatus() != Ydb::StatusIds::SUCCESS) {
            attempt.SetLocalStatus(source.GetStatus());
            *attempt.MutableIssues() = source.GetIssues();
        } else if (now.MicroSeconds() >= attempt.GetDeadlineUs()) {
            Fail(attempt, Ydb::StatusIds::TIMEOUT, "CDC checksum attempt expired");
        }
        const auto localStatus = attempt.GetLocalStatus();
        const bool failed = localStatus != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED && localStatus != Ydb::StatusIds::SUCCESS;
        const bool completed = localStatus == Ydb::StatusIds::SUCCESS && attempt.HasSourceResult();
        if (failed || completed) {
            attempt.SetFinished(true);
            attempt.SetFinishedAtUs(now.MicroSeconds());
            if (failed) {
                TabletCounters->Cumulative()[COUNTER_CHECKSUM_FAILURES] += 1;
                if (localStatus == Ydb::StatusIds::TIMEOUT) {
                    TabletCounters->Cumulative()[COUNTER_CHECKSUM_TIMEOUTS] += 1;
                }
            }
        }
        const auto version = Version(attempt.GetCheckpoint());
        if (completed && (!state.HasLastCompared() || version > TRowVersion::FromProto(state.GetLastCompared()))) {
            version.ToProto(state.MutableLastCompared());
            auto* result = state.MutableStatus()->mutable_last_result();
            result->set_checkpoint_id(attempt.GetCheckpoint().GetCheckpointId());
            *result->mutable_source() = source.GetChecksum();
            *result->mutable_destination() = attempt.GetDestinationChecksum();
            result->set_matches(source.GetChecksum().row_count() == attempt.GetDestinationChecksum().row_count() &&
                source.GetChecksum().hash() == attempt.GetDestinationChecksum().hash());
            SetTime(result->mutable_finished_at(), attempt.GetFinishedAtUs());
        }
        if (state.HasLatest() && version < TRowVersion::FromProto(state.GetLatest())) {
            continue;
        }
        version.ToProto(state.MutableLatest());
        auto* status = state.MutableStatus();
        status->set_checkpoint_id(attempt.GetCheckpoint().GetCheckpointId());
        status->set_status(attempt.GetFinished() ? localStatus : Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);
        *status->mutable_issues() = attempt.GetIssues();
        if (attempt.GetFinished()) {
            SetTime(status->mutable_finished_at(), attempt.GetFinishedAtUs());
        } else {
            status->clear_finished_at();
        }
    }
    db.Table<Schema::Targets>().Key(key.first, key.second).Update(
        NIceDb::TUpdate<Schema::Targets::CdcChecksum>(state.SerializeAsString()));
}

bool TController::ObserveChecksum(const TKey& key, const NKikimrChangeExchange::TCdcChecksum& control,
        NIceDb::TNiceDb& db, TInstant now) {
    auto& state = ChecksumStates[key];
    auto* attempt = FindAttempt(state, control.GetCheckpointId());
    if (control.GetResult()) {
        // A result can be replayed after its completed join state was collected.
        if (attempt && !attempt->GetFinished()) {
            if (!SameCheckpoint(attempt->GetCheckpoint(), control)) {
                Fail(*attempt, Ydb::StatusIds::PRECONDITION_FAILED, "Conflicting checksum result metadata");
            } else if (attempt->HasSourceResult() && attempt->GetSourceResult().SerializeAsString() != control.SerializeAsString()) {
                Fail(*attempt, Ydb::StatusIds::PRECONDITION_FAILED, "Conflicting duplicate checksum result");
            } else {
                *attempt->MutableSourceResult() = control;
            }
            PersistChecksum(key, db, now);
        }
        return true;
    }
    if (!attempt) {
        if (state.HasLatest() && Version(control) <= TRowVersion::FromProto(state.GetLatest())) {
            return true;
        }
        attempt = state.AddAttempts();
        *attempt->MutableCheckpoint() = control;
        attempt->SetDeadlineUs((now + TDuration::Hours(1)).MicroSeconds());
        auto* target = FindTarget(TWorkerId(key.first, key.second, 0));
        if (!AppData()->FeatureFlags.GetEnableAsyncReplicationChecksumCheck() || !target ||
            target->GetKind() != TReplication::ETargetKind::Table || target->IsIndexBuild())
        {
            Fail(*attempt, Ydb::StatusIds::UNSUPPORTED, "Checksum comparisons are disabled or unsupported for this target");
        } else if (const auto replication = Find(key.first); replication && replication->GetConfig().GetConsistencySettings().HasGlobal()) {
            const auto interval = replication->GetConfig().GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds();
            if (control.GetProtocolVersion() != 2 || !control.GetCommitBoundaryStep() || control.GetCommitBoundaryStep() % interval ||
                !LegacyTxIds.empty()) {
                Fail(*attempt, Ydb::StatusIds::PRECONDITION_FAILED,
                    "GLOBAL checksum requires an aligned checkpoint and drained legacy transaction assignments");
            }
        }
    } else if (!SameCheckpoint(attempt->GetCheckpoint(), control)) {
        Fail(*attempt, Ydb::StatusIds::PRECONDITION_FAILED, "Conflicting checksum checkpoint metadata");
    }
    PersistChecksum(key, db, now);
    const auto barrier = SchemaBarriers.find(key);
    return attempt->GetFinished() && (barrier == SchemaBarriers.end() ||
        barrier->second.Schema.GetChecksum().GetCheckpointId() != control.GetCheckpointId());
}

void TController::SaveChecksumSnapshot(const TKey& key, TRowVersion snapshot,
        Ydb::StatusIds::StatusCode status, NIceDb::TNiceDb& db, TInstant now) {
    auto& state = ChecksumStates.at(key);
    auto* attempt = FindAttempt(state, SchemaBarriers.at(key).Schema.GetChecksum().GetCheckpointId());
    Y_ENSURE(attempt);
    if (!attempt->HasDestinationSnapshot() && !attempt->GetFinished()) {
        const auto started = TInstant::MicroSeconds(attempt->GetDeadlineUs()) - TDuration::Hours(1);
        TabletCounters->Percentile()[COUNTER_CHECKSUM_BARRIER_DURATION_MS].IncrementFor((now - started).MilliSeconds());
        if (status == Ydb::StatusIds::SUCCESS) {
            snapshot.ToProto(attempt->MutableDestinationSnapshot());
        } else {
            Fail(*attempt, status, "Cannot retain destination checksum snapshot");
        }
    }
    PersistChecksum(key, db, now);
}

bool TController::HasChecksumReads(const TKey& key) const {
    const auto it = ChecksumStates.find(key);
    if (it == ChecksumStates.end()) {
        return false;
    }
    for (const auto& attempt : it->second.GetAttempts()) {
        if (!attempt.GetFinished() && attempt.HasDestinationSnapshot() &&
            attempt.GetLocalStatus() == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED)
        {
            return true;
        }
    }
    return false;
}

void TController::AdvanceChecksumBarriers(NIceDb::TNiceDb& db, TInstant now) {
    for (auto& [key, barrier] : SchemaBarriers) {
        if (!barrier.Schema.HasChecksum() || barrier.Phase != ESchemaBarrierPhase::Collecting ||
            barrier.ReportedWorkers.size() != barrier.ExpectedWorkers.size()) {
            continue;
        }
        const auto replication = Find(key.first);
        if (!replication || !replication->GetConfig().GetConsistencySettings().HasGlobal()) {
            continue;
        }
        auto* attempt = FindAttempt(ChecksumStates.at(key), barrier.Schema.GetChecksum().GetCheckpointId());
        if (!attempt || attempt->GetFinished()) {
            continue;
        }
        const auto boundary = TRowVersion(barrier.Schema.GetChecksum().GetCommitBoundaryStep(), 0);
        bool cycle = false;
        for (const auto& [otherKey, other] : SchemaBarriers) {
            if (otherKey != key && !other.Schema.HasChecksum() && other.Phase == ESchemaBarrierPhase::Verifying &&
                TRowVersion::FromProto(other.Schema.GetVersion()) >= boundary) {
                cycle = true;
                break;
            }
        }
        if (cycle) {
            Fail(*attempt, Ydb::StatusIds::PRECONDITION_FAILED, "Checksum checkpoint conflicts with schema-change verification");
            PersistChecksum(key, db, now);
            continue;
        }
        // A drained checkpoint proves the same progress as its heartbeat.
        // Reestablish it after recovery or an unrelated DDL heartbeat reset.
        for (const auto& id : barrier.ReportedWorkers) {
            auto it = Workers.find(id);
            if (it == Workers.end() || (it->second.HasHeartbeat() && it->second.GetHeartbeat() >= boundary)) {
                continue;
            }
            auto& worker = it->second;
            if (worker.HasHeartbeat()) {
                auto old = WorkersByHeartbeat.find(worker.GetHeartbeat());
                if (old != WorkersByHeartbeat.end()) {
                    old->second.erase(id);
                    if (old->second.empty()) {
                        WorkersByHeartbeat.erase(old);
                    }
                }
            }
            worker.SetHeartbeat(boundary);
            WorkersWithHeartbeat.insert(id);
            WorkersByHeartbeat[boundary].insert(id);
            db.Table<Schema::Workers>()
                .Key(id.ReplicationId(), id.TargetId(), id.WorkerId())
                .Update(NIceDb::TUpdate<Schema::Workers::HeartbeatVersionStep>(boundary.Step),
                    NIceDb::TUpdate<Schema::Workers::HeartbeatVersionTxId>(boundary.TxId));
        }
        if (CommittingTxId || !LegacyTxIds.empty() || !CanCommitIndexBuilds(boundary) || !CountHeartbeatParticipants() ||
            CountHeartbeatParticipants() != WorkersWithHeartbeat.size() || WorkersByHeartbeat.empty() ||
            WorkersByHeartbeat.begin()->first < boundary || (!AssignedTxIds.empty() && AssignedTxIds.begin()->first <= boundary)) {
            continue;
        }
        barrier.Phase = ESchemaBarrierPhase::Altering;
        db.Table<Schema::Targets>()
            .Key(key.first, key.second)
            .Update(NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(static_cast<ui8>(barrier.Phase)));
    }
}

void TController::ProgressChecksums(const TActorContext& ctx) {
    ui64 active = 0;
    ui64 mismatches = 0;
    bool needsTimer = false;
    for (auto it = ChecksumQueries.begin(); it != ChecksumQueries.end();) {
        auto state = ChecksumStates.find(it->second.Target);
        auto* attempt = state == ChecksumStates.end() ? nullptr : FindAttempt(state->second, it->second.CheckpointId);
        const auto replication = Find(it->second.Target.first);
        if (!attempt || attempt->GetFinished() || !replication || replication->GetState() == TReplication::EState::Removing) {
            ctx.Send(it->first, new TEvents::TEvPoison());
            ChecksumQueries.erase(it++);
        } else {
            ++it;
        }
    }
    for (const auto& [key, state] : ChecksumStates) {
        auto replication = Find(key.first);
        auto* target = replication ? replication->FindTarget(key.second) : nullptr;
        if (!target || replication->GetState() == TReplication::EState::Removing) {
            continue;
        }
        mismatches += state.HasStatus() && state.GetStatus().has_last_result() &&
            !state.GetStatus().last_result().matches();
        needsTimer |= state.AttemptsSize() != 0;
        for (const auto& attempt : state.GetAttempts()) {
            active += !attempt.GetFinished();
            if (!attempt.HasDestinationSnapshot()) {
                continue;
            }
            const auto snapshot = TRowVersion::FromProto(attempt.GetDestinationSnapshot());
            bool running = false;
            for (const auto& [actor, query] : ChecksumQueries) {
                if (query.Target == key && query.CheckpointId == attempt.GetCheckpoint().GetCheckpointId()) {
                    running = true;
                    if (attempt.GetFinished()) {
                        ctx.Send(actor, new TEvents::TEvPoison());
                    }
                }
            }
            if (attempt.GetFinished() || attempt.GetLocalStatus() != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
                continue;
            }
            if (!running) {
                NChecksum::TSettings settings;
                settings.Database = replication->GetDatabase();
                settings.Path = target->GetDstPath();
                settings.TableId = TTableId(target->GetDstPathId());
                settings.Schema = attempt.GetCheckpoint().GetSchema();
                settings.Snapshot = snapshot;
                settings.Deadline = TInstant::MicroSeconds(attempt.GetDeadlineUs());
                const auto actor = ctx.Register(NChecksum::CreateChecksum(ctx.SelfID, std::move(settings)));
                ChecksumQueries.emplace(actor, TChecksumQuery{key, attempt.GetCheckpoint().GetCheckpointId()});
            }
        }
    }
    TabletCounters->Simple()[COUNTER_CHECKSUM_MISMATCH] = mismatches;
    TabletCounters->Simple()[COUNTER_CHECKSUM_ACTIVE] = active;
    if (needsTimer && !ChecksumTimerScheduled) {
        ChecksumTimerScheduled = true;
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }
    for (const auto& [key, barrier] : SchemaBarriers) {
        if (barrier.Phase == ESchemaBarrierPhase::Altering) {
            StartSchemaChangeDstAlter(key, ctx);
        }
    }
    for (const auto replicationId : DeferredAlters) {
        if (!HasPendingAlter(replicationId)) {
            ctx.Send(SelfId(), new TEvPrivate::TEvResumeDeferredAlter(replicationId));
        }
    }
}

void TController::Handle(NChecksum::TEvSnapshot::TPtr& ev, const TActorContext& ctx) {
    for (const auto& [key, actor] : SchemaChangeDstAlterers) {
        if (actor != ev->Sender) {
            continue;
        }
        auto event = MakeHolder<TEvPrivate::TEvSchemaChangeDstAlterResult>(key.first, key.second, 0);
        event->ChecksumSnapshot = ev->Get()->Snapshot;
        event->ChecksumSnapshotStatus = ev->Get()->Status;
        ctx.Send(new IEventHandle(SelfId(), actor, event.Release()));
        return;
    }
}

class TController::TTxChecksum: public TTxBase {
    NChecksum::TEvResult::TPtr Result;
    TVector<std::tuple<TWorkerId, NKikimrReplication::TSchemaChange, ui64>> Release;
    TVector<std::tuple<TString, TString, TRowVersion>> Discard;
public:
    TTxChecksum(TController* self, NChecksum::TEvResult::TPtr result = {})
        : TTxBase("TxChecksum", self), Result(std::move(result)) {}

    TTxType GetTxType() const override { return TXTYPE_CHECKSUM; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        NIceDb::TNiceDb db(txc.DB);
        if (Result) {
            const auto query = Self->ChecksumQueries.find(Result->Sender);
            if (query == Self->ChecksumQueries.end()) {
                return true;
            }
            auto state = Self->ChecksumStates.find(query->second.Target);
            if (state != Self->ChecksumStates.end()) {
                auto* attempt = FindAttempt(state->second, query->second.CheckpointId);
                if (attempt && !attempt->GetFinished()) {
                    attempt->SetLocalStatus(Result->Get()->Status);
                    *attempt->MutableDestinationChecksum() = Result->Get()->Checksum;
                    NYql::IssuesToMessage(Result->Get()->Issues, attempt->MutableIssues());
                }
            }
        }
        for (auto& [key, state] : Self->ChecksumStates) {
            if (!Self->FindTarget(TWorkerId(key.first, key.second, 0))) {
                continue;
            }
            Self->PersistChecksum(key, db, ctx.Now());
            Self->AdvanceChecksumBarriers(db, ctx.Now());
            auto barrier = Self->SchemaBarriers.find(key);
            if (barrier != Self->SchemaBarriers.end() && barrier->second.Schema.HasChecksum()) {
                auto* attempt = FindAttempt(state, barrier->second.Schema.GetChecksum().GetCheckpointId());
                if (attempt && attempt->GetFinished() && barrier->second.Phase == ESchemaBarrierPhase::Collecting) {
                    const auto started = TInstant::MicroSeconds(attempt->GetDeadlineUs()) - TDuration::Hours(1);
                    Self->TabletCounters->Percentile()[COUNTER_CHECKSUM_BARRIER_DURATION_MS]
                        .IncrementFor((ctx.Now() - started).MilliSeconds());
                    // No flush/snapshot has started. Release arrivals and skip
                    // later arrivals using the durable terminal attempt.
                    for (const auto& [id, offset] : barrier->second.WorkerOffsets) {
                        Release.emplace_back(id, barrier->second.Schema, offset);
                    }
                    for (const auto& id : barrier->second.ExpectedWorkers) {
                        db.Table<Schema::SchemaBarrierWorkers>().Key(id.ReplicationId(), id.TargetId(), id.WorkerId()).Delete();
                    }
                    db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                        NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(0),
                        NIceDb::TUpdate<Schema::Targets::SchemaBarrierChange>(TString()));
                    Self->SchemaBarriers.erase(barrier);
                }
            }
            for (auto& attempt : *state.MutableAttempts()) {
                if (attempt.HasDestinationSnapshot() && (attempt.GetFinished() ||
                    attempt.GetLocalStatus() != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED))
                {
                    const auto replication = Self->Find(key.first);
                    auto* target = replication->FindTarget(key.second);
                    Discard.emplace_back(replication->GetDatabase(), target->GetDstPath(),
                        TRowVersion::FromProto(attempt.GetDestinationSnapshot()));
                    // Local completion is already durable. If the discard is
                    // lost on restart, bounded snapshot retention cleans it up.
                    attempt.ClearDestinationSnapshot();
                }
            }
            for (int i = state.AttemptsSize() - 1; i >= 0; --i) {
                const auto& attempt = state.GetAttempts(i);
                const auto activeBarrier = Self->SchemaBarriers.find(key);
                if (attempt.GetFinished() &&
                    ctx.Now() > TInstant::MicroSeconds(attempt.GetDeadlineUs()) + TDuration::Hours(1) &&
                    (activeBarrier == Self->SchemaBarriers.end() ||
                     activeBarrier->second.Schema.GetChecksum().GetCheckpointId() != attempt.GetCheckpoint().GetCheckpointId()))
                {
                    state.MutableAttempts()->DeleteSubrange(i, 1);
                }
            }
            db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::Targets::CdcChecksum>(state.SerializeAsString()));
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Result) {
            Self->ChecksumQueries.erase(Result->Sender);
        }
        for (const auto& [id, schema, offset] : Release) {
            Self->SendSchemaChangeResult(id, schema, offset, false, false, ctx);
        }
        for (const auto& [database, path, snapshot] : Discard) {
            NChecksum::DiscardSnapshot(ctx, database, path, snapshot);
        }
        Self->ProgressChecksums(ctx);
    }
};

void TController::Handle(NChecksum::TEvResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxChecksum(this, ev), ctx);
}

void TController::Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    ChecksumTimerScheduled = false;
    Execute(new TTxChecksum(this), ctx);
}

}
