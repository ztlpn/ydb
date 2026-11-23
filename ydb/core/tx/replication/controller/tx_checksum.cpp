#include <ydb/core/protos/change_exchange.pb.h>
#include "controller_impl.h"

namespace NKikimr::NReplication::NController {
namespace {

using TAttempt = NKikimrReplication::TCdcChecksumAttempt;
using TKey = std::pair<ui64, ui64>;

TRowVersion Version(const NKikimrChangeExchange::TCdcChecksum& record) {
    return {record.GetSnapshotStep(), record.GetSnapshotTxId()};
}

TAttempt* FindAttempt(NKikimrReplication::TCdcChecksumState& state, TRowVersion version) {
    for (auto& attempt : *state.MutableAttempts()) {
        if (Version(attempt.GetCheckpoint()) == version) {
            return &attempt;
        }
    }
    return nullptr;
}

bool SameCheckpoint(const NKikimrChangeExchange::TCdcChecksum& lhs,
        const NKikimrChangeExchange::TCdcChecksum& rhs) {
    const auto& left = lhs.GetExpectedPartitions();
    const auto& right = rhs.GetExpectedPartitions();
    return std::equal(left.begin(), left.end(), right.begin(), right.end()) && Version(lhs) == Version(rhs) &&
           lhs.GetAlgorithm() == rhs.GetAlgorithm() && lhs.GetAlgorithmVersion() == rhs.GetAlgorithmVersion() &&
           lhs.GetProtocolVersion() == rhs.GetProtocolVersion();
}

void Fail(TAttempt& attempt, Ydb::StatusIds::StatusCode status, const TString& error) {
    attempt.SetLocalStatus(status);
    attempt.AddIssues()->set_message(error);
}

void SetTime(google::protobuf::Timestamp* timestamp, ui64 micros) {
    timestamp->set_seconds(micros / 1000000);
    timestamp->set_nanos((micros % 1000000) * 1000);
}

void WriteChecksumState(NIceDb::TNiceDb& db, const TKey& key, const NKikimrReplication::TCdcChecksumState& state) {
    using TTargets = TControllerSchema::Targets;
    db.Table<TTargets>().Key(key.first, key.second).Update(NIceDb::TUpdate<TTargets::CdcChecksum>(state.SerializeAsString()));
}

}

void TController::UpdateChecksumStatus(const TKey& key, TInstant now) {
    auto& state = ChecksumStates.at(key);
    const bool global = Find(key.first)->GetConfig().GetConsistencySettings().HasGlobal();
    for (auto& attempt : *state.MutableAttempts()) {
        if (attempt.HasFinishedAtUs()) {
            continue;
        }
        const auto& partitions = attempt.GetCheckpoint().GetExpectedPartitions();
        if (!global && CompleteWorkerSets.contains(key)) {
            if (auto* target = FindTarget(TWorkerId(key.first, key.second, 0))) {
                auto workers = target->GetWorkers();
                Sort(workers);
                if (!std::equal(partitions.begin(), partitions.end(), workers.begin(), workers.end())) {
                    Fail(attempt, Ydb::StatusIds::PRECONDITION_FAILED, "Changefeed partition membership changed");
                }
            }
        }
        const auto& source = attempt.GetSourceResult();
        if (global) {
            Fail(attempt, Ydb::StatusIds::UNSUPPORTED, "Destination checksum comparisons are unsupported for GLOBAL replication");
        } else if (attempt.HasSourceResult() && source.GetStatus() != Ydb::StatusIds::SUCCESS) {
            attempt.SetLocalStatus(source.GetStatus());
            *attempt.MutableIssues() = source.GetIssues();
        } else if (now.MicroSeconds() >= attempt.GetDeadlineUs()) {
            Fail(attempt, Ydb::StatusIds::TIMEOUT, "CDC checksum attempt expired");
        }
        const auto localStatus = attempt.GetLocalStatus();
        const bool failed = localStatus != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED && localStatus != Ydb::StatusIds::SUCCESS;
        const bool completed = localStatus == Ydb::StatusIds::SUCCESS && attempt.HasSourceResult() && CompleteWorkerSets.contains(key);
        if (failed || completed) {
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
        status->set_status(attempt.HasFinishedAtUs() ? localStatus : Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);
        *status->mutable_issues() = attempt.GetIssues();
        if (attempt.HasFinishedAtUs()) {
            SetTime(status->mutable_finished_at(), attempt.GetFinishedAtUs());
        } else {
            status->clear_finished_at();
        }
    }
}

bool TController::ObserveChecksum(const TKey& key, const NKikimrChangeExchange::TCdcChecksum& control,
        NIceDb::TNiceDb& db, TInstant now) {
    auto& state = ChecksumStates[key];
    auto* attempt = FindAttempt(state, Version(control));
    if (control.GetResult()) {
        if (!attempt && control.GetStatus() != Ydb::StatusIds::SUCCESS &&
            (!state.HasLatest() || Version(control) > TRowVersion::FromProto(state.GetLatest()))) {
            // A new partition can receive the invalidation before an original worker reports its checkpoint.
            attempt = state.AddAttempts();
            *attempt->MutableCheckpoint() = control;
            attempt->SetDeadlineUs((now + TDuration::Hours(1)).MicroSeconds());
        }
        // A result can be replayed after its completed join state was collected.
        if (attempt && !attempt->HasFinishedAtUs()) {
            if (!SameCheckpoint(attempt->GetCheckpoint(), control)) {
                Fail(*attempt, Ydb::StatusIds::PRECONDITION_FAILED, "Conflicting checksum result metadata");
            } else if (attempt->HasSourceResult() && attempt->GetSourceResult().SerializeAsString() != control.SerializeAsString()) {
                Fail(*attempt, Ydb::StatusIds::PRECONDITION_FAILED, "Conflicting duplicate checksum result");
            } else {
                *attempt->MutableSourceResult() = control;
            }
            UpdateChecksumStatus(key, now);
            WriteChecksumState(db, key, state);
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
        if (!AppData()->FeatureFlags.GetEnableChecksumTable() || !AppData()->FeatureFlags.GetEnableAsyncReplicationChecksumCheck() || !target ||
            target->GetKind() != TReplication::ETargetKind::Table || target->IsIndexBuild())
        {
            Fail(*attempt, Ydb::StatusIds::UNSUPPORTED, "Checksum comparisons are disabled or unsupported for this target");
        }
    } else if (!SameCheckpoint(attempt->GetCheckpoint(), control)) {
        Fail(*attempt, Ydb::StatusIds::PRECONDITION_FAILED, "Conflicting checksum checkpoint metadata");
    }
    UpdateChecksumStatus(key, now);
    WriteChecksumState(db, key, state);
    const auto barrier = SchemaBarriers.find(key);
    return attempt->HasFinishedAtUs() &&
           (barrier == SchemaBarriers.end() || Version(barrier->second.Schema.GetChecksum()) != Version(control));
}

void TController::SaveChecksumSnapshot(const TKey& key, TRowVersion snapshot,
        Ydb::StatusIds::StatusCode status, NIceDb::TNiceDb& db, TInstant now) {
    auto& state = ChecksumStates.at(key);
    auto* attempt = FindAttempt(state, Version(SchemaBarriers.at(key).Schema.GetChecksum()));
    Y_ENSURE(attempt);
    if (!attempt->HasDestinationSnapshot() && !attempt->HasFinishedAtUs()) {
        const auto started = TInstant::MicroSeconds(attempt->GetDeadlineUs()) - TDuration::Hours(1);
        TabletCounters->Percentile()[COUNTER_CHECKSUM_BARRIER_DURATION_MS].IncrementFor((now - started).MilliSeconds());
        if (status == Ydb::StatusIds::SUCCESS) {
            snapshot.ToProto(attempt->MutableDestinationSnapshot());
        } else {
            Fail(*attempt, status, "Cannot retain destination checksum snapshot");
        }
    }
    UpdateChecksumStatus(key, now);
    WriteChecksumState(db, key, state);
}

bool TController::HasChecksumReads(const TKey& key) const {
    const auto it = ChecksumStates.find(key);
    if (it == ChecksumStates.end()) {
        return false;
    }
    for (const auto& attempt : it->second.GetAttempts()) {
        if (!attempt.HasFinishedAtUs() && attempt.HasDestinationSnapshot() &&
            attempt.GetLocalStatus() == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED)
        {
            return true;
        }
    }
    return false;
}

void TController::ProgressChecksums(const TActorContext& ctx) {
    ui64 active = 0;
    ui64 mismatches = 0;
    bool needsTimer = false;
    for (auto it = ChecksumQueries.begin(); it != ChecksumQueries.end();) {
        auto state = ChecksumStates.find(it->second.Target);
        auto* attempt = state == ChecksumStates.end() ? nullptr : FindAttempt(state->second, it->second.SourceSnapshot);
        const auto replication = Find(it->second.Target.first);
        if (!attempt || attempt->HasFinishedAtUs() || !replication || replication->GetState() == TReplication::EState::Removing) {
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
            active += !attempt.HasFinishedAtUs();
            if (!attempt.HasDestinationSnapshot()) {
                continue;
            }
            const auto snapshot = TRowVersion::FromProto(attempt.GetDestinationSnapshot());
            bool running = false;
            for (const auto& [actor, query] : ChecksumQueries) {
                if (query.Target == key && query.SourceSnapshot == Version(attempt.GetCheckpoint())) {
                    running = true;
                    if (attempt.HasFinishedAtUs()) {
                        ctx.Send(actor, new TEvents::TEvPoison());
                    }
                }
            }
            if (attempt.HasFinishedAtUs() || attempt.GetLocalStatus() != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
                continue;
            }
            if (!running) {
                NChecksum::TSettings settings;
                settings.Database = replication->GetDatabase();
                settings.Path = target->GetDstPath();
                settings.TableId = TTableId(target->GetDstPathId());
                settings.Snapshot = snapshot;
                settings.Algorithm = attempt.GetCheckpoint().GetAlgorithm();
                settings.Deadline = TInstant::MicroSeconds(attempt.GetDeadlineUs());
                const auto actor = ctx.Register(NChecksum::CreateChecksum(ctx.SelfID, std::move(settings)));
                ChecksumQueries.emplace(actor, TChecksumQuery{ key, Version(attempt.GetCheckpoint()) });
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
                auto* attempt = FindAttempt(state->second, query->second.SourceSnapshot);
                if (attempt && !attempt->HasFinishedAtUs()) {
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
            Self->UpdateChecksumStatus(key, ctx.Now());
            auto barrier = Self->SchemaBarriers.find(key);
            if (barrier != Self->SchemaBarriers.end() && barrier->second.Schema.HasChecksum()) {
                auto* attempt = FindAttempt(state, Version(barrier->second.Schema.GetChecksum()));
                const auto phase = barrier->second.Phase;
                if (attempt && attempt->HasFinishedAtUs() &&
                    (phase == ESchemaBarrierPhase::Collecting ||
                        (phase == ESchemaBarrierPhase::Applied && attempt->GetLocalStatus() != Ydb::StatusIds::SUCCESS))) {
                    const auto started = TInstant::MicroSeconds(attempt->GetDeadlineUs()) - TDuration::Hours(1);
                    if (phase == ESchemaBarrierPhase::Collecting) {
                        Self->TabletCounters->Percentile()[COUNTER_CHECKSUM_BARRIER_DURATION_MS].IncrementFor(
                            (ctx.Now() - started).MilliSeconds());
                    }
                    // A departed partition may never report completion. Later arrivals use the durable terminal attempt.
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
                if (attempt.HasDestinationSnapshot() && (attempt.HasFinishedAtUs() ||
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
                if (attempt.HasFinishedAtUs() && ctx.Now() > TInstant::MicroSeconds(attempt.GetDeadlineUs()) + TDuration::Hours(1) &&
                    (activeBarrier == Self->SchemaBarriers.end() ||
                        Version(activeBarrier->second.Schema.GetChecksum()) != Version(attempt.GetCheckpoint())))
                {
                    state.MutableAttempts()->DeleteSubrange(i, 1);
                }
            }
            WriteChecksumState(db, key, state);
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
