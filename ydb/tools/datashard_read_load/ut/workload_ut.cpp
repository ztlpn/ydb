#include <ydb/tools/datashard_read_load/workload.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <stdexcept>

namespace NDataShardReadLoad {

TEST(ArrivalClock, CountsLateArrivalsWithoutCatchUp) {
    TArrivalClock clock(100, 0, 1000000);
    EXPECT_EQ(clock.Poll(0), 1u);
    EXPECT_EQ(clock.NextUs(), 10000);
    EXPECT_EQ(clock.Poll(95000), 9u); // caller skips eight and launches one
    EXPECT_EQ(clock.LastDueUs(), 90000);
    EXPECT_EQ(clock.NextUs(), 100000);
    EXPECT_EQ(clock.Poll(95000), 0u);
    EXPECT_EQ(clock.Poll(1000000), 90u);
    EXPECT_EQ(clock.Poll(2000000), 0u);
}

TEST(ArrivalClock, ZeroRateAndPhaseBoundaries) {
    TArrivalClock idle(0, 1000000, 2000000);
    EXPECT_EQ(idle.Poll(1500000), 0u);
    EXPECT_EQ(idle.NextUs(), 2000000);
    TArrivalClock before(1, 0, 1000000), after(1, 1000000, 2000000);
    EXPECT_EQ(before.Poll(1000000), 1u);
    EXPECT_EQ(after.Poll(999999), 0u);
    EXPECT_EQ(after.Poll(1000000), 1u);
    EXPECT_EQ(after.Poll(2000000), 0u);
}

TEST(ArrivalClock, FractionalMicrosecondIntervalsRoundUp) {
    TArrivalClock clock(3, 0, 1000000);
    EXPECT_EQ(clock.Poll(0), 1u);
    EXPECT_EQ(clock.NextUs(), 333334);
    EXPECT_EQ(clock.Poll(333333), 0u);
    EXPECT_EQ(clock.Poll(333334), 1u);
    EXPECT_EQ(clock.LastDueUs(), 333334);
    EXPECT_EQ(clock.Poll(1000000), 1u);
}

TEST(ArrivalClock, BatchingPreservesRateDespiteWakeupJitter) {
    TArrivalClock clock(50000, 0, 1000000);
    uint64_t scheduled = 0, dispatched = 0;
    // An 80us wakeup interval cannot deliver 50k/s with one dispatch per wakeup.
    // Batching elapsed arrivals should deliver the configured rate exactly.
    for (int64_t now = 0; now < 1000000; now += 80) {
        const auto batch = clock.PollBatch(now, 64);
        scheduled += batch.Scheduled;
        dispatched += batch.Dispatch;
        for (uint64_t i = 0; i < batch.Dispatch; ++i) {
            EXPECT_LE(clock.DueUs(batch.FirstIndex + i), now);
        }
    }
    const auto last = clock.PollBatch(999999, 64);
    EXPECT_EQ(scheduled + last.Scheduled, 50000u);
    EXPECT_EQ(dispatched + last.Dispatch, 50000u);
    EXPECT_EQ(clock.PollBatch(1000000, 64).Scheduled, 0u);
}

TEST(ArrivalClock, BatchesAreBoundedAndDoNotRetainMissedArrivals) {
    TArrivalClock clock(50000, 0, 1000000);
    EXPECT_EQ(clock.PollBatch(0, 64).Dispatch, 1u);
    const auto stalled = clock.PollBatch(100000, 64);
    EXPECT_EQ(stalled.Scheduled, 5000u);
    EXPECT_EQ(stalled.Dispatch, 64u);
    EXPECT_EQ(clock.DueUs(stalled.FirstIndex), 98740);
    EXPECT_EQ(clock.PollBatch(100000, 64).Dispatch, 0u);
    const auto next = clock.PollBatch(100080, 64);
    EXPECT_EQ(next.Scheduled, 4u);
    EXPECT_EQ(next.Dispatch, 4u);
    EXPECT_EQ(clock.DueUs(next.FirstIndex), 100020);
}

TEST(ArrivalClock, NoBatchLaunchesAtOrAfterPhaseEnd) {
    TArrivalClock clock(10000, 0, 1000000);
    EXPECT_EQ(clock.PollBatch(0, 64).Dispatch, 1u);
    const auto end = clock.PollBatch(1000000, 64);
    EXPECT_EQ(end.Scheduled, 9999u);
    EXPECT_EQ(end.Dispatch, 0u);
    EXPECT_EQ(clock.PollBatch(1001000, 64).Dispatch, 0u);
}

TEST(ArrivalClock, BatchLimitOnePreservesOriginalPacing) {
    TArrivalClock clock(50000, 0, 1000000);
    EXPECT_EQ(clock.PollBatch(0, 1).Dispatch, 1u);
    const auto late = clock.PollBatch(80, 1);
    EXPECT_EQ(late.Scheduled, 4u);
    EXPECT_EQ(late.Dispatch, 1u);
    EXPECT_EQ(clock.DueUs(late.FirstIndex), 80);
}

TEST(Capacity, PopulationsAndPhaseTransitionsAreIndependent) {
    TCapacity capacity({2, 1, 1});
    EXPECT_TRUE(capacity.Acquire(Long));
    EXPECT_TRUE(capacity.Acquire(Long));
    EXPECT_FALSE(capacity.Acquire(Long));
    EXPECT_TRUE(capacity.Acquire(Short));
    EXPECT_TRUE(capacity.Acquire(Probe));
    TArrivalClock nextPhase(10, 1000000, 2000000);
    EXPECT_EQ(nextPhase.Poll(1000000), 1u);
    EXPECT_FALSE(capacity.Acquire(Long));
    capacity.Release(Long);
    EXPECT_TRUE(capacity.Acquire(Long));
    capacity.Release(Probe);
    EXPECT_EQ(capacity.Outstanding(Probe), 0u);
    EXPECT_THROW(capacity.Release(Probe), std::logic_error);
}

TEST(Capacity, StopsLaunchingAndDrainsOutstandingWork) {
    TCapacity capacity({1, 1, 1});
    TArrivalClock clock(100, 0, 10000);
    EXPECT_EQ(clock.Poll(0), 1u);
    EXPECT_TRUE(capacity.Acquire(Long));
    EXPECT_EQ(clock.Poll(10000), 0u);
    EXPECT_EQ(capacity.Outstanding(Long), 1u);
    capacity.Release(Long);
    EXPECT_EQ(capacity.Outstanding(Long), 0u);
}

TOptions Parse(std::initializer_list<const char*> arguments) {
    std::vector<char*> argv;
    for (const auto* argument : arguments) {
        argv.push_back(const_cast<char*>(argument));
    }
    return ParseOptions(argv.size(), argv.data());
}

TEST(Options, RequiresExplicitLoadConfiguration) {
    EXPECT_THROW(Parse({"load", "run", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load"}), std::runtime_error);
    auto options = Parse({"load", "run", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load",
        "--long-inflight", "1", "--short-inflight", "0", "--probe-inflight", "1", "--phase", "baseline:2s:10:0"});
    EXPECT_EQ(options.Phases.size(), 1u);
    EXPECT_EQ(options.Phases[0].DurationUs, 2000000);
    EXPECT_EQ(options.Phases[0].Rates, (std::array<uint64_t, 3>{10, 0, 1}));
    EXPECT_EQ(options.DispatchBatch, 64u);
}

TEST(Options, RejectsInvalidPhaseAndDeadlines) {
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load", "--phase", "x:1s:-1:0"}), std::runtime_error);
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load", "--cancel-after", "60s"}), std::runtime_error);
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/`bad"}), std::runtime_error);
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load", "--dispatch-batch", "0"}), std::runtime_error);
}

} // namespace NDataShardReadLoad
