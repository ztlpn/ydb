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
}

TEST(Options, RejectsInvalidPhaseAndDeadlines) {
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load", "--phase", "x:1s:-1:0"}), std::runtime_error);
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load", "--cancel-after", "60s"}), std::runtime_error);
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/`bad"}), std::runtime_error);
}

} // namespace NDataShardReadLoad
