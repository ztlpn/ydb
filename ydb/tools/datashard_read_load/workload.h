#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace NDataShardReadLoad {

constexpr size_t PopulationCount = 3;
enum EPopulation : size_t { Long, Short, Probe };
constexpr std::array<const char*, PopulationCount> PopulationNames = {"long", "short", "probe"};

struct TPhase {
    std::string Name;
    int64_t DurationUs = 0;
    std::array<uint64_t, PopulationCount> Rates = {};
};

struct TOptions {
    std::string Command;
    std::string Endpoint;
    std::string Database;
    std::string Table;
    uint64_t Rows = 4096;
    uint64_t PayloadBytes = 64;
    uint64_t RowsPerRead = 1;
    uint64_t Seed = 1;
    std::array<uint64_t, PopulationCount> Caps = {};
    uint64_t ProbeRate = 1;
    int64_t CancelAfterUs = 100000;
    int64_t LongTimeoutUs = 60000000;
    int64_t RpcGraceUs = 5000000;
    int64_t DrainTimeoutUs = 70000000;
    std::vector<TPhase> Phases;
};

TOptions ParseOptions(int argc, char** argv);

// A deterministic open-loop clock. Poll consumes every elapsed arrival but the
// caller launches at most one, counting the others as skipped. No catch-up queue.
class TArrivalClock {
public:
    TArrivalClock(uint64_t rate, int64_t startUs, int64_t endUs);
    uint64_t Poll(int64_t nowUs);
    int64_t NextUs() const;
    int64_t LastDueUs() const;

private:
    uint64_t Rate_;
    int64_t StartUs_;
    int64_t EndUs_;
    uint64_t Consumed_ = 0;
};

// Capacity belongs to a population, not a phase. Changing phases must not
// release capacity or cancel outstanding requests.
class TCapacity {
public:
    explicit TCapacity(std::array<uint64_t, PopulationCount> caps);
    bool Acquire(size_t population);
    void Release(size_t population);
    uint64_t Outstanding(size_t population) const;

private:
    std::array<uint64_t, PopulationCount> Caps_;
    std::array<uint64_t, PopulationCount> Outstanding_ = {};
};

} // namespace NDataShardReadLoad
