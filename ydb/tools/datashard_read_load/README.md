# DataShard read/cancel load

A C++ SDK workload for investigating DataShard mailbox congestion and preserving
the cancellation behavior introduced by [#15398](https://github.com/ydb-platform/ydb/pull/15398)
for [#14936](https://github.com/ydb-platform/ydb/issues/14936).
The first target is a local single-node YDB. The tool requires no server patches,
pauses, failpoints, or client access to internal actor events.

## Build and initialize

```bash
./ya make --build relwithdebinfo ydb/tools/datashard_read_load
./ya make --build relwithdebinfo -tA ydb/tools/datashard_read_load 2>&1 | tail -80

ydb/tools/datashard_read_load/datashard_read_load init \
  --endpoint localhost:2136 --database /Root --table /Root/mailbox_load
```

Use `YDB_TOKEN` for authentication when required. `init` creates a dedicated row
table with primary key `(Bucket, Item)`, all keys under `Bucket = 0`, and a nullable
String `Payload`. Defaults are 4,096 rows and 64 payload bytes per row. Override
them with `--rows` and `--payload-bytes`. Size and load splitting are disabled,
and the table is limited to one partition. An existing table causes failure;
the tool never drops tables. Interrupted initialization can leave a partial table.

## Load controls

Each `--phase NAME:DURATION:LONG_RPS:SHORT_RPS` specifies independent arrival
rates. Repeat this argument to compose an experiment. Rates are whole requests
per second; zero disables a stream in that phase. Durations accept units such as
`ms`, `s`, and `m`. Names must be unique. An independent probe stream defaults to
one request per second (`--probe-rps`).

All three in-flight caps are explicit: `--long-inflight`, `--short-inflight`,
and `--probe-inflight`. A disabled stream can have a zero cap. Each slot reserves
one SDK session before measurement, overriding the SDK's default limit of 50.
Session creation and warmup happen outside the measured phases. Session capacity
on the server must also accommodate the chosen caps.

Reads execute cached, parameterized `OnlineRO` SQL ranges over the primary key.
`--rows-per-read` defaults to one and can be increased to vary execution cost.
Pass the initialization row count again with `--rows` when overriding it. Warmup
covers the configured keyspace and checks that reads return the expected count.
Random range positions use `--seed` (default 1).

Short requests set server-side `CancelAfter` to `--cancel-after` (default 100ms).
All requests use `--long-timeout` as their operation timeout (default 60s);
the RPC timeout adds `--rpc-grace` (default 5s). Short cancellation deadlines
must be less than the operation timeout. First measure healthy latency and choose
a cancellation deadline above it, so requests normally reach the shard before
becoming obsolete under congestion.

Arrivals follow a monotonic clock independently of completions. When the clock
falls behind, elapsed arrivals are counted and skipped, with at most one launch
per stream per scheduling pass. An occupied or unavailable session also causes
a counted skip. There is no unbounded waiting queue or catch-up burst. SDK query
retries are disabled; normal KQP iterator retries remain part of server behavior.
Broken sessions are replaced asynchronously, with failures and replacements
reported separately. Failed replacements are retried at most once per reporting
interval.

Phase changes preserve outstanding requests, including their original deadlines.
After the final phase, arrivals stop and existing requests drain for up to
`--drain-timeout` (default 70s). SIGINT/SIGTERM stop arrivals and use the same bounded
drain. Probe arrivals continue during zero-rate phases, but stop during final
draining. Exit codes are 0 for a completed run, 1 for setup errors or unexpected
successful row counts, 2 for an incomplete drain, and 130 for a drained interrupted
run. Ordinary query error statuses are measurements, not process failures.

## Experiments

These rates are starting examples, not established reproduction thresholds.
Calibrate sustainable throughput, then sweep burst size, arrival rates, short
deadline, rows per read, and capacities. Keep the client and server CPU capacity
in view when sharing a machine.

Cancellation effectiveness: mostly expiring requests with independent useful-work
probes. Increase the burst rate until requests queue beyond the short deadline.

```bash
ydb/tools/datashard_read_load/datashard_read_load run \
  --endpoint localhost:2136 --database /Root --table /Root/mailbox_load \
  --long-inflight 0 --short-inflight 4096 --probe-inflight 16 \
  --phase baseline:20s:0:100 \
  --phase burst:20s:0:20000 \
  --phase recovery:40s:0:100 > cancellation.jsonl
```

Executor progress and recovery: long-deadline reads provide admitted work while
short-deadline reads and server retries generate read/cancel traffic. Return to
the original rate after the burst, then remove bulk traffic while keeping probes.

```bash
ydb/tools/datashard_read_load/datashard_read_load run \
  --endpoint localhost:2136 --database /Root --table /Root/mailbox_load \
  --long-inflight 20000 --short-inflight 4096 --probe-inflight 16 \
  --phase baseline:20s:100:100 \
  --phase burst:10s:30000:20000 \
  --phase recovery:40s:100:100 \
  --phase probes:20s:0:0 > progress.jsonl
```

Reaching the usual 15,000-iterator admission limit requires enough live requests;
the small-cap smoke run cannot test that condition. Large session populations can
hit KQP/session, gRPC, client CPU, or memory limits first. Inspect actual launches,
skips, statuses, and shard counters before attributing a limit to the mailbox.
Adjust operation timeouts if admitted work must survive longer than a phase.

## Output and interpreting results

Stdout is JSON Lines; diagnostics and bounded error samples go to stderr.
`run_start` records effective settings, query text, and the explained query plan.
Then `phase_start`, approximately one-second `interval`, `phase_end`, and `final`
records report populations separately as `long`, `short`, and `probe`.

`totals` are cumulative since measurement began. `interval` contains deltas and
interval histograms with the actual `interval_s`; derive rates by dividing counts
by that duration. Completion counters include requests launched in earlier phases.
`origin` in phase records describes requests scheduled in that phase; `origins`
in the final record includes later completions attributed to their launch phase.

For each population, cumulative accounting obeys:

```text
scheduled = launched + skipped_scheduler + skipped_capacity
launched = completed + outstanding
```

Also reported: completion status counts, successful rows, unexpected row counts,
session failures/replacements, all-completion and success-only latency percentiles,
and launch scheduling delay, in microseconds. A zero histogram has no samples.
At phase end, origin `outstanding` may be positive; later final origin records
show what eventually completed. `final.drained` indicates whether any measured
requests remained when the bounded drain ended.

Before interpreting a run as evidence, verify that the SQL uses DataShard read
iterators: inspect the emitted plan and correlate actual reads with iterator
creation/row counters on the target shard. Collect aligned server measurements:

- `ReadIteratorsCount`, `ExecutorTxInFly`, queued/finished transactions and rows read.
- Read admissions/rejections and cancellation lookup hits versus misses; a sent
  cancellation can target an already rejected read and free no slot.
- Target `TEvActivateLowExecution`/`TEvActivateExecution` delivery delays using
  sampled `EventSlowDelivery` traces, plus executor queue/wait states.
- Actual CPU/profile evidence alongside reported shard CPU, whose transaction
  timers omit some mailbox handling costs.

The cancellation experiment checks whether queued obsolete reads avoid execution;
client `CANCELLED` statuses alone cannot establish that. Current servers already
contain #15398, so this is a regression workload unless run against a server with
the historical behavior. Compare the same recipes on baseline and candidate
server builds. The progress experiment supports the handoff hypothesis only if
admitted runnable work and executor activations stall behind read/cancel traffic.
Successful recovery at the pre-burst rate is a useful before/after comparison.
Single-node testing omits inter-node `TEvRead` deserialization costs; failure to
reproduce locally does not rule out the incident mechanism.
