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

`--threads N` selects the number of dispatch threads (default 1, range 1–64).
All rates and in-flight caps are **totals for the process**: keep the same values
when changing the thread count. The tool divides each population's sessions
among the threads and interleaves its scheduled arrivals. For example, two
threads with a 50,000/s long rate and a 40,000 long cap target 50,000/s combined
and reserve 40,000 long sessions in total. Low rates, including the default
one-per-second probe, are preserved without rounding or multiplication. Sessions
stay with their assigned thread, so idle capacity is not borrowed between threads.
All threads use the same phase timeline and preserve outstanding work across
phase changes. One coordinator emits aggregate counts and merged latency
histograms in the existing JSONL format.

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

Arrivals follow a monotonic clock independently of completions. Each scheduling
pass dispatches up to `--dispatch-batch` elapsed arrivals per stream per thread
(default 64, range 1–1,024). Batching absorbs ordinary timer wakeup jitter: for example, 50,000
requests/s requires four launches per 80us wakeup. A limit of one would drop three
of those arrivals and impose an artificial rate ceiling. Use `--dispatch-batch 1`
to reproduce the original single-dispatch behavior for comparison.

When more arrivals have elapsed than the batch limit, only the most recent batch
is eligible and older arrivals are counted as scheduler skips. Skipped arrivals
are never retained for later dispatch. An occupied or unavailable session also
causes a counted skip. Batch dispatch stops at phase end or interruption. SDK query
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

To test whether cancellation traffic sustains an already populated queue, keep
the short stream enabled in a separate pressure phase. For example, after the
burst above, add `--phase pressure:30s:2000:20000` before recovery. A long-only
burst does not generate the intended deadline cancellations; KQP may still send
cancellations when retrying or cleaning up iterators.

Check achieved load before increasing concurrency. In the final `origins` entry
for each phase, divide `launched` by the configured phase duration. A large
`skipped_scheduler` count with zero `skipped_capacity` means raising the in-flight
cap cannot fix the lost arrivals. Batching removes the one-dispatch-per-wakeup
limit, but client CPU or SDK processing can still constrain the achieved rate.
Increase `--threads` to dispatch concurrently while keeping the configured total
rates and caps. Separate tool processes are also possible, but their rates and
caps must be divided manually. Adding a database node does not establish that
`TEvRead` crosses nodes;
verify the reader actor and target shard placement when testing remote decoding.

## Output and interpreting results

Stdout is JSON Lines; diagnostics and bounded error samples go to stderr.
`run_start` records effective settings, including `threads`, query text, and the
explained query plan.
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

### Analyze saved runs

The standalone Python script needs no extra packages:

```bash
python3 ydb/tools/datashard_read_load/analyze_retries.py retries.burst.jsonl
python3 ydb/tools/datashard_read_load/analyze_retries.py retries.noburst.jsonl retries.burst.jsonl
python3 ydb/tools/datashard_read_load/analyze_retries.py retries.burst.jsonl --timeline
python3 ydb/tools/datashard_read_load/analyze_retries.py retries.burst.jsonl --json > summary.json
```

With no filenames it reads `retries.burst.jsonl`. Each file must contain one
finished run, including its `final` record; interrupted runs with a final record
are accepted and labeled. The summary shows achieved dispatch rates, scheduler
and capacity skips, completion statuses, success-only latency, probes, and
outstanding requests. Multiple files also get a sustained long-read comparison
and a list of differing client settings. Server builds and executor policies
are not recorded in the input and must be tracked separately.

Dispatch rates divide phase-origin launches by configured phase duration.
Delivered percentages divide launches by scheduled arrivals. For interrupted
phases the configured duration can exceed the time actually run; consult the
interrupted flag and `window_s` in JSON output. Successful completion rates use
cumulative counter differences between phase boundaries divided by measured
elapsed time, including completions of requests launched in earlier phases.
The script never treats eventual cohort successes as completions during load.
`--timeline` also derives rates from cumulative counters and `elapsed_s`, avoiding
historical `interval_s` reporting errors. Histogram percentiles are taken directly
from final cohorts; they are not averaged or subtracted.

A **clean tail** is a sampled suffix lasting at least five seconds with client
outstanding at most `max(1, floor(1% of cap))`, no failed completions or skipped
arrivals, and both dispatch and successful completion rates at least 95% of the
configured target. Its start is relative to the phase's `phase_start` record.
This is an explicit recovery heuristic, not a measurement of server queue state.
JSON output preserves full cohort statistics, all-status and success-only latency
in milliseconds, and the measured phase windows. Add `--timeline` to include its
interval counts and rates in JSON too. Accounting inconsistencies and malformed
or unfinished reports produce an error and a nonzero exit code.

For `cancellation.sh` runs, use the cancellation-specific report:

```bash
python3 ydb/tools/datashard_read_load/analyze_cancellation.py cancellation.burst.jsonl
python3 ydb/tools/datashard_read_load/analyze_cancellation.py cancellation.noburst.jsonl cancellation.burst.jsonl
python3 ydb/tools/datashard_read_load/analyze_cancellation.py cancellation.burst.jsonl --timeline
python3 ydb/tools/datashard_read_load/analyze_cancellation.py cancellation.burst.jsonl --json > cancellation.summary.json
```

The default input is `cancellation.burst.jsonl`. This script reuses the retry
analyzer's accounting and adds short-request success/cancellation percentages,
all-status and success-only latency, and probe latency relative to baseline.
Probe counts distinguish scheduled, launched, eventually successful, completed
by phase end, and still pending at phase end. Probe scheduler/capacity skips and
outstanding totals appear in the load table. Thus eventual probe success cannot
hide arrivals skipped at the capacity limit or completions delayed until recovery.
The recovery phase also reports the same short-request clean-tail heuristic
defined above. Multiple inputs produce a sustained-phase comparison.

`--timeline` includes probes as well as bulk requests, with completion rates,
cancellation rates, skips, outstanding counts, and all-status p99/max latency.
It uses cumulative differences for counts and the original interval histograms
for latency. Latency is omitted (`null` in JSON) for the first interval in a phase
and for its final boundary, because those histogram and counter windows differ.
No cancellation-only latency histogram is available: all-status short latency
includes successful and cancelled requests. Client `CANCELLED` outcomes do not
prove that obsolete work was avoided on the server.

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
