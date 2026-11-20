# Heartbeat-aligned CDC checksums

## Objective

Preserve GLOBAL atomic visibility while publishing manual checksums through CDC.
A checkpoint closes an exclusive heartbeat-grid boundary H, retaining source
snapshot S = H.Prev(). Only the checked table parks; ordinary all-table commits
establish the destination cut. Keep the public RPC, feature flag, ROW behavior,
and result-display API.

## Implementation

1. Use strict [begin, end) GLOBAL assignments. Preserve the five-ID bound using
   an admitted interval window and deferred-record replay from CDC. Validate
   both assignment bounds in controller, service, and writers. Return window
   information instead of indefinitely blocking assignments outside it.
2. Workers retain the earliest deferred offset and minimum unapplied source
   version. They do not commit past deferred data. Resolve progress only through
   min(observed resolved version, minimum unapplied version), after admitted
   writes complete. On window advancement, replay from the uncommitted offset;
   use gap-aware offset deduplication and the committed frontier for replayed prefixes.
3. Derive a common source grid from positive selected-feed heartbeat intervals.
   Arm a future boundary on all table shards, retain S without advancing write
   visibility, and durably hold records at/after H. Once the prefix is resolved,
   emit the checkpoint and release the held suffix with monotonic outgoing
   sequence numbers. Preserve ordering, retention, and deferred state through
   restart, cancellation, and source shard handoff. No-heartbeat ROW feeds keep
   their existing path.
4. Carry the exclusive commit boundary in a versioned checksum control. The
   checkpoint also counts as heartbeat H after its prefix is applied. Wait for
   all participants to reach H and all global commits through H to finish;
   retain destination snapshot D and persist it before release. Do not use a
   table-only flush, reset heartbeats, or enter DDL verification for checksums.
5. Preserve existing schema/index behavior. Fail comparisons that would create
   circular waits with existing schema barriers. Keep the shared DataShard
   snapshot-preservation fix. Drain legacy assignments before strict mode and
   reject incompatible checks safely during transition.
6. Update protocol documentation and existing counters. Keep scans request-
   scoped and manual; add no scheduler, table-group registry, public settings,
   or per-check assignment epochs.

## Validation

- Cross-table transactions immediately before/after H and a fast unrelated
  table: no partial visibility, matching checksums, and later writes excluded.
- Exact boundaries, idle partitions, empty intervals, mixed batches, skipped
  heartbeats, and multiple feed intervals.
- More than five intervals, missing older intervals, out-of-order records,
  bounded IDs/memory, replay, stale replies, and safe source offsets.
- Volatile writes, source restarts/splits, retained future snapshots, monotonic
  producer sequence numbers, cancellation, and consecutive checks.
- Destination recovery during replay, commit waiting, snapshot creation, and
  release; schema/index concurrency without deadlocks.
- Existing ROW, schema-barrier, assignment, reader/writer, checksum, and SDK
  regressions. Use repository build/test guidance and clang-format with
  .github/config/cpp_format/.clang-format. Do not edit during compilation.

## Status

Implementation is complete. On 2026-11-20, all 536 tests passed across:

- `replication/controller/ut_assign_tx_id`
- `replication/controller/ut_schema_change`
- `replication/service/ut_table_writer`
- `replication/service/ut_json_change_record`
- `replication/service/ut_worker`
- `datashard/ut_replication`
- `datashard/ut_snapshot`
- `datashard/ut_change_exchange`

These paths are relative to `ydb/core/tx`. The run used `./ya make --build
relwithdebinfo -tA` with `-DUSER_CXXFLAGS=-Werror` and
`-DUSER_LDFLAGS=-Wl,--strip-debug`. Both `ChecksumFenceSurvivesHandoff`
variants also passed a focused rerun after initializing the test's snapshot
assertion state. Changed C++ lines were formatted with the ya-provided
clang-format and `.github/config/cpp_format/.clang-format`; `git diff --check`
passed.
