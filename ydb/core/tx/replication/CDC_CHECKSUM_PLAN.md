# Manual checksums through CDC checkpoints

## Summary and baseline

Implement on `cdc-checksum-checkpoint`, based on `main`. Preserve
`checksum-replication` as the reference branch and leave unrelated files untouched.

A manual source RPC establishes a retained snapshot, publishes an ordered
checkpoint, calculates the checksum, and publishes the result. Destinations
establish matching table snapshots and compare locally. All checksum
communication between clusters uses CDC records.

Agreed behavior:

- Table-scoped barriers; temporary cross-table inconsistency during GLOBAL
  flushing is accepted.
- Support ROW and GLOBAL replication.
- Support existing and new CDC feeds through coordinated rollout.
- Publish to all ready native-JSON user feeds on the selected table, including
  ordinary CDC feeds.
- Require fixed topic partitions; retain source table-shard split support.
- Source execution is request-scoped. No durable source job, periodic scheduler,
  or automatic retry of interrupted scans.

## Public interfaces and compatibility

- Reuse `EnableAsyncReplicationChecksumCheck`, defaulting to false.
- Port the existing per-item `ChecksumStatus`, checksum values, conversion code,
  and C++ SDK result-display support. Mismatches are `SUCCESS` with
  `matches=false`; failed attempts retain the previous completed comparison.
- Add checkpoint IDs to the latest attempt and retained comparison for
  correlation with the manual request.
- Add synchronous `ReplicationService.PublishTableChecksum`, accepting one
  source table path and standard operation parameters. Return the checkpoint ID,
  source snapshot, checksum, and selected feed names.
- RPC success means the result is durably queued for delivery to every selected
  feed, not that destination comparison has completed. Reject asynchronous
  operation mode; default the attempt timeout to one hour.
- Require authenticated table-read and schema-alter permissions. Run the
  checksum query using the caller's identity.
- No trigger SDK helper or CLI command in this version. No changes to
  `CHECKSUM TABLE` SQL semantics.

Preflight rejects requests with no eligible feeds, feeds still performing initial
scans, unsupported active user-feed formats, or automatic topic partitioning.
Disabled feeds and internal backup streams are excluded. Freeze feed identities
for the attempt; newly created feeds do not join it.

Upgrade source servers, destinations, and external consumers before enabling
publication. No new changefeed setting or changefeed-alter API is required.

## Source execution and CDC transport

### Source coordination

- Add internal SchemeShard/DataShard operations for checkpoint creation and
  terminal-result publication, reusing coordinated CDC scheme operations.
- Checkpoint creation establishes one source version `S` across the table's
  shards. Each shard atomically retains an expiring snapshot and persists
  checkpoint records into its change queue.
- Wait for preceding writes, including volatile transactions, and prevent later
  changes or resolved timestamps from overtaking the checkpoint.
- An expiring per-table reservation rejects concurrent attempts with
  `OVERLOADED`. It tracks ownership and cleanup, not a resumable checksum job.
- Serialize checkpoint installation with schema and topology operations.
  Prevent topic partition-layout changes during the reservation. Preserve
  snapshots and producer-control state through ordinary table-shard splits.
- Run `CHECKSUM TABLE ... CityHash64` at `S`, extracting the query helper from
  the reference branch without its private streaming protocol.
- Revalidate table identity, schema, and original feed identities before
  publishing. Persist the terminal result through durable change queues, then
  release the source snapshot and reservation.

On cancellation or query failure, publish a terminal failure when possible and
clean up. An abrupt node failure may leave a checkpoint without a result;
snapshots and reservations expire, and destinations terminate the attempt.
Recovery may finish committed record emission but does not restart the scan.

### Records and ordering

Introduce native-JSON `checksum_checkpoint` and `checksum_result` control
envelopes, backed by dedicated internal record types. Both identify the protocol
version, checkpoint ID, table identity, source snapshot, canonical schema, and
checksum algorithm/version. Results carry status/issues and, on success, row
count and a 16-digit uppercase hexadecimal hash. The checksum covers the whole
table regardless of feed mode.

Every selected topic partition must preserve:

```
all table changes through S -> checkpoint -> later table changes
```

- Broadcast checkpoints to every partition, including idle partitions.
- Extend sender batching and PQ producer coordination using the schema-barrier
  pattern: hold subsequent records until the partition durably emits the
  checkpoint after all contributing producers arrive.
- Track checkpoint identity explicitly; never coalesce different checkpoints
  using a latest-version watermark.
- Publish terminal results after their checkpoints, deduplicating producer
  deliveries. Results do not impose another data barrier.
- Persist acknowledgment and deduplication state, including producer handoff
  across source shard splits.

## Destination execution and result storage

- Recognize controls before row parsing. Workers stop at checkpoints, including
  checkpoints inside mixed data/control batches.
- Persist expected topic-partition membership, offsets, arrivals, and execution
  progress in the replication controller.
- After every partition arrives, drain preceding writes. GLOBAL mode performs
  the existing table-specific staged-write flush; ROW mode waits for ordinary
  writes to finish.
- Keep workers parked while creating a fresh coordinated volatile snapshot `D`
  after the flush. Persist its identity and retention state before release.
- Reuse snapshot creation/discard facilities, retaining each snapshot through
  the bounded attempt deadline. Do not port checksum transaction ranges or
  planned-commit retention changes.
- For GLOBAL mode, preserve target-flush transaction-ID and fresh-heartbeat
  recovery rules. Coordinate with normal commits and schema barriers.
- When a later record reuses a transaction ID already committed on this table,
  apply it at a fresh local MVCC version. Reusing the original commit version
  would change the retained snapshot after workers resume.
- Resume CDC once `D` is secured. Compute and persist the local checksum and
  release `D` independently of source-result arrival.
- Join results by checkpoint ID. Waiting for the source result never holds
  workers. Serialize local flush/snapshot establishment and retain separate
  durable join state for outstanding checkpoints.
- Defer destination schema changes while a checksum depends on the old schema.
  Identity/schema mismatches fail the attempt rather than report a mismatch.
- Recover controller progress after restart. If a recorded snapshot expires
  after workers resume, fail that attempt; never substitute a newer snapshot.
- Use a one-hour destination deadline from first checkpoint observation.
  Missing results, lost snapshots, and unsupported attempts terminate without
  replacing the last valid comparison. Late records cannot overwrite newer
  status.
- Upgraded consumers always recognize controls. Disabling the flag prevents
  new comparisons but permits cleanup and consumption of existing records.

Port only the flag, result-display interfaces, query helper, and relevant tests.
Omit periodic configuration, scheduling, the private source RPC/client, and the
old destination admission subsystem.

## Validation and acceptance

- End-to-end ROW and GLOBAL replication with multiple source shards, topic
  partitions, and destination shards; empty tables and idle partitions.
- Concurrent writes around `S`, mixed batches, faster unrelated tables, and
  cross-table transactions. Verify the selected table while exercising the
  accepted temporary GLOBAL relaxation.
- Existing ordinary JSON feeds, multiple feeds, every feed mode, and preflight
  rejection without partial publication.
- Duplicate records, offset replay, worker migration, controller/PQ/DataShard
  restarts, source shard splits, and delayed producer acknowledgments.
- Cancellation and source loss before/after checkpoint creation and result
  persistence; incomplete attempts expire without permanently blocking CDC.
- Results completing in either order, snapshot expiry, consecutive checkpoints,
  schema changes, and dropped/recreated tables or feeds.
- Permissions, flag behavior, RPC timeout/status mapping, checkpoint
  correlation, retained previous results, and SDK presence semantics.
- Existing schema barriers and ordinary CDC behavior. Include PQ message-size,
  batching, and offset-layout regression cases required by repository guidance.

Add counters for active attempts, publication failures, comparison
failures/timeouts, mismatches, and barrier duration. Document control records,
coordinated rollout, request-scoped failures, and temporary GLOBAL relaxation.

## Implementation notes

The implementation uses the existing opaque PQ schema-barrier transport for
checkpoints. Sender batching stops at each checkpoint until PQ acknowledges it,
so successive checkpoint identities cannot be collapsed into a latest-version
watermark. A terminal operation selects one current DataShard to broadcast its
result through ordinary producer deduplication. No PQ implementation changes
are needed.

The destination reuses the durable schema-control handshake, with a checksum
payload that does not alter the table schema. Snapshot creation replaces the
schema-alter step. The latest attempt and latest completed comparison have
separate durable version watermarks, so an earlier comparison can finish while
a newer attempt is pending.

The source exposes active-publication and publication-failure counters under
`replication/subsystem=checksum_publication`. Controller counters cover active
attempts, retained mismatches, failures, timeouts, and checkpoint barrier duration.
Deadline and garbage-collection wakeups exist only while attempt state remains;
they do not initiate checks.

See [CDC_CHECKSUM_PROTOCOL.md](CDC_CHECKSUM_PROTOCOL.md) for operational behavior.
Regression cases cover JSON controls and malformed inputs, source publication on
ordinary feeds, disabled comparisons, ROW/GLOBAL snapshot comparisons, results
arriving after a newer checkpoint, and failure/replay preserving the comparison.
The broader acceptance scenarios above remain the rollout validation checklist.
Validation includes warning-clean builds and focused parsing, checksum, and
existing schema-barrier tests, following the subsequent request to run tests.

## Validation completed (2026-11-19)

149 distinct tests passed across five targets:

- Controller: all 7 `ChecksumBarrier` and 49 `SchemaChangeBarrier` cases.
- DataShard: all 36 `DataShardReplication` cases, including retention of a
  snapshot across later writes under a committed transaction ID, with and
  without a shard restart.
- Table writer: all 11 `LocalTableWriter` cases.
- JSON controls: all 40 `JsonChangeRecord` cases.
- SDK descriptions: all 6 `ReplicationDescription` cases.

The end-to-end ROW and GLOBAL cases use two source shards, fixed topic
partitions, two replicated tables, cross-table writes, and consecutive checksum
publications. Each comparison excludes writes after its checkpoint. GLOBAL uses
a one-hour commit interval to exercise table-specific flushing.

Validation found and fixed two data-path issues: checksum outbox records needed
an explicit post-commit handoff to the live sender, and writes reusing a
table-flushed transaction ID needed fresh MVCC versions to preserve retained
snapshots. The controller test target now links both checksum UDF modules.

Tests used `./ya make --build relwithdebinfo -DUSER_CXXFLAGS=-Werror -tA` with
the suite filters above. `-DUSER_LDFLAGS=-Wl,--strip-debug` was necessary because
the controller test executable otherwise exceeded the linker's 32-bit debug
section relocation limit. The SDK dependency check passed for 125 `ya.make`
files, and `git diff --check` passed. Formatting used `ya tool clang-format`
with `.github/config/cpp_format/.clang-format`, limited to changed regions.
`clang-tidy` was unavailable in the environment and the ya tool catalog.

The broader restart, split, permission, cancellation, and rollout scenarios in
the acceptance checklist remain additional validation work; these results do
not claim coverage of every item in that checklist.
