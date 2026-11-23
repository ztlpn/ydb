# Manual CDC checksums

Run the following statement against the source database to calculate a checksum
and publish it to the named changefeeds of the table.

```sql
CHECKSUM TABLE `/Root/source` WITH (ALGORITHM = 'Sha256', PUBLISH_TO_CHANGEFEEDS = ('audit', 'replica_feed'));
```

Publication runs as a standalone statement, either in autocommit or in a fresh
transaction that commits with the statement. The query deadline bounds the
attempt, up to one hour. Preparing or explaining the query only builds its plan.

The source and each comparing ROW destination require both `EnableChecksumTable`
and `EnableAsyncReplicationChecksumCheck`. Both flags default to false. Enable CDC
checksums after upgrading source nodes, destination nodes, and every external
consumer of the selected changefeeds. No changefeed setting enables publication, and
ordinary `CHECKSUM TABLE` queries have no CDC side effects.

The caller needs table-read and schema-alter permissions. The scan runs with
the caller's identity. Supply a nonempty parenthesized list of distinct SQL
string literals. Each string names one changefeed on this table. Spelling,
case, and embedded commas are significant. Every named feed must exist, be
ready, and use native JSON. Validation finishes
before the first checkpoint is enqueued. For an async replication, look up
the feed name in the corresponding item's `source_changefeed_name` in
`DescribeReplication`. DataShard splits remain supported.

Fixed and automatically partitioned topics are supported. The publisher captures
the active partition IDs once. A split can invalidate this attempt only between
that capture and durable checkpoint publication in all captured partitions.
Checksum calculation runs concurrently with checkpoint delivery; a split after
publication does not invalidate the scan or its result. No operation freezes
topic partitioning or automatically retries a failed attempt.

Every checksum statement returns one row with `algorithm`, `count`, `hash`
(uppercase hexadecimal), `version`, and `mvcc_timestamp`. The timestamp is an optional struct
with two Uint64 members, `plan_step` and `tx_id`. It identifies the snapshot
read by the checksum. For example, a read incorporating transaction-local
writes has no single committed MVCC version and returns NULL.

Publication supports `CityHash64` (the default), `CityHash128`, and `Sha256`.
Each comparing destination reads its table with the algorithm named in the checkpoint.
Publication always returns its retained source version. Success means the
source has durably enqueued the result. Destination comparisons finish
asynchronously. The checksum covers the entire table for every feed mode.

## Records

A checkpoint has this native-JSON envelope:

```json
{
  "checksum_checkpoint": {
    "version": 1,
    "ts": [100, 20],
    "partition_count": 2,
    "algorithm": "Sha256",
    "algorithm_version": 1
  }
}
```

The terminal envelope is named `checksum_result`. It repeats the timestamp
and algorithm fields and adds a named YDB `status`
(such as `"SUCCESS"` or `"TIMEOUT"`), optional `issues` strings, and,
on success, `row_count` and `hash`. The hash has 16 uppercase hexadecimal digits for
`CityHash64`, 32 for `CityHash128`, and 64 for `Sha256`. An empty table has count zero
and a hash of the same width filled with zeros.

The stream's replication target identifies the table. The pair `ts = [step, txId]`
identifies an attempt within that target; the same pair appears in its checkpoint
and result. Neither a UUID nor source table/schema metadata is transmitted.
The envelope `version` is independent of `algorithm_version`.

These are control records, with no row key or update. Consumers must recognize
them before parsing row data. The destination also recognizes and consumes
controls when comparisons are disabled.

## Ordering and snapshots

Each DataShard atomically retains snapshot `S` and queues a checkpoint in a
coordinated TxProxy snapshot transaction. Its dependencies include preceding volatile
transactions. Every topic partition receives the checkpoint after all changes
through `S`, before later changes from any producer.

Checkpoints use the existing opaque PQ schema-barrier transport. A producer
cannot send past a checkpoint until PQ acknowledges its durable emission after
all producers arrive. Each coordinated operation has a distinct `(step, txId)`;
the payload carries that pair as `ts`. This sender invariant prevents the
watermark-based transport from coalescing consecutive checkpoints. Source shard
handoff drains the old producer before registering its successors.

The terminal operation chooses one current DataShard to publish the result
to one partition per feed. A stable synthetic partition key routes results
without a row key and keeps retries on the same partition while the layout is
unchanged. Ordinary producer sequence-number deduplication handles retries.
Results do not introduce another PQ quorum barrier. A checkpoint invalidated
during delivery still completes its existing per-partition barriers with a
failure result so waiting producers can proceed. When a retry discovers a new
layout, it checks durable barrier emission watermarks on the original partitions,
including inactive parents. If all original partitions already emitted the
checkpoint, it completes delivery without emitting another checkpoint or failure
in the children. The watermarks survive PQ restarts. An unavailable watermark
causes delivery to retry, not a partition-layout failure.

On a ROW destination, workers use the durable control-record handshake also
used for schema changes. At a checkpoint they have committed the preceding
rows and park until the controller retains a destination snapshot. The source
publishes the checkpoint only to the partitions it captured. The controller
waits for `partition_count` distinct source partition IDs to report that
checkpoint. It persists each report, so a worker replay after a restart counts
only once. Required child workers may register later. A parent at the checkpoint
cannot finish its partition until its checkpoint offset is committed, so its children cannot
apply later rows before the destination snapshot is retained.

While workers are parked, the controller creates a fresh retained snapshot `D`.
It persists `D` and the worker release state together. Workers then resume while
the local checksum runs at `D`. Source and destination hashes are joined by
replication target and source `ts`; `D` is a separate destination version.
Waiting for a source result never holds workers at the checkpoint.
The local read checks the destination table identity and verifies its own
column layout before and after calculation. Replicated schema changes wait
while a local scan depends on the earlier schema. Matching initial schemas and
ordered schema replication are required; records no longer compare source and
destination schema descriptions.

GLOBAL destinations acknowledge checkpoint and result records. The controller
reports `UNSUPPORTED` for the attempt in DescribeReplication. A checksum
checkpoint leaves the normal cross-table commit schedule intact. Historical
comparisons remain visible as the last completed result.

## Failures and recovery

KQP owns the source scan for the lifetime of the SQL execution. Multiple
publications can run concurrently. DDL can change a table or feed during a scan.
The publisher rechecks the schema and original feed/topic identities before
enqueuing success. It preserves the original partition list even if the current
layout has changed. Results use ordinary routing to one current partition and
are joined by target and source timestamp. A successful publication's result cannot
overtake the checkpoint: an unchanged partition preserves order, and a child waits
for its parent to drain. Failure markers for interrupted publication can arrive
without a checkpoint. Results do not need a partition quorum.

If the source node fails after committing a checkpoint, its snapshot expires
and the destination terminates the incomplete attempt after one hour.

Destination membership, offsets, snapshot identity, local checksum, and join
state are durable. After releasing workers, recovery never substitutes a newer
snapshot for `D`. Missing snapshots or changed table identities/schemas fail the
attempt. Snapshots are discarded after local completion, independently of when
the source result arrives; expiration bounds retention if cleanup is interrupted.

DescribeReplication exposes a checksum status on each item, without a checkpoint
identifier. An unfinished attempt has unspecified
status. A completed comparison has `SUCCESS`, with `last_result.matches` indicating
equality. A failed attempt preserves the last completed comparison. Late older
records do not overwrite the latest attempt's status.

The latest attempt and latest completed comparison advance independently: a
previous check may finish while a newer check is pending. Its comparison remains
available without replacing the newer attempt's status.

Source publication counters track active requests and failures. Replication
controller counters expose active attempts, targets whose retained comparison is a mismatch,
failure/timeout totals, and checkpoint barrier duration in milliseconds.

There is no periodic checksum scheduler, trigger SDK helper, or trigger CLI.
