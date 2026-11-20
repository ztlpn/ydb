# Manual CDC checksums

`Ydb.Replication.V1.ReplicationService.PublishTableChecksum` publishes a checksum
for one source row table. The request contains `path` and ordinary
`operation_params`. Only synchronous operation mode is supported. The default
and maximum attempt duration is one hour; smaller operation or cancellation
timeouts shorten it.

Enable `EnableAsyncReplicationChecksumCheck` only after upgrading source nodes,
destination nodes, and every external consumer of the selected changefeeds.
The flag defaults to false. No changefeed setting enables publication, and
ordinary `CHECKSUM TABLE` queries have no CDC side effects.

The caller needs table-read and schema-alter permissions. The scan runs with
the caller's identity. Publication selects all ready native-JSON user feeds;
it excludes disabled feeds and internal backup feeds. An initial scan,
unsupported active format, or automatic topic partitioning rejects the request
before checkpoint publication. Topic partition layouts must remain fixed during
the attempt. DataShard splits remain supported.

ROW replication normally creates automatically partitioned feeds when
`EnableTopicAutopartitioningForReplication` is enabled. Such feeds must use fixed
partitioning before publication is possible. For new replications, disabling that
flag on the destination cluster before creation selects fixed partitioning.
Enabling the checksum flag does not change existing feed settings.

A successful response contains `checkpoint_id`, `snapshot`, `checksum`, and
`changefeed_names`. Success means the result is persisted in the source change
queues. Destination comparisons finish asynchronously. The checksum covers the
whole table, including when a feed emits only keys or changed columns.

## Records

A checkpoint has this native-JSON envelope:

```json
{
  "checksum_checkpoint": {
    "version": 2,
    "id": "checkpoint-id",
    "table": [42, 17],
    "snapshot": [999, 18446744073709551615],
    "commit_boundary": 1000,
    "schema": "canonical-column-description",
    "algorithm": "CityHash64",
    "algorithm_version": 1
  }
}
```

The terminal envelope is named `checksum_result`. It repeats the identity and
schema fields and adds a numeric YDB `status`, optional `issues` strings, and,
on success, `row_count` and `hash`. The hash is 16 uppercase hexadecimal digits.
An empty table has count zero and hash `0000000000000000`.

These are control records, with no row key or update. Consumers must recognize
them before parsing row data. The destination also recognizes and consumes
controls when comparisons are disabled.

## Ordering and snapshots

With heartbeat-enabled feeds, publication uses protocol version 2. The source
chooses a common grid from the least common multiple of the selected feeds'
positive heartbeat intervals. The coordinated arming transaction reserves the
next boundary `H` strictly after its plan step and the previous reserved boundary.
The source snapshot is `S = H.Prev()`, or `(H.Step - 1, UINT64_MAX)`.

Each shard retains S without advancing its write version. Retention alone does
not make a future snapshot readable: checksum scans use ordinary MVCC admission
and wait for preceding transactions. The arming transaction completes promptly;
heartbeat processing finishes publication independently of the RPC.

Records at or beyond H stay in the durable source change queue. Once all writes
below H have resolved, each shard queues the checkpoint before releasing those
records. Released records receive fresh, increasing producer sequence numbers,
preserving their original order. This also covers locked/volatile writes,
heartbeats, and a terminal failure that arrives before H. Restart reloads fences
and deferred records. A source split drains pending fences on its old producer
before capturing the handoff snapshot.

Checkpoints use the existing opaque PQ schema-barrier transport. A producer
cannot send past a checkpoint until PQ acknowledges its durable emission after
all producers arrive. Results use one current shard to broadcast to all topic
partitions and do not introduce another quorum barrier.

GLOBAL assignments cover exact half-open intervals `[begin, end)`. A bounded
window admits five ordinary intervals. Empty intervals can be skipped only when
all participants have proved sufficient progress. Existing merged assignments
are drained under their old routing before strict assignments begin.

Workers may scan beyond records outside the admitted window to find a heartbeat
or control record. They retain the earliest deferred source offset and the minimum
unapplied version separately: the former caps CDC acknowledgements, and the latter
caps reported resolved progress. When the window advances, they replay the
uncommitted prefix from CDC. Destination deduplication records individual applied
offsets above that prefix, so later offsets cannot hide deferred records. These
entries survive shard handoff and are retired as the processed prefix advances.
Prefix proofs travel with those deletions, so merging independently advanced
shards cannot make an already applied offset appear new. A deferred prefix also
retains checksum results for replay while the worker scans ahead to a heartbeat;
the result's older snapshot version cannot resolve that prefix.
Partition-end notifications wait until deferred records drain.

A drained checkpoint proves resolved progress H. The checked table's workers
park while the controller finishes normal all-table commits through H. Once all
participants have reached H and those commits have finished, it retains a fresh
destination snapshot D. It persists D and release state before resuming workers.
Checksum checks do not flush only the selected table, reset heartbeat progress,
or enter schema-change verification. GLOBAL atomic visibility is preserved.
An overlapping schema verification that would create a circular wait fails the
comparison; existing schema-change behavior is otherwise unchanged.

ROW workers already commit their prefix before parking. If no selected feed has
a positive heartbeat interval, publication retains the original protocol v1
coordinated checkpoint, without `commit_boundary`. GLOBAL comparisons require v2,
an aligned H, and drained legacy assignments; incompatible controls are consumed
with a failed comparison status.

Local checksum scans run at the retained D after workers resume. Results are
joined by checkpoint ID. Waiting for the source result never holds a worker at
the checkpoint. Replicated schema changes wait while a local scan depends on the
earlier schema.

## Failures and recovery

The source scan belongs to the RPC. It is not resumed after node loss. A
checkpoint without a terminal result can therefore occur. An armed fence still
finishes publication even if the RPC disappears; source snapshots expire, and the
destination terminates the attempt after one hour.
Concurrent source attempts for the table return `OVERLOADED`.

Destination membership, offsets, snapshot identity, local checksum, and join
state are durable. After releasing workers, recovery never substitutes a newer
snapshot for `D`. Missing snapshots or changed table identities/schemas fail the
attempt. Snapshots are discarded after local completion, independently of when
the source result arrives; expiration bounds retention if cleanup is interrupted.

DescribeReplication exposes a checksum status on each item. The checkpoint ID
correlates it with the publication response. An unfinished attempt has unspecified
status. A completed comparison has `SUCCESS`, with `last_result.matches` indicating
equality. A failed attempt preserves the last completed comparison. Late older
records do not overwrite the latest attempt's status.

The latest attempt and latest completed comparison advance independently: a
previous check may finish while a newer check is pending. Its comparison remains
available without replacing the newer attempt's status.

Publication counters expose active requests and failures. Replication controller
counters expose active attempts, targets whose retained comparison is a mismatch,
failure/timeout totals, and checkpoint barrier duration in milliseconds.

There is no periodic checksum scheduler, trigger SDK helper, or trigger CLI.
