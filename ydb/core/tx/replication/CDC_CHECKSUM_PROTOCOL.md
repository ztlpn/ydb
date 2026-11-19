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
    "version": 1,
    "id": "checkpoint-id",
    "table": [42, 17],
    "snapshot": [100, 20],
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

Each DataShard atomically retains snapshot `S` and queues a checkpoint in a
coordinated scheme transaction. Its dependencies include preceding volatile
transactions. Every topic partition receives the checkpoint after all changes
through `S`, before later changes from any producer.

Checkpoints use the existing opaque PQ schema-barrier transport. A producer
cannot send past a checkpoint until PQ acknowledges its durable emission after
all producers arrive. Each coordinated operation has a distinct `(step, txId)`;
the payload carries its checkpoint ID. This sender invariant prevents the
watermark-based transport from coalescing consecutive checkpoints. Source shard
handoff drains the old producer before registering its successors.

The terminal operation chooses one current DataShard to broadcast the result
to every partition. Ordinary producer sequence-number deduplication handles
retries. Results do not introduce another PQ quorum barrier.

Destination workers use the durable control-record handshake also used for
schema changes. They drain the data preceding a checkpoint and park. GLOBAL
replication flushes staged writes for the selected destination table, retaining
the globally assigned transaction IDs for the subsequent ordinary commit. ROW
replication has already committed its prefix.

Later records can still carry a GLOBAL interval's already committed transaction
ID. They must receive a fresh local MVCC version rather than inherit that ID's
earlier commit version, which would change the retained checksum snapshot.

While workers are parked, the controller creates a fresh retained snapshot `D`.
It persists `D` and the worker release state together. Workers then resume while
the local checksum runs at `D`. Source and destination hashes are joined by
checkpoint ID. Waiting for a source result never holds workers at the checkpoint.
Replicated schema changes wait while a local scan depends on the earlier schema.

GLOBAL target flushing can temporarily expose changes to this table before
changes to other tables from the same source transaction. This is the accepted
table-scoped consistency relaxation for a manual check.

## Failures and recovery

The source scan belongs to the RPC. It is not resumed after node loss. A
checkpoint without a terminal result can therefore occur; source reservations
and snapshots expire, and the destination terminates the attempt after one hour.
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
