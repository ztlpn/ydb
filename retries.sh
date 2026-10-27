#!/bin/bash

ydb/tools/datashard_read_load/datashard_read_load \
    --endpoint localhost:12415 --database /local/testdb --table /local/testdb/mailbox_load \
    run \
    --threads 4 \
    --long-inflight 40000 --short-inflight 4096 --probe-inflight 16 \
    --dispatch-batch 256 \
    --phase baseline:10s:100:100 \
    --phase burst:40s:32000:0 \
    --phase sustained:60s:8000:0 \
> retries.burst.jsonl
