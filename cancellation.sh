#!/bin/bash

ydb/tools/datashard_read_load/datashard_read_load run \
    --endpoint localhost:12415 \
    --database /local/testdb --table /local/testdb/mailbox_load \
    --threads 4 \
    --rows-per-read 256 \
    --long-inflight 0 --short-inflight 16384 --probe-inflight 16 \
    --cancel-after 20ms \
    --phase baseline:10s:0:100 \
    --phase burst:20s:0:20000 \
    --phase sustained:40s:0:8000 \
    --phase recovery:20s:0:100 \
    > cancellation.burst.jsonl
