#!/bin/bash

set -x

#export OLAP_YDB_OAUTH=mytoken
export TPCC_TIME_MINUTES=2
export TPCC_WAREHOUSES=12000
#export NO_KUBER_LOGS=1

./ya make -r -tA --test-tag ya:manual ydb/tests/olap/load \
-F '*TestTpccUniversalT0Snapshot*' \
--test-disable-timeout \
--allure=./allure_tpcc \
--test-param ydb-endpoint=grpc://vla5-7660.search.yandex.net:2135 \
--test-param ydb-db=/Root/db1 \
--test-param ydb-cli=/home/ztlpn/y/ydb/ydb/apps/ydb/ydb \
--test-param tables-path=tests \
--test-param client-host=vla5-7765.search.yandex.net \
--test-param tpcc-compaction-mode=sdk