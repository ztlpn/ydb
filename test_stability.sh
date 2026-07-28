#!/bin/bash

set -x

# When both paths are provided, cluster_path is the cluster YAML
# (config.hosts, config.bridge_config) and yaml_config is the
# database template (domains).  The nemesis CLI expects:
#   --yaml-config-location  → cluster YAML (cluster_path / config.yaml)
#   --database-config-location → database template (yaml_config / databases.yaml)

ya make -r -tA --test-tag ya:manual ydb/tests/stability/tests \
-F test_per_workload.py::TestPerWorkload::test_stress_util[Tpcc-nemesis_true] \
--test-disable-timeout \
--allure=./allure_nemesis \
--test-param workload_duration=10 \
--test-param ydb-endpoint=grpc://vla5-7660.search.yandex.net:2135 \
--test-param ydb-db=/Root/db1 \
--test-param cluster_path=/home/ztlpn/y/run/slice2/stat_config.yaml \
--test-param yaml-config=/home/ztlpn/y/run/slice2/databases.yaml \
--test-param nemesis-static-location=/home/ztlpn/y/ydb/ydb/tests/stability/nemesis/static