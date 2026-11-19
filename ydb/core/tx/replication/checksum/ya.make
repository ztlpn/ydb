LIBRARY()

SRCS(checksum.cpp)

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/core/tx/scheme_cache
    ydb/core/tx/tx_proxy
    ydb/library/query_actor
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/result
)

END()
