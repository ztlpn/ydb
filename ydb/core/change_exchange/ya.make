LIBRARY()

SRCS(
    cdc_checksum.cpp
    change_exchange.cpp
    change_record.cpp
    change_sender.cpp
    change_sender_monitoring.cpp
    resolve_partition.cpp
    util.cpp
)

GENERATE_ENUM_SERIALIZATION(change_record.h)

PEERDIR(
    library/cpp/json
    ydb/core/protos
    ydb/core/base
    ydb/core/scheme
    ydb/library/actors/core
    ydb/library/table_digest
    library/cpp/monlib/service/pages
)

END()
