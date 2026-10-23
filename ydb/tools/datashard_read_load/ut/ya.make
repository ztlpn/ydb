GTEST()

SRCDIR(ydb/tools/datashard_read_load)

SRCS(
    workload_ut.cpp
    workload.cpp
)

PEERDIR(
    library/cpp/getopt
)

END()
