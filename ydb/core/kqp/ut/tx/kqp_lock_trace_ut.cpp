#include "kqp_sink_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TString FormatLock(const NKikimrDataEvents::TLock& lock) {
    return TStringBuilder()
        << "TLock{"
        << "LockId=" << lock.GetLockId()
        << " DataShard=" << lock.GetDataShard()
        << " Generation=" << lock.GetGeneration()
        << " Counter=" << lock.GetCounter()
        << " SchemeShard=" << lock.GetSchemeShard()
        << " PathId=" << lock.GetPathId()
        << " HasWrites=" << lock.GetHasWrites()
        << "}";
}

TString FormatLocksOp(NKikimrDataEvents::TKqpLocks::ELocksOp op) {
    switch (op) {
        case NKikimrDataEvents::TKqpLocks::Commit:   return "Commit";
        case NKikimrDataEvents::TKqpLocks::Rollback: return "Rollback";
        default:                                      return "Unspecified";
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpLockTrace) {

Y_UNIT_TEST(UpdateWhereTraceLocks) {
    auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableReadCommittedIsolation(true);

    TKikimrRunner kikimr(settings);
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    auto client = kikimr.GetQueryClient();

    // Create a table split into two shards at Key=2, then populate it
    kikimr.RunCall([&] {
        auto result = client.ExecuteQuery(R"(
            CREATE TABLE `/Root/Test` (
                Key Uint32 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            ) WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_BY_LOAD = DISABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2,
                PARTITION_AT_KEYS = (2)
            );
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        return true;
    });

    kikimr.RunCall([&] {
        auto result = client.ExecuteQuery(R"(
            REPLACE INTO `/Root/Test` (Key, Value) VALUES
                (1u, "target"),
                (2u, "other"),
                (3u, "target");
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        return true;
    });

    auto traceObserver = runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
        if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
            auto* msg = ev->Get<TEvDataShard::TEvRead>();
            const auto& record = msg->Record;
            Cerr << ">>> TEvRead"
                << " LockTxId=" << record.GetLockTxId()
                << " Snapshot=" << record.GetSnapshot()
                << Endl;
        } else if (ev->GetTypeRewrite() == TEvDataShard::TEvReadResult::EventType) {
            auto* msg = ev->Get<TEvDataShard::TEvReadResult>();
            const auto& record = msg->Record;
            Cerr << "<<< TEvReadResult"
                 << " Status=" << record.GetStatus().GetCode()
                 << " RowCount=" << record.GetRowCount()
                 << Endl;
            for (const auto& lock : record.GetTxLocks()) {
                Cerr << "    " << FormatLock(lock) << Endl;
            }
            for (const auto& lock : record.GetBrokenTxLocks()) {
                Cerr << "    broken: " << FormatLock(lock) << Endl;
            }
        } else if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvLockRows::EventType) {
            auto* msg = ev->Get<NEvents::TDataEvents::TEvLockRows>();
            const auto& record = msg->Record;
            Cerr << ">>> TEvLockRows"
                << " LockId=" << record.GetLockId()
                << " Snapshot=" << record.GetSnapshot()
                << " SkipAbsent=" << record.GetSkipAbsent()
                << Endl;
        } else if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvLockRowsResult::EventType) {
            auto* msg = ev->Get<NEvents::TDataEvents::TEvLockRowsResult>();
            const auto& record = msg->Record;
            Cerr << "<<< TEvLockRowsResult"
                 << " Status=" << NKikimrDataEvents::TEvLockRowsResult::EStatus_Name(record.GetStatus())
                 << " LockedKeys.size()=" << record.GetLockedKeys().size()
                 << " SkippedAbsentKeys.size()=" << record.GetSkippedAbsentKeys().size()
                 << " ModifiedKeys.size()=" << record.GetModifiedKeys().size()
                 << Endl;
            for (const auto& lock : record.GetLocks()) {
                Cerr << "    " << FormatLock(lock) << Endl;
            }
        } else if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType) {
            auto* msg = ev->Get<NEvents::TDataEvents::TEvWrite>();
            const auto& record = msg->Record;
            const auto& kqpLocks = record.GetLocks();
            Cerr << ">>> TEvWrite"
                << " TxId=" << record.GetTxId()
                << " LockTxId=" << record.GetLockTxId()
                << " LocksOp=" << FormatLocksOp(kqpLocks.GetOp())
                << Endl;
            for (const auto& lock : kqpLocks.GetLocks()) {
                Cerr << "    " << FormatLock(lock) << Endl;
            }
        } else if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
            auto* msg = ev->Get<NEvents::TDataEvents::TEvWriteResult>();
            const auto& record = msg->Record;
            Cerr << "<<< TEvWriteResult"
                 << " Status=" << NKikimrDataEvents::TEvWriteResult::EStatus_Name(record.GetStatus())
                 << " TxId=" << record.GetTxId()
                 << Endl;
            for (const auto& lock : record.GetTxLocks()) {
                Cerr << "    " << FormatLock(lock) << Endl;
            }
        }
    });

    auto result = kikimr.RunCall([&] {
        return client.ExecuteQuery(R"(
            UPDATE `/Root/Test` SET Value = "updated" WHERE Value = "target";
        )", TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).GetValueSync();
    });
    UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
}

} // Y_UNIT_TEST_SUITE

} // namespace NKqp
} // namespace NKikimr
