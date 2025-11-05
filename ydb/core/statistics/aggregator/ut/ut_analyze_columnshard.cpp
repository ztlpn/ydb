#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/library/query_actor/query_actor.h>

namespace NKikimr {
namespace NStat {

namespace {

TTableInfo PrepareDatabaseAndTable(TTestEnv& env) {
    CreateDatabase(env, "Database");
    return PrepareColumnTable(env, "Database", "Table", 1);
}

void PrintEvent(const IEventHandle& ev) {
    Cerr << "FFF EV from:" << ev.Sender << " to:" << ev.Recipient
        << " t:" << ev.Type
        << " tn:" << ev.GetTypeName()
        << " tr:" << ev.GetTypeRewrite()
        << " str: " << ev.ToString() << Endl;
}

} // namespace

Y_UNIT_TEST_SUITE(AnalyzeColumnshard) {
    Y_UNIT_TEST(ManualScan) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        // CreateUniformTable(env, "Database", "Table");
        CreateColumnTable(env, "Database", "Table", 4);
        InsertDataIntoTable(env, "Database", "Table", RowsWithFewDistinctValues(1000));

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);
        Y_UNUSED(pathId);

        auto shards = GetColumnTableShards(runtime, runtime.AllocateEdgeActor(), "/Root/Database/Table");
        for (auto s : shards) {
            Cerr << "FFF " << s << Endl;
        }

        // TActorId scanActorId;
        // TActorId scanFetcherId;
        // auto observer1 = runtime.AddObserver<NKqp::TEvKqpCompute::TEvScanInitActor>([&](auto& ev) {
        //     PrintEvent(*ev);
        //     scanActorId = ActorIdFromProto(ev->Get()->Record.GetScanActorId());
        //     scanFetcherId = ev->Recipient;
        // });

        // auto observer2 = runtime.AddObserver<NKqp::TEvKqpCompute::TEvScanData>(
        //     [&](NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
        //     PrintEvent(*ev);
        //     Cerr << "FFF DATA nr:" << ev->Get()->GetRowsCount() << Endl;
        // });

        // auto observer3 = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
        //     if (ev->Sender == scanFetcherId && ev->Recipient == scanActorId) {
        //         PrintEvent(*ev);
        //     }
        // });

        auto observer4 = runtime.AddObserver<TEvDataShard::TEvKqpScan>(
            [&](TEvDataShard::TEvKqpScan::TPtr& ev) {
            PrintEvent(*ev);
        });

        // {
        //     runtime.SimulateSleep(TDuration::Seconds(1));

        //     int nodeIdx = 1;

        //     TString sessionId;
        //     {
        //         using TEvCreateSessionRequest = NGRpcService::TGrpcRequestOperationCall<
        //             Ydb::Table::CreateSessionRequest,
        //             Ydb::Table::CreateSessionResponse>;
        //         Ydb::Table::CreateSessionRequest request;
        //         // request.mutable_operation_params()
        //         auto future = NRpcService::DoLocalRpc<TEvCreateSessionRequest>(
        //             std::move(request), "", "", runtime.GetActorSystem(nodeIdx));
        //         auto response = runtime.WaitFuture(std::move(future));
        //         UNIT_ASSERT(response.operation().ready());
        //         UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

        //         Ydb::Table::CreateSessionResult result;
        //         response.operation().result().UnpackTo(&result);
        //         sessionId = result.session_id();
        //     }

        //     using TEvRequest = NGRpcService::TGrpcRequestOperationCall<
        //         Ydb::Table::ExecuteDataQueryRequest,
        //         Ydb::Table::ExecuteDataQueryResponse>;

        //     Ydb::Table::ExecuteDataQueryRequest request;
        //     request.set_session_id(sessionId);
        //     request.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();
        //     request.mutable_tx_control()->set_commit_tx(true);
        //     request.mutable_query()->set_yql_text("SELECT count(*) FROM `/Root/Database/Table`");

        //     auto future = NRpcService::DoLocalRpc<TEvRequest>(
        //         std::move(request), "", "", runtime.GetActorSystem(nodeIdx));
        //     auto response = runtime.WaitFuture(std::move(future));

        //     UNIT_ASSERT(response.operation().ready());
        //     for (const auto& issue : response.operation().issues()) {
        //         Cerr << "FFF ISSUE " << issue << Endl;
        //     }

        //     UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        // }

        {
            runtime.SimulateSleep(TDuration::Seconds(1));

            int nodeIdx = 1;
            auto sender = runtime.AllocateEdgeActor(nodeIdx);

            class TScanActor : public TQueryBase {
                TActorId Parent;
            public:
                TScanActor(TActorId parent)
                : TQueryBase(NKikimrServices::STATISTICS)
                , Parent(parent) {}

                void OnRunQuery() override {
                    RunStreamQuery(R"(
                        $cms_factory = AggregationFactory(
                            "UDAF",
                            ($item, $parent) -> { return StatisticsInternal::CountMinSketchCreate($item) },
                            ($state, $item, $parent) -> { return StatisticsInternal::CountMinSketchAddValue($state, $item) },
                            StatisticsInternal::CountMinSketchMerge,
                            StatisticsInternal::CountMinSketchSerialize,
                            StatisticsInternal::CountMinSketchSerialize,
                            StatisticsInternal::CountMinSketchDeserialize,
                            StatisticsInternal::CountMinSketchDefault(),
                        );

                        SELECT AGGREGATE_BY(Value, $cms_factory) from `/Root/Database/Table`;
                    )");
                }

                void OnStreamResult(NYdb::TResultSet&& resultSet) override {
                    NYdb::TResultSetParser result(std::move(resultSet));
                    UNIT_ASSERT(result.TryNextRow());
                    const auto& cmsData = NYdb::TValueParser(result.GetValue(0)).GetBytes();
                    std::unique_ptr<TCountMinSketch> cms(
                        TCountMinSketch::FromString(cmsData.data(), cmsData.size()));
                    Cerr << "FFF RES count:" << cms->GetElementCount() << Endl;
                    Finish();
                }

                void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
                    Cerr << "FFF STATUS " << status << Endl;
                    for (const auto& issue : issues) {
                        Cerr << "FFF ISSUE " << issue << Endl;
                    }

                    UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::SUCCESS);
                    Send(Parent, new TEvStatistics::TEvAnalyzeResponse());
                }
            };

            runtime.Register(new TScanActor(sender), nodeIdx);

            size_t scanCount = 0;
            std::optional<TActorId> blockedActor;
            TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime,
                [&](const TEvDataShard::TEvKqpScan::TPtr& ev) {
                if (ev->Get()->Record.GetLocalPathId() != pathId.LocalPathId) {
                    return false;
                }

                ++scanCount;
                if (!blockedActor) {
                    blockedActor = ev->Recipient;
                    return true;
                } else if (ev->Recipient == blockedActor) {
                    return true;
                } else {
                    return false;
                }
            });
            runtime.WaitFor("scans sent", [&]{ return scanCount >= 4; });

            RebootTablet(runtime, shards[0], runtime.AllocateEdgeActor());
            Cerr << "FFF tablet " << shards[0] << " rebooted" << Endl;
            runtime.SimulateSleep(TDuration::Seconds(1));

            block.Unblock();
            block.Stop();

            runtime.GrabEdgeEvent<TEvStatistics::TEvAnalyzeResponse>();
        }

        return;

        // Acquire read snapshot
        NKikimrKqp::TKqpSnapshot snapshot;
        {
            auto sender = runtime.AllocateEdgeActor(1);
            runtime.Send(
                NLongTxService::MakeLongTxServiceID(runtime.GetNodeId(1)), sender,
                new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(),
                1);
            auto ev = runtime.GrabEdgeEventRethrow<
                NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>(sender);
            const auto& record = ev->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), Ydb::StatusIds::SUCCESS);
            snapshot.SetStep(record.GetSnapshotStep());
            snapshot.SetTxId(record.GetSnapshotTxId());
            Cerr << "FFF SNAPSHOT " << snapshot.AsJSON() << Endl;
        }

        ui64 shardId = shards[0];
        {
            auto evScan = std::make_unique<TEvDataShard::TEvKqpScan>();
            auto& record = evScan->Record;
            record.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);
            record.MutableSnapshot()->CopyFrom(snapshot);
            record.SetLocalPathId(pathId.LocalPathId);
            record.AddColumnTags(2);

            auto sender = runtime.AllocateEdgeActor(1);
            runtime.SendToPipe(shardId, sender, evScan.release(), 1);

            TAutoPtr<IEventHandle> handle;
            auto* initEv = runtime.GrabEdgeEvent<NKqp::TEvKqpCompute::TEvScanInitActor>(
                handle);
            PrintEvent(*handle);

            auto scanActorId = ActorIdFromProto(initEv->Record.GetScanActorId());
            while (true) {
                ui32 resultLimit = 1024 * 1024;
                runtime.Send(scanActorId, sender,
                    new NKqp::TEvKqpCompute::TEvScanDataAck(resultLimit, 0, 1),
                    1);

                auto* scan = runtime.GrabEdgeEvent<NKqp::TEvKqpCompute::TEvScanData>(handle);
                PrintEvent(*handle);
                if (scan->Finished) {
                    UNIT_ASSERT(!scan->ArrowBatch || !scan->ArrowBatch->num_rows());
                    break;
                }
                UNIT_ASSERT(scan->ArrowBatch);
                auto batch = NArrow::ToBatch(scan->ArrowBatch);
                Cerr << "FFF BATCH rs:" << batch->num_rows() << Endl;
            }

            runtime.SimulateSleep(TDuration::Seconds(5));
        }
    }

    Y_UNIT_TEST(AnalyzeShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);

        AnalyzeShard(runtime, tableInfo.ShardIds[0], tableInfo.PathId);
    }

    Y_UNIT_TEST(Analyze) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});
    }

    Y_UNIT_TEST(AnalyzeEmptyTable) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = CreateColumnTable(env, "Database", "Table", 4);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});
    }

    Y_UNIT_TEST(AnalyzeServerless) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Database", "/Root/Shared");
        auto tableInfo = PrepareColumnTable(env, "Database", "Table", 1);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId}, "operationId", "/Root/Database");
    }

    Y_UNIT_TEST(AnalyzeAnalyzeOneColumnTableSpecificColumns) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);

        Analyze(runtime, tableInfo.SaTabletId, {{tableInfo.PathId, {1, 2}}});
    }

    Y_UNIT_TEST(AnalyzeTwoColumnTables) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto table1 = PrepareColumnTable(env, "Database", "Table1", 1);
        const auto table2 = PrepareColumnTable(env, "Database", "Table2", 1);

        Analyze(runtime, table1.SaTabletId, {table1.PathId, table2.PathId});
    }

    Y_UNIT_TEST(AnalyzeStatus) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);
        const auto tableInfo = PrepareDatabaseAndTable(env);

        const TString operationId = "operationId";
        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);

        auto analyzeRequest = MakeAnalyzeRequest({{tableInfo.PathId, {1, 2}}}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_IN_PROGRESS);

        // Check EvRemoteHttpInfo
        {
            auto httpRequest = std::make_unique<NActors::NMon::TEvRemoteHttpInfo>("/app?");
            runtime.SendToPipe(tableInfo.SaTabletId, sender, httpRequest.release(), 0, {});
            auto httpResponse = runtime.GrabEdgeEventRethrow<NActors::NMon::TEvRemoteHttpInfoRes>(sender);
            TString body = httpResponse->Get()->Html;
            Cerr << body << Endl;
            UNIT_ASSERT(body.size() > 500);
            UNIT_ASSERT(body.Contains("ForceTraversals: 1"));
        }

        block.Unblock();
        block.Stop();

        auto analyzeResonse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(analyzeResonse->Get()->Record.GetOperationId(), operationId);

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);
    }    

    Y_UNIT_TEST(AnalyzeSameOperationId) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();
        const TString operationId = "operationId";

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto tabletPipe = runtime.ConnectToPipe(tableInfo.SaTabletId, sender, 0, {});

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tabletPipe, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tabletPipe, sender, analyzeRequest2.release());

        block.Unblock();
        block.Stop();
        
        auto response1 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT(response1);
        UNIT_ASSERT_VALUES_EQUAL(response1->Get()->Record.GetOperationId(), operationId);

        auto response2 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender, TDuration::Seconds(5));
        UNIT_ASSERT(!response2);
    }

    Y_UNIT_TEST(AnalyzeMultiOperationId) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();

        auto GetOperationId = [] (size_t i) { return TStringBuilder() << "operationId" << i; };

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        const size_t numEvents = 10;

        auto tabletPipe = runtime.ConnectToPipe(tableInfo.SaTabletId, sender, 0, {});

        for (size_t i = 0; i < numEvents; ++i) {
            auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId}, GetOperationId(i));
            runtime.SendToPipe(tabletPipe, sender, analyzeRequest.release());
        }

        block.Unblock();
        block.Stop();

        for (size_t i = 0; i < numEvents; ++i) {
            auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetOperationId(), GetOperationId(i));
        }        
    }    

    Y_UNIT_TEST(AnalyzeRebootSa) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvDataShard::TEvKqpScan>([&](auto& ev) {
            eventSeen = true;
            ev.Reset();
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return eventSeen; });
        observer.Remove();
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }


    Y_UNIT_TEST(AnalyzeRebootColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });
        RebootTablet(runtime, tableInfo.ShardIds[0], sender);
        block.Unblock();
        block.Stop();

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }

    Y_UNIT_TEST(AnalyzeDeadline) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });
        runtime.AdvanceCurrentTime(TDuration::Days(2));

        auto analyzeResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        const auto& record = analyzeResponse->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), "operationId");
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
    }    
}

} // NStat
} // NKikimr
