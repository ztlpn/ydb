#include "cdc_checksum.h"

#include <ydb/library/table_digest/aggregate.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NChangeExchange {

bool IsCdcChecksum(const NJson::TJsonValue& json) {
    return json.Has("checksum_checkpoint") || json.Has("checksum_result");
}

NJson::TJsonValue SerializeCdcChecksum(const NKikimrChangeExchange::TCdcChecksum& record) {
    NJson::TJsonValue json;
    auto& value = json[record.GetResult() ? "checksum_result" : "checksum_checkpoint"];
    value["version"] = record.GetProtocolVersion();
    value["ts"].AppendValue(record.GetSnapshotStep());
    value["ts"].AppendValue(record.GetSnapshotTxId());
    value["algorithm"] = record.GetAlgorithm();
    value["algorithm_version"] = record.GetAlgorithmVersion();
    for (ui32 partition : record.GetExpectedPartitions()) {
        value["partitions"].AppendValue(partition);
    }
    if (record.GetResult()) {
        value["status"] = Ydb::StatusIds::StatusCode_Name(record.GetStatus());
        for (const auto& issue : record.GetIssues()) {
            value["issues"].AppendValue(issue.message());
        }
        if (record.GetStatus() == Ydb::StatusIds::SUCCESS) {
            value["row_count"] = record.GetChecksum().row_count();
            value["hash"] = record.GetChecksum().hash();
        }
    }
    return json;
}

bool ParseCdcChecksum(const NJson::TJsonValue& json,
        NKikimrChangeExchange::TCdcChecksum& record, TString& error) {
    record.Clear();
    const bool result = json.Has("checksum_result");
    const auto& value = json[result ? "checksum_result" : "checksum_checkpoint"];
    auto fail = [&error] {
        error = "Invalid or unsupported CDC checksum control record";
        return false;
    };
    if (!IsCdcChecksum(json) || (result && json.Has("checksum_checkpoint")) ||
        !value.IsMap() || !value["version"].IsUInteger() || value["version"].GetUInteger() != 1 ||
        !value["algorithm"].IsString() ||
        !value["algorithm_version"].IsUInteger() || value["algorithm_version"].GetUInteger() != 1)
    {
        return fail();
    }
    const auto& name = value["algorithm"].GetString();
    const auto algorithm = NYql::NDigest::TryParseAlgorithm(name);
    if (!algorithm) {
        return fail();
    }
    const auto& timestamp = value["ts"];
    if (!timestamp.IsArray() || timestamp.GetArray().size() != 2 || !timestamp[0].IsUInteger() || !timestamp[1].IsUInteger() ||
        !timestamp[0].GetUInteger() || !timestamp[1].GetUInteger())
    {
        return fail();
    }
    const auto& partitions = value["partitions"];
    if (!partitions.IsArray() || partitions.GetArray().empty()) {
        return fail();
    }
    for (const auto& partition : partitions.GetArray()) {
        if (!partition.IsUInteger() || partition.GetUInteger() > Max<ui32>() ||
            (record.ExpectedPartitionsSize() &&
                partition.GetUInteger() <= record.GetExpectedPartitions(record.ExpectedPartitionsSize() - 1))) {
            return fail();
        }
        record.AddExpectedPartitions(partition.GetUInteger());
    }
    record.SetProtocolVersion(1);
    record.SetSnapshotStep(timestamp[0].GetUInteger());
    record.SetSnapshotTxId(timestamp[1].GetUInteger());
    record.SetAlgorithm(name);
    record.SetAlgorithmVersion(1);
    record.SetResult(result);
    if (result) {
        Ydb::StatusIds::StatusCode status;
        if (!value["status"].IsString() || !Ydb::StatusIds::StatusCode_Parse(value["status"].GetString(), &status) ||
            status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED)
        {
            return fail();
        }
        record.SetStatus(status);
        if (value.Has("issues")) {
            if (!value["issues"].IsArray()) {
                return fail();
            }
            for (const auto& issue : value["issues"].GetArray()) {
                if (!issue.IsString()) {
                    return fail();
                }
                record.AddIssues()->set_message(issue.GetString());
            }
        }
        if (record.GetStatus() == Ydb::StatusIds::SUCCESS) {
            if (!value["row_count"].IsUInteger() || !value["hash"].IsString()) {
                return fail();
            }
            const auto& hash = value["hash"].GetString();
            if (!NYql::NDigest::IsValidHexDigest(*algorithm, hash)) {
                return fail();
            }
            record.MutableChecksum()->set_row_count(value["row_count"].GetUInteger());
            record.MutableChecksum()->set_hash(hash);
        }
    }
    return true;
}

}
