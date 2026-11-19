#include "cdc_checksum.h"

#include <ydb/core/scheme/protos/pathid.pb.h>
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
    value["id"] = record.GetCheckpointId();
    value["table"].AppendValue(record.GetTableId().GetOwnerId());
    value["table"].AppendValue(record.GetTableId().GetLocalId());
    value["snapshot"].AppendValue(record.GetSnapshotStep());
    value["snapshot"].AppendValue(record.GetSnapshotTxId());
    value["schema"] = record.GetSchema();
    value["algorithm"] = record.GetAlgorithm();
    value["algorithm_version"] = record.GetAlgorithmVersion();
    if (record.GetResult()) {
        value["status"] = static_cast<ui32>(record.GetStatus());
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
        !value["id"].IsString() || value["id"].GetString().empty() ||
        !value["schema"].IsString() ||
        !value["algorithm"].IsString() || value["algorithm"].GetString() != "CityHash64" ||
        !value["algorithm_version"].IsUInteger() || value["algorithm_version"].GetUInteger() != 1)
    {
        return fail();
    }
    for (const auto name : {"table", "snapshot"}) {
        const auto& pair = value[name];
        if (!pair.IsArray() || pair.GetArray().size() != 2 ||
            !pair[0].IsUInteger() || !pair[1].IsUInteger() ||
            !pair[0].GetUInteger() || !pair[1].GetUInteger())
        {
            return fail();
        }
    }
    record.SetProtocolVersion(1);
    record.SetCheckpointId(value["id"].GetString());
    record.MutableTableId()->SetOwnerId(value["table"][0].GetUInteger());
    record.MutableTableId()->SetLocalId(value["table"][1].GetUInteger());
    record.SetSnapshotStep(value["snapshot"][0].GetUInteger());
    record.SetSnapshotTxId(value["snapshot"][1].GetUInteger());
    record.SetSchema(value["schema"].GetString());
    record.SetAlgorithm("CityHash64");
    record.SetAlgorithmVersion(1);
    record.SetResult(result);
    if (result) {
        if (!value["status"].IsUInteger() ||
            value["status"].GetUInteger() > static_cast<ui64>(Max<int>()) ||
            value["status"].GetUInteger() == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED ||
            !Ydb::StatusIds::StatusCode_IsValid(value["status"].GetUInteger()))
        {
            return fail();
        }
        record.SetStatus(static_cast<Ydb::StatusIds::StatusCode>(value["status"].GetUInteger()));
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
            if (hash.size() != 16 || hash.find_first_not_of("0123456789ABCDEF") != TString::npos) {
                return fail();
            }
            record.MutableChecksum()->set_row_count(value["row_count"].GetUInteger());
            record.MutableChecksum()->set_hash(hash);
        }
    }
    return true;
}

}
