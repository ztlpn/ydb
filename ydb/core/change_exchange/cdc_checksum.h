#pragma once

#include <ydb/core/protos/change_exchange.pb.h>
#include <library/cpp/json/json_value.h>

namespace NKikimr::NChangeExchange {

// A missing envelope is ordinary CDC data. A present but invalid envelope is an
// error, including when checksum execution is disabled on the consumer.
bool IsCdcChecksum(const NJson::TJsonValue& json);
NJson::TJsonValue SerializeCdcChecksum(const NKikimrChangeExchange::TCdcChecksum& record);
bool ParseCdcChecksum(const NJson::TJsonValue& json,
    NKikimrChangeExchange::TCdcChecksum& record, TString& error);

}
