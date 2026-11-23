#pragma once

#include "checksum.h"

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr::NReplication::NChecksum {

TString DescribeSchema(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, TString& error);
IActor* ProposeChecksum(const TActorId& parent, const TSettings& settings, NKikimrChangeExchange::TCdcChecksum checksum);

}
