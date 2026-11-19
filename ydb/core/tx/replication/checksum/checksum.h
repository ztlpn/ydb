#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NReplication::NChecksum {

struct TEvResult: TEventLocal<TEvResult, EventSpaceBegin(TKikimrEvents::ES_REPLICATION_CONTROLLER) + 100> {
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    NYql::TIssues Issues;
    Ydb::Replication::TableChecksum Checksum;
};

struct TEvSnapshot: TEventLocal<TEvSnapshot, EventSpaceBegin(TKikimrEvents::ES_REPLICATION_CONTROLLER) + 101> {
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    TRowVersion Snapshot;
};

struct TSettings {
    TString Database;
    TString Path;
    TTableId TableId;
    TString Schema;
    TRowVersion Snapshot;
    TInstant Deadline;
    TMaybe<TString> UserToken;
    ui32 MaxShardsInFlight = 0;
};

TString DescribeSchema(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, TString& error);
IActor* CreateChecksum(const TActorId& parent, TSettings settings);
IActor* CreateSnapshot(const TActorId& parent, TString database, TString path, TInstant deadline);
void DiscardSnapshot(const TActorContext& ctx, const TString& database, const TString& path, TRowVersion version);

}
