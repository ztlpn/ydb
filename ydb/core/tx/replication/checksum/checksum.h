#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <util/generic/hash_set.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NReplication::NChecksum {

struct TEvResult: TEventLocal<TEvResult, EventSpaceBegin(TKikimrEvents::ES_REPLICATION_CONTROLLER) + 100> {
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    NYql::TIssues Issues;
    Ydb::Replication::TableChecksum Checksum;
    TRowVersion Snapshot = TRowVersion::Min();
};

struct TEvSnapshot: TEventLocal<TEvSnapshot, EventSpaceBegin(TKikimrEvents::ES_REPLICATION_CONTROLLER) + 101> {
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    TRowVersion Snapshot = TRowVersion::Min();
};

struct TSettings {
    TString Database;
    TString Path;
    TTableId TableId;
    TString Schema;
    TRowVersion Snapshot = TRowVersion::Min();
    TInstant Deadline;
    TMaybe<TString> UserToken;
    TString Algorithm = "CityHash64";
};

IActor* CreateChecksum(const TActorId& parent, TSettings settings);
IActor* PublishChecksum(const TActorId& parent, TSettings settings, THashSet<TString> changefeeds);
IActor* CreateSnapshot(const TActorId& parent, TString database, TString path, TInstant deadline);
void DiscardSnapshot(const TActorContext& ctx, const TString& database, const TString& path, TRowVersion version);

}
