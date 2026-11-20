#include "schema.h"
#include "sys_params.h"

namespace NKikimr::NReplication::NController {

TSysParams::TSysParams() {
    Reset();
}

void TSysParams::Reset() {
    NextReplicationId = 1;
    GridAssignments = false;
}

void TSysParams::Load(ESysParam type, ISysParamLoader* loader) {
    switch (type) {
    case ESysParam::NextReplicationId:
        NextReplicationId = loader->LoadInt();
        break;
    case ESysParam::GridAssignments:
        GridAssignments = loader->LoadInt();
        break;
    default:
        break; // ignore
    }
}

void TSysParams::Load(ui32 type, ISysParamLoader* loader) {
    Load(ESysParam(type), loader);
}

void TSysParams::EnableGridAssignments(NIceDb::TNiceDb& db) {
    GridAssignments = true;
    db.Table<TControllerSchema::SysParams>()
        .Key(ui32(ESysParam::GridAssignments))
        .Update(NIceDb::TUpdate<TControllerSchema::SysParams::IntValue>(1));
}

ui64 TSysParams::AllocateReplicationId(NIceDb::TNiceDb& db) {
    using Schema = TControllerSchema;
    const auto result = NextReplicationId++;

    db.Table<Schema::SysParams>().Key(ui32(ESysParam::NextReplicationId)).Update(
        NIceDb::TUpdate<Schema::SysParams::IntValue>(NextReplicationId)
    );

    return result;
}

}
