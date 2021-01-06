#include "pg_gate_api.h"

#include "yb/common/ybc-internal.h"
#include "yb/common/ybc_util.h"
#include "yb/common/env.h"
#include "yb/pggate/pg_env.h"
#include "yb/pggate/pg_gate_defaults.h"
#include "yb/pggate/pg_gate_thread_local_vars.h"
#include "yb/pggate/pg_gate_impl.h"

namespace k2pg {
namespace gate {

using yb::Status;
using k2pg::sql::catalog::SqlCatalogManager;

namespace {
// Using a raw pointer here to fully control object initialization and destruction.
k2pg::gate::PgGateApiImpl* api_impl;
std::atomic<bool> api_impl_shutdown_done;

template<class T>
YBCStatus ExtractValueFromResult(const Result<T>& result, T* value) {
    if (result.ok()) {
        *value = *result;
        return YBCStatusOK();
    }
    return ToYBCStatus(result.status());
}

} // anonymous namespace

extern "C" {

void YBCInitPgGate(const YBCPgTypeEntity *YBCDataTypeTable, int count, PgCallbacks pg_callbacks) {
    K2ASSERT(api_impl == nullptr, "can only be called once");
    api_impl_shutdown_done.exchange(false);
    api_impl = new k2pg::gate::PgGateApiImpl(YBCDataTypeTable, count, pg_callbacks);
    //K2DEBUG("K2 PgGate open");
}

void YBCDestroyPgGate() {
    if (api_impl_shutdown_done.exchange(true)) {
        K2ERROR("should only be called once");
    } else {
        k2pg::gate::PgGateApiImpl* local_api_impl = api_impl;
        api_impl = nullptr; // YBCPgIsYugaByteEnabled() must return false from now on.
        delete local_api_impl;
    }
}

YBCStatus YBCPgCreateEnv(YBCPgEnv *pg_env) {
  //K2DEBUG("PgGateAPI: YBCPgCreateEnv");
  return ToYBCStatus(api_impl->CreateEnv(pg_env));
}

YBCStatus YBCPgDestroyEnv(YBCPgEnv pg_env) {
  //K2DEBUG("PgGateAPI: YBCPgDestroyEnv");
  return ToYBCStatus(api_impl->DestroyEnv(pg_env));
}

// Initialize a session to process statements that come from the same client connection.
YBCStatus YBCPgInitSession(const YBCPgEnv pg_env, const char *database_name) {
  //K2DEBUG("PgGateAPI: YBCPgInitSession " << database_name);
  const string db_name(database_name ? database_name : "");
  return ToYBCStatus(api_impl->InitSession(pg_env, db_name));
}

// Initialize YBCPgMemCtx.
// - Postgres uses memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
// - There YugaByte objects are bound to Postgres operations. All of these objects' allocated
//   memory will be held by YBCPgMemCtx, whose handle belongs to Postgres MemoryContext. Once all
//   Postgres operations are done, associated YugaByte memory context (YBCPgMemCtx) will be
//   destroyed toghether with Postgres memory context.
YBCPgMemctx YBCPgCreateMemctx() {
  //K2DEBUG("PgGateAPI: YBCPgCreateMemctx");
  return api_impl->CreateMemctx();
}

YBCStatus YBCPgDestroyMemctx(YBCPgMemctx memctx) {
  //K2DEBUG("PgGateAPI: YBCPgDestroyMemctx");
  return ToYBCStatus(api_impl->DestroyMemctx(memctx));
}

YBCStatus YBCPgResetMemctx(YBCPgMemctx memctx) {
  //K2DEBUG("PgGateAPI: YBCPgResetMemctx");
  return ToYBCStatus(api_impl->ResetMemctx(memctx));
}

// Invalidate the sessions table cache.
YBCStatus YBCPgInvalidateCache() {
  //K2DEBUG("PgGateAPI: YBCPgInvalidateCache");
  return ToYBCStatus(api_impl->InvalidateCache());
}

// Clear all values and expressions that were bound to the given statement.
YBCStatus YBCPgClearBinds(YBCPgStatement handle) {
  //K2DEBUG("PgGateAPI: YBCPgClearBind");
  return ToYBCStatus(api_impl->ClearBinds(handle));
}

// Check if initdb has been already run.
YBCStatus YBCPgIsInitDbDone(bool* initdb_done) {
  //K2DEBUG("PgGateAPI: YBCPgIsInitDbDone");
  return ExtractValueFromResult(api_impl->IsInitDbDone(), initdb_done);
}

// Sets catalog_version to the local tserver's catalog version stored in shared
// memory, or an error if the shared memory has not been initialized (e.g. in initdb).
YBCStatus YBCGetSharedCatalogVersion(uint64_t* catalog_version) {
  //K2DEBUG("PgGateAPI: YBCGetSharedCatalogVersion");
  return ExtractValueFromResult(api_impl->GetSharedCatalogVersion(), catalog_version);
}

//--------------------------------------------------------------------------------------------------
// DDL Statements
//--------------------------------------------------------------------------------------------------

// K2 InitPrimaryCluster
YBCStatus K2PGInitPrimaryCluster()
{
  //K2DEBUG("PgGateAPI: K2PGInitPrimaryCluster");
  return ToYBCStatus(api_impl->PGInitPrimaryCluster());
}

YBCStatus K2PGFinishInitDB()
{
  //K2DEBUG("PgGateAPI: K2PGFinishInitDB()");
  return ToYBCStatus(api_impl->PGFinishInitDB());
}

// DATABASE ----------------------------------------------------------------------------------------
// Connect database. Switch the connected database to the given "database_name".
YBCStatus YBCPgConnectDatabase(const char *database_name) {
  //K2DEBUG("PgGateAPI: YBCPgConnectDatabase " << database_name);
  return ToYBCStatus(api_impl->ConnectDatabase(database_name));
}

// Get whether the given database is colocated.
YBCStatus YBCPgIsDatabaseColocated(const YBCPgOid database_oid, bool *colocated) {
  //K2DEBUG("PgGateAPI: YBCPgIsDatabaseColocated");
  *colocated = false;
  return YBCStatusOK();
}

// Create database.
YBCStatus YBCPgNewCreateDatabase(const char *database_name,
                                 YBCPgOid database_oid,
                                 YBCPgOid source_database_oid,
                                 YBCPgOid next_oid,
                                 const bool colocated,
                                 YBCPgStatement *handle) {
  //K2DEBUG("PgGateAPI: YBCPgNewCreateDatabase " << database_name << ", " << database_oid << ", " << source_database_oid << ", " << next_oid);
  return ToYBCStatus(api_impl->NewCreateDatabase(database_name, database_oid, source_database_oid, next_oid, handle));
}

YBCStatus YBCPgExecCreateDatabase(YBCPgStatement handle) {
  //K2DEBUG("PgGateAPI: YBCPgExecCreateDatabase");
  return ToYBCStatus(api_impl->ExecCreateDatabase(handle));
}

// Drop database.
YBCStatus YBCPgNewDropDatabase(const char *database_name,
                               YBCPgOid database_oid,
                               YBCPgStatement *handle) {
  //K2DEBUG("PgGateAPI: YBCPgNewDropDatabase " << database_name << ", " << database_oid);
  return ToYBCStatus(api_impl->NewDropDatabase(database_name, database_oid, handle));
}

YBCStatus YBCPgExecDropDatabase(YBCPgStatement handle) {
  //K2DEBUG("PgGateAPI: YBCPgExecDropDatabase");
  return ToYBCStatus(api_impl->ExecDropDatabase(handle));
}

// Alter database.
YBCStatus YBCPgNewAlterDatabase(const char *database_name,
                               YBCPgOid database_oid,
                               YBCPgStatement *handle) {
  //K2DEBUG("PgGateAPI: YBCPgNewAlterDatabase " << database_name << ", " << database_oid);
  return ToYBCStatus(api_impl->NewAlterDatabase(database_name, database_oid, handle));
}

YBCStatus YBCPgAlterDatabaseRenameDatabase(YBCPgStatement handle, const char *new_name) {
  //K2DEBUG("PgGateAPI: YBCPgAlterDatabaseRenameDatabase " << new_name);
  return ToYBCStatus(api_impl->AlterDatabaseRenameDatabase(handle, new_name));
}

YBCStatus YBCPgExecAlterDatabase(YBCPgStatement handle) {
  //K2DEBUG("PgGateAPI: YBCPgExecAlterDatabase");
  return ToYBCStatus(api_impl->ExecAlterDatabase(handle));
}

// Reserve oids.
YBCStatus YBCPgReserveOids(YBCPgOid database_oid,
                           YBCPgOid next_oid,
                           uint32_t count,
                           YBCPgOid *begin_oid,
                           YBCPgOid *end_oid) {
  //K2DEBUG("PgGateAPI: YBCPgReserveOids " << database_oid << ", " << next_oid << ", " << count);
  return ToYBCStatus(api_impl->ReserveOids(database_oid, next_oid, count, begin_oid, end_oid));
}

YBCStatus YBCPgGetCatalogMasterVersion(uint64_t *version) {
  //K2DEBUG("PgGateAPI: YBCPgGetCatalogMasterVersion");
  return ToYBCStatus(api_impl->GetCatalogMasterVersion(version));
}

void YBCPgInvalidateTableCache(
    const YBCPgOid database_oid,
    const YBCPgOid table_oid) {
  const PgObjectId table_id(database_oid, table_oid);
  //K2DEBUG("PgGateAPI: YBCPgInvalidateTableCache" << database_oid << ", " << table_oid);
  api_impl->InvalidateTableCache(table_id);
}

YBCStatus YBCPgInvalidateTableCacheByTableId(const char *table_id) {
  //K2DEBUG("PgGateAPI: YBCPgInvalidateTableCacheByTableId " << table_id);
  if (table_id == NULL) {
    return ToYBCStatus(STATUS(InvalidArgument, "table_id is null"));
  }
  std::string table_id_str = table_id;
  const PgObjectId pg_object_id(table_id_str);
  api_impl->InvalidateTableCache(pg_object_id);
  return YBCStatusOK();
}

// Sequence Operations -----------------------------------------------------------------------------

YBCStatus YBCInsertSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called) {
  //K2DEBUG("PgGateAPI: YBCInsertSequenceTuple " << db_oid << ", " << seq_oid << ", " << ysql_catalog_version);
  return ToYBCStatus(api_impl->InsertSequenceTuple(db_oid, seq_oid, ysql_catalog_version, last_val, is_called));
}

YBCStatus YBCUpdateSequenceTupleConditionally(int64_t db_oid,
                                              int64_t seq_oid,
                                              uint64_t ysql_catalog_version,
                                              int64_t last_val,
                                              bool is_called,
                                              int64_t expected_last_val,
                                              bool expected_is_called,
                                              bool *skipped) {
  //K2DEBUG("PgGateAPI: YBCUpdateSequenceTupleConditionally " << db_oid << ", " << seq_oid << ", " << ysql_catalog_version);
  return ToYBCStatus(
      api_impl->UpdateSequenceTupleConditionally(db_oid, seq_oid, ysql_catalog_version,
          last_val, is_called, expected_last_val, expected_is_called, skipped));
}

YBCStatus YBCUpdateSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called,
                                 bool* skipped) {
  //K2DEBUG("PgGateAPI: YBCUpdateSequenceTuple " << db_oid << ", " << seq_oid << ", " << ysql_catalog_version);
  return ToYBCStatus(api_impl->UpdateSequenceTuple(
      db_oid, seq_oid, ysql_catalog_version, last_val, is_called, skipped));
}

YBCStatus YBCReadSequenceTuple(int64_t db_oid,
                               int64_t seq_oid,
                               uint64_t ysql_catalog_version,
                               int64_t *last_val,
                               bool *is_called) {
  //K2DEBUG("PgGateAPI: YBCReadSequenceTuple " << db_oid << ", " << seq_oid << ", " << ysql_catalog_version);
  return ToYBCStatus(api_impl->ReadSequenceTuple(
      db_oid, seq_oid, ysql_catalog_version, last_val, is_called));
}

YBCStatus YBCDeleteSequenceTuple(int64_t db_oid, int64_t seq_oid) {
  //K2DEBUG("PgGateAPI: YBCDeleteSequenceTuple " << db_oid << ", " << seq_oid);
  return ToYBCStatus(api_impl->DeleteSequenceTuple(db_oid, seq_oid));
}

// TABLE -------------------------------------------------------------------------------------------

// Create and drop table "database_name.schema_name.table_name()".
// - When "schema_name" is NULL, the table "database_name.table_name" is created.
// - When "database_name" is NULL, the table "connected_database_name.table_name" is created.
YBCStatus YBCPgNewCreateTable(const char *database_name,
                              const char *schema_name,
                              const char *table_name,
                              YBCPgOid database_oid,
                              YBCPgOid table_oid,
                              bool is_shared_table,
                              bool if_not_exist,
                              bool add_primary_key,
                              const bool colocated,
                              YBCPgStatement *handle) {
  //K2DEBUG("PgGateAPI: YBCPgNewCreateTable " << database_name << ", " << schema_name << ", " << table_name);
  const PgObjectId table_id(database_oid, table_oid);
  return ToYBCStatus(api_impl->NewCreateTable(
      database_name, schema_name, table_name, table_id, is_shared_table,
      if_not_exist, add_primary_key, handle));
}

YBCStatus YBCPgCreateTableAddColumn(YBCPgStatement handle, const char *attr_name, int attr_num,
                                    const YBCPgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first) {
  //K2DEBUG("PgGateAPI: YBCPgCreateTableAddColumn " << attr_name);
  return ToYBCStatus(api_impl->CreateTableAddColumn(handle, attr_name, attr_num, attr_type,
                                                 is_hash, is_range, is_desc, is_nulls_first));
}

YBCStatus YBCPgCreateTableSetNumTablets(YBCPgStatement handle, int32_t num_tablets) {
  //K2DEBUG("PgGateAPI: YBCPgCreateTableSetNumTablets");
  // no-op for setting up num_tablets
  // TODO: remove this api from PG
  return YBCStatusOK();
}

YBCStatus YBCPgCreateTableAddSplitRow(YBCPgStatement handle, int num_cols,
                                        YBCPgTypeEntity **types, uint64_t *data) {
  //K2DEBUG("PgGateAPI: YBCPgCreateTableAddSplitRow " << num_cols);
  return ToYBCStatus(api_impl->CreateTableAddSplitRow(handle, num_cols, types, data));
}

YBCStatus YBCPgExecCreateTable(YBCPgStatement handle) {
  //K2DEBUG("PgGateAPI: YBCPgExecCreateTable");
  return ToYBCStatus(api_impl->ExecCreateTable(handle));
}

YBCStatus YBCPgNewAlterTable(YBCPgOid database_oid,
                             YBCPgOid table_oid,
                             YBCPgStatement *handle){
  //K2DEBUG("PgGateAPI: YBCPgNewAlterTable " << database_oid << ", " << table_oid);
  const PgObjectId table_id(database_oid, table_oid);
  return ToYBCStatus(api_impl->NewAlterTable(table_id, handle));
}

YBCStatus YBCPgAlterTableAddColumn(YBCPgStatement handle, const char *name, int order,
                                   const YBCPgTypeEntity *attr_type, bool is_not_null){
  //K2DEBUG("PgGateAPI: YBCPgAlterTableAddColumn " << name);
  return ToYBCStatus(api_impl->AlterTableAddColumn(handle, name, order, attr_type, is_not_null));
}

YBCStatus YBCPgAlterTableRenameColumn(YBCPgStatement handle, const char *oldname,
                                      const char *newname){
  //K2DEBUG("PgGateAPI: YBCPgAlterTableRenameColumn " << oldname << ", " << newname);
  return ToYBCStatus(api_impl->AlterTableRenameColumn(handle, oldname, newname));
}

YBCStatus YBCPgAlterTableDropColumn(YBCPgStatement handle, const char *name){
  //K2DEBUG("PgGateAPI: YBCPgAlterTableDropColumn " << name);
  return ToYBCStatus(api_impl->AlterTableDropColumn(handle, name));
}

YBCStatus YBCPgAlterTableRenameTable(YBCPgStatement handle, const char *db_name,
                                     const char *newname){
  //K2DEBUG("PgGateAPI: YBCPgAlterTableRenameTable " << db_name << ", " << newname);
  return ToYBCStatus(api_impl->AlterTableRenameTable(handle, db_name, newname));
}

YBCStatus YBCPgExecAlterTable(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgExecAlterTable ");
  return ToYBCStatus(api_impl->ExecAlterTable(handle));
}

YBCStatus YBCPgNewDropTable(YBCPgOid database_oid,
                            YBCPgOid table_oid,
                            bool if_exist,
                            YBCPgStatement *handle) {
  //K2DEBUG("PgGateAPI: YBCPgNewDropTable " << database_oid << ", " << table_oid);
  const PgObjectId table_id(database_oid, table_oid);
  return ToYBCStatus(api_impl->NewDropTable(table_id, if_exist, handle));
}

YBCStatus YBCPgExecDropTable(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgExecDropTable");
  return ToYBCStatus(api_impl->ExecDropTable(handle));
}

YBCStatus YBCPgNewTruncateTable(YBCPgOid database_oid,
                                YBCPgOid table_oid,
                                YBCPgStatement *handle){
  //K2DEBUG("PgGateAPI: YBCPgNewTruncateTable " << database_oid << ", " << table_oid);
  return YBCStatusOK();
}

YBCStatus YBCPgExecTruncateTable(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgExecTruncateTable");
  return YBCStatusOK();
}

YBCStatus YBCPgGetTableDesc(YBCPgOid database_oid,
                            YBCPgOid table_oid,
                            YBCPgTableDesc *handle) {
  //K2DEBUG("PgGateAPI: YBCPgGetTableDesc " << database_oid << table_oid);
  const PgObjectId table_id(database_oid, table_oid);
  return ToYBCStatus(api_impl->GetTableDesc(table_id, handle));
}

YBCStatus YBCPgGetColumnInfo(YBCPgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary,
                             bool *is_hash) {
  //K2DEBUG("PgGateAPI: YBCPgGetTableDesc " << attr_number);
  return ToYBCStatus(api_impl->GetColumnInfo(table_desc, attr_number, is_primary, is_hash));
}

YBCStatus YBCPgGetTableProperties(YBCPgTableDesc table_desc,
                                  YBCPgTableProperties *properties){
  //K2DEBUG("PgGateAPI: YBCPgGetTableProperties");
  CHECK_NOTNULL(properties)->num_tablets = table_desc->num_hash_key_columns();
  properties->num_hash_key_columns = table_desc->num_hash_key_columns();
  properties->is_colocated = false;
  return YBCStatusOK();
}

YBCStatus YBCPgDmlModifiesRow(YBCPgStatement handle, bool *modifies_row){
  //K2DEBUG("PgGateAPI: YBCPgDmlModifiesRow");
  return ToYBCStatus(api_impl->DmlModifiesRow(handle, modifies_row));
}

YBCStatus YBCPgSetIsSysCatalogVersionChange(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgSetIsSysCatalogVersionChange");
  return ToYBCStatus(api_impl->SetIsSysCatalogVersionChange(handle));
}

YBCStatus YBCPgSetCatalogCacheVersion(YBCPgStatement handle, uint64_t catalog_cache_version){
  //K2DEBUG("PgGateAPI: YBCPgSetCatalogCacheVersion " << catalog_cache_version);
  return ToYBCStatus(api_impl->SetCatalogCacheVersion(handle, catalog_cache_version));
}

YBCStatus YBCPgIsTableColocated(const YBCPgOid database_oid,
                                const YBCPgOid table_oid,
                                bool *colocated) {
  //K2DEBUG("PgGateAPI: YBCPgIsTableColocated");
  *colocated = false;
  return YBCStatusOK();
}

// INDEX -------------------------------------------------------------------------------------------

// Create and drop index "database_name.schema_name.index_name()".
// - When "schema_name" is NULL, the index "database_name.index_name" is created.
// - When "database_name" is NULL, the index "connected_database_name.index_name" is created.
YBCStatus YBCPgNewCreateIndex(const char *database_name,
                              const char *schema_name,
                              const char *index_name,
                              YBCPgOid database_oid,
                              YBCPgOid index_oid,
                              YBCPgOid table_oid,
                              bool is_shared_index,
                              bool is_unique_index,
                              const bool skip_index_backfill,
                              bool if_not_exist,
                              YBCPgStatement *handle){
  //K2DEBUG("PgGateAPI: YBCPgNewCreateIndex " << database_name << ", " << schema_name << ", " << index_name);
  const PgObjectId index_id(database_oid, index_oid);
  const PgObjectId table_id(database_oid, table_oid);
  return ToYBCStatus(api_impl->NewCreateIndex(database_name, schema_name, index_name, index_id,
                                           table_id, is_shared_index, is_unique_index,
                                           skip_index_backfill, if_not_exist,
                                           handle));
}

YBCStatus YBCPgCreateIndexAddColumn(YBCPgStatement handle, const char *attr_name, int attr_num,
                                    const YBCPgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first){
  //K2DEBUG("PgGateAPI: YBCPgCreateIndexAddColumn " << attr_name << ", " << attr_num);
  return ToYBCStatus(api_impl->CreateIndexAddColumn(handle, attr_name, attr_num, attr_type,
                                                 is_hash, is_range, is_desc, is_nulls_first));
}

YBCStatus YBCPgCreateIndexSetNumTablets(YBCPgStatement handle, int32_t num_tablets){
  //K2DEBUG("PgGateAPI: YBCPgCreateIndexSetNumTablets");
  return YBCStatusOK();
}

YBCStatus YBCPgCreateIndexAddSplitRow(YBCPgStatement handle, int num_cols,
                                      YBCPgTypeEntity **types, uint64_t *data){
  //K2DEBUG("PgGateAPI: YBCPgCreateIndexAddSplitRow");
  return ToYBCStatus(api_impl->CreateIndexAddSplitRow(handle, num_cols, types, data));
}

YBCStatus YBCPgExecCreateIndex(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgExecCreateIndex");
  return ToYBCStatus(api_impl->ExecCreateIndex(handle));
}

YBCStatus YBCPgNewDropIndex(YBCPgOid database_oid,
                            YBCPgOid index_oid,
                            bool if_exist,
                            YBCPgStatement *handle){
  //K2DEBUG("PgGateAPI: YBCPgNewDropIndex " << database_oid << ", " << index_oid);
  const PgObjectId index_id(database_oid, index_oid);
  return ToYBCStatus(api_impl->NewDropIndex(index_id, if_exist, handle));
}

YBCStatus YBCPgExecDropIndex(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgExecDropIndex");
  return ToYBCStatus(api_impl->ExecDropIndex(handle));
}

YBCStatus YBCPgWaitUntilIndexPermissionsAtLeast(
    const YBCPgOid database_oid,
    const YBCPgOid table_oid,
    const YBCPgOid index_oid,
    const uint32_t target_index_permissions,
    uint32_t *actual_index_permissions) {
  //K2DEBUG("PgGateAPI: YBCPgWaitUntilIndexPermissionsAtLeast " << database_oid << ", " << table_oid << ", " << index_oid);
  const PgObjectId table_id(database_oid, table_oid);
  const PgObjectId index_id(database_oid, index_oid);
  IndexPermissions returned_index_permissions = IndexPermissions::INDEX_PERM_DELETE_ONLY;
  YBCStatus s = ExtractValueFromResult(api_impl->WaitUntilIndexPermissionsAtLeast(
        table_id,
        index_id,
        static_cast<IndexPermissions>(target_index_permissions)),
      &returned_index_permissions);
  if (s) {
    // Bad status.
    return s;
  }
  *actual_index_permissions = static_cast<uint32_t>(returned_index_permissions);
  return YBCStatusOK();
}

YBCStatus YBCPgAsyncUpdateIndexPermissions(
    const YBCPgOid database_oid,
    const YBCPgOid indexed_table_oid){
  //K2DEBUG("PgGateAPI: YBCPgAsyncUpdateIndexPermissions " << database_oid << ", " << indexed_table_oid);
  const PgObjectId indexed_table_id(database_oid, indexed_table_oid);
  return ToYBCStatus(api_impl->AsyncUpdateIndexPermissions(indexed_table_id));
}

//--------------------------------------------------------------------------------------------------
// DML statements (select, insert, update, delete, truncate)
//--------------------------------------------------------------------------------------------------

// This function is for specifying the selected or returned expressions.
// - SELECT target_expr1, target_expr2, ...
// - INSERT / UPDATE / DELETE ... RETURNING target_expr1, target_expr2, ...
YBCStatus YBCPgDmlAppendTarget(YBCPgStatement handle, YBCPgExpr target){
  //K2DEBUG("PgGateAPI: YBCPgDmlAppendTarget");
  return ToYBCStatus(api_impl->DmlAppendTarget(handle, target));
}

// Binding Columns: Bind column with a value (expression) in a statement.
// + This API is used to identify the rows you want to operate on. If binding columns are not
//   there, that means you want to operate on all rows (full scan). You can view this as a
//   a definitions of an initial rowset or an optimization over full-scan.
//
// + There are some restrictions on when BindColumn() can be used.
//   Case 1: INSERT INTO tab(x) VALUES(x_expr)
//   - BindColumn() can be used for BOTH primary-key and regular columns.
//   - This bind-column function is used to bind "x" with "x_expr", and "x_expr" that can contain
//     bind-variables (placeholders) and constants whose values can be updated for each execution
//     of the same allocated statement.
//
//   Case 2: SELECT / UPDATE / DELETE <WHERE key = "key_expr">
//   - BindColumn() can only be used for primary-key columns.
//   - This bind-column function is used to bind the primary column "key" with "key_expr" that can
//     contain bind-variables (placeholders) and constants whose values can be updated for each
//     execution of the same allocated statement.
//
// NOTE ON KEY BINDING
// - For Sequential Scan, the target columns of the bind are those in the main table.
// - For Primary Scan, the target columns of the bind are those in the main table.
// - For Index Scan, the target columns of the bind are those in the index table.
//   The index-scan will use the bind to find base-ybctid which is then use to read data from
//   the main-table, and therefore the bind-arguments are not associated with columns in main table.
YBCStatus YBCPgDmlBindColumn(YBCPgStatement handle, int attr_num, YBCPgExpr attr_value){
  //K2DEBUG("PgGateAPI: YBCPgDmlBindColumn " << attr_num);
  return ToYBCStatus(api_impl->DmlBindColumn(handle, attr_num, attr_value));
}

YBCStatus YBCPgDmlBindColumnCondEq(YBCPgStatement handle, int attr_num, YBCPgExpr attr_value){
  //K2DEBUG("PgGateAPI: YBCPgDmlBindColumnCondEq " << attr_num);
  return ToYBCStatus(api_impl->DmlBindColumnCondEq(handle, attr_num, attr_value));
}

YBCStatus YBCPgDmlBindColumnCondBetween(YBCPgStatement handle, int attr_num, YBCPgExpr attr_value,
    YBCPgExpr attr_value_end){
  //K2DEBUG("PgGateAPI: YBCPgDmlBindColumnCondBetween " << attr_num);
  return ToYBCStatus(api_impl->DmlBindColumnCondBetween(handle, attr_num, attr_value, attr_value_end));
}

YBCStatus YBCPgDmlBindColumnCondIn(YBCPgStatement handle, int attr_num, int n_attr_values,
    YBCPgExpr *attr_values){
  //K2DEBUG("PgGateAPI: YBCPgDmlBindColumnCondIn " << attr_num);
  return ToYBCStatus(api_impl->DmlBindColumnCondIn(handle, attr_num, n_attr_values, attr_values));
}

// Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
YBCStatus YBCPgDmlBindTable(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgDmlBindTable");
  return ToYBCStatus(api_impl->DmlBindTable(handle));
}

// API for SET clause.
YBCStatus YBCPgDmlAssignColumn(YBCPgStatement handle,
                               int attr_num,
                               YBCPgExpr attr_value){
  //K2DEBUG("PgGateAPI: YBCPgDmlAssignColumn " << attr_num);
  return ToYBCStatus(api_impl->DmlAssignColumn(handle, attr_num, attr_value));
}

// This function is to fetch the targets in YBCPgDmlAppendTarget() from the rows that were defined
// by YBCPgDmlBindColumn().
YBCStatus YBCPgDmlFetch(YBCPgStatement handle, int32_t natts, uint64_t *values, bool *isnulls,
                        YBCPgSysColumns *syscols, bool *has_data){
  //K2DEBUG("PgGateAPI: YBCPgDmlFetch " << natts);
  return ToYBCStatus(api_impl->DmlFetch(handle, natts, values, isnulls, syscols, has_data));
}

// Utility method that checks stmt type and calls either exec insert, update, or delete internally.
YBCStatus YBCPgDmlExecWriteOp(YBCPgStatement handle, int32_t *rows_affected_count){
  //K2DEBUG("PgGateAPI: YBCPgDmlExecWriteOp");
  return ToYBCStatus(api_impl->DmlExecWriteOp(handle, rows_affected_count));
}

// This function returns the tuple id (ybctid) of a Postgres tuple.
YBCStatus YBCPgDmlBuildYBTupleId(YBCPgStatement handle, const YBCPgAttrValueDescriptor *attrs,
                                 int32_t nattrs, uint64_t *ybctid){
  //K2DEBUG("PgGateAPI: YBCPgDmlBuildYBTupleId " << nattrs);
  return ToYBCStatus(api_impl->DmlBuildYBTupleId(handle, attrs, nattrs, ybctid));
}

// DB Operations: WHERE, ORDER_BY, GROUP_BY, etc.
// + The following operations are run by DocDB.
//   - Not yet
//
// + The following operations are run by Postgres layer. An API might be added to move these
//   operations to DocDB.
//   - API for "where_expr"
//   - API for "order_by_expr"
//   - API for "group_by_expr"


// Buffer write operations.
void YBCPgStartOperationsBuffering() {
  //K2DEBUG("PgGateAPI: YBCPgStartOperationsBuffering");
  api_impl->StartOperationsBuffering();
}

YBCStatus YBCPgStopOperationsBuffering() {
  //K2DEBUG("PgGateAPI: YBCPgStopOperationsBuffering");
  return ToYBCStatus(api_impl->StopOperationsBuffering());
}

YBCStatus YBCPgResetOperationsBuffering() {
  //K2DEBUG("PgGateAPI: YBCPgResetOperationsBuffering");
  return ToYBCStatus(api_impl->ResetOperationsBuffering());
}

YBCStatus YBCPgFlushBufferedOperations() {
  //K2DEBUG("PgGateAPI: YBCPgFlushBufferedOperations");
  return ToYBCStatus(api_impl->FlushBufferedOperations());
}

void YBCPgDropBufferedOperations() {
  //K2DEBUG("PgGateAPI: YBCPgDropBufferedOperations");
  api_impl->DropBufferedOperations();
}

// INSERT ------------------------------------------------------------------------------------------

YBCStatus YBCPgNewInsert(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         bool is_single_row_txn,
                         YBCPgStatement *handle){
  //K2DEBUG("PgGateAPI: YBCPgNewInsert " << database_oid << ", " << table_oid << ", " << is_single_row_txn);
  const PgObjectId table_id(database_oid, table_oid);
  return ToYBCStatus(api_impl->NewInsert(table_id, is_single_row_txn, handle));
  return YBCStatusOK();
}

YBCStatus YBCPgExecInsert(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgExecInsert");
  return ToYBCStatus(api_impl->ExecInsert(handle));
}

YBCStatus YBCPgInsertStmtSetUpsertMode(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgInsertStmtSetUpsertMode");
  return ToYBCStatus(api_impl->InsertStmtSetUpsertMode(handle));
}

YBCStatus YBCPgInsertStmtSetWriteTime(YBCPgStatement handle, const uint64_t write_time){
  //K2DEBUG("PgGateAPI: YBCPgInsertStmtSetWriteTime " << write_time);
  return ToYBCStatus(api_impl->InsertStmtSetWriteTime(handle, write_time));
}

// UPDATE ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewUpdate(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         bool is_single_row_txn,
                         YBCPgStatement *handle){
  //K2DEBUG("PgGateAPI: YBCPgNewUpdate " << database_oid << ", " << table_oid << ", " << is_single_row_txn);
  const PgObjectId table_id(database_oid, table_oid);
  return ToYBCStatus(api_impl->NewUpdate(table_id, is_single_row_txn, handle));
}

YBCStatus YBCPgExecUpdate(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgExecUpdate");
  return ToYBCStatus(api_impl->ExecUpdate(handle));
}

// DELETE ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewDelete(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         bool is_single_row_txn,
                         YBCPgStatement *handle){
  //K2DEBUG("PgGateAPI: YBCPgNewDelete " << database_oid << ", " << table_oid << ", " << is_single_row_txn);
  const PgObjectId table_id(database_oid, table_oid);
  return ToYBCStatus(api_impl->NewDelete(table_id, is_single_row_txn, handle));
}

YBCStatus YBCPgExecDelete(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgExecDelete");
  return ToYBCStatus(api_impl->ExecDelete(handle));
}

// Colocated TRUNCATE ------------------------------------------------------------------------------
YBCStatus YBCPgNewTruncateColocated(YBCPgOid database_oid,
                                    YBCPgOid table_oid,
                                    bool is_single_row_txn,
                                    YBCPgStatement *handle){
  //K2DEBUG("PgGateAPI: YBCPgNewTruncateColocated " << database_oid << ", " << table_oid << ", " << is_single_row_txn);
  return YBCStatusOK();
}

YBCStatus YBCPgExecTruncateColocated(YBCPgStatement handle){
  //K2DEBUG("PgGateAPI: YBCPgExecTruncateColocated");
  return YBCStatusOK();
}

// SELECT ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewSelect(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         const YBCPgPrepareParameters *prepare_params,
                         YBCPgStatement *handle){
  //K2DEBUG("PgGateAPI: YBCPgNewSelect " << database_oid << ", " << table_oid);
  const PgObjectId table_id(database_oid, table_oid);
  const PgObjectId index_id(database_oid,
                            prepare_params ? prepare_params->index_oid : kInvalidOid);
  return ToYBCStatus(api_impl->NewSelect(table_id, index_id, prepare_params, handle));
}

// Set forward/backward scan direction.
YBCStatus YBCPgSetForwardScan(YBCPgStatement handle, bool is_forward_scan){
  //K2DEBUG("PgGateAPI: YBCPgSetForwardScan " << is_forward_scan);
  return ToYBCStatus(api_impl->SetForwardScan(handle, is_forward_scan));
}

YBCStatus YBCPgExecSelect(YBCPgStatement handle, const YBCPgExecParameters *exec_params){
  //K2DEBUG("PgGateAPI: YBCPgExecSelect");
  return ToYBCStatus(api_impl->ExecSelect(handle, exec_params));
}

// Transaction control -----------------------------------------------------------------------------

YBCStatus YBCPgBeginTransaction(){
  //K2DEBUG("PgGateAPI: YBCPgBeginTransaction");
  return ToYBCStatus(api_impl->BeginTransaction());
}

YBCStatus YBCPgRestartTransaction(){
  //K2DEBUG("PgGateAPI: YBCPgRestartTransaction");
  return ToYBCStatus(api_impl->RestartTransaction());
}

YBCStatus YBCPgCommitTransaction(){
  //K2DEBUG("PgGateAPI: YBCPgCommitTransaction");
  return ToYBCStatus(api_impl->CommitTransaction());
}

YBCStatus YBCPgAbortTransaction(){
  //K2DEBUG("PgGateAPI: YBCPgAbortTransaction");
  return ToYBCStatus(api_impl->AbortTransaction());
}

YBCStatus YBCPgSetTransactionIsolationLevel(int isolation){
  //K2DEBUG("PgGateAPI: YBCPgSetTransactionIsolationLevel " << isolation);
  return ToYBCStatus(api_impl->SetTransactionIsolationLevel(isolation));
}

YBCStatus YBCPgSetTransactionReadOnly(bool read_only){
  //K2DEBUG("PgGateAPI: YBCPgSetTransactionReadOnly " << read_only);
  return ToYBCStatus(api_impl->SetTransactionReadOnly(read_only));
}

YBCStatus YBCPgSetTransactionDeferrable(bool deferrable){
  //K2DEBUG("PgGateAPI: YBCPgSetTransactionReadOnly " << deferrable);
  return ToYBCStatus(api_impl->SetTransactionDeferrable(deferrable));
}

YBCStatus YBCPgEnterSeparateDdlTxnMode(){
  //K2DEBUG("PgGateAPI: YBCPgEnterSeparateDdlTxnMode");
  return ToYBCStatus(api_impl->EnterSeparateDdlTxnMode());
}

YBCStatus YBCPgExitSeparateDdlTxnMode(bool success){
  //K2DEBUG("PgGateAPI: YBCPgExitSeparateDdlTxnMode");
  return ToYBCStatus(api_impl->ExitSeparateDdlTxnMode(success));
}

//--------------------------------------------------------------------------------------------------
// Expressions.

// Column references.
YBCStatus YBCPgNewColumnRef(YBCPgStatement stmt, int attr_num, const YBCPgTypeEntity *type_entity,
                            const YBCPgTypeAttrs *type_attrs, YBCPgExpr *expr_handle){
  //K2DEBUG("PgGateAPI: YBCPgNewColumnRef " << attr_num);
  return ToYBCStatus(api_impl->NewColumnRef(stmt, attr_num, type_entity, type_attrs, expr_handle));
}

// Constant expressions.
YBCStatus YBCPgNewConstant(YBCPgStatement stmt, const YBCPgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, YBCPgExpr *expr_handle){
  //K2DEBUG("PgGateAPI: YBCPgNewConstant " << datum << ", " << is_null);
  return ToYBCStatus(api_impl->NewConstant(stmt, type_entity, datum, is_null, expr_handle));
}

YBCStatus YBCPgNewConstantOp(YBCPgStatement stmt, const YBCPgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, YBCPgExpr *expr_handle, bool is_gt){
  //K2DEBUG("PgGateAPI: YBCPgNewConstantOp " << datum << ", " << is_null << ", " << is_gt);
  return ToYBCStatus(api_impl->NewConstantOp(stmt, type_entity, datum, is_null, expr_handle, is_gt));
}

// The following update functions only work for constants.
// Overwriting the constant expression with new value.
YBCStatus YBCPgUpdateConstInt2(YBCPgExpr expr, int16_t value, bool is_null){
  //K2DEBUG("PgGateAPI: YBCPgUpdateConstInt2 " << value << ", " << is_null);
  return ToYBCStatus(api_impl->UpdateConstant(expr, value, is_null));
}

YBCStatus YBCPgUpdateConstInt4(YBCPgExpr expr, int32_t value, bool is_null){
  //K2DEBUG("PgGateAPI: YBCPgUpdateConstInt4 " << value << ", " << is_null);
  return ToYBCStatus(api_impl->UpdateConstant(expr, value, is_null));
}

YBCStatus YBCPgUpdateConstInt8(YBCPgExpr expr, int64_t value, bool is_null){
  //K2DEBUG("PgGateAPI: YBCPgUpdateConstInt8 " << value << ", " << is_null);
  return ToYBCStatus(api_impl->UpdateConstant(expr, value, is_null));
}

YBCStatus YBCPgUpdateConstFloat4(YBCPgExpr expr, float value, bool is_null){
  //K2DEBUG("PgGateAPI: YBCPgUpdateConstFloat4 " << value << ", " << is_null);
  return ToYBCStatus(api_impl->UpdateConstant(expr, value, is_null));
}

YBCStatus YBCPgUpdateConstFloat8(YBCPgExpr expr, double value, bool is_null){
  //K2DEBUG("PgGateAPI: YBCPgUpdateConstFloat8 " << value << ", " << is_null);
  return ToYBCStatus(api_impl->UpdateConstant(expr, value, is_null));
}

YBCStatus YBCPgUpdateConstText(YBCPgExpr expr, const char *value, bool is_null){
  //K2DEBUG("PgGateAPI: YBCPgUpdateConstText " << value << ", " << is_null);
  return ToYBCStatus(api_impl->UpdateConstant(expr, value, is_null));
}

YBCStatus YBCPgUpdateConstChar(YBCPgExpr expr, const char *value, int64_t bytes, bool is_null){
  //K2DEBUG("PgGateAPI: YBCPgUpdateConstChar " << value << ", " << bytes << ", " << is_null);
  return ToYBCStatus(api_impl->UpdateConstant(expr, value, bytes, is_null));
}

// Expressions with operators "=", "+", "between", "in", ...
YBCStatus YBCPgNewOperator(YBCPgStatement stmt, const char *opname,
                           const YBCPgTypeEntity *type_entity,
                           YBCPgExpr *op_handle){
  //K2DEBUG("PgGateAPI: YBCPgNewOperator " << opname);
  return ToYBCStatus(api_impl->NewOperator(stmt, opname, type_entity, op_handle));
}

YBCStatus YBCPgOperatorAppendArg(YBCPgExpr op_handle, YBCPgExpr arg){
  //K2DEBUG("PgGateAPI: YBCPgOperatorAppendArg");
  return ToYBCStatus(api_impl->OperatorAppendArg(op_handle, arg));
}

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool YBCForeignKeyReferenceExists(YBCPgOid table_id, const char* ybctid, int64_t ybctid_size) {
  //K2DEBUG("PgGateAPI: YBCForeignKeyReferenceExists " << table_id);
  return api_impl->ForeignKeyReferenceExists(table_id, std::string(ybctid, ybctid_size));
}

// Add an entry to foreign key reference cache.
YBCStatus YBCCacheForeignKeyReference(YBCPgOid table_id, const char* ybctid, int64_t ybctid_size){
  //K2DEBUG("PgGateAPI: YBCCacheForeignKeyReference " << table_id);
  return ToYBCStatus(api_impl->CacheForeignKeyReference(table_id, std::string(ybctid, ybctid_size)));
}

// Delete an entry from foreign key reference cache.
YBCStatus YBCPgDeleteFromForeignKeyReferenceCache(YBCPgOid table_id, uint64_t ybctid){
  //K2DEBUG("PgGateAPI: YBCPgDeleteFromForeignKeyReferenceCache " << table_id << ", " << ybctid);
  char *value;
  int64_t bytes;
  const YBCPgTypeEntity *type_entity = api_impl->FindTypeEntity(kPgByteArrayOid);
  type_entity->datum_to_yb(ybctid, &value, &bytes);
  return ToYBCStatus(api_impl->DeleteForeignKeyReference(table_id, std::string(value, bytes)));
}

void ClearForeignKeyReferenceCache() {
  //K2DEBUG("PgGateAPI: ClearForeignKeyReferenceCache");
  api_impl->ClearForeignKeyReferenceCache();
}

bool YBCIsInitDbModeEnvVarSet() {
  //K2DEBUG("PgGateAPI: YBCIsInitDbModeEnvVarSet");
  static bool cached_value = false;
  static bool cached = false;

  if (!cached) {
    const char* initdb_mode_env_var_value = getenv("YB_PG_INITDB_MODE");
    cached_value = initdb_mode_env_var_value && strcmp(initdb_mode_env_var_value, "1") == 0;
    cached = true;
  }

  return cached_value;
}

// This is called by initdb. Used to customize some behavior.
void YBCInitFlags() {
  //K2DEBUG("PgGateAPI: YBCInitFlags");
}

// Retrieves value of ysql_max_read_restart_attempts gflag
int32_t YBCGetMaxReadRestartAttempts() {
  //K2DEBUG("PgGateAPI: YBCGetMaxReadRestartAttempts");
  return default_max_read_restart_attempts;
}

// Retrieves value of ysql_output_buffer_size gflag
int32_t YBCGetOutputBufferSize() {
  //K2DEBUG("PgGateAPI: YBCGetOutputBufferSize");
  return default_output_buffer_size;
}

// Retrieve value of ysql_disable_index_backfill gflag.
bool YBCGetDisableIndexBackfill() {
  //K2DEBUG("PgGateAPI: YBCGetDisableIndexBackfill");
  return default_disable_index_backfill;
}

bool YBCPgIsYugaByteEnabled() {
  return api_impl != nullptr;
}

// Sets the specified timeout in the rpc service.
void YBCSetTimeout(int timeout_ms, void* extra) {
  //K2DEBUG("PgGateAPI: YBCSetTimeout " << timeout_ms);
  if (timeout_ms <= 0) {
    // The timeout is not valid. Use the default GFLAG value.
    return;
  }
  timeout_ms = std::min(timeout_ms, default_client_read_write_timeout_ms);
  api_impl->SetTimeout(timeout_ms);
}

//--------------------------------------------------------------------------------------------------
// Thread-Local variables.

void* YBCPgGetThreadLocalCurrentMemoryContext() {
  //K2DEBUG("PgGateAPI: YBCPgGetThreadLocalCurrentMemoryContext");
  return PgGetThreadLocalCurrentMemoryContext();
}

void* YBCPgSetThreadLocalCurrentMemoryContext(void *memctx) {
  //K2DEBUG("PgGateAPI: YBCPgSetThreadLocalCurrentMemoryContext");
  return PgSetThreadLocalCurrentMemoryContext(memctx);
}

void YBCPgResetCurrentMemCtxThreadLocalVars() {
  //K2DEBUG("PgGateAPI: YBCPgResetCurrentMemCtxThreadLocalVars");
  PgResetCurrentMemCtxThreadLocalVars();
}

void* YBCPgGetThreadLocalStrTokPtr() {
  //K2DEBUG("PgGateAPI: YBCPgGetThreadLocalStrTokPtr");
  return PgGetThreadLocalStrTokPtr();
}

void YBCPgSetThreadLocalStrTokPtr(char *new_pg_strtok_ptr) {
  //K2DEBUG("PgGateAPI: YBCPgSetThreadLocalStrTokPtr " << new_pg_strtok_ptr);
  PgSetThreadLocalStrTokPtr(new_pg_strtok_ptr);
}

void* YBCPgSetThreadLocalJumpBuffer(void* new_buffer) {
  //K2DEBUG("PgGateAPI: YBCPgSetThreadLocalJumpBuffer");
  return PgSetThreadLocalJumpBuffer(new_buffer);
}

void* YBCPgGetThreadLocalJumpBuffer() {
  //K2DEBUG("PgGateAPI: YBCPgGetThreadLocalJumpBuffer");
  return PgGetThreadLocalJumpBuffer();
}

void YBCPgSetThreadLocalErrMsg(const void* new_msg) {
  //K2DEBUG("PgGateAPI: YBCPgSetThreadLocalErrMsg " << new_msg);
  PgSetThreadLocalErrMsg(new_msg);
}

const void* YBCPgGetThreadLocalErrMsg() {
  //K2DEBUG("PgGateAPI: YBCPgGetThreadLocalErrMsg");
  return PgGetThreadLocalErrMsg();
}

const YBCPgTypeEntity *YBCPgFindTypeEntity(int type_oid) {
  //K2DEBUG("PgGateAPI: YBCPgFindTypeEntity " << type_oid);
  return api_impl->FindTypeEntity(type_oid);
}

YBCPgDataType YBCPgGetType(const YBCPgTypeEntity *type_entity) {
  //K2DEBUG("PgGateAPI: YBCPgGetType");
  if (type_entity) {
    return type_entity->yb_type;
  }
  return YB_YQL_DATA_TYPE_UNKNOWN_DATA;
}

bool YBCPgAllowForPrimaryKey(const YBCPgTypeEntity *type_entity) {
  //K2DEBUG("PgGateAPI: YBCPgAllowForPrimaryKey");
  if (type_entity) {
    return type_entity->allow_for_primary_key;
  }
  return false;
}

void YBCAssignTransactionPriorityLowerBound(double newval, void* extra) {
  //K2DEBUG("PgGateAPI: YBCAssignTransactionPriorityLowerBound " << newval);
}

void YBCAssignTransactionPriorityUpperBound(double newval, void* extra) {
  //K2DEBUG("PgGateAPI: YBCAssignTransactionPriorityUpperBound " << newval);
}

// the following APIs are called by pg_dump.c only
// TODO: check if we really need to implement them

// Setup the master IP(s) before calling YBCInitPgGate().
void YBCSetMasterAddresses(const char* hosts) {
}

YBCStatus YBCInitPgGateBackend() {
    return YBCStatusOK();
}

void YBCShutdownPgGateBackend() {
}

} // extern "C"

}  // namespace gate
}  // namespace k2pg
