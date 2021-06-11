#include "pg_gate_api.h"

#include "common/k2pg-internal.h"
#include "common/k2pg_util.h"
#include "common/env.h"
#include "entities/entity_ids.h"
#include "pggate/pg_env.h"
#include "pggate/pg_gate_defaults.h"
#include "pggate/pg_gate_thread_local_vars.h"
#include "pggate/pg_gate_impl.h"
#include "k2_log.h"

namespace k2pg {
namespace gate {

using k2pg::Status;
using k2pg::sql::kPgByteArrayOid;
using k2pg::sql::catalog::SqlCatalogManager;

namespace {
// Using a raw pointer here to fully control object initialization and destruction.
k2pg::gate::PgGateApiImpl* api_impl;
std::atomic<bool> api_impl_shutdown_done;

template<class T>
K2PgStatus ExtractValueFromResult(const Result<T>& result, T* value) {
    if (result.ok()) {
        *value = *result;
        return K2PgStatusOK();
    }
    return ToK2PgStatus(result.status());
}

} // anonymous namespace

extern "C" {

void K2PgInitPgGate(const K2PgTypeEntity *YBCDataTypeTable, int count, PgCallbacks pg_callbacks) {
    K2ASSERT(log::pg, api_impl == nullptr, "can only be called once");
    api_impl_shutdown_done.exchange(false);
    api_impl = new k2pg::gate::PgGateApiImpl(YBCDataTypeTable, count, pg_callbacks);
    K2LOG_I(log::pg, "K2 PgGate open");
}

void K2PgDestroyPgGate() {
    if (api_impl_shutdown_done.exchange(true)) {
        K2LOG_E(log::pg, "should only be called once");
    } else {
        k2pg::gate::PgGateApiImpl* local_api_impl = api_impl;
        api_impl = nullptr; // K2PgIsYugaByteEnabled() must return false from now on.
        delete local_api_impl;
    }
}

K2PgStatus K2PgCreateEnv(K2PgEnv *pg_env) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgCreateEnv");
  return ToK2PgStatus(api_impl->CreateEnv(pg_env));
}

K2PgStatus K2PgDestroyEnv(K2PgEnv pg_env) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgDestroyEnv");
  return ToK2PgStatus(api_impl->DestroyEnv(pg_env));
}

// Initialize a session to process statements that come from the same client connection.
K2PgStatus K2PgInitSession(const K2PgEnv pg_env, const char *database_name) {
  K2LOG_D(log::pg, "PgGateAPI: K2PgInitSession {}", database_name);
  const string db_name(database_name ? database_name : "");
  return ToK2PgStatus(api_impl->InitSession(pg_env, db_name));
}

// Initialize YBCPgMemCtx.
// - Postgres uses memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
// - There YugaByte objects are bound to Postgres operations. All of these objects' allocated
//   memory will be held by YBCPgMemCtx, whose handle belongs to Postgres MemoryContext. Once all
//   Postgres operations are done, associated YugaByte memory context (YBCPgMemCtx) will be
//   destroyed toghether with Postgres memory context.
K2PgMemctx K2PgCreateMemctx() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgCreateMemctx");
  return api_impl->CreateMemctx();
}

K2PgStatus K2PgDestroyMemctx(K2PgMemctx memctx) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgDestroyMemctx");
  return ToK2PgStatus(api_impl->DestroyMemctx(memctx));
}

K2PgStatus K2PgResetMemctx(K2PgMemctx memctx) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgResetMemctx");
  return ToK2PgStatus(api_impl->ResetMemctx(memctx));
}

// Invalidate the sessions table cache.
K2PgStatus K2PgInvalidateCache() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgInvalidateCache");
  return ToK2PgStatus(api_impl->InvalidateCache());
}

// Clear all values and expressions that were bound to the given statement.
K2PgStatus K2PgClearBinds(K2PgStatement handle) {
  K2LOG_V(log::pg, "PgGateAPI: YBCPgClearBind");
  return ToK2PgStatus(api_impl->ClearBinds(handle));
}

// Check if initdb has been already run.
K2PgStatus K2PgIsInitDbDone(bool* initdb_done) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgIsInitDbDone");
  return ExtractValueFromResult(api_impl->IsInitDbDone(), initdb_done);
}

// Sets catalog_version to the local tserver's catalog version stored in shared
// memory, or an error if the shared memory has not been initialized (e.g. in initdb).
K2PgStatus K2PgGetSharedCatalogVersion(uint64_t* catalog_version) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetSharedCatalogVersion");
  return ExtractValueFromResult(api_impl->GetSharedCatalogVersion(), catalog_version);
}

//--------------------------------------------------------------------------------------------------
// DDL Statements
//--------------------------------------------------------------------------------------------------

// K2 InitPrimaryCluster
K2PgStatus K2PgInitPrimaryCluster()
{
  K2LOG_V(log::pg, "PgGateAPI: K2PgInitPrimaryCluster");
  return ToK2PgStatus(api_impl->PGInitPrimaryCluster());
}

K2PgStatus K2PgFinishInitDB()
{
  K2LOG_V(log::pg, "PgGateAPI: K2PgFinishInitDB()");
  return ToK2PgStatus(api_impl->PGFinishInitDB());
}

// DATABASE ----------------------------------------------------------------------------------------
// Connect database. Switch the connected database to the given "database_name".
K2PgStatus K2PgConnectDatabase(const char *database_name) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgConnectDatabase {}", database_name);
  return ToK2PgStatus(api_impl->ConnectDatabase(database_name));
}

// Get whether the given database is colocated.
K2PgStatus K2PgIsDatabaseColocated(const K2PgOid database_oid, bool *colocated) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgIsDatabaseColocated");
  *colocated = false;
  return K2PgStatusOK();
}

// Create database.
K2PgStatus K2PgNewCreateDatabase(const char *database_name,
                                 K2PgOid database_oid,
                                 K2PgOid source_database_oid,
                                 K2PgOid next_oid,
                                 const bool colocated,
                                 K2PgStatement *handle) {
  K2LOG_D(log::pg, "PgGateAPI: K2PgNewCreateDatabase {}, {}, {}, {}",
         database_name, database_oid, source_database_oid, next_oid);
  return ToK2PgStatus(api_impl->NewCreateDatabase(database_name, database_oid, source_database_oid, next_oid, handle));
}

K2PgStatus K2PgExecCreateDatabase(K2PgStatement handle) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecCreateDatabase");
  return ToK2PgStatus(api_impl->ExecCreateDatabase(handle));
}

// Drop database.
K2PgStatus K2PgNewDropDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewDropDatabase {}, {}", database_name, database_oid);
  return ToK2PgStatus(api_impl->NewDropDatabase(database_name, database_oid, handle));
}

K2PgStatus K2PgExecDropDatabase(K2PgStatement handle) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecDropDatabase");
  return ToK2PgStatus(api_impl->ExecDropDatabase(handle));
}

// Alter database.
K2PgStatus K2PgNewAlterDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewAlterDatabase {}, {}", database_name, database_oid);
  return ToK2PgStatus(api_impl->NewAlterDatabase(database_name, database_oid, handle));
}

K2PgStatus K2PgAlterDatabaseRenameDatabase(K2PgStatement handle, const char *new_name) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgAlterDatabaseRenameDatabase {}", new_name);
  return ToK2PgStatus(api_impl->AlterDatabaseRenameDatabase(handle, new_name));
}

K2PgStatus K2PgExecAlterDatabase(K2PgStatement handle) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecAlterDatabase");
  return ToK2PgStatus(api_impl->ExecAlterDatabase(handle));
}

// Reserve oids.
K2PgStatus K2PgReserveOids(K2PgOid database_oid,
                           K2PgOid next_oid,
                           uint32_t count,
                           K2PgOid *begin_oid,
                           K2PgOid *end_oid) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgReserveOids {}, {}, {}", database_oid, next_oid, count);
  return ToK2PgStatus(api_impl->ReserveOids(database_oid, next_oid, count, begin_oid, end_oid));
}

K2PgStatus K2PgGetCatalogMasterVersion(uint64_t *version) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetCatalogMasterVersion");
  return ToK2PgStatus(api_impl->GetCatalogMasterVersion(version));
}

void K2PgInvalidateTableCache(
    const K2PgOid database_oid,
    const K2PgOid table_oid) {
  const PgObjectId table_object_id(database_oid, table_oid);
  K2LOG_V(log::pg, "PgGateAPI: K2PgInvalidateTableCache {}, {}", database_oid, table_oid);
  api_impl->InvalidateTableCache(table_object_id);
}

K2PgStatus K2PgInvalidateTableCacheByTableId(const char *table_uuid) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgInvalidateTableCacheByTableId {}", table_uuid);
  if (table_uuid == NULL) {
    return ToK2PgStatus(STATUS(InvalidArgument, "table_uuid is null"));
  }
  std::string table_uuid_str = table_uuid;
  const PgObjectId table_object_id(table_uuid_str);
  api_impl->InvalidateTableCache(table_object_id);
  return K2PgStatusOK();
}

// Sequence Operations -----------------------------------------------------------------------------

K2PgStatus K2PgInsertSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgInsertSequenceTuple {}, {}, {}", db_oid, seq_oid, ysql_catalog_version);
  return ToK2PgStatus(api_impl->InsertSequenceTuple(db_oid, seq_oid, ysql_catalog_version, last_val, is_called));
}

K2PgStatus K2PgUpdateSequenceTupleConditionally(int64_t db_oid,
                                              int64_t seq_oid,
                                              uint64_t ysql_catalog_version,
                                              int64_t last_val,
                                              bool is_called,
                                              int64_t expected_last_val,
                                              bool expected_is_called,
                                              bool *skipped) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgUpdateSequenceTupleConditionally {}, {}, {}", db_oid, seq_oid, ysql_catalog_version);
  return ToK2PgStatus(
      api_impl->UpdateSequenceTupleConditionally(db_oid, seq_oid, ysql_catalog_version,
          last_val, is_called, expected_last_val, expected_is_called, skipped));
}

K2PgStatus K2PgUpdateSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called,
                                 bool* skipped) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgUpdateSequenceTuple {}, {}, {}", db_oid, seq_oid, ysql_catalog_version);
  return ToK2PgStatus(api_impl->UpdateSequenceTuple(
      db_oid, seq_oid, ysql_catalog_version, last_val, is_called, skipped));
}

K2PgStatus K2PgReadSequenceTuple(int64_t db_oid,
                               int64_t seq_oid,
                               uint64_t ysql_catalog_version,
                               int64_t *last_val,
                               bool *is_called) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgReadSequenceTuple {}, {}, {}", db_oid, seq_oid, ysql_catalog_version);
  return ToK2PgStatus(api_impl->ReadSequenceTuple(
      db_oid, seq_oid, ysql_catalog_version, last_val, is_called));
}

K2PgStatus K2PgDeleteSequenceTuple(int64_t db_oid, int64_t seq_oid) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgDeleteSequenceTuple {}, {}", db_oid, seq_oid);
  return ToK2PgStatus(api_impl->DeleteSequenceTuple(db_oid, seq_oid));
}

// TABLE -------------------------------------------------------------------------------------------

// Create and drop table "database_name.schema_name.table_name()".
// - When "schema_name" is NULL, the table "database_name.table_name" is created.
// - When "database_name" is NULL, the table "connected_database_name.table_name" is created.
K2PgStatus K2PgNewCreateTable(const char *database_name,
                              const char *schema_name,
                              const char *table_name,
                              K2PgOid database_oid,
                              K2PgOid table_oid,
                              bool is_shared_table,
                              bool if_not_exist,
                              bool add_primary_key,
                              const bool colocated,
                              K2PgStatement *handle) {
  if (is_shared_table) {
    K2LOG_D(log::pg, "PgGateAPI: K2PgNewCreateTable (shared) {}, {}, {}", database_name, schema_name, table_name);
  } else {
    K2LOG_V(log::pg, "PgGateAPI: K2PgNewCreateTable {}, {}, {}", database_name, schema_name, table_name);
  }
  const PgObjectId table_object_id(database_oid, table_oid);
  return ToK2PgStatus(api_impl->NewCreateTable(
      database_name, schema_name, table_name, table_object_id, is_shared_table,
      if_not_exist, add_primary_key, handle));
}

K2PgStatus K2PgCreateTableAddColumn(K2PgStatement handle, const char *attr_name, int attr_num,
                                    const K2PgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgCreateTableAddColumn (name: {}, order: {}, is_hash {}, is_range {})",
    attr_name, attr_num, is_hash, is_range);
  return ToK2PgStatus(api_impl->CreateTableAddColumn(handle, attr_name, attr_num, attr_type,
                                                 is_hash, is_range, is_desc, is_nulls_first));
}

K2PgStatus K2PgExecCreateTable(K2PgStatement handle) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecCreateTable");
  return ToK2PgStatus(api_impl->ExecCreateTable(handle));
}

K2PgStatus K2PgNewAlterTable(K2PgOid database_oid,
                             K2PgOid table_oid,
                             K2PgStatement *handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewAlterTable {}, {}", database_oid, table_oid);
  const PgObjectId table_object_id(database_oid, table_oid);
  return ToK2PgStatus(api_impl->NewAlterTable(table_object_id, handle));
}

K2PgStatus K2PgAlterTableAddColumn(K2PgStatement handle, const char *name, int order,
                                   const K2PgTypeEntity *attr_type, bool is_not_null){
  K2LOG_V(log::pg, "PgGateAPI: K2PgAlterTableAddColumn {}", name);
  return ToK2PgStatus(api_impl->AlterTableAddColumn(handle, name, order, attr_type, is_not_null));
}

K2PgStatus K2PgAlterTableRenameColumn(K2PgStatement handle, const char *oldname,
                                      const char *newname){
  K2LOG_V(log::pg, "PgGateAPI: K2PgAlterTableRenameColumn {}, {}", oldname, newname);
  return ToK2PgStatus(api_impl->AlterTableRenameColumn(handle, oldname, newname));
}

K2PgStatus K2PgAlterTableDropColumn(K2PgStatement handle, const char *name){
  K2LOG_V(log::pg, "PgGateAPI: K2PgAlterTableDropColumn {}", name);
  return ToK2PgStatus(api_impl->AlterTableDropColumn(handle, name));
}

K2PgStatus K2PgAlterTableRenameTable(K2PgStatement handle, const char *db_name,
                                     const char *newname){
  K2LOG_V(log::pg, "PgGateAPI: K2PgAlterTableRenameTable {}, {}", db_name, newname);
  return ToK2PgStatus(api_impl->AlterTableRenameTable(handle, db_name, newname));
}

K2PgStatus K2PgExecAlterTable(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecAlterTable");
  return ToK2PgStatus(api_impl->ExecAlterTable(handle));
}

K2PgStatus K2PgNewDropTable(K2PgOid database_oid,
                            K2PgOid table_oid,
                            bool if_exist,
                            K2PgStatement *handle) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewDropTable {}, {}", database_oid, table_oid);
  const PgObjectId table_object_id(database_oid, table_oid);
  return ToK2PgStatus(api_impl->NewDropTable(table_object_id, if_exist, handle));
}

K2PgStatus K2PgExecDropTable(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecDropTable");
  return ToK2PgStatus(api_impl->ExecDropTable(handle));
}

K2PgStatus K2PgNewTruncateTable(K2PgOid database_oid,
                                K2PgOid table_oid,
                                K2PgStatement *handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewTruncateTable {}, {}", database_oid, table_oid);
  return K2PgStatusOK();
}

K2PgStatus K2PgExecTruncateTable(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecTruncateTable");
  return K2PgStatusOK();
}

K2PgStatus K2PgGetTableDesc(K2PgOid database_oid,
                            K2PgOid table_oid,
                            K2PgTableDesc *handle) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetTableDesc {}, {}", database_oid, table_oid);
  const PgObjectId table_object_id(database_oid, table_oid);
  return ToK2PgStatus(api_impl->GetTableDesc(table_object_id, handle));
}

K2PgStatus K2PgGetColumnInfo(K2PgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary,
                             bool *is_hash) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetTableDesc {}", attr_number);
  return ToK2PgStatus(api_impl->GetColumnInfo(table_desc, attr_number, is_primary, is_hash));
}

K2PgStatus K2PgGetTableProperties(K2PgTableDesc table_desc,
                                  K2PgTableProperties *properties){
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetTableProperties");
  properties->num_hash_key_columns = table_desc->num_hash_key_columns();
  properties->is_colocated = false;
  return K2PgStatusOK();
}

K2PgStatus K2PgDmlModifiesRow(K2PgStatement handle, bool *modifies_row){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlModifiesRow");
  return ToK2PgStatus(api_impl->DmlModifiesRow(handle, modifies_row));
}

K2PgStatus K2PgSetIsSysCatalogVersionChange(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetIsSysCatalogVersionChange");
  return ToK2PgStatus(api_impl->SetIsSysCatalogVersionChange(handle));
}

K2PgStatus K2PgSetCatalogCacheVersion(K2PgStatement handle, uint64_t catalog_cache_version){
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetCatalogCacheVersion {}", catalog_cache_version);
  return ToK2PgStatus(api_impl->SetCatalogCacheVersion(handle, catalog_cache_version));
}

K2PgStatus K2PgIsTableColocated(const K2PgOid database_oid,
                                const K2PgOid table_oid,
                                bool *colocated) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgIsTableColocated");
  *colocated = false;
  return K2PgStatusOK();
}

// INDEX -------------------------------------------------------------------------------------------

// Create and drop index "database_name.schema_name.index_name()".
// - When "schema_name" is NULL, the index "database_name.index_name" is created.
// - When "database_name" is NULL, the index "connected_database_name.index_name" is created.
K2PgStatus K2PgNewCreateIndex(const char *database_name,
                              const char *schema_name,
                              const char *index_name,
                              K2PgOid database_oid,
                              K2PgOid index_oid,
                              K2PgOid table_oid,
                              bool is_shared_index,
                              bool is_unique_index,
                              const bool skip_index_backfill,
                              bool if_not_exist,
                              K2PgStatement *handle){
  if (is_shared_index) {
    K2LOG_D(log::pg, "PgGateAPI: K2PgNewCreateIndex (shared) {}, {}, {}", database_name, schema_name, index_name);
  } else {
    K2LOG_V(log::pg, "PgGateAPI: K2PgNewCreateIndex {}, {}, {}", database_name, schema_name, index_name);
  }
  const PgObjectId index_object_id(database_oid, index_oid);
  const PgObjectId table_object_id(database_oid, table_oid);
  return ToK2PgStatus(api_impl->NewCreateIndex(database_name, schema_name, index_name, index_object_id,
                                           table_object_id, is_shared_index, is_unique_index,
                                           skip_index_backfill, if_not_exist,
                                           handle));
}

K2PgStatus K2PgCreateIndexAddColumn(K2PgStatement handle, const char *attr_name, int attr_num,
                                    const K2PgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first){
  K2LOG_V(log::pg, "PgGateAPI: K2PgCreateIndexAddColumn (name: {}, order: {}, is_hash: {}, is_range: {})", attr_name, attr_num, is_hash, is_range);
  return ToK2PgStatus(api_impl->CreateIndexAddColumn(handle, attr_name, attr_num, attr_type,
                                                 is_hash, is_range, is_desc, is_nulls_first));
}

K2PgStatus K2PgExecCreateIndex(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecCreateIndex");
  return ToK2PgStatus(api_impl->ExecCreateIndex(handle));
}

K2PgStatus K2PgNewDropIndex(K2PgOid database_oid,
                            K2PgOid index_oid,
                            bool if_exist,
                            K2PgStatement *handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewDropIndex {}, {}", database_oid, index_oid);
  const PgObjectId index_id(database_oid, index_oid);
  return ToK2PgStatus(api_impl->NewDropIndex(index_id, if_exist, handle));
}

K2PgStatus K2PgExecDropIndex(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecDropIndex");
  return ToK2PgStatus(api_impl->ExecDropIndex(handle));
}

K2PgStatus K2PgWaitUntilIndexPermissionsAtLeast(
    const K2PgOid database_oid,
    const K2PgOid table_oid,
    const K2PgOid index_oid,
    const uint32_t target_index_permissions,
    uint32_t *actual_index_permissions) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgWaitUntilIndexPermissionsAtLeast {}, {}, {}", database_oid, table_oid, index_oid);
  const PgObjectId table_object_id(database_oid, table_oid);
  const PgObjectId index_object_id(database_oid, index_oid);
  IndexPermissions returned_index_permissions = IndexPermissions::INDEX_PERM_DELETE_ONLY;
  K2PgStatus s = ExtractValueFromResult(api_impl->WaitUntilIndexPermissionsAtLeast(
        table_object_id,
        index_object_id,
        static_cast<IndexPermissions>(target_index_permissions)),
      &returned_index_permissions);
  if (s) {
    // Bad status.
    return s;
  }
  *actual_index_permissions = static_cast<uint32_t>(returned_index_permissions);
  return K2PgStatusOK();
}

K2PgStatus K2PgAsyncUpdateIndexPermissions(
    const K2PgOid database_oid,
    const K2PgOid indexed_table_oid){
  K2LOG_V(log::pg, "PgGateAPI: K2PgAsyncUpdateIndexPermissions {}, {}", database_oid,  indexed_table_oid);
  const PgObjectId indexed_table_object_id(database_oid, indexed_table_oid);
  return ToK2PgStatus(api_impl->AsyncUpdateIndexPermissions(indexed_table_object_id));
}

//--------------------------------------------------------------------------------------------------
// DML statements (select, insert, update, delete, truncate)
//--------------------------------------------------------------------------------------------------

// This function is for specifying the selected or returned expressions.
// - SELECT target_expr1, target_expr2, ...
// - INSERT / UPDATE / DELETE ... RETURNING target_expr1, target_expr2, ...
K2PgStatus K2PgDmlAppendTarget(K2PgStatement handle, K2PgExpr target){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlAppendTarget");
  return ToK2PgStatus(api_impl->DmlAppendTarget(handle, target));
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
K2PgStatus K2PgDmlBindColumn(K2PgStatement handle, int attr_num, K2PgExpr attr_value){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlBindColumn {}", attr_num);
  return ToK2PgStatus(api_impl->DmlBindColumn(handle, attr_num, attr_value));
}

K2PgStatus K2PgDmlBindColumnCondEq(K2PgStatement handle, int attr_num, K2PgExpr attr_value){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlBindColumnCondEq {}", attr_num);
  return ToK2PgStatus(api_impl->DmlBindColumnCondEq(handle, attr_num, attr_value));
}

K2PgStatus K2PgDmlBindColumnCondBetween(K2PgStatement handle, int attr_num, K2PgExpr attr_value,
    K2PgExpr attr_value_end){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlBindColumnCondBetween {}", attr_num);
  return ToK2PgStatus(api_impl->DmlBindColumnCondBetween(handle, attr_num, attr_value, attr_value_end));
}

K2PgStatus K2PgDmlBindColumnCondIn(K2PgStatement handle, int attr_num, int n_attr_values,
    K2PgExpr *attr_values){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlBindColumnCondIn {}", attr_num);
  return ToK2PgStatus(api_impl->DmlBindColumnCondIn(handle, attr_num, n_attr_values, attr_values));
}

K2PgStatus K2PgDmlBindRangeConds(K2PgStatement handle, K2PgExpr range_conds) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlBindRangeConds");
  return ToK2PgStatus(api_impl->DmlBindRangeConds(handle, range_conds));
}

K2PgStatus K2PgDmlBindWhereConds(K2PgStatement handle, K2PgExpr where_conds) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlBindWhereConds");
  return ToK2PgStatus(api_impl->DmlBindWhereConds(handle, where_conds));
}

// Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
K2PgStatus K2PgDmlBindTable(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlBindTable");
  return ToK2PgStatus(api_impl->DmlBindTable(handle));
}

// API for SET clause.
K2PgStatus K2PgDmlAssignColumn(K2PgStatement handle,
                               int attr_num,
                               K2PgExpr attr_value){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlAssignColumn {}", attr_num);
  return ToK2PgStatus(api_impl->DmlAssignColumn(handle, attr_num, attr_value));
}

// This function is to fetch the targets in K2PgDmlAppendTarget() from the rows that were defined
// by K2PgDmlBindColumn().
K2PgStatus K2PgDmlFetch(K2PgStatement handle, int32_t natts, uint64_t *values, bool *isnulls,
                        K2PgSysColumns *syscols, bool *has_data){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlFetch {}", natts);
  return ToK2PgStatus(api_impl->DmlFetch(handle, natts, values, isnulls, syscols, has_data));
}

// Utility method that checks stmt type and calls either exec insert, update, or delete internally.
K2PgStatus K2PgDmlExecWriteOp(K2PgStatement handle, int32_t *rows_affected_count){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlExecWriteOp");
  return ToK2PgStatus(api_impl->DmlExecWriteOp(handle, rows_affected_count));
}

// This function returns the tuple id (ybctid) of a Postgres tuple.
K2PgStatus K2PgDmlBuildYBTupleId(K2PgStatement handle, const K2PgAttrValueDescriptor *attrs,
                                 int32_t nattrs, uint64_t *ybctid){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDmlBuildYBTupleId {}", nattrs);
  return ToK2PgStatus(api_impl->DmlBuildYBTupleId(handle, attrs, nattrs, ybctid));
}

// DB Operations: WHERE(partially supported by K2-SKV)
// TODO: ORDER_BY, GROUP_BY, etc.

// INSERT ------------------------------------------------------------------------------------------

K2PgStatus K2PgNewInsert(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewInsert {}, {}, {}", database_oid, table_oid, is_single_row_txn);
  const PgObjectId table_object_id(database_oid, table_oid);
  return ToK2PgStatus(api_impl->NewInsert(table_object_id, is_single_row_txn, handle));
  return K2PgStatusOK();
}

K2PgStatus K2PgExecInsert(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecInsert");
  return ToK2PgStatus(api_impl->ExecInsert(handle));
}

K2PgStatus K2PgInsertStmtSetUpsertMode(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgInsertStmtSetUpsertMode");
  return ToK2PgStatus(api_impl->InsertStmtSetUpsertMode(handle));
}

K2PgStatus K2PgInsertStmtSetWriteTime(K2PgStatement handle, const uint64_t write_time){
  K2LOG_V(log::pg, "PgGateAPI: K2PgInsertStmtSetWriteTime {}", write_time);
  return ToK2PgStatus(api_impl->InsertStmtSetWriteTime(handle, write_time));
}

// UPDATE ------------------------------------------------------------------------------------------
K2PgStatus K2PgNewUpdate(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewUpdate {}, {}, {}", database_oid, table_oid, is_single_row_txn);
  const PgObjectId table_object_id(database_oid, table_oid);
  return ToK2PgStatus(api_impl->NewUpdate(table_object_id, is_single_row_txn, handle));
}

K2PgStatus K2PgExecUpdate(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecUpdate");
  return ToK2PgStatus(api_impl->ExecUpdate(handle));
}

// DELETE ------------------------------------------------------------------------------------------
K2PgStatus K2PgNewDelete(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewDelete {}, {}, {}", database_oid, table_oid, is_single_row_txn);
  const PgObjectId table_object_id(database_oid, table_oid);
  return ToK2PgStatus(api_impl->NewDelete(table_object_id, is_single_row_txn, handle));
}

K2PgStatus K2PgExecDelete(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecDelete");
  return ToK2PgStatus(api_impl->ExecDelete(handle));
}

// Colocated TRUNCATE ------------------------------------------------------------------------------
K2PgStatus K2PgNewTruncateColocated(K2PgOid database_oid,
                                    K2PgOid table_oid,
                                    bool is_single_row_txn,
                                    K2PgStatement *handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewTruncateColocated {}, {}, {}", database_oid, table_oid, is_single_row_txn);
  return K2PgStatusOK();
}

K2PgStatus K2PgExecTruncateColocated(K2PgStatement handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecTruncateColocated");
  return K2PgStatusOK();
}

// SELECT ------------------------------------------------------------------------------------------
K2PgStatus K2PgNewSelect(K2PgOid database_oid,
                         K2PgOid table_oid,
                         const K2PgPrepareParameters *prepare_params,
                         K2PgStatement *handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewSelect {}, {}", database_oid, table_oid);
  const PgObjectId table_object_id(database_oid, table_oid);
  const PgObjectId index_object_id(database_oid,
                            prepare_params ? prepare_params->index_oid : kInvalidOid);
  return ToK2PgStatus(api_impl->NewSelect(table_object_id, index_object_id, prepare_params, handle));
}

// Set forward/backward scan direction.
K2PgStatus K2PgSetForwardScan(K2PgStatement handle, bool is_forward_scan){
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetForwardScan {}", is_forward_scan);
  return ToK2PgStatus(api_impl->SetForwardScan(handle, is_forward_scan));
}

K2PgStatus K2PgExecSelect(K2PgStatement handle, const K2PgExecParameters *exec_params){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExecSelect");
  return ToK2PgStatus(api_impl->ExecSelect(handle, exec_params));
}

// Transaction control -----------------------------------------------------------------------------

K2PgStatus K2PgBeginTransaction(){
  K2LOG_V(log::pg, "PgGateAPI: K2PgBeginTransaction");
  return ToK2PgStatus(api_impl->BeginTransaction());
}

K2PgStatus K2PgRestartTransaction(){
  K2LOG_V(log::pg, "PgGateAPI: K2PgRestartTransaction");
  return ToK2PgStatus(api_impl->RestartTransaction());
}

K2PgStatus K2PgCommitTransaction(){
  K2LOG_V(log::pg, "PgGateAPI: K2PgCommitTransaction");
  return ToK2PgStatus(api_impl->CommitTransaction());
}

K2PgStatus K2PgAbortTransaction(){
  K2LOG_V(log::pg, "PgGateAPI: K2PgAbortTransaction");
  return ToK2PgStatus(api_impl->AbortTransaction());
}

K2PgStatus K2PgSetTransactionIsolationLevel(int isolation){
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetTransactionIsolationLevel {}", isolation);
  return ToK2PgStatus(api_impl->SetTransactionIsolationLevel(isolation));
}

K2PgStatus K2PgSetTransactionReadOnly(bool read_only){
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetTransactionReadOnly {}", read_only);
  return ToK2PgStatus(api_impl->SetTransactionReadOnly(read_only));
}

K2PgStatus K2PgSetTransactionDeferrable(bool deferrable){
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetTransactionReadOnly {}", deferrable);
  return ToK2PgStatus(api_impl->SetTransactionDeferrable(deferrable));
}

K2PgStatus K2PgEnterSeparateDdlTxnMode(){
  K2LOG_V(log::pg, "PgGateAPI: K2PgEnterSeparateDdlTxnMode");
  return ToK2PgStatus(api_impl->EnterSeparateDdlTxnMode());
}

K2PgStatus K2PgExitSeparateDdlTxnMode(bool success){
  K2LOG_V(log::pg, "PgGateAPI: K2PgExitSeparateDdlTxnMode");
  return ToK2PgStatus(api_impl->ExitSeparateDdlTxnMode(success));
}

//--------------------------------------------------------------------------------------------------
// Expressions.

// Column references.
K2PgStatus K2PgNewColumnRef(K2PgStatement stmt, int attr_num, const K2PgTypeEntity *type_entity,
                            const K2PgTypeAttrs *type_attrs, K2PgExpr *expr_handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewColumnRef {}", attr_num);
  return ToK2PgStatus(api_impl->NewColumnRef(stmt, attr_num, type_entity, type_attrs, expr_handle));
}

// Constant expressions.
K2PgStatus K2PgNewConstant(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewConstant {}, {}", datum, is_null);
  return ToK2PgStatus(api_impl->NewConstant(stmt, type_entity, datum, is_null, expr_handle));
}

K2PgStatus K2PgNewConstantOp(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle, bool is_gt){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewConstantOp {}, {}, {}", datum, is_null, is_gt);
  return ToK2PgStatus(api_impl->NewConstantOp(stmt, type_entity, datum, is_null, expr_handle, is_gt));
}

// The following update functions only work for constants.
// Overwriting the constant expression with new value.
K2PgStatus K2PgUpdateConstInt2(K2PgExpr expr, int16_t value, bool is_null){
  K2LOG_V(log::pg, "PgGateAPI: K2PgUpdateConstInt2 {}, {}", value, is_null);
  return ToK2PgStatus(api_impl->UpdateConstant(expr, value, is_null));
}

K2PgStatus K2PgUpdateConstInt4(K2PgExpr expr, int32_t value, bool is_null){
  K2LOG_V(log::pg, "PgGateAPI: K2PgUpdateConstInt4 {}, {}", value, is_null);
  return ToK2PgStatus(api_impl->UpdateConstant(expr, value, is_null));
}

K2PgStatus K2PgUpdateConstInt8(K2PgExpr expr, int64_t value, bool is_null){
  K2LOG_V(log::pg, "PgGateAPI: K2PgUpdateConstInt8 {}, {}", value, is_null);
  return ToK2PgStatus(api_impl->UpdateConstant(expr, value, is_null));
}

K2PgStatus K2PgUpdateConstFloat4(K2PgExpr expr, float value, bool is_null){
  K2LOG_V(log::pg, "PgGateAPI: K2PgUpdateConstFloat4 {}, {}", value, is_null);
  return ToK2PgStatus(api_impl->UpdateConstant(expr, value, is_null));
}

K2PgStatus K2PgUpdateConstFloat8(K2PgExpr expr, double value, bool is_null){
  K2LOG_V(log::pg, "PgGateAPI: K2PgUpdateConstFloat8 {}, {}", value, is_null);
  return ToK2PgStatus(api_impl->UpdateConstant(expr, value, is_null));
}

K2PgStatus K2PgUpdateConstText(K2PgExpr expr, const char *value, bool is_null){
  K2LOG_V(log::pg, "PgGateAPI: K2PgUpdateConstText {}, {}", value, is_null);
  return ToK2PgStatus(api_impl->UpdateConstant(expr, value, is_null));
}

K2PgStatus K2PgUpdateConstChar(K2PgExpr expr, const char *value, int64_t bytes, bool is_null){
  K2LOG_V(log::pg, "PgGateAPI: K2PgUpdateConstChar {}, {}, {}", value, bytes, is_null);
  return ToK2PgStatus(api_impl->UpdateConstant(expr, value, bytes, is_null));
}

// Expressions with operators "=", "+", "between", "in", ...
K2PgStatus K2PgNewOperator(K2PgStatement stmt, const char *opname,
                           const K2PgTypeEntity *type_entity,
                           K2PgExpr *op_handle){
  K2LOG_V(log::pg, "PgGateAPI: K2PgNewOperator {}", opname);
  return ToK2PgStatus(api_impl->NewOperator(stmt, opname, type_entity, op_handle));
}

K2PgStatus K2PgOperatorAppendArg(K2PgExpr op_handle, K2PgExpr arg){
  K2LOG_V(log::pg, "PgGateAPI: K2PgOperatorAppendArg");
  return ToK2PgStatus(api_impl->OperatorAppendArg(op_handle, arg));
}

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool K2PgForeignKeyReferenceExists(K2PgOid table_oid, const char* ybctid, int64_t ybctid_size) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgForeignKeyReferenceExists {}", table_oid);
  return api_impl->ForeignKeyReferenceExists(table_oid, std::string(ybctid, ybctid_size));
}

// Add an entry to foreign key reference cache.
K2PgStatus K2PgCacheForeignKeyReference(K2PgOid table_oid, const char* ybctid, int64_t ybctid_size){
  K2LOG_V(log::pg, "PgGateAPI: K2PgCacheForeignKeyReference {}", table_oid);
  return ToK2PgStatus(api_impl->CacheForeignKeyReference(table_oid, std::string(ybctid, ybctid_size)));
}

// Delete an entry from foreign key reference cache.
K2PgStatus K2PgDeleteFromForeignKeyReferenceCache(K2PgOid table_oid, uint64_t ybctid){
  K2LOG_V(log::pg, "PgGateAPI: K2PgDeleteFromForeignKeyReferenceCache {}, {}", table_oid, ybctid);
  char *value;
  int64_t bytes;
  const K2PgTypeEntity *type_entity = api_impl->FindTypeEntity(kPgByteArrayOid);
  type_entity->datum_to_k2pg(ybctid, &value, &bytes);
  return ToK2PgStatus(api_impl->DeleteForeignKeyReference(table_oid, std::string(value, bytes)));
}

void K2PgClearForeignKeyReferenceCache() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgClearForeignKeyReferenceCache");
  api_impl->K2PgClearForeignKeyReferenceCache();
}

bool K2PgIsInitDbModeEnvVarSet() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgIsInitDbModeEnvVarSet");
  static bool cached_value = false;
  static bool cached = false;

  if (!cached) {
    const char* initdb_mode_env_var_value = getenv("K2PG_INITDB_MODE");
    cached_value = initdb_mode_env_var_value && strcmp(initdb_mode_env_var_value, "1") == 0;
    cached = true;
  }

  return cached_value;
}

// This is called by initdb. Used to customize some behavior.
void K2PgInitFlags() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgInitFlags");
}

// Retrieves value of ysql_max_read_restart_attempts gflag
int32_t K2PgGetMaxReadRestartAttempts() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetMaxReadRestartAttempts");
  return default_max_read_restart_attempts;
}

// Retrieves value of ysql_output_buffer_size gflag
int32_t K2PgGetOutputBufferSize() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetOutputBufferSize");
  return default_output_buffer_size;
}

// Retrieve value of ysql_disable_index_backfill gflag.
bool K2PgGetDisableIndexBackfill() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetDisableIndexBackfill");
  return default_disable_index_backfill;
}

bool K2PgIsYugaByteEnabled() {
  return api_impl != nullptr;
}

// Sets the specified timeout in the rpc service.
void K2PgSetTimeout(int timeout_ms, void* extra) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetTimeout {}", timeout_ms);
  if (timeout_ms <= 0) {
    // The timeout is not valid. Use the default GFLAG value.
    return;
  }
  timeout_ms = std::min(timeout_ms, default_client_read_write_timeout_ms);
  api_impl->SetTimeout(timeout_ms);
}

//--------------------------------------------------------------------------------------------------
// Thread-Local variables.

void* K2PgGetThreadLocalCurrentMemoryContext() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetThreadLocalCurrentMemoryContext");
  return PgGetThreadLocalCurrentMemoryContext();
}

void* K2PgSetThreadLocalCurrentMemoryContext(void *memctx) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetThreadLocalCurrentMemoryContext");
  return PgSetThreadLocalCurrentMemoryContext(memctx);
}

void K2PgResetCurrentMemCtxThreadLocalVars() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgResetCurrentMemCtxThreadLocalVars");
  PgResetCurrentMemCtxThreadLocalVars();
}

void* K2PgGetThreadLocalStrTokPtr() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetThreadLocalStrTokPtr");
  return PgGetThreadLocalStrTokPtr();
}

void K2PgSetThreadLocalStrTokPtr(char *new_pg_strtok_ptr) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetThreadLocalStrTokPtr {}", new_pg_strtok_ptr);
  PgSetThreadLocalStrTokPtr(new_pg_strtok_ptr);
}

void* K2PgSetThreadLocalJumpBuffer(void* new_buffer) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetThreadLocalJumpBuffer");
  return PgSetThreadLocalJumpBuffer(new_buffer);
}

void* K2PgGetThreadLocalJumpBuffer() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetThreadLocalJumpBuffer");
  return PgGetThreadLocalJumpBuffer();
}

void K2PgSetThreadLocalErrMsg(const void* new_msg) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgSetThreadLocalErrMsg {}", new_msg);
  PgSetThreadLocalErrMsg(new_msg);
}

const void* K2PgGetThreadLocalErrMsg() {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetThreadLocalErrMsg");
  return PgGetThreadLocalErrMsg();
}

const K2PgTypeEntity *K2PgFindTypeEntity(int type_oid) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgFindTypeEntity {}", type_oid);
  return api_impl->FindTypeEntity(type_oid);
}

K2PgDataType K2PgGetType(const K2PgTypeEntity *type_entity) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgGetType");
  if (type_entity) {
    return type_entity->k2pg_type;
  }
  return K2SQL_DATA_TYPE_UNKNOWN_DATA;
}

bool K2PgAllowForPrimaryKey(const K2PgTypeEntity *type_entity) {
  K2LOG_V(log::pg, "PgGateAPI: K2PgAllowForPrimaryKey");
  if (type_entity) {
    return type_entity->allow_for_primary_key;
  }
  return false;
}

void YBCAssignTransactionPriorityLowerBound(double newval, void* extra) {
  K2LOG_V(log::pg, "PgGateAPI: YBCAssignTransactionPriorityLowerBound {}", newval);
}

void YBCAssignTransactionPriorityUpperBound(double newval, void* extra) {
  K2LOG_V(log::pg, "PgGateAPI: YBCAssignTransactionPriorityUpperBound {}", newval);
}

// the following APIs are called by pg_dump.c only
// TODO: check if we really need to implement them

K2PgStatus K2PgInitPgGateBackend() {
    return K2PgStatusOK();
}

void K2PgShutdownPgGateBackend() {
}

} // extern "C"

}  // namespace gate
}  // namespace k2pg
