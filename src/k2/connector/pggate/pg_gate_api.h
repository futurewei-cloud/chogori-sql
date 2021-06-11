// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

// C wrappers around "pggate" for PostgreSQL to call.

#pragma once

#include <stdint.h>

#include "common/k2pg_util.h"
#include "pg_gate_typedefs.h"

#ifdef __cplusplus
extern "C" {
#endif

// This must be called exactly once to initialize the YPostgreSQL/SKV gateway API before any other
// functions in this API are called.
void K2PgInitPgGate(const K2PgTypeEntity *YBCDataTypeTable, int count, K2PgCallbacks pg_callbacks);
void K2PgDestroyPgGate();

// Initialize ENV within which PGSQL calls will be executed.
K2PgStatus K2PgCreateEnv(K2PgEnv *pg_env);
K2PgStatus K2PgDestroyEnv(K2PgEnv pg_env);

// Initialize a session to process statements that come from the same client connection.
K2PgStatus K2PgInitSession(const K2PgEnv pg_env, const char *database_name);

// Initialize YBCPgMemCtx.
// - Postgres uses memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
// - There YugaByte objects are bound to Postgres operations. All of these objects' allocated
//   memory will be held by YBCPgMemCtx, whose handle belongs to Postgres MemoryContext. Once all
//   Postgres operations are done, associated YugaByte memory context (YBCPgMemCtx) will be
//   destroyed toghether with Postgres memory context.
K2PgMemctx K2PgCreateMemctx();
K2PgStatus K2PgDestroyMemctx(K2PgMemctx memctx);
K2PgStatus K2PgResetMemctx(K2PgMemctx memctx);

// Invalidate the sessions table cache.
K2PgStatus K2PgInvalidateCache();

// Clear all values and expressions that were bound to the given statement.
K2PgStatus K2PgClearBinds(K2PgStatement handle);

// Check if initdb has been already run.
K2PgStatus K2PgIsInitDbDone(bool* initdb_done);

// Sets catalog_version to the local tserver's catalog version stored in shared
// memory, or an error if the shared memory has not been initialized (e.g. in initdb).
K2PgStatus K2PgGetSharedCatalogVersion(uint64_t* catalog_version);

//--------------------------------------------------------------------------------------------------
// DDL Statements
//--------------------------------------------------------------------------------------------------

// DATABASE ----------------------------------------------------------------------------------------
// Connect database. Switch the connected database to the given "database_name".
K2PgStatus K2PgConnectDatabase(const char *database_name);

// Get whether the given database is colocated.
K2PgStatus K2PgIsDatabaseColocated(const K2PgOid database_oid, bool *colocated);

K2PgStatus K2PgInsertSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called);

K2PgStatus K2PgUpdateSequenceTupleConditionally(int64_t db_oid,
                                              int64_t seq_oid,
                                              uint64_t ysql_catalog_version,
                                              int64_t last_val,
                                              bool is_called,
                                              int64_t expected_last_val,
                                              bool expected_is_called,
                                              bool *skipped);

K2PgStatus K2PgUpdateSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called,
                                 bool* skipped);

K2PgStatus K2PgReadSequenceTuple(int64_t db_oid,
                               int64_t seq_oid,
                               uint64_t ysql_catalog_version,
                               int64_t *last_val,
                               bool *is_called);

K2PgStatus K2PgDeleteSequenceTuple(int64_t db_oid, int64_t seq_oid);

// K2 InitPrimaryCluster
K2PgStatus K2PgInitPrimaryCluster();

K2PgStatus K2PgFinishInitDB();

// Create database.
K2PgStatus K2PgNewCreateDatabase(const char *database_name,
                                 K2PgOid database_oid,
                                 K2PgOid source_database_oid,
                                 K2PgOid next_oid,
                                 const bool colocated,
                                 K2PgStatement *handle);
K2PgStatus K2PgExecCreateDatabase(K2PgStatement handle);

// Drop database.
K2PgStatus K2PgNewDropDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle);
K2PgStatus K2PgExecDropDatabase(K2PgStatement handle);

// Alter database.
K2PgStatus K2PgNewAlterDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle);
K2PgStatus K2PgAlterDatabaseRenameDatabase(K2PgStatement handle, const char *new_name);
K2PgStatus K2PgExecAlterDatabase(K2PgStatement handle);

// Reserve oids.
K2PgStatus K2PgReserveOids(K2PgOid database_oid,
                           K2PgOid next_oid,
                           uint32_t count,
                           K2PgOid *begin_oid,
                           K2PgOid *end_oid);

K2PgStatus K2PgGetCatalogMasterVersion(uint64_t *version);

void K2PgInvalidateTableCache(
    const K2PgOid database_oid,
    const K2PgOid table_oid);

K2PgStatus K2PgInvalidateTableCacheByTableId(const char *table_uuid);

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
                              K2PgStatement *handle);

K2PgStatus K2PgCreateTableAddColumn(K2PgStatement handle, const char *attr_name, int attr_num,
                                    const K2PgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first);

K2PgStatus K2PgExecCreateTable(K2PgStatement handle);

K2PgStatus K2PgNewAlterTable(K2PgOid database_oid,
                             K2PgOid table_oid,
                             K2PgStatement *handle);

K2PgStatus K2PgAlterTableAddColumn(K2PgStatement handle, const char *name, int order,
                                   const K2PgTypeEntity *attr_type, bool is_not_null);

K2PgStatus K2PgAlterTableRenameColumn(K2PgStatement handle, const char *oldname,
                                      const char *newname);

K2PgStatus K2PgAlterTableDropColumn(K2PgStatement handle, const char *name);

K2PgStatus K2PgAlterTableRenameTable(K2PgStatement handle, const char *db_name,
                                     const char *newname);

K2PgStatus K2PgExecAlterTable(K2PgStatement handle);

K2PgStatus K2PgNewDropTable(K2PgOid database_oid,
                            K2PgOid table_oid,
                            bool if_exist,
                            K2PgStatement *handle);

K2PgStatus K2PgExecDropTable(K2PgStatement handle);

K2PgStatus K2PgNewTruncateTable(K2PgOid database_oid,
                                K2PgOid table_oid,
                                K2PgStatement *handle);

K2PgStatus K2PgExecTruncateTable(K2PgStatement handle);

K2PgStatus K2PgGetTableDesc(K2PgOid database_oid,
                            K2PgOid table_oid,
                            K2PgTableDesc *handle);

K2PgStatus K2PgGetColumnInfo(K2PgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary,
                             bool *is_hash);

K2PgStatus K2PgGetTableProperties(K2PgTableDesc table_desc,
                                  K2PgTableProperties *properties);

K2PgStatus K2PgDmlModifiesRow(K2PgStatement handle, bool *modifies_row);

K2PgStatus K2PgSetIsSysCatalogVersionChange(K2PgStatement handle);

K2PgStatus K2PgSetCatalogCacheVersion(K2PgStatement handle, uint64_t catalog_cache_version);

K2PgStatus K2PgIsTableColocated(const K2PgOid database_oid,
                                const K2PgOid table_oid,
                                bool *colocated);

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
                              K2PgStatement *handle);

K2PgStatus K2PgCreateIndexAddColumn(K2PgStatement handle, const char *attr_name, int attr_num,
                                    const K2PgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first);

K2PgStatus K2PgExecCreateIndex(K2PgStatement handle);

K2PgStatus K2PgNewDropIndex(K2PgOid database_oid,
                            K2PgOid index_oid,
                            bool if_exist,
                            K2PgStatement *handle);

K2PgStatus K2PgExecDropIndex(K2PgStatement handle);

K2PgStatus K2PgWaitUntilIndexPermissionsAtLeast(
    const K2PgOid database_oid,
    const K2PgOid table_oid,
    const K2PgOid index_oid,
    const uint32_t target_index_permissions,
    uint32_t *actual_index_permissions);

K2PgStatus K2PgAsyncUpdateIndexPermissions(
    const K2PgOid database_oid,
    const K2PgOid indexed_table_oid);

//--------------------------------------------------------------------------------------------------
// DML statements (select, insert, update, delete, truncate)
//--------------------------------------------------------------------------------------------------

// This function is for specifying the selected or returned expressions.
// - SELECT target_expr1, target_expr2, ...
// - INSERT / UPDATE / DELETE ... RETURNING target_expr1, target_expr2, ...
K2PgStatus K2PgDmlAppendTarget(K2PgStatement handle, K2PgExpr target);

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
K2PgStatus K2PgDmlBindColumn(K2PgStatement handle, int attr_num, K2PgExpr attr_value);
K2PgStatus K2PgDmlBindColumnCondEq(K2PgStatement handle, int attr_num, K2PgExpr attr_value);
K2PgStatus K2PgDmlBindColumnCondBetween(K2PgStatement handle, int attr_num, K2PgExpr attr_value,
    K2PgExpr attr_value_end);
K2PgStatus K2PgDmlBindColumnCondIn(K2PgStatement handle, int attr_num, int n_attr_values,
    K2PgExpr *attr_values);

// bind range condition so as to derive key prefix
K2PgStatus K2PgDmlBindRangeConds(K2PgStatement handle, K2PgExpr where_conds);

// bind where clause for a DML operation
K2PgStatus K2PgDmlBindWhereConds(K2PgStatement handle, K2PgExpr where_conds);

// Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
K2PgStatus K2PgDmlBindTable(K2PgStatement handle);

// API for SET clause.
K2PgStatus K2PgDmlAssignColumn(K2PgStatement handle,
                               int attr_num,
                               K2PgExpr attr_value);

// This function is to fetch the targets in K2PgDmlAppendTarget() from the rows that were defined
// by K2PgDmlBindColumn().
K2PgStatus K2PgDmlFetch(K2PgStatement handle, int32_t natts, uint64_t *values, bool *isnulls,
                        K2PgSysColumns *syscols, bool *has_data);

// Utility method that checks stmt type and calls either exec insert, update, or delete internally.
K2PgStatus K2PgDmlExecWriteOp(K2PgStatement handle, int32_t *rows_affected_count);

// This function returns the tuple id (ybctid) of a Postgres tuple.
K2PgStatus K2PgDmlBuildYBTupleId(K2PgStatement handle, const K2PgAttrValueDescriptor *attrs,
                                 int32_t nattrs, uint64_t *ybctid);

// DB Operations: WHERE(partially supported by K2-SKV)
// TODO: ORDER_BY, GROUP_BY, etc.

// INSERT ------------------------------------------------------------------------------------------
K2PgStatus K2PgNewInsert(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

K2PgStatus K2PgExecInsert(K2PgStatement handle);

K2PgStatus K2PgInsertStmtSetUpsertMode(K2PgStatement handle);

K2PgStatus K2PgInsertStmtSetWriteTime(K2PgStatement handle, const uint64_t write_time);

// UPDATE ------------------------------------------------------------------------------------------
K2PgStatus K2PgNewUpdate(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

K2PgStatus K2PgExecUpdate(K2PgStatement handle);

// DELETE ------------------------------------------------------------------------------------------
K2PgStatus K2PgNewDelete(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

K2PgStatus K2PgExecDelete(K2PgStatement handle);

// Colocated TRUNCATE ------------------------------------------------------------------------------
K2PgStatus K2PgNewTruncateColocated(K2PgOid database_oid,
                                    K2PgOid table_oid,
                                    bool is_single_row_txn,
                                    K2PgStatement *handle);

K2PgStatus K2PgExecTruncateColocated(K2PgStatement handle);

// SELECT ------------------------------------------------------------------------------------------
K2PgStatus K2PgNewSelect(K2PgOid database_oid,
                         K2PgOid table_oid,
                         const K2PgPrepareParameters *prepare_params,
                         K2PgStatement *handle);

// Set forward/backward scan direction.
K2PgStatus K2PgSetForwardScan(K2PgStatement handle, bool is_forward_scan);

K2PgStatus K2PgExecSelect(K2PgStatement handle, const K2PgExecParameters *exec_params);

// Transaction control -----------------------------------------------------------------------------
K2PgStatus K2PgBeginTransaction();
K2PgStatus K2PgRestartTransaction();
K2PgStatus K2PgCommitTransaction();
K2PgStatus K2PgAbortTransaction();
K2PgStatus K2PgSetTransactionIsolationLevel(int isolation);
K2PgStatus K2PgSetTransactionReadOnly(bool read_only);
K2PgStatus K2PgSetTransactionDeferrable(bool deferrable);
K2PgStatus K2PgEnterSeparateDdlTxnMode();
K2PgStatus K2PgExitSeparateDdlTxnMode(bool success);

//--------------------------------------------------------------------------------------------------
// Expressions.

// Column references.
K2PgStatus K2PgNewColumnRef(K2PgStatement stmt, int attr_num, const K2PgTypeEntity *type_entity,
                            const K2PgTypeAttrs *type_attrs, K2PgExpr *expr_handle);

// Constant expressions.
K2PgStatus K2PgNewConstant(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle);
K2PgStatus K2PgNewConstantOp(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle, bool is_gt);

// The following update functions only work for constants.
// Overwriting the constant expression with new value.
K2PgStatus K2PgUpdateConstInt2(K2PgExpr expr, int16_t value, bool is_null);
K2PgStatus K2PgUpdateConstInt4(K2PgExpr expr, int32_t value, bool is_null);
K2PgStatus K2PgUpdateConstInt8(K2PgExpr expr, int64_t value, bool is_null);
K2PgStatus K2PgUpdateConstFloat4(K2PgExpr expr, float value, bool is_null);
K2PgStatus K2PgUpdateConstFloat8(K2PgExpr expr, double value, bool is_null);
K2PgStatus K2PgUpdateConstText(K2PgExpr expr, const char *value, bool is_null);
K2PgStatus K2PgUpdateConstChar(K2PgExpr expr, const char *value, int64_t bytes, bool is_null);

// Expressions with operators "=", "+", "between", "in", ...
K2PgStatus K2PgNewOperator(K2PgStatement stmt, const char *opname,
                           const K2PgTypeEntity *type_entity,
                           K2PgExpr *op_handle);
K2PgStatus K2PgOperatorAppendArg(K2PgExpr op_handle, K2PgExpr arg);

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool K2PgForeignKeyReferenceExists(K2PgOid table_oid, const char* ybctid, int64_t ybctid_size);

// Add an entry to foreign key reference cache.
K2PgStatus K2PgCacheForeignKeyReference(K2PgOid table_oid, const char* ybctid, int64_t ybctid_size);

// Delete an entry from foreign key reference cache.
K2PgStatus K2PgDeleteFromForeignKeyReferenceCache(K2PgOid table_oid, uint64_t ybctid);

void K2PgClearForeignKeyReferenceCache();

bool K2PgIsInitDbModeEnvVarSet();

// This is called by initdb. Used to customize some behavior.
void K2PgInitFlags();

// Retrieves value of ysql_max_read_restart_attempts gflag
int32_t K2PgGetMaxReadRestartAttempts();

// Retrieves value of ysql_output_buffer_size gflag
int32_t K2PgGetOutputBufferSize();

// Retrieve value of ysql_disable_index_backfill gflag.
bool K2PgGetDisableIndexBackfill();

bool K2PgIsYugaByteEnabled();

// Sets the specified timeout in the rpc service.
void K2PgSetTimeout(int timeout_ms, void* extra);

//--------------------------------------------------------------------------------------------------
// Thread-Local variables.

void* K2PgGetThreadLocalCurrentMemoryContext();

void* K2PgSetThreadLocalCurrentMemoryContext(void *memctx);

void K2PgResetCurrentMemCtxThreadLocalVars();

void* K2PgGetThreadLocalStrTokPtr();

void K2PgSetThreadLocalStrTokPtr(char *new_pg_strtok_ptr);

void* K2PgSetThreadLocalJumpBuffer(void* new_buffer);

void* K2PgGetThreadLocalJumpBuffer();

void K2PgSetThreadLocalErrMsg(const void* new_msg);

const void* K2PgGetThreadLocalErrMsg();

// APIs called by pg_dump.c only
void K2PgShutdownPgGateBackend();

K2PgStatus K2PgInitPgGateBackend();

#ifdef __cplusplus
}  // extern "C"
#endif
