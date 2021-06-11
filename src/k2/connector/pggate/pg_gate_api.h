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
void YBCInitPgGate(const K2PgTypeEntity *YBCDataTypeTable, int count, K2PgCallbacks pg_callbacks);
void YBCDestroyPgGate();

// Initialize ENV within which PGSQL calls will be executed.
K2PgStatus YBCPgCreateEnv(K2PgEnv *pg_env);
K2PgStatus YBCPgDestroyEnv(K2PgEnv pg_env);

// Initialize a session to process statements that come from the same client connection.
K2PgStatus YBCPgInitSession(const K2PgEnv pg_env, const char *database_name);

// Initialize YBCPgMemCtx.
// - Postgres uses memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
// - There YugaByte objects are bound to Postgres operations. All of these objects' allocated
//   memory will be held by YBCPgMemCtx, whose handle belongs to Postgres MemoryContext. Once all
//   Postgres operations are done, associated YugaByte memory context (YBCPgMemCtx) will be
//   destroyed toghether with Postgres memory context.
K2PgMemctx YBCPgCreateMemctx();
K2PgStatus YBCPgDestroyMemctx(K2PgMemctx memctx);
K2PgStatus YBCPgResetMemctx(K2PgMemctx memctx);

// Invalidate the sessions table cache.
K2PgStatus YBCPgInvalidateCache();

// Clear all values and expressions that were bound to the given statement.
K2PgStatus YBCPgClearBinds(K2PgStatement handle);

// Check if initdb has been already run.
K2PgStatus YBCPgIsInitDbDone(bool* initdb_done);

// Sets catalog_version to the local tserver's catalog version stored in shared
// memory, or an error if the shared memory has not been initialized (e.g. in initdb).
K2PgStatus YBCGetSharedCatalogVersion(uint64_t* catalog_version);

//--------------------------------------------------------------------------------------------------
// DDL Statements
//--------------------------------------------------------------------------------------------------

// DATABASE ----------------------------------------------------------------------------------------
// Connect database. Switch the connected database to the given "database_name".
K2PgStatus YBCPgConnectDatabase(const char *database_name);

// Get whether the given database is colocated.
K2PgStatus YBCPgIsDatabaseColocated(const K2PgOid database_oid, bool *colocated);

K2PgStatus YBCInsertSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called);

K2PgStatus YBCUpdateSequenceTupleConditionally(int64_t db_oid,
                                              int64_t seq_oid,
                                              uint64_t ysql_catalog_version,
                                              int64_t last_val,
                                              bool is_called,
                                              int64_t expected_last_val,
                                              bool expected_is_called,
                                              bool *skipped);

K2PgStatus YBCUpdateSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called,
                                 bool* skipped);

K2PgStatus YBCReadSequenceTuple(int64_t db_oid,
                               int64_t seq_oid,
                               uint64_t ysql_catalog_version,
                               int64_t *last_val,
                               bool *is_called);

K2PgStatus YBCDeleteSequenceTuple(int64_t db_oid, int64_t seq_oid);

// K2 InitPrimaryCluster
K2PgStatus K2PGInitPrimaryCluster();

K2PgStatus K2PGFinishInitDB();

// Create database.
K2PgStatus YBCPgNewCreateDatabase(const char *database_name,
                                 K2PgOid database_oid,
                                 K2PgOid source_database_oid,
                                 K2PgOid next_oid,
                                 const bool colocated,
                                 K2PgStatement *handle);
K2PgStatus YBCPgExecCreateDatabase(K2PgStatement handle);

// Drop database.
K2PgStatus YBCPgNewDropDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle);
K2PgStatus YBCPgExecDropDatabase(K2PgStatement handle);

// Alter database.
K2PgStatus YBCPgNewAlterDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle);
K2PgStatus YBCPgAlterDatabaseRenameDatabase(K2PgStatement handle, const char *new_name);
K2PgStatus YBCPgExecAlterDatabase(K2PgStatement handle);

// Reserve oids.
K2PgStatus YBCPgReserveOids(K2PgOid database_oid,
                           K2PgOid next_oid,
                           uint32_t count,
                           K2PgOid *begin_oid,
                           K2PgOid *end_oid);

K2PgStatus YBCPgGetCatalogMasterVersion(uint64_t *version);

void YBCPgInvalidateTableCache(
    const K2PgOid database_oid,
    const K2PgOid table_oid);

K2PgStatus YBCPgInvalidateTableCacheByTableId(const char *table_uuid);

// TABLE -------------------------------------------------------------------------------------------
// Create and drop table "database_name.schema_name.table_name()".
// - When "schema_name" is NULL, the table "database_name.table_name" is created.
// - When "database_name" is NULL, the table "connected_database_name.table_name" is created.
K2PgStatus YBCPgNewCreateTable(const char *database_name,
                              const char *schema_name,
                              const char *table_name,
                              K2PgOid database_oid,
                              K2PgOid table_oid,
                              bool is_shared_table,
                              bool if_not_exist,
                              bool add_primary_key,
                              const bool colocated,
                              K2PgStatement *handle);

K2PgStatus YBCPgCreateTableAddColumn(K2PgStatement handle, const char *attr_name, int attr_num,
                                    const K2PgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first);

K2PgStatus YBCPgExecCreateTable(K2PgStatement handle);

K2PgStatus YBCPgNewAlterTable(K2PgOid database_oid,
                             K2PgOid table_oid,
                             K2PgStatement *handle);

K2PgStatus YBCPgAlterTableAddColumn(K2PgStatement handle, const char *name, int order,
                                   const K2PgTypeEntity *attr_type, bool is_not_null);

K2PgStatus YBCPgAlterTableRenameColumn(K2PgStatement handle, const char *oldname,
                                      const char *newname);

K2PgStatus YBCPgAlterTableDropColumn(K2PgStatement handle, const char *name);

K2PgStatus YBCPgAlterTableRenameTable(K2PgStatement handle, const char *db_name,
                                     const char *newname);

K2PgStatus YBCPgExecAlterTable(K2PgStatement handle);

K2PgStatus YBCPgNewDropTable(K2PgOid database_oid,
                            K2PgOid table_oid,
                            bool if_exist,
                            K2PgStatement *handle);

K2PgStatus YBCPgExecDropTable(K2PgStatement handle);

K2PgStatus YBCPgNewTruncateTable(K2PgOid database_oid,
                                K2PgOid table_oid,
                                K2PgStatement *handle);

K2PgStatus YBCPgExecTruncateTable(K2PgStatement handle);

K2PgStatus YBCPgGetTableDesc(K2PgOid database_oid,
                            K2PgOid table_oid,
                            K2PgTableDesc *handle);

K2PgStatus YBCPgGetColumnInfo(K2PgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary,
                             bool *is_hash);

K2PgStatus YBCPgGetTableProperties(K2PgTableDesc table_desc,
                                  K2PgTableProperties *properties);

K2PgStatus YBCPgDmlModifiesRow(K2PgStatement handle, bool *modifies_row);

K2PgStatus YBCPgSetIsSysCatalogVersionChange(K2PgStatement handle);

K2PgStatus YBCPgSetCatalogCacheVersion(K2PgStatement handle, uint64_t catalog_cache_version);

K2PgStatus YBCPgIsTableColocated(const K2PgOid database_oid,
                                const K2PgOid table_oid,
                                bool *colocated);

// INDEX -------------------------------------------------------------------------------------------
// Create and drop index "database_name.schema_name.index_name()".
// - When "schema_name" is NULL, the index "database_name.index_name" is created.
// - When "database_name" is NULL, the index "connected_database_name.index_name" is created.
K2PgStatus YBCPgNewCreateIndex(const char *database_name,
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

K2PgStatus YBCPgCreateIndexAddColumn(K2PgStatement handle, const char *attr_name, int attr_num,
                                    const K2PgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first);

K2PgStatus YBCPgExecCreateIndex(K2PgStatement handle);

K2PgStatus YBCPgNewDropIndex(K2PgOid database_oid,
                            K2PgOid index_oid,
                            bool if_exist,
                            K2PgStatement *handle);

K2PgStatus YBCPgExecDropIndex(K2PgStatement handle);

K2PgStatus YBCPgWaitUntilIndexPermissionsAtLeast(
    const K2PgOid database_oid,
    const K2PgOid table_oid,
    const K2PgOid index_oid,
    const uint32_t target_index_permissions,
    uint32_t *actual_index_permissions);

K2PgStatus YBCPgAsyncUpdateIndexPermissions(
    const K2PgOid database_oid,
    const K2PgOid indexed_table_oid);

//--------------------------------------------------------------------------------------------------
// DML statements (select, insert, update, delete, truncate)
//--------------------------------------------------------------------------------------------------

// This function is for specifying the selected or returned expressions.
// - SELECT target_expr1, target_expr2, ...
// - INSERT / UPDATE / DELETE ... RETURNING target_expr1, target_expr2, ...
K2PgStatus YBCPgDmlAppendTarget(K2PgStatement handle, K2PgExpr target);

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
K2PgStatus YBCPgDmlBindColumn(K2PgStatement handle, int attr_num, K2PgExpr attr_value);
K2PgStatus YBCPgDmlBindColumnCondEq(K2PgStatement handle, int attr_num, K2PgExpr attr_value);
K2PgStatus YBCPgDmlBindColumnCondBetween(K2PgStatement handle, int attr_num, K2PgExpr attr_value,
    K2PgExpr attr_value_end);
K2PgStatus YBCPgDmlBindColumnCondIn(K2PgStatement handle, int attr_num, int n_attr_values,
    K2PgExpr *attr_values);

// bind range condition so as to derive key prefix
K2PgStatus PgDmlBindRangeConds(K2PgStatement handle, K2PgExpr where_conds);

// bind where clause for a DML operation
K2PgStatus PgDmlBindWhereConds(K2PgStatement handle, K2PgExpr where_conds);

// Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
K2PgStatus YBCPgDmlBindTable(K2PgStatement handle);

// API for SET clause.
K2PgStatus YBCPgDmlAssignColumn(K2PgStatement handle,
                               int attr_num,
                               K2PgExpr attr_value);

// This function is to fetch the targets in YBCPgDmlAppendTarget() from the rows that were defined
// by YBCPgDmlBindColumn().
K2PgStatus YBCPgDmlFetch(K2PgStatement handle, int32_t natts, uint64_t *values, bool *isnulls,
                        K2PgSysColumns *syscols, bool *has_data);

// Utility method that checks stmt type and calls either exec insert, update, or delete internally.
K2PgStatus YBCPgDmlExecWriteOp(K2PgStatement handle, int32_t *rows_affected_count);

// This function returns the tuple id (ybctid) of a Postgres tuple.
K2PgStatus YBCPgDmlBuildYBTupleId(K2PgStatement handle, const K2PgAttrValueDescriptor *attrs,
                                 int32_t nattrs, uint64_t *ybctid);

// DB Operations: WHERE(partially supported by K2-SKV)
// TODO: ORDER_BY, GROUP_BY, etc.

// INSERT ------------------------------------------------------------------------------------------
K2PgStatus YBCPgNewInsert(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

K2PgStatus YBCPgExecInsert(K2PgStatement handle);

K2PgStatus YBCPgInsertStmtSetUpsertMode(K2PgStatement handle);

K2PgStatus YBCPgInsertStmtSetWriteTime(K2PgStatement handle, const uint64_t write_time);

// UPDATE ------------------------------------------------------------------------------------------
K2PgStatus YBCPgNewUpdate(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

K2PgStatus YBCPgExecUpdate(K2PgStatement handle);

// DELETE ------------------------------------------------------------------------------------------
K2PgStatus YBCPgNewDelete(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

K2PgStatus YBCPgExecDelete(K2PgStatement handle);

// Colocated TRUNCATE ------------------------------------------------------------------------------
K2PgStatus YBCPgNewTruncateColocated(K2PgOid database_oid,
                                    K2PgOid table_oid,
                                    bool is_single_row_txn,
                                    K2PgStatement *handle);

K2PgStatus YBCPgExecTruncateColocated(K2PgStatement handle);

// SELECT ------------------------------------------------------------------------------------------
K2PgStatus YBCPgNewSelect(K2PgOid database_oid,
                         K2PgOid table_oid,
                         const K2PgPrepareParameters *prepare_params,
                         K2PgStatement *handle);

// Set forward/backward scan direction.
K2PgStatus YBCPgSetForwardScan(K2PgStatement handle, bool is_forward_scan);

K2PgStatus YBCPgExecSelect(K2PgStatement handle, const K2PgExecParameters *exec_params);

// Transaction control -----------------------------------------------------------------------------
K2PgStatus YBCPgBeginTransaction();
K2PgStatus YBCPgRestartTransaction();
K2PgStatus YBCPgCommitTransaction();
K2PgStatus YBCPgAbortTransaction();
K2PgStatus YBCPgSetTransactionIsolationLevel(int isolation);
K2PgStatus YBCPgSetTransactionReadOnly(bool read_only);
K2PgStatus YBCPgSetTransactionDeferrable(bool deferrable);
K2PgStatus YBCPgEnterSeparateDdlTxnMode();
K2PgStatus YBCPgExitSeparateDdlTxnMode(bool success);

//--------------------------------------------------------------------------------------------------
// Expressions.

// Column references.
K2PgStatus YBCPgNewColumnRef(K2PgStatement stmt, int attr_num, const K2PgTypeEntity *type_entity,
                            const K2PgTypeAttrs *type_attrs, K2PgExpr *expr_handle);

// Constant expressions.
K2PgStatus YBCPgNewConstant(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle);
K2PgStatus YBCPgNewConstantOp(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle, bool is_gt);

// The following update functions only work for constants.
// Overwriting the constant expression with new value.
K2PgStatus YBCPgUpdateConstInt2(K2PgExpr expr, int16_t value, bool is_null);
K2PgStatus YBCPgUpdateConstInt4(K2PgExpr expr, int32_t value, bool is_null);
K2PgStatus YBCPgUpdateConstInt8(K2PgExpr expr, int64_t value, bool is_null);
K2PgStatus YBCPgUpdateConstFloat4(K2PgExpr expr, float value, bool is_null);
K2PgStatus YBCPgUpdateConstFloat8(K2PgExpr expr, double value, bool is_null);
K2PgStatus YBCPgUpdateConstText(K2PgExpr expr, const char *value, bool is_null);
K2PgStatus YBCPgUpdateConstChar(K2PgExpr expr, const char *value, int64_t bytes, bool is_null);

// Expressions with operators "=", "+", "between", "in", ...
K2PgStatus YBCPgNewOperator(K2PgStatement stmt, const char *opname,
                           const K2PgTypeEntity *type_entity,
                           K2PgExpr *op_handle);
K2PgStatus YBCPgOperatorAppendArg(K2PgExpr op_handle, K2PgExpr arg);

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool YBCForeignKeyReferenceExists(K2PgOid table_oid, const char* ybctid, int64_t ybctid_size);

// Add an entry to foreign key reference cache.
K2PgStatus YBCCacheForeignKeyReference(K2PgOid table_oid, const char* ybctid, int64_t ybctid_size);

// Delete an entry from foreign key reference cache.
K2PgStatus YBCPgDeleteFromForeignKeyReferenceCache(K2PgOid table_oid, uint64_t ybctid);

void ClearForeignKeyReferenceCache();

bool YBCIsInitDbModeEnvVarSet();

// This is called by initdb. Used to customize some behavior.
void YBCInitFlags();

// Retrieves value of ysql_max_read_restart_attempts gflag
int32_t YBCGetMaxReadRestartAttempts();

// Retrieves value of ysql_output_buffer_size gflag
int32_t YBCGetOutputBufferSize();

// Retrieve value of ysql_disable_index_backfill gflag.
bool YBCGetDisableIndexBackfill();

bool YBCPgIsYugaByteEnabled();

// Sets the specified timeout in the rpc service.
void YBCSetTimeout(int timeout_ms, void* extra);

//--------------------------------------------------------------------------------------------------
// Thread-Local variables.

void* YBCPgGetThreadLocalCurrentMemoryContext();

void* YBCPgSetThreadLocalCurrentMemoryContext(void *memctx);

void YBCPgResetCurrentMemCtxThreadLocalVars();

void* YBCPgGetThreadLocalStrTokPtr();

void YBCPgSetThreadLocalStrTokPtr(char *new_pg_strtok_ptr);

void* YBCPgSetThreadLocalJumpBuffer(void* new_buffer);

void* YBCPgGetThreadLocalJumpBuffer();

void YBCPgSetThreadLocalErrMsg(const void* new_msg);

const void* YBCPgGetThreadLocalErrMsg();

// APIs called by pg_dump.c only
void YBCSetMasterAddresses(const char *hosts);

void YBCShutdownPgGateBackend();

K2PgStatus YBCInitPgGateBackend();

#ifdef __cplusplus
}  // extern "C"
#endif
