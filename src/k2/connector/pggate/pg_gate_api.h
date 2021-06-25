// Copyright (c) YugaByte, Inc.
// Portions Copyright (c) 2021 Futurewei Cloud
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
void PgGate_InitPgGate(const K2PgTypeEntity *k2PgDataTypeTable, int count, K2PgCallbacks pg_callbacks);
void PgGate_DestroyPgGate();

// Initialize ENV within which PGSQL calls will be executed.
K2PgStatus PgGate_CreateEnv(K2PgEnv *pg_env);
K2PgStatus PgGate_DestroyEnv(K2PgEnv pg_env);

// Initialize a session to process statements that come from the same client connection.
K2PgStatus PgGate_InitSession(const K2PgEnv pg_env, const char *database_name);

// Initialize K2PgMemCtx.
// - Postgres uses memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
// - There K2Pg objects are bound to Postgres operations. All of these objects' allocated
//   memory will be held by K2PgMemCtx, whose handle belongs to Postgres MemoryContext. Once all
//   Postgres operations are done, associated YugaByte memory context (K2PgMemCtx) will be
//   destroyed toghether with Postgres memory context.
K2PgMemctx PgGate_CreateMemctx();
K2PgStatus PgGate_DestroyMemctx(K2PgMemctx memctx);
K2PgStatus PgGate_ResetMemctx(K2PgMemctx memctx);

// Invalidate the sessions table cache.
K2PgStatus PgGate_InvalidateCache();

// Clear all values and expressions that were bound to the given statement.
K2PgStatus PgGate_ClearBinds(K2PgStatement handle);

// Check if initdb has been already run.
K2PgStatus PgGate_IsInitDbDone(bool* initdb_done);

// Sets catalog_version to the local tserver's catalog version stored in shared
// memory, or an error if the shared memory has not been initialized (e.g. in initdb).
K2PgStatus PgGate_GetSharedCatalogVersion(uint64_t* catalog_version);

//--------------------------------------------------------------------------------------------------
// DDL Statements
//--------------------------------------------------------------------------------------------------

// DATABASE ----------------------------------------------------------------------------------------
// Connect database. Switch the connected database to the given "database_name".
K2PgStatus PgGate_ConnectDatabase(const char *database_name);

// Get whether the given database is colocated.
K2PgStatus PgGate_IsDatabaseColocated(const K2PgOid database_oid, bool *colocated);

K2PgStatus PgGate_InsertSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called);

K2PgStatus PgGate_UpdateSequenceTupleConditionally(int64_t db_oid,
                                              int64_t seq_oid,
                                              uint64_t ysql_catalog_version,
                                              int64_t last_val,
                                              bool is_called,
                                              int64_t expected_last_val,
                                              bool expected_is_called,
                                              bool *skipped);

K2PgStatus PgGate_UpdateSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called,
                                 bool* skipped);

K2PgStatus PgGate_ReadSequenceTuple(int64_t db_oid,
                               int64_t seq_oid,
                               uint64_t ysql_catalog_version,
                               int64_t *last_val,
                               bool *is_called);

K2PgStatus PgGate_DeleteSequenceTuple(int64_t db_oid, int64_t seq_oid);

// K2 InitPrimaryCluster
K2PgStatus PgGate_InitPrimaryCluster();

K2PgStatus PgGate_FinishInitDB();

// Create database.
K2PgStatus PgGate_NewCreateDatabase(const char *database_name,
                                 K2PgOid database_oid,
                                 K2PgOid source_database_oid,
                                 K2PgOid next_oid,
                                 const bool colocated,
                                 K2PgStatement *handle);
K2PgStatus PgGate_ExecCreateDatabase(K2PgStatement handle);

// Drop database.
K2PgStatus PgGate_NewDropDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle);
K2PgStatus PgGate_ExecDropDatabase(K2PgStatement handle);

// Alter database.
K2PgStatus PgGate_NewAlterDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle);
K2PgStatus PgGate_AlterDatabaseRenameDatabase(K2PgStatement handle, const char *new_name);
K2PgStatus PgGate_ExecAlterDatabase(K2PgStatement handle);

// Reserve oids.
K2PgStatus PgGate_ReserveOids(K2PgOid database_oid,
                           K2PgOid next_oid,
                           uint32_t count,
                           K2PgOid *begin_oid,
                           K2PgOid *end_oid);

K2PgStatus PgGate_GetCatalogMasterVersion(uint64_t *version);

void PgGate_InvalidateTableCache(
    const K2PgOid database_oid,
    const K2PgOid table_oid);

K2PgStatus PgGate_InvalidateTableCacheByTableId(const char *table_uuid);

// TABLE -------------------------------------------------------------------------------------------
// Create and drop table "database_name.schema_name.table_name()".
// - When "schema_name" is NULL, the table "database_name.table_name" is created.
// - When "database_name" is NULL, the table "connected_database_name.table_name" is created.
K2PgStatus PgGate_NewCreateTable(const char *database_name,
                              const char *schema_name,
                              const char *table_name,
                              K2PgOid database_oid,
                              K2PgOid table_oid,
                              bool is_shared_table,
                              bool if_not_exist,
                              bool add_primary_key,
                              const bool colocated,
                              K2PgStatement *handle);

K2PgStatus PgGate_CreateTableAddColumn(K2PgStatement handle, const char *attr_name, int attr_num,
                                    const K2PgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first);

K2PgStatus PgGate_ExecCreateTable(K2PgStatement handle);

K2PgStatus PgGate_NewAlterTable(K2PgOid database_oid,
                             K2PgOid table_oid,
                             K2PgStatement *handle);

K2PgStatus PgGate_AlterTableAddColumn(K2PgStatement handle, const char *name, int order,
                                   const K2PgTypeEntity *attr_type, bool is_not_null);

K2PgStatus PgGate_AlterTableRenameColumn(K2PgStatement handle, const char *oldname,
                                      const char *newname);

K2PgStatus PgGate_AlterTableDropColumn(K2PgStatement handle, const char *name);

K2PgStatus PgGate_AlterTableRenameTable(K2PgStatement handle, const char *db_name,
                                     const char *newname);

K2PgStatus PgGate_ExecAlterTable(K2PgStatement handle);

K2PgStatus PgGate_NewDropTable(K2PgOid database_oid,
                            K2PgOid table_oid,
                            bool if_exist,
                            K2PgStatement *handle);

K2PgStatus PgGate_ExecDropTable(K2PgStatement handle);

K2PgStatus PgGate_NewTruncateTable(K2PgOid database_oid,
                                K2PgOid table_oid,
                                K2PgStatement *handle);

K2PgStatus PgGate_ExecTruncateTable(K2PgStatement handle);

K2PgStatus PgGate_GetTableDesc(K2PgOid database_oid,
                            K2PgOid table_oid,
                            K2PgTableDesc *handle);

K2PgStatus PgGate_GetColumnInfo(K2PgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary,
                             bool *is_hash);

K2PgStatus PgGate_GetTableProperties(K2PgTableDesc table_desc,
                                  K2PgTableProperties *properties);

K2PgStatus PgGate_DmlModifiesRow(K2PgStatement handle, bool *modifies_row);

K2PgStatus PgGate_SetIsSysCatalogVersionChange(K2PgStatement handle);

K2PgStatus PgGate_SetCatalogCacheVersion(K2PgStatement handle, uint64_t catalog_cache_version);

K2PgStatus PgGate_IsTableColocated(const K2PgOid database_oid,
                                const K2PgOid table_oid,
                                bool *colocated);

// INDEX -------------------------------------------------------------------------------------------
// Create and drop index "database_name.schema_name.index_name()".
// - When "schema_name" is NULL, the index "database_name.index_name" is created.
// - When "database_name" is NULL, the index "connected_database_name.index_name" is created.
K2PgStatus PgGate_NewCreateIndex(const char *database_name,
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

K2PgStatus PgGate_CreateIndexAddColumn(K2PgStatement handle, const char *attr_name, int attr_num,
                                    const K2PgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first);

K2PgStatus PgGate_ExecCreateIndex(K2PgStatement handle);

K2PgStatus PgGate_NewDropIndex(K2PgOid database_oid,
                            K2PgOid index_oid,
                            bool if_exist,
                            K2PgStatement *handle);

K2PgStatus PgGate_ExecDropIndex(K2PgStatement handle);

K2PgStatus PgGate_WaitUntilIndexPermissionsAtLeast(
    const K2PgOid database_oid,
    const K2PgOid table_oid,
    const K2PgOid index_oid,
    const uint32_t target_index_permissions,
    uint32_t *actual_index_permissions);

K2PgStatus PgGate_AsyncUpdateIndexPermissions(
    const K2PgOid database_oid,
    const K2PgOid indexed_table_oid);

//--------------------------------------------------------------------------------------------------
// DML statements (select, insert, update, delete, truncate)
//--------------------------------------------------------------------------------------------------

// This function is for specifying the selected or returned expressions.
// - SELECT target_expr1, target_expr2, ...
// - INSERT / UPDATE / DELETE ... RETURNING target_expr1, target_expr2, ...
K2PgStatus PgGate_DmlAppendTarget(K2PgStatement handle, K2PgExpr target);

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
//   The index-scan will use the bind to find base-k2pgctid which is then use to read data from
//   the main-table, and therefore the bind-arguments are not associated with columns in main table.
K2PgStatus PgGate_DmlBindColumn(K2PgStatement handle, int attr_num, K2PgExpr attr_value);
K2PgStatus PgGate_DmlBindColumnCondEq(K2PgStatement handle, int attr_num, K2PgExpr attr_value);
K2PgStatus PgGate_DmlBindColumnCondBetween(K2PgStatement handle, int attr_num, K2PgExpr attr_value,
    K2PgExpr attr_value_end);
K2PgStatus PgGate_DmlBindColumnCondIn(K2PgStatement handle, int attr_num, int n_attr_values,
    K2PgExpr *attr_values);

// bind range condition so as to derive key prefix
K2PgStatus PgGate_DmlBindRangeConds(K2PgStatement handle, K2PgExpr where_conds);

// bind where clause for a DML operation
K2PgStatus PgGate_DmlBindWhereConds(K2PgStatement handle, K2PgExpr where_conds);

// Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
K2PgStatus PgGate_DmlBindTable(K2PgStatement handle);

// API for SET clause.
K2PgStatus PgGate_DmlAssignColumn(K2PgStatement handle,
                               int attr_num,
                               K2PgExpr attr_value);

// This function is to fetch the targets in PgGate_DmlAppendTarget() from the rows that were defined
// by PgGate_DmlBindColumn().
K2PgStatus PgGate_DmlFetch(K2PgStatement handle, int32_t natts, uint64_t *values, bool *isnulls,
                        K2PgSysColumns *syscols, bool *has_data);

// Utility method that checks stmt type and calls either exec insert, update, or delete internally.
K2PgStatus PgGate_DmlExecWriteOp(K2PgStatement handle, int32_t *rows_affected_count);

// This function returns the tuple id (k2pgctid) of a Postgres tuple.
K2PgStatus PgGate_DmlBuildPgTupleId(K2PgStatement handle, const K2PgAttrValueDescriptor *attrs,
                                 int32_t nattrs, uint64_t *k2pgctid);

// DB Operations: WHERE(partially supported by K2-SKV)
// TODO: ORDER_BY, GROUP_BY, etc.

// INSERT ------------------------------------------------------------------------------------------
K2PgStatus PgGate_NewInsert(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

K2PgStatus PgGate_ExecInsert(K2PgStatement handle);

K2PgStatus PgGate_InsertStmtSetUpsertMode(K2PgStatement handle);

K2PgStatus PgGate_InsertStmtSetWriteTime(K2PgStatement handle, const uint64_t write_time);

// UPDATE ------------------------------------------------------------------------------------------
K2PgStatus PgGate_NewUpdate(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

K2PgStatus PgGate_ExecUpdate(K2PgStatement handle);

// DELETE ------------------------------------------------------------------------------------------
K2PgStatus PgGate_NewDelete(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

K2PgStatus PgGate_ExecDelete(K2PgStatement handle);

// Colocated TRUNCATE ------------------------------------------------------------------------------
K2PgStatus PgGate_NewTruncateColocated(K2PgOid database_oid,
                                    K2PgOid table_oid,
                                    bool is_single_row_txn,
                                    K2PgStatement *handle);

K2PgStatus PgGate_ExecTruncateColocated(K2PgStatement handle);

// SELECT ------------------------------------------------------------------------------------------
K2PgStatus PgGate_NewSelect(K2PgOid database_oid,
                         K2PgOid table_oid,
                         const K2PgPrepareParameters *prepare_params,
                         K2PgStatement *handle);

// Set forward/backward scan direction.
K2PgStatus PgGate_SetForwardScan(K2PgStatement handle, bool is_forward_scan);

K2PgStatus PgGate_ExecSelect(K2PgStatement handle, const K2PgExecParameters *exec_params);

// Transaction control -----------------------------------------------------------------------------
K2PgStatus PgGate_BeginTransaction();
K2PgStatus PgGate_RestartTransaction();
K2PgStatus PgGate_CommitTransaction();
K2PgStatus PgGate_AbortTransaction();
K2PgStatus PgGate_SetTransactionIsolationLevel(int isolation);
K2PgStatus PgGate_SetTransactionReadOnly(bool read_only);
K2PgStatus PgGate_SetTransactionDeferrable(bool deferrable);
K2PgStatus PgGate_EnterSeparateDdlTxnMode();
K2PgStatus PgGate_ExitSeparateDdlTxnMode(bool success);

//--------------------------------------------------------------------------------------------------
// Expressions.

// Column references.
K2PgStatus PgGate_NewColumnRef(K2PgStatement stmt, int attr_num, const K2PgTypeEntity *type_entity,
                            const K2PgTypeAttrs *type_attrs, K2PgExpr *expr_handle);

// Constant expressions.
K2PgStatus PgGate_NewConstant(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle);
K2PgStatus PgGate_NewConstantOp(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle, bool is_gt);

// The following update functions only work for constants.
// Overwriting the constant expression with new value.
K2PgStatus PgGate_UpdateConstInt2(K2PgExpr expr, int16_t value, bool is_null);
K2PgStatus PgGate_UpdateConstInt4(K2PgExpr expr, int32_t value, bool is_null);
K2PgStatus PgGate_UpdateConstInt8(K2PgExpr expr, int64_t value, bool is_null);
K2PgStatus PgGate_UpdateConstFloat4(K2PgExpr expr, float value, bool is_null);
K2PgStatus PgGate_UpdateConstFloat8(K2PgExpr expr, double value, bool is_null);
K2PgStatus PgGate_UpdateConstText(K2PgExpr expr, const char *value, bool is_null);
K2PgStatus PgGate_UpdateConstChar(K2PgExpr expr, const char *value, int64_t bytes, bool is_null);

// Expressions with operators "=", "+", "between", "in", ...
K2PgStatus PgGate_NewOperator(K2PgStatement stmt, const char *opname,
                           const K2PgTypeEntity *type_entity,
                           K2PgExpr *op_handle);
K2PgStatus PgGate_OperatorAppendArg(K2PgExpr op_handle, K2PgExpr arg);

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool PgGate_ForeignKeyReferenceExists(K2PgOid table_oid, const char* k2pgctid, int64_t k2pgctid_size);

// Add an entry to foreign key reference cache.
K2PgStatus PgGate_CacheForeignKeyReference(K2PgOid table_oid, const char* k2pgctid, int64_t k2pgctid_size);

// Delete an entry from foreign key reference cache.
K2PgStatus PgGate_DeleteFromForeignKeyReferenceCache(K2PgOid table_oid, uint64_t k2pgctid);

void PgGate_ClearForeignKeyReferenceCache();

bool PgGate_IsInitDbModeEnvVarSet();

// This is called by initdb. Used to customize some behavior.
void PgGate_InitFlags();

// Retrieves value of ysql_max_read_restart_attempts gflag
int32_t PgGate_GetMaxReadRestartAttempts();

// Retrieves value of ysql_output_buffer_size gflag
int32_t PgGate_GetOutputBufferSize();

// Retrieve value of ysql_disable_index_backfill gflag.
bool PgGate_GetDisableIndexBackfill();

bool PgGate_IsK2PgEnabled();

// Sets the specified timeout in the rpc service.
void PgGate_SetTimeout(int timeout_ms, void* extra);

//--------------------------------------------------------------------------------------------------
// Thread-Local variables.

void* PgGate_GetThreadLocalCurrentMemoryContext();

void* PgGate_SetThreadLocalCurrentMemoryContext(void *memctx);

void PgGate_ResetCurrentMemCtxThreadLocalVars();

void* PgGate_GetThreadLocalStrTokPtr();

void PgGate_SetThreadLocalStrTokPtr(char *new_pg_strtok_ptr);

void* PgGate_SetThreadLocalJumpBuffer(void* new_buffer);

void* PgGate_GetThreadLocalJumpBuffer();

void PgGate_SetThreadLocalErrMsg(const void* new_msg);

const void* PgGate_GetThreadLocalErrMsg();

// APIs called by pg_dump.c only
void PgGate_ShutdownPgGateBackend();

K2PgStatus PgGate_InitPgGateBackend();

#ifdef __cplusplus
}  // extern "C"
#endif
