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
YBCStatus YBCPgCreateEnv(K2PgEnv *pg_env);
YBCStatus YBCPgDestroyEnv(K2PgEnv pg_env);

// Initialize a session to process statements that come from the same client connection.
YBCStatus YBCPgInitSession(const K2PgEnv pg_env, const char *database_name);

// Initialize YBCPgMemCtx.
// - Postgres uses memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
// - There YugaByte objects are bound to Postgres operations. All of these objects' allocated
//   memory will be held by YBCPgMemCtx, whose handle belongs to Postgres MemoryContext. Once all
//   Postgres operations are done, associated YugaByte memory context (YBCPgMemCtx) will be
//   destroyed toghether with Postgres memory context.
K2PgMemctx YBCPgCreateMemctx();
YBCStatus YBCPgDestroyMemctx(K2PgMemctx memctx);
YBCStatus YBCPgResetMemctx(K2PgMemctx memctx);

// Invalidate the sessions table cache.
YBCStatus YBCPgInvalidateCache();

// Clear all values and expressions that were bound to the given statement.
YBCStatus YBCPgClearBinds(K2PgStatement handle);

// Check if initdb has been already run.
YBCStatus YBCPgIsInitDbDone(bool* initdb_done);

// Sets catalog_version to the local tserver's catalog version stored in shared
// memory, or an error if the shared memory has not been initialized (e.g. in initdb).
YBCStatus YBCGetSharedCatalogVersion(uint64_t* catalog_version);

//--------------------------------------------------------------------------------------------------
// DDL Statements
//--------------------------------------------------------------------------------------------------

// DATABASE ----------------------------------------------------------------------------------------
// Connect database. Switch the connected database to the given "database_name".
YBCStatus YBCPgConnectDatabase(const char *database_name);

// Get whether the given database is colocated.
YBCStatus YBCPgIsDatabaseColocated(const K2PgOid database_oid, bool *colocated);

YBCStatus YBCInsertSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called);

YBCStatus YBCUpdateSequenceTupleConditionally(int64_t db_oid,
                                              int64_t seq_oid,
                                              uint64_t ysql_catalog_version,
                                              int64_t last_val,
                                              bool is_called,
                                              int64_t expected_last_val,
                                              bool expected_is_called,
                                              bool *skipped);

YBCStatus YBCUpdateSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called,
                                 bool* skipped);

YBCStatus YBCReadSequenceTuple(int64_t db_oid,
                               int64_t seq_oid,
                               uint64_t ysql_catalog_version,
                               int64_t *last_val,
                               bool *is_called);

YBCStatus YBCDeleteSequenceTuple(int64_t db_oid, int64_t seq_oid);

// K2 InitPrimaryCluster
YBCStatus K2PGInitPrimaryCluster();

YBCStatus K2PGFinishInitDB();

// Create database.
YBCStatus YBCPgNewCreateDatabase(const char *database_name,
                                 K2PgOid database_oid,
                                 K2PgOid source_database_oid,
                                 K2PgOid next_oid,
                                 const bool colocated,
                                 K2PgStatement *handle);
YBCStatus YBCPgExecCreateDatabase(K2PgStatement handle);

// Drop database.
YBCStatus YBCPgNewDropDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle);
YBCStatus YBCPgExecDropDatabase(K2PgStatement handle);

// Alter database.
YBCStatus YBCPgNewAlterDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle);
YBCStatus YBCPgAlterDatabaseRenameDatabase(K2PgStatement handle, const char *new_name);
YBCStatus YBCPgExecAlterDatabase(K2PgStatement handle);

// Reserve oids.
YBCStatus YBCPgReserveOids(K2PgOid database_oid,
                           K2PgOid next_oid,
                           uint32_t count,
                           K2PgOid *begin_oid,
                           K2PgOid *end_oid);

YBCStatus YBCPgGetCatalogMasterVersion(uint64_t *version);

void YBCPgInvalidateTableCache(
    const K2PgOid database_oid,
    const K2PgOid table_oid);

YBCStatus YBCPgInvalidateTableCacheByTableId(const char *table_uuid);

// TABLE -------------------------------------------------------------------------------------------
// Create and drop table "database_name.schema_name.table_name()".
// - When "schema_name" is NULL, the table "database_name.table_name" is created.
// - When "database_name" is NULL, the table "connected_database_name.table_name" is created.
YBCStatus YBCPgNewCreateTable(const char *database_name,
                              const char *schema_name,
                              const char *table_name,
                              K2PgOid database_oid,
                              K2PgOid table_oid,
                              bool is_shared_table,
                              bool if_not_exist,
                              bool add_primary_key,
                              const bool colocated,
                              K2PgStatement *handle);

YBCStatus YBCPgCreateTableAddColumn(K2PgStatement handle, const char *attr_name, int attr_num,
                                    const K2PgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first);

YBCStatus YBCPgExecCreateTable(K2PgStatement handle);

YBCStatus YBCPgNewAlterTable(K2PgOid database_oid,
                             K2PgOid table_oid,
                             K2PgStatement *handle);

YBCStatus YBCPgAlterTableAddColumn(K2PgStatement handle, const char *name, int order,
                                   const K2PgTypeEntity *attr_type, bool is_not_null);

YBCStatus YBCPgAlterTableRenameColumn(K2PgStatement handle, const char *oldname,
                                      const char *newname);

YBCStatus YBCPgAlterTableDropColumn(K2PgStatement handle, const char *name);

YBCStatus YBCPgAlterTableRenameTable(K2PgStatement handle, const char *db_name,
                                     const char *newname);

YBCStatus YBCPgExecAlterTable(K2PgStatement handle);

YBCStatus YBCPgNewDropTable(K2PgOid database_oid,
                            K2PgOid table_oid,
                            bool if_exist,
                            K2PgStatement *handle);

YBCStatus YBCPgExecDropTable(K2PgStatement handle);

YBCStatus YBCPgNewTruncateTable(K2PgOid database_oid,
                                K2PgOid table_oid,
                                K2PgStatement *handle);

YBCStatus YBCPgExecTruncateTable(K2PgStatement handle);

YBCStatus YBCPgGetTableDesc(K2PgOid database_oid,
                            K2PgOid table_oid,
                            K2PgTableDesc *handle);

YBCStatus YBCPgGetColumnInfo(K2PgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary,
                             bool *is_hash);

YBCStatus YBCPgGetTableProperties(K2PgTableDesc table_desc,
                                  K2PgTableProperties *properties);

YBCStatus YBCPgDmlModifiesRow(K2PgStatement handle, bool *modifies_row);

YBCStatus YBCPgSetIsSysCatalogVersionChange(K2PgStatement handle);

YBCStatus YBCPgSetCatalogCacheVersion(K2PgStatement handle, uint64_t catalog_cache_version);

YBCStatus YBCPgIsTableColocated(const K2PgOid database_oid,
                                const K2PgOid table_oid,
                                bool *colocated);

// INDEX -------------------------------------------------------------------------------------------
// Create and drop index "database_name.schema_name.index_name()".
// - When "schema_name" is NULL, the index "database_name.index_name" is created.
// - When "database_name" is NULL, the index "connected_database_name.index_name" is created.
YBCStatus YBCPgNewCreateIndex(const char *database_name,
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

YBCStatus YBCPgCreateIndexAddColumn(K2PgStatement handle, const char *attr_name, int attr_num,
                                    const K2PgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first);

YBCStatus YBCPgExecCreateIndex(K2PgStatement handle);

YBCStatus YBCPgNewDropIndex(K2PgOid database_oid,
                            K2PgOid index_oid,
                            bool if_exist,
                            K2PgStatement *handle);

YBCStatus YBCPgExecDropIndex(K2PgStatement handle);

YBCStatus YBCPgWaitUntilIndexPermissionsAtLeast(
    const K2PgOid database_oid,
    const K2PgOid table_oid,
    const K2PgOid index_oid,
    const uint32_t target_index_permissions,
    uint32_t *actual_index_permissions);

YBCStatus YBCPgAsyncUpdateIndexPermissions(
    const K2PgOid database_oid,
    const K2PgOid indexed_table_oid);

//--------------------------------------------------------------------------------------------------
// DML statements (select, insert, update, delete, truncate)
//--------------------------------------------------------------------------------------------------

// This function is for specifying the selected or returned expressions.
// - SELECT target_expr1, target_expr2, ...
// - INSERT / UPDATE / DELETE ... RETURNING target_expr1, target_expr2, ...
YBCStatus YBCPgDmlAppendTarget(K2PgStatement handle, K2PgExpr target);

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
YBCStatus YBCPgDmlBindColumn(K2PgStatement handle, int attr_num, K2PgExpr attr_value);
YBCStatus YBCPgDmlBindColumnCondEq(K2PgStatement handle, int attr_num, K2PgExpr attr_value);
YBCStatus YBCPgDmlBindColumnCondBetween(K2PgStatement handle, int attr_num, K2PgExpr attr_value,
    K2PgExpr attr_value_end);
YBCStatus YBCPgDmlBindColumnCondIn(K2PgStatement handle, int attr_num, int n_attr_values,
    K2PgExpr *attr_values);

// bind range condition so as to derive key prefix
YBCStatus PgDmlBindRangeConds(K2PgStatement handle, K2PgExpr where_conds);

// bind where clause for a DML operation
YBCStatus PgDmlBindWhereConds(K2PgStatement handle, K2PgExpr where_conds);

// Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
YBCStatus YBCPgDmlBindTable(K2PgStatement handle);

// API for SET clause.
YBCStatus YBCPgDmlAssignColumn(K2PgStatement handle,
                               int attr_num,
                               K2PgExpr attr_value);

// This function is to fetch the targets in YBCPgDmlAppendTarget() from the rows that were defined
// by YBCPgDmlBindColumn().
YBCStatus YBCPgDmlFetch(K2PgStatement handle, int32_t natts, uint64_t *values, bool *isnulls,
                        K2PgSysColumns *syscols, bool *has_data);

// Utility method that checks stmt type and calls either exec insert, update, or delete internally.
YBCStatus YBCPgDmlExecWriteOp(K2PgStatement handle, int32_t *rows_affected_count);

// This function returns the tuple id (ybctid) of a Postgres tuple.
YBCStatus YBCPgDmlBuildYBTupleId(K2PgStatement handle, const K2PgAttrValueDescriptor *attrs,
                                 int32_t nattrs, uint64_t *ybctid);

// DB Operations: WHERE(partially supported by K2-SKV)
// TODO: ORDER_BY, GROUP_BY, etc.

// INSERT ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewInsert(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

YBCStatus YBCPgExecInsert(K2PgStatement handle);

YBCStatus YBCPgInsertStmtSetUpsertMode(K2PgStatement handle);

YBCStatus YBCPgInsertStmtSetWriteTime(K2PgStatement handle, const uint64_t write_time);

// UPDATE ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewUpdate(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

YBCStatus YBCPgExecUpdate(K2PgStatement handle);

// DELETE ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewDelete(K2PgOid database_oid,
                         K2PgOid table_oid,
                         bool is_single_row_txn,
                         K2PgStatement *handle);

YBCStatus YBCPgExecDelete(K2PgStatement handle);

// Colocated TRUNCATE ------------------------------------------------------------------------------
YBCStatus YBCPgNewTruncateColocated(K2PgOid database_oid,
                                    K2PgOid table_oid,
                                    bool is_single_row_txn,
                                    K2PgStatement *handle);

YBCStatus YBCPgExecTruncateColocated(K2PgStatement handle);

// SELECT ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewSelect(K2PgOid database_oid,
                         K2PgOid table_oid,
                         const K2PgPrepareParameters *prepare_params,
                         K2PgStatement *handle);

// Set forward/backward scan direction.
YBCStatus YBCPgSetForwardScan(K2PgStatement handle, bool is_forward_scan);

YBCStatus YBCPgExecSelect(K2PgStatement handle, const K2PgExecParameters *exec_params);

// Transaction control -----------------------------------------------------------------------------
YBCStatus YBCPgBeginTransaction();
YBCStatus YBCPgRestartTransaction();
YBCStatus YBCPgCommitTransaction();
YBCStatus YBCPgAbortTransaction();
YBCStatus YBCPgSetTransactionIsolationLevel(int isolation);
YBCStatus YBCPgSetTransactionReadOnly(bool read_only);
YBCStatus YBCPgSetTransactionDeferrable(bool deferrable);
YBCStatus YBCPgEnterSeparateDdlTxnMode();
YBCStatus YBCPgExitSeparateDdlTxnMode(bool success);

//--------------------------------------------------------------------------------------------------
// Expressions.

// Column references.
YBCStatus YBCPgNewColumnRef(K2PgStatement stmt, int attr_num, const K2PgTypeEntity *type_entity,
                            const K2PgTypeAttrs *type_attrs, K2PgExpr *expr_handle);

// Constant expressions.
YBCStatus YBCPgNewConstant(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle);
YBCStatus YBCPgNewConstantOp(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle, bool is_gt);

// The following update functions only work for constants.
// Overwriting the constant expression with new value.
YBCStatus YBCPgUpdateConstInt2(K2PgExpr expr, int16_t value, bool is_null);
YBCStatus YBCPgUpdateConstInt4(K2PgExpr expr, int32_t value, bool is_null);
YBCStatus YBCPgUpdateConstInt8(K2PgExpr expr, int64_t value, bool is_null);
YBCStatus YBCPgUpdateConstFloat4(K2PgExpr expr, float value, bool is_null);
YBCStatus YBCPgUpdateConstFloat8(K2PgExpr expr, double value, bool is_null);
YBCStatus YBCPgUpdateConstText(K2PgExpr expr, const char *value, bool is_null);
YBCStatus YBCPgUpdateConstChar(K2PgExpr expr, const char *value, int64_t bytes, bool is_null);

// Expressions with operators "=", "+", "between", "in", ...
YBCStatus YBCPgNewOperator(K2PgStatement stmt, const char *opname,
                           const K2PgTypeEntity *type_entity,
                           K2PgExpr *op_handle);
YBCStatus YBCPgOperatorAppendArg(K2PgExpr op_handle, K2PgExpr arg);

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool YBCForeignKeyReferenceExists(K2PgOid table_oid, const char* ybctid, int64_t ybctid_size);

// Add an entry to foreign key reference cache.
YBCStatus YBCCacheForeignKeyReference(K2PgOid table_oid, const char* ybctid, int64_t ybctid_size);

// Delete an entry from foreign key reference cache.
YBCStatus YBCPgDeleteFromForeignKeyReferenceCache(K2PgOid table_oid, uint64_t ybctid);

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

YBCStatus YBCInitPgGateBackend();

#ifdef __cplusplus
}  // extern "C"
#endif
