#include "ybc_pggate.h"

#include "yb/common/ybc_util.h"
#include "yb/common/env.h"
#include "k23si.h"

k2gate::K23SIGate* k23si;
std::atomic<bool> k23si_shutdown_done;

void YBCInitPgGate(const YBCPgTypeEntity *YBCDataTypeTable, int count, PgCallbacks pg_callbacks) {
    CHECK(k23si == nullptr) << ": " << __PRETTY_FUNCTION__ << " can only be called once";

    k23si_shutdown_done.exchange(false);
    k23si = new k2gate::K23SIGate();

    VLOG(1) << "K23SI gate open";
}

void YBCDestroyPgGate() {
    if (k23si_shutdown_done.exchange(true)) {
        LOG(DFATAL) << __PRETTY_FUNCTION__ << " should only be called once";
    } else {
        k2gate::K23SIGate *local_k23si = k23si;
        k23si = nullptr;  // YBCPgIsYugaByteEnabled() must return false from now on.
        delete local_k23si;
        VLOG(1) << __PRETTY_FUNCTION__ << "K23SI gate closed";
    }
}

// Initialize a session to process statements that come from the same client connection.
YBCStatus YBCPgInitSession(const YBCPgEnv pg_env, const char *database_name){
    return YBCStatusOK();
}

// Initialize YBCPgMemCtx.
// - Postgres uses memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
// - There YugaByte objects are bound to Postgres operations. All of these objects' allocated
//   memory will be held by YBCPgMemCtx, whose handle belongs to Postgres MemoryContext. Once all
//   Postgres operations are done, associated YugaByte memory context (YBCPgMemCtx) will be
//   destroyed toghether with Postgres memory context.
YBCStatus YBCPgDestroyMemctx(YBCPgMemctx memctx){
    return YBCStatusOK();
}
YBCStatus YBCPgResetMemctx(YBCPgMemctx memctx){
    return YBCStatusOK();
}

// Invalidate the sessions table cache.
YBCStatus YBCPgInvalidateCache(){
    return YBCStatusOK();
}

// Clear all values and expressions that were bound to the given statement.
YBCStatus YBCPgClearBinds(YBCPgStatement handle){
    return YBCStatusOK();
}

// Check if initdb has been already run.
YBCStatus YBCPgIsInitDbDone(bool* initdb_done){
    return YBCStatusOK();
}

// Sets catalog_version to the local tserver's catalog version stored in shared
// memory, or an error if the shared memory has not been initialized (e.g. in initdb).
YBCStatus YBCGetSharedCatalogVersion(uint64_t* catalog_version){
    return YBCStatusOK();
}

//--------------------------------------------------------------------------------------------------
// DDL Statements
//--------------------------------------------------------------------------------------------------

// DATABASE ----------------------------------------------------------------------------------------
// Connect database. Switch the connected database to the given "database_name".
YBCStatus YBCPgConnectDatabase(const char *database_name){
    return YBCStatusOK();
}

// Get whether the given database is colocated.
YBCStatus YBCPgIsDatabaseColocated(const YBCPgOid database_oid, bool *colocated){
    return YBCStatusOK();
}

YBCStatus YBCInsertSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called){
                                     return YBCStatusOK();
                                 }

YBCStatus YBCUpdateSequenceTupleConditionally(int64_t db_oid,
                                              int64_t seq_oid,
                                              uint64_t ysql_catalog_version,
                                              int64_t last_val,
                                              bool is_called,
                                              int64_t expected_last_val,
                                              bool expected_is_called,
                                              bool *skipped){
                                                  return YBCStatusOK();
                                              }

YBCStatus YBCUpdateSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called,
                                 bool* skipped){
                                     return YBCStatusOK();
                                 }

YBCStatus YBCReadSequenceTuple(int64_t db_oid,
                               int64_t seq_oid,
                               uint64_t ysql_catalog_version,
                               int64_t *last_val,
                               bool *is_called){
                                   return YBCStatusOK();
                               }

YBCStatus YBCDeleteSequenceTuple(int64_t db_oid, int64_t seq_oid){
    return YBCStatusOK();
}

// Create database.
YBCStatus YBCPgNewCreateDatabase(const char *database_name,
                                 YBCPgOid database_oid,
                                 YBCPgOid source_database_oid,
                                 YBCPgOid next_oid,
                                 const bool colocated,
                                 YBCPgStatement *handle){
                                     return YBCStatusOK();
                                 }
YBCStatus YBCPgExecCreateDatabase(YBCPgStatement handle){
    return YBCStatusOK();
}

// Drop database.
YBCStatus YBCPgNewDropDatabase(const char *database_name,
                               YBCPgOid database_oid,
                               YBCPgStatement *handle){
                                   return YBCStatusOK();
                               }
YBCStatus YBCPgExecDropDatabase(YBCPgStatement handle){
    return YBCStatusOK();
}

// Alter database.
YBCStatus YBCPgNewAlterDatabase(const char *database_name,
                               YBCPgOid database_oid,
                               YBCPgStatement *handle){
                                   return YBCStatusOK();
                               }
YBCStatus YBCPgAlterDatabaseRenameDatabase(YBCPgStatement handle, const char *newname){
    return YBCStatusOK();
}
YBCStatus YBCPgExecAlterDatabase(YBCPgStatement handle){
    return YBCStatusOK();
}

// Reserve oids.
YBCStatus YBCPgReserveOids(YBCPgOid database_oid,
                           YBCPgOid next_oid,
                           uint32_t count,
                           YBCPgOid *begin_oid,
                           YBCPgOid *end_oid){
                               return YBCStatusOK();
                           }

YBCStatus YBCPgGetCatalogMasterVersion(uint64_t *version){
    return YBCStatusOK();
}

YBCStatus YBCPgInvalidateTableCacheByTableId(const char *table_id){
    return YBCStatusOK();
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
                              YBCPgStatement *handle){
                                  return YBCStatusOK();
                              }

YBCStatus YBCPgCreateTableAddColumn(YBCPgStatement handle, const char *attr_name, int attr_num,
                                    const YBCPgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first){
                                        return YBCStatusOK();
                                    }

YBCStatus YBCPgCreateTableSetNumTablets(YBCPgStatement handle, int32_t num_tablets){
    return YBCStatusOK();
}

YBCStatus YBCPgCreateTableAddSplitRow(YBCPgStatement handle, int num_cols,
                                        YBCPgTypeEntity **types, uint64_t *data){
                                            return YBCStatusOK();
                                        }

YBCStatus YBCPgExecCreateTable(YBCPgStatement handle){
    return YBCStatusOK();
}

YBCStatus YBCPgNewAlterTable(YBCPgOid database_oid,
                             YBCPgOid table_oid,
                             YBCPgStatement *handle){
                                 return YBCStatusOK();
                             }

YBCStatus YBCPgAlterTableAddColumn(YBCPgStatement handle, const char *name, int order,
                                   const YBCPgTypeEntity *attr_type, bool is_not_null){
                                       return YBCStatusOK();
                                   }

YBCStatus YBCPgAlterTableRenameColumn(YBCPgStatement handle, const char *oldname,
                                      const char *newname){
                                          return YBCStatusOK();
                                      }

YBCStatus YBCPgAlterTableDropColumn(YBCPgStatement handle, const char *name){
    return YBCStatusOK();
}

YBCStatus YBCPgAlterTableRenameTable(YBCPgStatement handle, const char *db_name,
                                     const char *newname){
                                         return YBCStatusOK();
                                     }

YBCStatus YBCPgExecAlterTable(YBCPgStatement handle){
    return YBCStatusOK();
}

YBCStatus YBCPgNewDropTable(YBCPgOid database_oid,
                            YBCPgOid table_oid,
                            bool if_exist,
                            YBCPgStatement *handle){
                                return YBCStatusOK();
                            }

YBCStatus YBCPgExecDropTable(YBCPgStatement handle){
    return YBCStatusOK();
}

YBCStatus YBCPgNewTruncateTable(YBCPgOid database_oid,
                                YBCPgOid table_oid,
                                YBCPgStatement *handle){
                                    return YBCStatusOK();
                                }

YBCStatus YBCPgExecTruncateTable(YBCPgStatement handle){
    return YBCStatusOK();
}

YBCStatus YBCPgGetTableDesc(YBCPgOid database_oid,
                            YBCPgOid table_oid,
                            YBCPgTableDesc *handle){
                                return YBCStatusOK();
                            }

YBCStatus YBCPgGetColumnInfo(YBCPgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary,
                             bool *is_hash){
                                 return YBCStatusOK();
                             }

YBCStatus YBCPgGetTableProperties(YBCPgTableDesc table_desc,
                                  YBCPgTableProperties *properties){
                                      return YBCStatusOK();
                                  }

YBCStatus YBCPgDmlModifiesRow(YBCPgStatement handle, bool *modifies_row){
    return YBCStatusOK();
}

YBCStatus YBCPgSetIsSysCatalogVersionChange(YBCPgStatement handle){
    return YBCStatusOK();
}

YBCStatus YBCPgSetCatalogCacheVersion(YBCPgStatement handle, uint64_t catalog_cache_version){
    return YBCStatusOK();
}

YBCStatus YBCPgIsTableColocated(const YBCPgOid database_oid,
                                const YBCPgOid table_oid,
                                bool *colocated){
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
                                  return YBCStatusOK();
                              }

YBCStatus YBCPgCreateIndexAddColumn(YBCPgStatement handle, const char *attr_name, int attr_num,
                                    const YBCPgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first){
                                        return YBCStatusOK();
                                    }

YBCStatus YBCPgCreateIndexSetNumTablets(YBCPgStatement handle, int32_t num_tablets){
    return YBCStatusOK();
}

YBCStatus YBCPgCreateIndexAddSplitRow(YBCPgStatement handle, int num_cols,
                                      YBCPgTypeEntity **types, uint64_t *data){
                                          return YBCStatusOK();
                                      }

YBCStatus YBCPgExecCreateIndex(YBCPgStatement handle){
    return YBCStatusOK();
}

YBCStatus YBCPgNewDropIndex(YBCPgOid database_oid,
                            YBCPgOid index_oid,
                            bool if_exist,
                            YBCPgStatement *handle){
                                return YBCStatusOK();
                            }

YBCStatus YBCPgExecDropIndex(YBCPgStatement handle){
    return YBCStatusOK();
}

YBCStatus YBCPgWaitUntilIndexPermissionsAtLeast(
    const YBCPgOid database_oid,
    const YBCPgOid table_oid,
    const YBCPgOid index_oid,
    const uint32_t target_index_permissions,
    uint32_t *actual_index_permissions){
        return YBCStatusOK();
    }

YBCStatus YBCPgAsyncUpdateIndexPermissions(
    const YBCPgOid database_oid,
    const YBCPgOid indexed_table_oid){
        return YBCStatusOK();
    }

//--------------------------------------------------------------------------------------------------
// DML statements (select, insert, update, delete, truncate)
//--------------------------------------------------------------------------------------------------

// This function is for specifying the selected or returned expressions.
// - SELECT target_expr1, target_expr2, ...
// - INSERT / UPDATE / DELETE ... RETURNING target_expr1, target_expr2, ...
YBCStatus YBCPgDmlAppendTarget(YBCPgStatement handle, YBCPgExpr target){
    return YBCStatusOK();
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
    return YBCStatusOK();
}
YBCStatus YBCPgDmlBindColumnCondEq(YBCPgStatement handle, int attr_num, YBCPgExpr attr_value){
    return YBCStatusOK();
}
YBCStatus YBCPgDmlBindColumnCondBetween(YBCPgStatement handle, int attr_num, YBCPgExpr attr_value,
    YBCPgExpr attr_value_end){
        return YBCStatusOK();
    }
YBCStatus YBCPgDmlBindColumnCondIn(YBCPgStatement handle, int attr_num, int n_attr_values,
    YBCPgExpr *attr_values){
        return YBCStatusOK();
    }

// Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
YBCStatus YBCPgDmlBindTable(YBCPgStatement handle){
    return YBCStatusOK();
}

// API for SET clause.
YBCStatus YBCPgDmlAssignColumn(YBCPgStatement handle,
                               int attr_num,
                               YBCPgExpr attr_value){
                                   return YBCStatusOK();
                               }

// This function is to fetch the targets in YBCPgDmlAppendTarget() from the rows that were defined
// by YBCPgDmlBindColumn().
YBCStatus YBCPgDmlFetch(YBCPgStatement handle, int32_t natts, uint64_t *values, bool *isnulls,
                        YBCPgSysColumns *syscols, bool *has_data){
                            return YBCStatusOK();
                        }

// Utility method that checks stmt type and calls either exec insert, update, or delete internally.
YBCStatus YBCPgDmlExecWriteOp(YBCPgStatement handle, int32_t *rows_affected_count){
    return YBCStatusOK();
}

// This function returns the tuple id (ybctid) of a Postgres tuple.
YBCStatus YBCPgDmlBuildYBTupleId(YBCPgStatement handle, const YBCPgAttrValueDescriptor *attrs,
                                 int32_t nattrs, uint64_t *ybctid){
                                     return YBCStatusOK();
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
YBCStatus YBCPgStopOperationsBuffering(){
    return YBCStatusOK();
}
YBCStatus YBCPgResetOperationsBuffering(){
    return YBCStatusOK();
}
YBCStatus YBCPgFlushBufferedOperations(){
    return YBCStatusOK();
}

// INSERT ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewInsert(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         bool is_single_row_txn,
                         YBCPgStatement *handle){
                             return YBCStatusOK();
                         }

YBCStatus YBCPgExecInsert(YBCPgStatement handle){
    return YBCStatusOK();
}

YBCStatus YBCPgInsertStmtSetUpsertMode(YBCPgStatement handle){
    return YBCStatusOK();
}

YBCStatus YBCPgInsertStmtSetWriteTime(YBCPgStatement handle, const uint64_t write_time){
    return YBCStatusOK();
}

// UPDATE ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewUpdate(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         bool is_single_row_txn,
                         YBCPgStatement *handle){
                             return YBCStatusOK();
                         }

YBCStatus YBCPgExecUpdate(YBCPgStatement handle){
    return YBCStatusOK();
}

// DELETE ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewDelete(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         bool is_single_row_txn,
                         YBCPgStatement *handle){
                             return YBCStatusOK();
                         }

YBCStatus YBCPgExecDelete(YBCPgStatement handle){
    return YBCStatusOK();
}

// Colocated TRUNCATE ------------------------------------------------------------------------------
YBCStatus YBCPgNewTruncateColocated(YBCPgOid database_oid,
                                    YBCPgOid table_oid,
                                    bool is_single_row_txn,
                                    YBCPgStatement *handle){
                                        return YBCStatusOK();
                                    }

YBCStatus YBCPgExecTruncateColocated(YBCPgStatement handle){
    return YBCStatusOK();
}

// SELECT ------------------------------------------------------------------------------------------
YBCStatus YBCPgNewSelect(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         const YBCPgPrepareParameters *prepare_params,
                         YBCPgStatement *handle){
                             return YBCStatusOK();
                         }

// Set forward/backward scan direction.
YBCStatus YBCPgSetForwardScan(YBCPgStatement handle, bool is_forward_scan){
    return YBCStatusOK();
}

YBCStatus YBCPgExecSelect(YBCPgStatement handle, const YBCPgExecParameters *exec_params){
    return YBCStatusOK();
}

// Transaction control -----------------------------------------------------------------------------
YBCStatus YBCPgBeginTransaction(){
    return YBCStatusOK();
}
YBCStatus YBCPgRestartTransaction(){
    return YBCStatusOK();
}
YBCStatus YBCPgCommitTransaction(){
    return YBCStatusOK();
}
YBCStatus YBCPgAbortTransaction(){
    return YBCStatusOK();
}
YBCStatus YBCPgSetTransactionIsolationLevel(int isolation){
    return YBCStatusOK();
}
YBCStatus YBCPgSetTransactionReadOnly(bool read_only){
    return YBCStatusOK();
}
YBCStatus YBCPgSetTransactionDeferrable(bool deferrable){
    return YBCStatusOK();
}
YBCStatus YBCPgEnterSeparateDdlTxnMode(){
    return YBCStatusOK();
}
YBCStatus YBCPgExitSeparateDdlTxnMode(bool success){
    return YBCStatusOK();
}

//--------------------------------------------------------------------------------------------------
// Expressions.

// Column references.
YBCStatus YBCPgNewColumnRef(YBCPgStatement stmt, int attr_num, const YBCPgTypeEntity *type_entity,
                            const YBCPgTypeAttrs *type_attrs, YBCPgExpr *expr_handle){
                                return YBCStatusOK();
                            }

// Constant expressions.
YBCStatus YBCPgNewConstant(YBCPgStatement stmt, const YBCPgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, YBCPgExpr *expr_handle){
                               return YBCStatusOK();
                           }
YBCStatus YBCPgNewConstantOp(YBCPgStatement stmt, const YBCPgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, YBCPgExpr *expr_handle, bool is_gt){
                               return YBCStatusOK();
                           }

// The following update functions only work for constants.
// Overwriting the constant expression with new value.
YBCStatus YBCPgUpdateConstInt2(YBCPgExpr expr, int16_t value, bool is_null){
    return YBCStatusOK();
}
YBCStatus YBCPgUpdateConstInt4(YBCPgExpr expr, int32_t value, bool is_null){
    return YBCStatusOK();
}
YBCStatus YBCPgUpdateConstInt8(YBCPgExpr expr, int64_t value, bool is_null){
    return YBCStatusOK();
}
YBCStatus YBCPgUpdateConstFloat4(YBCPgExpr expr, float value, bool is_null){
    return YBCStatusOK();
}
YBCStatus YBCPgUpdateConstFloat8(YBCPgExpr expr, double value, bool is_null){
    return YBCStatusOK();
}
YBCStatus YBCPgUpdateConstText(YBCPgExpr expr, const char *value, bool is_null){
    return YBCStatusOK();
}
YBCStatus YBCPgUpdateConstChar(YBCPgExpr expr, const char *value, int64_t bytes, bool is_null){
    return YBCStatusOK();
}

// Expressions with operators "=", "+", "between", "in", ...
YBCStatus YBCPgNewOperator(YBCPgStatement stmt, const char *opname,
                           const YBCPgTypeEntity *type_entity,
                           YBCPgExpr *op_handle){
                               return YBCStatusOK();
                           }
YBCStatus YBCPgOperatorAppendArg(YBCPgExpr op_handle, YBCPgExpr arg){
    return YBCStatusOK();
}

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool YBCForeignKeyReferenceExists(YBCPgOid table_id, const char* ybctid, int64_t ybctid_size) {
    return false;
}

// Add an entry to foreign key reference cache.
YBCStatus YBCCacheForeignKeyReference(YBCPgOid table_id, const char* ybctid, int64_t ybctid_size){
    return YBCStatusOK();
}

// Delete an entry from foreign key reference cache.
YBCStatus YBCPgDeleteFromForeignKeyReferenceCache(YBCPgOid table_id, uint64_t ybctid){
    return YBCStatusOK();
}

void ClearForeignKeyReferenceCache() {

}

bool YBCIsInitDbModeEnvVarSet() {
    return false;
}

// This is called by initdb. Used to customize some behavior.
void YBCInitFlags() {

}

// Retrieves value of ysql_max_read_restart_attempts gflag
int32_t YBCGetMaxReadRestartAttempts() {
    return 0;
}

// Retrieves value of ysql_output_buffer_size gflag
int32_t YBCGetOutputBufferSize() {
    return 0;
}

// Retrieve value of ysql_disable_index_backfill gflag.
bool YBCGetDisableIndexBackfill() {
    return false;
}

bool YBCPgIsYugaByteEnabled() {
    return k23si != nullptr;
}

// Sets the specified timeout in the rpc service.
void YBCSetTimeout(int timeout_ms, void* extra) {
}

//--------------------------------------------------------------------------------------------------
// Thread-Local variables.

void* YBCPgGetThreadLocalCurrentMemoryContext() {
    return 0;
}

void* YBCPgSetThreadLocalCurrentMemoryContext(void *memctx) {
    return 0;
}

void YBCPgResetCurrentMemCtxThreadLocalVars() {
}

void* YBCPgGetThreadLocalStrTokPtr() {
    return 0;
}

void YBCPgSetThreadLocalStrTokPtr(char *new_pg_strtok_ptr) {
}

void* YBCPgSetThreadLocalJumpBuffer(void* new_buffer) {
    return 0;
}

void* YBCPgGetThreadLocalJumpBuffer() {
    return 0;
}

void YBCPgSetThreadLocalErrMsg(const void* new_msg) {
}

const void* YBCPgGetThreadLocalErrMsg() {
    return 0;
}

const YBCPgTypeEntity *YBCPgFindTypeEntity(int type_oid) {
    return 0;
}

YBCPgDataType YBCPgGetType(const YBCPgTypeEntity *type_entity) {
  return YB_YQL_DATA_TYPE_UNKNOWN_DATA;
}

bool YBCPgAllowForPrimaryKey(const YBCPgTypeEntity *type_entity) {
  return false;
}

YBCStatus YBCInit(const char* argv0,
                  YBCPAllocFn palloc_fn,
                  YBCCStringToTextWithLenFn cstring_to_text_with_len_fn) {
    return YBCStatusOK();
}

YBCPgMemctx YBCPgCreateMemctx() {
  return 0;
}

extern "C" {
void YBCPgDropBufferedOperations() {
}

void YBCPgStartOperationsBuffering() {
}

void YBCAssignTransactionPriorityLowerBound(double newval, void* extra) {
}

void YBCAssignTransactionPriorityUpperBound(double newval, void* extra) {
}

// Setup the master IP(s) before calling YBCInitPgGate().
void YBCSetMasterAddresses(const char* hosts) {
}

YBCStatus YBCInitPgGateBackend() {
    return YBCStatusOK();
}

void YBCShutdownPgGateBackend() {
}

}

yb::Env* yb::Env::Default() {
  return nullptr;
}
