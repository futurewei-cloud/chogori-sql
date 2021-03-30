// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
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
//
// Copyright(c) 2020 Futurewei Cloud
//
// Permission is hereby granted,
//        free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
#ifndef CHOGORI_GATE_API_H
#define CHOGORI_GATE_API_H

#include <algorithm>
#include <functional>
#include <thread>
#include <unordered_map>

#include "common/metrics/metrics.h"
#include "common/sys/mem_tracker.h"
#include "entities/entity_ids.h"
#include "entities/expr.h"
#include "entities/index.h"
#include "pggate/pg_gate_typedefs.h"
#include "pggate/pg_env.h"
#include "pggate/pg_memctx.h"
#include "pggate/pg_tabledesc.h"
#include "pggate/pg_session.h"
#include "pggate/pg_statement.h"
#include "pggate/pg_txn_handler.h"
#include "pggate/k2_adapter.h"
#include "pggate/catalog/sql_catalog_client.h"
#include "pggate/catalog/sql_catalog_manager.h"

namespace k2pg {
namespace gate {

using yb::MemTracker;
using yb::MetricEntity;
using yb::MetricRegistry;
using yb::Status;
using k2pg::sql::PgExpr;
using k2pg::sql::PgObjectId;
using k2pg::sql::PgOid;
using k2pg::sql::catalog::SqlCatalogClient;
using k2pg::sql::catalog::SqlCatalogManager;

//--------------------------------------------------------------------------------------------------
// Implements support for CAPI.
class PgGateApiImpl {
  public:
  PgGateApiImpl(const YBCPgTypeEntity *YBCDataTypeTable, int count, YBCPgCallbacks pg_callbacks);
  virtual ~PgGateApiImpl();

 // Initialize ENV within which PGSQL calls will be executed.
  CHECKED_STATUS CreateEnv(PgEnv **pg_env);
  CHECKED_STATUS DestroyEnv(PgEnv *pg_env);

  // Initialize a session to process statements that come from the same client connection.
  // If database_name is empty, a session is created without connecting to any database.
  CHECKED_STATUS InitSession(const PgEnv *pg_env, const string& database_name);

  // YB Memctx: Create, Destroy, and Reset must be "static" because a few contexts are created
  //            before YugaByte environments including PgGate are created and initialized.
  // Create YB Memctx. Each memctx will be associated with a Postgres's MemoryContext.
  static PgMemctx *CreateMemctx();

  // Destroy YB Memctx.
  static CHECKED_STATUS DestroyMemctx(PgMemctx *memctx);

  // Reset YB Memctx.
  static CHECKED_STATUS ResetMemctx(PgMemctx *memctx);

  // Cache statements in YB Memctx. When Memctx is destroyed, the statement is destructed.
  CHECKED_STATUS AddToCurrentMemctx(const std::shared_ptr<PgStatement> &stmt,
                                      PgStatement **handle);

  // Cache table descriptor in YB Memctx. When Memctx is destroyed, the descriptor is destructed.
  CHECKED_STATUS AddToCurrentMemctx(size_t table_desc_id,
                                      const std::shared_ptr<PgTableDesc> &table_desc);

  // Read table descriptor that was cached in YB Memctx.
  CHECKED_STATUS GetTabledescFromCurrentMemctx(size_t table_desc_id, PgTableDesc **handle);

  // Invalidate the sessions table cache.
  CHECKED_STATUS InvalidateCache();

  Result<bool> IsInitDbDone();

  Result<uint64_t> GetSharedCatalogVersion();

  // Remove all values and expressions that were bound to the given statement.
  CHECKED_STATUS ClearBinds(PgStatement *handle);

  // Search for type_entity.
  const YBCPgTypeEntity *FindTypeEntity(int type_oid);

  //------------------------------------------------------------------------------------------------
  CHECKED_STATUS PGInitPrimaryCluster();

  CHECKED_STATUS PGFinishInitDB();

  //------------------------------------------------------------------------------------------------
  // Connect database. Switch the connected database to the given "database_name".
  CHECKED_STATUS ConnectDatabase(const char *database_name);

  // Create database.
  CHECKED_STATUS NewCreateDatabase(const char *database_name,
                                   PgOid database_oid,
                                   PgOid source_database_oid,
                                   PgOid next_oid,
                                   PgStatement **handle);

  CHECKED_STATUS ExecCreateDatabase(PgStatement *handle);

  // Drop database.
  CHECKED_STATUS NewDropDatabase(const char *database_name,
                                 PgOid database_oid,
                                 PgStatement **handle);
  CHECKED_STATUS ExecDropDatabase(PgStatement *handle);

  // Alter database.
  CHECKED_STATUS NewAlterDatabase(const char *database_name,
                                 PgOid database_oid,
                                 PgStatement **handle);
  CHECKED_STATUS AlterDatabaseRenameDatabase(PgStatement *handle, const char *new_name);

  CHECKED_STATUS ExecAlterDatabase(PgStatement *handle);

  // Reserve oids.
  CHECKED_STATUS ReserveOids(PgOid database_oid,
                             PgOid next_oid,
                             uint32_t count,
                             PgOid *begin_oid,
                             PgOid *end_oid);

  CHECKED_STATUS GetCatalogMasterVersion(uint64_t *version);

  // Load table.
  Result<std::shared_ptr<PgTableDesc>> LoadTable(const PgObjectId& table_object_id);

  // Invalidate the cache entry corresponding to table_object_id from the PgSession table cache.
  void InvalidateTableCache(const PgObjectId& table_object_id);

  //------------------------------------------------------------------------------------------------
  // Create, alter and drop table.
  CHECKED_STATUS NewCreateTable(const char *database_name,
                                const char *schema_name,
                                const char *table_name,
                                const PgObjectId& table_object_id,
                                bool is_shared_table,
                                bool if_not_exist,
                                bool add_primary_key,
                                PgStatement **handle);

  CHECKED_STATUS CreateTableAddColumn(PgStatement *handle, const char *attr_name, int attr_num,
                                      const YBCPgTypeEntity *attr_type, bool is_hash,
                                      bool is_range, bool is_desc, bool is_nulls_first);

  CHECKED_STATUS ExecCreateTable(PgStatement *handle);

  CHECKED_STATUS NewAlterTable(const PgObjectId& table_object_id,
                               PgStatement **handle);

  CHECKED_STATUS AlterTableAddColumn(PgStatement *handle, const char *name,
                                     int order, const YBCPgTypeEntity *attr_type, bool is_not_null);

  CHECKED_STATUS AlterTableRenameColumn(PgStatement *handle, const char *oldname,
                                        const char *newname);

  CHECKED_STATUS AlterTableDropColumn(PgStatement *handle, const char *name);

  CHECKED_STATUS AlterTableRenameTable(PgStatement *handle, const char *db_name,
                                       const char *newname);

  CHECKED_STATUS ExecAlterTable(PgStatement *handle);

  CHECKED_STATUS NewDropTable(const PgObjectId& table_object_id,
                              bool if_exist,
                              PgStatement **handle);

  CHECKED_STATUS ExecDropTable(PgStatement *handle);

  CHECKED_STATUS GetTableDesc(const PgObjectId& table_object_id,
                              PgTableDesc **handle);

  CHECKED_STATUS GetColumnInfo(PgTableDesc* table_desc,
                               int16_t attr_number,
                               bool *is_primary,
                               bool *is_hash);

  CHECKED_STATUS DmlModifiesRow(PgStatement *handle, bool *modifies_row);

  CHECKED_STATUS SetIsSysCatalogVersionChange(PgStatement *handle);

  CHECKED_STATUS SetCatalogCacheVersion(PgStatement *handle, uint64_t catalog_cache_version);

  // Index --------------------------------------------------------------------------------------------

  // Create and drop index.
  CHECKED_STATUS NewCreateIndex(const char *database_name,
                                const char *schema_name,
                                const char *index_name,
                                const PgObjectId& index_object_id,
                                const PgObjectId& table_object_id,
                                bool is_shared_index,
                                bool is_unique_index,
                                const bool skip_index_backfill,
                                bool if_not_exist,
                                PgStatement **handle);

  CHECKED_STATUS CreateIndexAddColumn(PgStatement *handle, const char *attr_name, int attr_num,
                                      const YBCPgTypeEntity *attr_type, bool is_hash,
                                      bool is_range, bool is_desc, bool is_nulls_first);

  CHECKED_STATUS ExecCreateIndex(PgStatement *handle);

  CHECKED_STATUS NewDropIndex(const PgObjectId& index_id,
                              bool if_exist,
                              PgStatement **handle);

  CHECKED_STATUS ExecDropIndex(PgStatement *handle);

  Result<IndexPermissions> WaitUntilIndexPermissionsAtLeast(
      const PgObjectId& table_object_id,
      const PgObjectId& index_object_id,
      const IndexPermissions& target_index_permissions);

  CHECKED_STATUS AsyncUpdateIndexPermissions(const PgObjectId& indexed_table_object_id);

  // Sequence Operations -----------------------------------------------------------------------------

  // Setup the table to store sequences data.
  CHECKED_STATUS CreateSequencesDataTable();

  CHECKED_STATUS InsertSequenceTuple(int64_t db_oid,
                                     int64_t seq_oid,
                                     uint64_t ysql_catalog_version,
                                     int64_t last_val,
                                     bool is_called);

  CHECKED_STATUS UpdateSequenceTupleConditionally(int64_t db_oid,
                                                  int64_t seq_oid,
                                                  uint64_t ysql_catalog_version,
                                                  int64_t last_val,
                                                  bool is_called,
                                                  int64_t expected_last_val,
                                                  bool expected_is_called,
                                                  bool *skipped);

  CHECKED_STATUS UpdateSequenceTuple(int64_t db_oid,
                                     int64_t seq_oid,
                                     uint64_t ysql_catalog_version,
                                     int64_t last_val,
                                     bool is_called,
                                     bool* skipped);

  CHECKED_STATUS ReadSequenceTuple(int64_t db_oid,
                                   int64_t seq_oid,
                                   uint64_t ysql_catalog_version,
                                   int64_t *last_val,
                                   bool *is_called);

  CHECKED_STATUS DeleteSequenceTuple(int64_t db_oid, int64_t seq_oid);

  //------------------------------------------------------------------------------------------------
  // All DML statements
  CHECKED_STATUS DmlAppendTarget(PgStatement *handle, PgExpr *expr);

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
  CHECKED_STATUS DmlBindColumn(PgStatement *handle, int attr_num, PgExpr *attr_value);
  CHECKED_STATUS DmlBindColumnCondEq(PgStatement *handle, int attr_num, PgExpr *attr_value);
  CHECKED_STATUS DmlBindColumnCondBetween(PgStatement *handle, int attr_num, PgExpr *attr_value,
      PgExpr *attr_value_end);
  CHECKED_STATUS DmlBindColumnCondIn(PgStatement *handle, int attr_num, int n_attr_values,
      PgExpr **attr_value);

  CHECKED_STATUS DmlBindRangeConds(PgStatement *handle, PgExpr *range_conds);

  CHECKED_STATUS DmlBindWhereConds(PgStatement *handle, PgExpr *where_conds);

  // Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
  CHECKED_STATUS DmlBindTable(PgStatement *handle);

  // API for SET clause.
  CHECKED_STATUS DmlAssignColumn(PgStatement *handle, int attr_num, PgExpr *attr_value);

  // This function is to fetch the targets in YBCPgDmlAppendTarget() from the rows that were defined
  // by YBCPgDmlBindColumn().
  CHECKED_STATUS DmlFetch(PgStatement *handle, int32_t natts, uint64_t *values, bool *isnulls,
                          PgSysColumns *syscols, bool *has_data);

  // Utility method that checks stmt type and calls exec insert, update, or delete internally.
  CHECKED_STATUS DmlExecWriteOp(PgStatement *handle, int32_t *rows_affected_count);

  // This function adds a primary column to be used in the construction of the tuple id (ybctid).
  CHECKED_STATUS DmlAddYBTupleIdColumn(PgStatement *handle, int attr_num, uint64_t datum,
                                       bool is_null, const YBCPgTypeEntity *type_entity);


  // This function returns the tuple id (ybctid) of a Postgres tuple.
  CHECKED_STATUS DmlBuildYBTupleId(PgStatement *handle, const PgAttrValueDescriptor *attrs,
                                   int32_t nattrs, uint64_t *ybctid);

// DB Operations: WHERE(partially supported by K2-SKV)
// TODO: ORDER_BY, GROUP_BY, etc.

  //------------------------------------------------------------------------------------------------
  // Select.
  CHECKED_STATUS NewSelect(const PgObjectId& table_object_id,
                           const PgObjectId& index_object_id,
                           const PgPrepareParameters *prepare_params,
                           PgStatement **handle);

  CHECKED_STATUS SetForwardScan(PgStatement *handle, bool is_forward_scan);

  CHECKED_STATUS ExecSelect(PgStatement *handle, const PgExecParameters *exec_params);

// INSERT ------------------------------------------------------------------------------------------

  CHECKED_STATUS NewInsert(const PgObjectId& table_object_id,
                           bool is_single_row_txn,
                           PgStatement **handle);

  CHECKED_STATUS ExecInsert(PgStatement *handle);

  CHECKED_STATUS InsertStmtSetUpsertMode(PgStatement *handle);

  CHECKED_STATUS InsertStmtSetWriteTime(PgStatement *handle, const uint64_t write_time);

  //------------------------------------------------------------------------------------------------
  // Update.
  CHECKED_STATUS NewUpdate(const PgObjectId& table_object_id,
                           bool is_single_row_txn,
                           PgStatement **handle);

  CHECKED_STATUS ExecUpdate(PgStatement *handle);

  //------------------------------------------------------------------------------------------------
  // Delete.
  CHECKED_STATUS NewDelete(const PgObjectId& table_object_id,
                           bool is_single_row_txn,
                           PgStatement **handle);

  CHECKED_STATUS ExecDelete(PgStatement *handle);

  //------------------------------------------------------------------------------------------------
  // Expressions.
  //------------------------------------------------------------------------------------------------
  // Column reference.
  CHECKED_STATUS NewColumnRef(PgStatement *stmt, int attr_num, const PgTypeEntity *type_entity,
                              const PgTypeAttrs *type_attrs, PgExpr **expr_handle);

  // Constant expressions.
  CHECKED_STATUS NewConstant(PgStatement *stmt, const YBCPgTypeEntity *type_entity,
                             uint64_t datum, bool is_null, PgExpr **expr_handle);
  CHECKED_STATUS NewConstantOp(PgStatement *stmt, const YBCPgTypeEntity *type_entity,
                             uint64_t datum, bool is_null, PgExpr **expr_handle, bool is_gt);

  // TODO(neil) UpdateConstant should be merged into one.
  // Update constant.
  template<typename value_type>
  CHECKED_STATUS UpdateConstant(PgExpr *expr, value_type value, bool is_null) {
    if (expr->opcode() != PgExpr::Opcode::PG_EXPR_CONSTANT) {
      // Invalid handle.
      return STATUS(InvalidArgument, "Invalid expression handle for constant");
    }
    down_cast<PgConstant*>(expr)->UpdateConstant(value, is_null);
    return Status::OK();
  }

  CHECKED_STATUS UpdateConstant(PgExpr *expr, const char *value, bool is_null);

  CHECKED_STATUS UpdateConstant(PgExpr *expr, const char *value, int64_t bytes, bool is_null);

  // Operators.
  CHECKED_STATUS NewOperator(PgStatement *stmt, const char *opname,
                             const YBCPgTypeEntity *type_entity,
                             PgExpr **op_handle);
  CHECKED_STATUS OperatorAppendArg(PgExpr *op_handle, PgExpr *arg);

  // Foreign key reference caching.
  bool ForeignKeyReferenceExists(YBCPgOid table_oid, std::string&& ybctid);

  CHECKED_STATUS CacheForeignKeyReference(YBCPgOid table_oid, std::string&& ybctid);

  CHECKED_STATUS DeleteForeignKeyReference(YBCPgOid table_oid, std::string&& ybctid);

  void ClearForeignKeyReferenceCache();

  // Sets the specified timeout in the rpc service.
  void SetTimeout(int timeout_ms);

  //------------------------------------------------------------------------------------------------
  // Transaction control.

  CHECKED_STATUS BeginTransaction();

  CHECKED_STATUS RestartTransaction();

  CHECKED_STATUS CommitTransaction();

  CHECKED_STATUS AbortTransaction();

  CHECKED_STATUS SetTransactionIsolationLevel(int isolation);

  CHECKED_STATUS SetTransactionReadOnly(bool read_only);

  CHECKED_STATUS SetTransactionDeferrable(bool deferrable);

  CHECKED_STATUS EnterSeparateDdlTxnMode();

  CHECKED_STATUS ExitSeparateDdlTxnMode(bool success);

  private:
  std::shared_ptr<K2Adapter> CreateK2Adapter();

  std::shared_ptr<SqlCatalogManager> CreateCatalogManager();

  std::shared_ptr<SqlCatalogClient> CreateCatalogClient();

  // Metrics.
  gscoped_ptr<MetricRegistry> metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;

  // Memory tracker.
  std::shared_ptr<MemTracker> mem_tracker_;

  // TODO(neil) Map for environments (we should have just one ENV?). Environments should contain
  // all the custom flags the PostgreSQL sets. We ignore them all for now.
  std::shared_ptr<PgEnv> pg_env_;

  // Mapping table of YugaByte and PostgreSQL datatypes.
  std::unordered_map<int, const YBCPgTypeEntity *> type_map_;

  std::shared_ptr<K2Adapter> k2_adapter_;

  std::shared_ptr<SqlCatalogManager> catalog_manager_;

  std::shared_ptr<SqlCatalogClient> catalog_client_;

  std::shared_ptr<PgSession> pg_session_;

  YBCPgCallbacks pg_callbacks_;

  // TODO: investigate that if the pg_gate_impl need to hold(and share with its session) the txnHandler
  //       or this handler should only be owned by it session.
  std::shared_ptr<PgTxnHandler> pg_txn_handler_;
};

}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_API_H
