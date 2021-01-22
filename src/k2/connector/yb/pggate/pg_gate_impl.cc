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

#include "yb/pggate/pg_gate_impl.h"
#include "yb/pggate/pg_ddl.h"
#include "yb/pggate/pg_dml.h"
#include "yb/pggate/pg_select.h"
#include "yb/pggate/pg_insert.h"
#include "yb/pggate/pg_update.h"
#include "yb/pggate/pg_delete.h"

namespace k2pg {
namespace gate {

PgGateApiImpl::PgGateApiImpl(const YBCPgTypeEntity *YBCDataTypeArray, int count, YBCPgCallbacks callbacks)
    : metric_registry_(new MetricRegistry()),
      metric_entity_(METRIC_ENTITY_server.Instantiate(metric_registry_.get(), "k2.pggate")),
      mem_tracker_(MemTracker::CreateTracker("PostgreSQL")),
      k2_adapter_(CreateK2Adapter()),
      catalog_manager_(CreateCatalogManager()),
      catalog_client_(CreateCatalogClient()),
      pg_callbacks_(callbacks),
      pg_txn_handler_(new PgTxnHandler(k2_adapter_)) {
  // Setup type mapping.
  for (int idx = 0; idx < count; idx++) {
    const YBCPgTypeEntity *type_entity = &YBCDataTypeArray[idx];
    type_map_[type_entity->type_oid] = type_entity;
  }
  catalog_manager_->Start();
  k2_adapter_->Init();
}

std::shared_ptr<K2Adapter> PgGateApiImpl::CreateK2Adapter() {
  std::shared_ptr<K2Adapter> adapter = std::make_shared<K2Adapter>();
  return adapter;
}

// create SqlCatalogManager here for now
// TODO: create catalog manager cross all PG gate connections
std::shared_ptr<SqlCatalogManager> PgGateApiImpl::CreateCatalogManager() {
  std::shared_ptr<SqlCatalogManager> catalog_manager = std::make_shared<SqlCatalogManager>(k2_adapter_);
  return catalog_manager;
}

std::shared_ptr<SqlCatalogClient> PgGateApiImpl::CreateCatalogClient() {
  std::shared_ptr<SqlCatalogClient> catalog_client = std::make_shared<SqlCatalogClient>(catalog_manager_);
  return catalog_client;
}

PgGateApiImpl::~PgGateApiImpl() {
  catalog_manager_->Shutdown();
  k2_adapter_->Shutdown();
}

const YBCPgTypeEntity *PgGateApiImpl::FindTypeEntity(int type_oid) {
  const auto iter = type_map_.find(type_oid);
  if (iter != type_map_.end()) {
    return iter->second;
  }
  return nullptr;
}

Status AddColumn(PgCreateTable* pg_stmt, const char *attr_name, int attr_num,
                         const YBCPgTypeEntity *attr_type, bool is_hash, bool is_range,
                         bool is_desc, bool is_nulls_first) {
  using SortingType = ColumnSchema::SortingType;
  SortingType sorting_type = SortingType::kNotSpecified;

  if (!is_hash && is_range) {
    if (is_desc) {
      sorting_type = is_nulls_first ? SortingType::kDescending : SortingType::kDescendingNullsLast;
    } else {
      sorting_type = is_nulls_first ? SortingType::kAscending : SortingType::kAscendingNullsLast;
    }
  }

  return pg_stmt->AddColumn(attr_name, attr_num, attr_type, is_hash, is_range, sorting_type);
}

//--------------------------------------------------------------------------------------------------

Status PgGateApiImpl::CreateEnv(PgEnv **pg_env) {
  *pg_env = pg_env_.get();
  return Status::OK();
}

Status PgGateApiImpl::DestroyEnv(PgEnv *pg_env) {
  pg_env_ = nullptr;
  return Status::OK();
}

//--------------------------------------------------------------------------------------------------

Status PgGateApiImpl::InitSession(const PgEnv *pg_env,
                              const string& database_name) {
  CHECK(!pg_session_);
  auto session = std::make_shared<PgSession>(catalog_client_,
                                               k2_adapter_,
                                               database_name,
                                               pg_txn_handler_,
                                               pg_callbacks_);
  if (!database_name.empty()) {
    RETURN_NOT_OK(session->ConnectDatabase(database_name));
  }

  pg_session_.swap(session);
  return Status::OK();
}

Status PgGateApiImpl::InvalidateCache() {
  pg_session_->InvalidateCache();
  return Status::OK();
}

Result<bool> PgGateApiImpl::IsInitDbDone() {
  return pg_session_->IsInitDbDone();
}

Result<uint64_t> PgGateApiImpl::GetSharedCatalogVersion() {
  return pg_session_->GetSharedCatalogVersion();
}

//--------------------------------------------------------------------------------------------------

PgMemctx *PgGateApiImpl::CreateMemctx() {
  // Postgres will create YB Memctx when it first use the Memctx to allocate YugaByte object.
  return PgMemctx::Create();
}

Status PgGateApiImpl::DestroyMemctx(PgMemctx *memctx) {
  // Postgres will destroy YB Memctx by releasing the pointer.
  return PgMemctx::Destroy(memctx);
}

Status PgGateApiImpl::ResetMemctx(PgMemctx *memctx) {
  // Postgres reset YB Memctx when clearing a context content without clearing its nested context.
  return PgMemctx::Reset(memctx);
}

// TODO(neil) Use Arena in the future.
// - PgStatement should have been declared as derived class of "MCBase".
// - All objects of PgStatement's derived class should be allocated by YbPgMemctx::Arena.
// - We cannot use Arena yet because quite a large number of YugaByte objects are being referenced
//   from other layers.  Those added code violated the original design as they assume ScopedPtr
//   instead of memory pool is being used. This mess should be cleaned up later.
//
// For now, statements is allocated as shared_ptr and cached in the memory context. The statements
// would then be destructed when the context is destroyed and all other references are also cleared.
Status PgGateApiImpl::AddToCurrentMemctx(const std::shared_ptr<PgStatement> &stmt,
                                       PgStatement **handle) {
  pg_callbacks_.GetCurrentYbMemctx()->Cache(stmt);
  *handle = stmt.get();
  return Status::OK();
}

// TODO(neil) Most like we don't need table_desc. If we do need it, use Arena here.
// - PgTableDesc should have been declared as derived class of "MCBase".
// - PgTableDesc objects should be allocated by YbPgMemctx::Arena.
//
// For now, table_desc is allocated as ScopedPtr and cached in the memory context. The table_desc
// would then be destructed when the context is destroyed.
Status PgGateApiImpl::AddToCurrentMemctx(size_t table_desc_id,
                                       const std::shared_ptr<PgTableDesc> &table_desc) {
  pg_callbacks_.GetCurrentYbMemctx()->Cache(table_desc_id, table_desc);
  return Status::OK();
}

Status PgGateApiImpl::GetTabledescFromCurrentMemctx(size_t table_desc_id, PgTableDesc **handle) {
  pg_callbacks_.GetCurrentYbMemctx()->GetCache(table_desc_id, handle);
  return Status::OK();
}

//--------------------------------------------------------------------------------------------------

Status PgGateApiImpl::ClearBinds(PgStatement *handle) {
  return handle->ClearBinds();
}

//--------------------------------------------------------------------------------------------------

Status PgGateApiImpl::PGInitPrimaryCluster()
{
  return pg_session_->InitPrimaryCluster();
}

Status PgGateApiImpl::PGFinishInitDB()
{
  return pg_session_->FinishInitDB();
}

//--------------------------------------------------------------------------------------------------

Status PgGateApiImpl::ConnectDatabase(const char *database_name) {
  return pg_session_->ConnectDatabase(database_name);
}

Status PgGateApiImpl::NewCreateDatabase(const char *database_name,
                                    const PgOid database_oid,
                                    const PgOid source_database_oid,
                                    const PgOid next_oid,
                                    PgStatement **handle) {
  auto stmt = std::make_shared<PgCreateDatabase>(pg_session_, database_name, database_oid,
                                                   source_database_oid, next_oid);
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::ExecCreateDatabase(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_CREATE_DATABASE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }

  return down_cast<PgCreateDatabase*>(handle)->Exec();
}

Status PgGateApiImpl::NewDropDatabase(const char *database_name,
                                  PgOid database_oid,
                                  PgStatement **handle) {
  auto stmt = std::make_shared<PgDropDatabase>(pg_session_, database_name, database_oid);
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::ExecDropDatabase(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_DROP_DATABASE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgDropDatabase*>(handle)->Exec();
}

Status PgGateApiImpl::NewAlterDatabase(const char *database_name,
                                  PgOid database_oid,
                                  PgStatement **handle) {
  auto stmt = std::make_shared<PgAlterDatabase>(pg_session_, database_name, database_oid);
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::AlterDatabaseRenameDatabase(PgStatement *handle, const char *new_name) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_ALTER_DATABASE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgAlterDatabase*>(handle)->RenameDatabase(new_name);
}

Status PgGateApiImpl::ExecAlterDatabase(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_ALTER_DATABASE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgAlterDatabase*>(handle)->Exec();
}

Status PgGateApiImpl::ReserveOids(const PgOid database_oid,
                              const PgOid next_oid,
                              const uint32_t count,
                              PgOid *begin_oid,
                              PgOid *end_oid) {
  return pg_session_->ReserveOids(database_oid, next_oid, count, begin_oid, end_oid);
}

Status PgGateApiImpl::GetCatalogMasterVersion(uint64_t *version) {
  return pg_session_->GetCatalogMasterVersion(version);
}

Result<std::shared_ptr<PgTableDesc>> PgGateApiImpl::LoadTable(const PgObjectId& table_id) {
  return pg_session_->LoadTable(table_id);
}

void PgGateApiImpl::InvalidateTableCache(const PgObjectId& table_id) {
  pg_session_->InvalidateTableCache(table_id);
}

//--------------------------------------------------------------------------------------------------

Status PgGateApiImpl::NewCreateTable(const char *database_name,
                                 const char *schema_name,
                                 const char *table_name,
                                 const PgObjectId& table_id,
                                 bool is_shared_table,
                                 bool if_not_exist,
                                 bool add_primary_key,
                                 PgStatement **handle) {
  auto stmt = std::make_shared<PgCreateTable>(
      pg_session_, database_name, schema_name, table_name,
      table_id, is_shared_table, if_not_exist, add_primary_key);
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::CreateTableAddColumn(PgStatement *handle, const char *attr_name, int attr_num,
                                       const YBCPgTypeEntity *attr_type,
                                       bool is_hash, bool is_range,
                                       bool is_desc, bool is_nulls_first) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_CREATE_TABLE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return AddColumn(down_cast<PgCreateTable*>(handle), attr_name, attr_num, attr_type,
      is_hash, is_range, is_desc, is_nulls_first);
}

Status PgGateApiImpl::CreateTableAddSplitRow(PgStatement *handle, int num_cols,
                                           YBCPgTypeEntity **types, uint64_t *data) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_CREATE_TABLE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }

  // do nothing here since we don't support pre-split table
  return Status::OK();
}

Status PgGateApiImpl::ExecCreateTable(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_CREATE_TABLE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgCreateTable*>(handle)->Exec();
}

Status PgGateApiImpl::NewAlterTable(const PgObjectId& table_id,
                                PgStatement **handle) {
  auto stmt = std::make_shared<PgAlterTable>(pg_session_, table_id);
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::AlterTableAddColumn(PgStatement *handle, const char *name,
                                      int order, const YBCPgTypeEntity *attr_type,
                                      bool is_not_null) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_ALTER_TABLE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }

  PgAlterTable *pg_stmt = down_cast<PgAlterTable*>(handle);
  return pg_stmt->AddColumn(name, attr_type, order, is_not_null);
}

Status PgGateApiImpl::AlterTableRenameColumn(PgStatement *handle, const char *oldname,
                                         const char *newname) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_ALTER_TABLE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }

  PgAlterTable *pg_stmt = down_cast<PgAlterTable*>(handle);
  return pg_stmt->RenameColumn(oldname, newname);
}

Status PgGateApiImpl::AlterTableDropColumn(PgStatement *handle, const char *name) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_ALTER_TABLE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }

  PgAlterTable *pg_stmt = down_cast<PgAlterTable*>(handle);
  return pg_stmt->DropColumn(name);
}

Status PgGateApiImpl::AlterTableRenameTable(PgStatement *handle, const char *db_name,
                                        const char *newname) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_ALTER_TABLE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }

  PgAlterTable *pg_stmt = down_cast<PgAlterTable*>(handle);
  return pg_stmt->RenameTable(db_name, newname);
}

Status PgGateApiImpl::ExecAlterTable(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_ALTER_TABLE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  PgAlterTable *pg_stmt = down_cast<PgAlterTable*>(handle);
  return pg_stmt->Exec();
}

Status PgGateApiImpl::NewDropTable(const PgObjectId& table_id,
                               bool if_exist,
                               PgStatement **handle) {
  auto stmt = std::make_shared<PgDropTable>(pg_session_, table_id, if_exist);
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::ExecDropTable(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_DROP_TABLE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgDropTable*>(handle)->Exec();
}

Status PgGateApiImpl::GetTableDesc(const PgObjectId& table_id,
                               PgTableDesc **handle) {
  // First read from memory context.
  size_t hash_id = hash_value(table_id);
  RETURN_NOT_OK(GetTabledescFromCurrentMemctx(hash_id, handle));

  // Read from environment.
  if (*handle == nullptr) {
    auto result = pg_session_->LoadTable(table_id);
    RETURN_NOT_OK(result);
    RETURN_NOT_OK(AddToCurrentMemctx(hash_id, *result));

    *handle = result->get();
  }

  return Status::OK();
}

Status PgGateApiImpl::GetColumnInfo(PgTableDesc* table_desc,
                                int16_t attr_number,
                                bool *is_primary,
                                bool *is_hash) {
  return table_desc->GetColumnInfo(attr_number, is_primary, is_hash);
}

Status PgGateApiImpl::DmlModifiesRow(PgStatement *handle, bool *modifies_row) {
  if (!handle) {
    return STATUS(InvalidArgument, "Invalid statement handle");
  }

  *modifies_row = false;

  switch (handle->stmt_op()) {
    case StmtOp::STMT_UPDATE:
    case StmtOp::STMT_DELETE:
      *modifies_row = true;
      break;
    default:
      break;
  }

  return Status::OK();
}

Status PgGateApiImpl::SetIsSysCatalogVersionChange(PgStatement *handle) {
  if (!handle) {
    return STATUS(InvalidArgument, "Invalid statement handle");
  }

  switch (handle->stmt_op()) {
    case StmtOp::STMT_UPDATE:
    case StmtOp::STMT_DELETE:
    case StmtOp::STMT_INSERT:
      down_cast<PgDmlWrite *>(handle)->SetIsSystemCatalogChange();
      return Status::OK();
    default:
      break;
  }

  return STATUS(InvalidArgument, "Invalid statement handle");
}

Status PgGateApiImpl::SetCatalogCacheVersion(PgStatement *handle, uint64_t catalog_cache_version) {
  if (!handle) {
    return STATUS(InvalidArgument, "Invalid statement handle");
  }

  switch (handle->stmt_op()) {
    case StmtOp::STMT_SELECT:
    case StmtOp::STMT_INSERT:
    case StmtOp::STMT_UPDATE:
    case StmtOp::STMT_DELETE:
      down_cast<PgDml *>(handle)->SetCatalogCacheVersion(catalog_cache_version);
      return Status::OK();
    default:
      break;
  }

  return STATUS(InvalidArgument, "Invalid statement handle");
}

// Index --------------------------------------------------------------------------------------------

Status PgGateApiImpl::NewCreateIndex(const char *database_name,
                                 const char *schema_name,
                                 const char *index_name,
                                 const PgObjectId& index_id,
                                 const PgObjectId& base_table_id,
                                 bool is_shared_index,
                                 bool is_unique_index,
                                 const bool skip_index_backfill,
                                 bool if_not_exist,
                                 PgStatement **handle) {
  auto stmt = std::make_shared<PgCreateIndex>(
      pg_session_, database_name, schema_name, index_name, index_id, base_table_id,
      is_shared_index, is_unique_index, skip_index_backfill, if_not_exist);
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::CreateIndexAddColumn(PgStatement *handle, const char *attr_name, int attr_num,
                                       const YBCPgTypeEntity *attr_type,
                                       bool is_hash, bool is_range,
                                       bool is_desc, bool is_nulls_first) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_CREATE_INDEX)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }

  return AddColumn(down_cast<PgCreateIndex*>(handle), attr_name, attr_num, attr_type,
      is_hash, is_range, is_desc, is_nulls_first);
}

Status PgGateApiImpl::CreateIndexAddSplitRow(PgStatement *handle, int num_cols,
                                         YBCPgTypeEntity **types, uint64_t *data) {
  SCHECK(PgStatement::IsValidStmt(handle, StmtOp::STMT_CREATE_INDEX),
      InvalidArgument,
      "Invalid statement handle");

  // do nothing here since we don't support pre-split table
  return Status::OK();
}

Status PgGateApiImpl::ExecCreateIndex(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_CREATE_INDEX)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgCreateIndex*>(handle)->Exec();
}

Status PgGateApiImpl::NewDropIndex(const PgObjectId& index_id,
                               bool if_exist,
                               PgStatement **handle) {
  auto stmt = std::make_shared<PgDropIndex>(pg_session_, index_id, if_exist);
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::ExecDropIndex(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_DROP_INDEX)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgDropIndex*>(handle)->Exec();
}

Result<IndexPermissions> PgGateApiImpl::WaitUntilIndexPermissionsAtLeast(
    const PgObjectId& table_id,
    const PgObjectId& index_id,
    const IndexPermissions& target_index_permissions) {
  return pg_session_->WaitUntilIndexPermissionsAtLeast(
      table_id,
      index_id,
      target_index_permissions);
}

Status PgGateApiImpl::AsyncUpdateIndexPermissions(const PgObjectId& indexed_table_id) {
  return pg_session_->AsyncUpdateIndexPermissions(indexed_table_id);
}

// Sequence -----------------------------------------------------------------------------------------

Status PgGateApiImpl::CreateSequencesDataTable() {
  return pg_session_->CreateSequencesDataTable();
}

Status PgGateApiImpl::InsertSequenceTuple(int64_t db_oid,
                                      int64_t seq_oid,
                                      uint64_t ysql_catalog_version,
                                      int64_t last_val,
                                      bool is_called) {
  return pg_session_->InsertSequenceTuple(
      db_oid, seq_oid, ysql_catalog_version, last_val, is_called);
}

Status PgGateApiImpl::UpdateSequenceTupleConditionally(int64_t db_oid,
                                                   int64_t seq_oid,
                                                   uint64_t ysql_catalog_version,
                                                   int64_t last_val,
                                                   bool is_called,
                                                   int64_t expected_last_val,
                                                   bool expected_is_called,
                                                   bool *skipped) {
  return pg_session_->UpdateSequenceTuple(
      db_oid, seq_oid, ysql_catalog_version, last_val, is_called,
      expected_last_val, expected_is_called, skipped);
}

Status PgGateApiImpl::UpdateSequenceTuple(int64_t db_oid,
                                      int64_t seq_oid,
                                      uint64_t ysql_catalog_version,
                                      int64_t last_val,
                                      bool is_called,
                                      bool* skipped) {
  return pg_session_->UpdateSequenceTuple(
      db_oid, seq_oid, ysql_catalog_version, last_val,
      is_called, std::nullopt, std::nullopt, skipped);
}

Status PgGateApiImpl::ReadSequenceTuple(int64_t db_oid,
                                    int64_t seq_oid,
                                    uint64_t ysql_catalog_version,
                                    int64_t *last_val,
                                    bool *is_called) {
  return pg_session_->ReadSequenceTuple(db_oid, seq_oid, ysql_catalog_version, last_val, is_called);
}

Status PgGateApiImpl::DeleteSequenceTuple(int64_t db_oid, int64_t seq_oid) {
  return pg_session_->DeleteSequenceTuple(db_oid, seq_oid);
}

// Binding -----------------------------------------------------------------------------------------

Status PgGateApiImpl::DmlAppendTarget(PgStatement *handle, PgExpr *target) {
  return down_cast<PgDml*>(handle)->AppendTarget(target);
}

Status PgGateApiImpl::DmlBindColumn(PgStatement *handle, int attr_num, PgExpr *attr_value) {
  return down_cast<PgDml*>(handle)->BindColumn(attr_num, attr_value);
}

Status PgGateApiImpl::DmlBindColumnCondEq(PgStatement *handle, int attr_num, PgExpr *attr_value) {
  return down_cast<PgDmlRead*>(handle)->BindColumnCondEq(attr_num, attr_value);
}

Status PgGateApiImpl::DmlBindColumnCondBetween(PgStatement *handle, int attr_num, PgExpr *attr_value,
    PgExpr *attr_value_end) {
  return down_cast<PgDmlRead*>(handle)->BindColumnCondBetween(attr_num, attr_value, attr_value_end);
}

Status PgGateApiImpl::DmlBindColumnCondIn(PgStatement *handle, int attr_num, int n_attr_values,
    PgExpr **attr_values) {
  return down_cast<PgDmlRead*>(handle)->BindColumnCondIn(attr_num, n_attr_values, attr_values);
}

Status PgGateApiImpl::DmlBindTable(PgStatement *handle) {
  return down_cast<PgDml*>(handle)->BindTable();
}

CHECKED_STATUS PgGateApiImpl::DmlAssignColumn(PgStatement *handle, int attr_num, PgExpr *attr_value) {
  return down_cast<PgDml*>(handle)->AssignColumn(attr_num, attr_value);
}

Status PgGateApiImpl::DmlFetch(PgStatement *handle, int32_t natts, uint64_t *values, bool *isnulls,
                           PgSysColumns *syscols, bool *has_data) {
  return down_cast<PgDml*>(handle)->Fetch(natts, values, isnulls, syscols, has_data);
}

Status PgGateApiImpl::DmlBuildYBTupleId(PgStatement *handle, const PgAttrValueDescriptor *attrs,
                                    int32_t nattrs, uint64_t *ybctid) {
  const string id = VERIFY_RESULT(down_cast<PgDml*>(handle)->BuildYBTupleId(attrs, nattrs));
  const YBCPgTypeEntity *type_entity = FindTypeEntity(kPgByteArrayOid);
  *ybctid = type_entity->yb_to_datum(id.data(), id.size(), nullptr /* type_attrs */);
  return Status::OK();
}

Status PgGateApiImpl::DmlExecWriteOp(PgStatement *handle, int32_t *rows_affected_count) {
  switch (handle->stmt_op()) {
    case StmtOp::STMT_INSERT:
    case StmtOp::STMT_UPDATE:
    case StmtOp::STMT_DELETE:
    case StmtOp::STMT_TRUNCATE:
      {
        auto dml_write = down_cast<PgDmlWrite *>(handle);
        RETURN_NOT_OK(dml_write->Exec(true /* force_non_bufferable */));
        if (rows_affected_count) {
          *rows_affected_count = dml_write->GetRowsAffectedCount();
        }
        return Status::OK();
      }
    default:
      break;
  }
  return STATUS(InvalidArgument, "Invalid statement handle");
}

bool PgGateApiImpl::ForeignKeyReferenceExists(YBCPgOid table_id, std::string&& ybctid) {
  return pg_session_->ForeignKeyReferenceExists(table_id, std::move(ybctid));
}

Status PgGateApiImpl::CacheForeignKeyReference(YBCPgOid table_id, std::string&& ybctid) {
  return pg_session_->CacheForeignKeyReference(table_id, std::move(ybctid));
}

Status PgGateApiImpl::DeleteForeignKeyReference(YBCPgOid table_id, std::string&& ybctid) {
  return pg_session_->DeleteForeignKeyReference(table_id, std::move(ybctid));
}

void PgGateApiImpl::ClearForeignKeyReferenceCache() {
  pg_session_->InvalidateForeignKeyReferenceCache();
}

void PgGateApiImpl::SetTimeout(const int timeout_ms) {
  pg_session_->SetTimeout(timeout_ms);
}

// Column references -------------------------------------------------------------------------------

Status PgGateApiImpl::NewColumnRef(PgStatement *stmt, int attr_num, const PgTypeEntity *type_entity,
                               const PgTypeAttrs *type_attrs, PgExpr **expr_handle) {
  if (!stmt) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  std::unique_ptr<PgExpr> colref = std::make_unique<PgColumnRef>(attr_num, type_entity, type_attrs);
  *expr_handle = colref.get();
  stmt->AddExpr(std::move(colref));

  return Status::OK();
}

// Constant ----------------------------------------------------------------------------------------

Status PgGateApiImpl::NewConstant(PgStatement *stmt, const YBCPgTypeEntity *type_entity,
                              uint64_t datum, bool is_null, PgExpr **expr_handle) {
  if (!stmt) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  std::unique_ptr<PgExpr> pg_const = std::make_unique<PgConstant>(type_entity, datum, is_null);
  *expr_handle = pg_const.get();
  stmt->AddExpr(std::move(pg_const));

  return Status::OK();
}

Status PgGateApiImpl::NewConstantOp(PgStatement *stmt, const YBCPgTypeEntity *type_entity,
                              uint64_t datum, bool is_null, PgExpr **expr_handle, bool is_gt) {
  if (!stmt) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  std::unique_ptr<PgExpr> pg_const = std::make_unique<PgConstant>(type_entity, datum, is_null,
      is_gt ? PgExpr::Opcode::PG_EXPR_GT : PgExpr::Opcode::PG_EXPR_LT);
  *expr_handle = pg_const.get();
  stmt->AddExpr(std::move(pg_const));

  return Status::OK();
}

// Text constant -----------------------------------------------------------------------------------

Status PgGateApiImpl::UpdateConstant(PgExpr *expr, const char *value, bool is_null) {
  if (expr->opcode() != PgExpr::Opcode::PG_EXPR_CONSTANT) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid expression handle for constant");
  }
  down_cast<PgConstant*>(expr)->UpdateConstant(value, is_null);
  return Status::OK();
}

Status PgGateApiImpl::UpdateConstant(PgExpr *expr, const char *value, int64_t bytes, bool is_null) {
  if (expr->opcode() != PgExpr::Opcode::PG_EXPR_CONSTANT) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid expression handle for constant");
  }
  down_cast<PgConstant*>(expr)->UpdateConstant(value, bytes, is_null);
  return Status::OK();
}

// Text constant -----------------------------------------------------------------------------------

Status PgGateApiImpl::NewOperator(PgStatement *stmt, const char *opname,
                              const YBCPgTypeEntity *type_entity,
                              PgExpr **op_handle) {
  if (!stmt) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  RETURN_NOT_OK(PgExpr::CheckOperatorName(opname));

  // Create operator.
  std::unique_ptr<PgExpr> pg_op = std::make_unique<PgOperator>(opname, type_entity);
  *op_handle = pg_op.get();
  stmt->AddExpr(std::move(pg_op));

  return Status::OK();
}

Status PgGateApiImpl::OperatorAppendArg(PgExpr *op_handle, PgExpr *arg) {
  if (!op_handle || !arg) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid expression handle");
  }
  down_cast<PgOperator*>(op_handle)->AppendArg(arg);
  return Status::OK();
}

void PgGateApiImpl::StartOperationsBuffering() {
  pg_session_->StartOperationsBuffering();
}

Status PgGateApiImpl::StopOperationsBuffering() {
  return pg_session_->StopOperationsBuffering();
}

Status PgGateApiImpl::ResetOperationsBuffering() {
  return pg_session_->ResetOperationsBuffering();
}

Status PgGateApiImpl::FlushBufferedOperations() {
  return pg_session_->FlushBufferedOperations();
}

void PgGateApiImpl::DropBufferedOperations() {
  pg_session_->DropBufferedOperations();
}

// Select ------------------------------------------------------------------------------------------

Status PgGateApiImpl::NewSelect(const PgObjectId& table_id,
                            const PgObjectId& index_id,
                            const PgPrepareParameters *prepare_params,
                            PgStatement **handle) {
  // Scenarios:
  // - Sequential Scan: PgSelect to read from table_id.
  // - Primary Scan: PgSelect from table_id. YugaByte does not have separate table for primary key.
  // - Index-Only-Scan: PgSelectIndex directly from secondary index_id.
  // - IndexScan: Use PgSelectIndex to read from index_id and then PgSelect to read from table_id.
  //     Note that for SysTable, only one request is send for both table_id and index_id.
  *handle = nullptr;
  std::shared_ptr<PgDmlRead> stmt;
  if (prepare_params && prepare_params->index_only_scan && prepare_params->use_secondary_index) {
    if (!index_id.IsValid()) {
      return STATUS(InvalidArgument, "Cannot run query with invalid index ID");
    }
    stmt = std::make_shared<PgSelectIndex>(pg_session_, table_id, index_id, prepare_params);
  } else {
    // For IndexScan PgSelect processing will create subquery PgSelectIndex.
    stmt = std::make_shared<PgSelect>(pg_session_, table_id, index_id, prepare_params);
  }

  RETURN_NOT_OK(stmt->Prepare());
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::SetForwardScan(PgStatement *handle, bool is_forward_scan) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_SELECT)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  down_cast<PgDmlRead*>(handle)->SetForwardScan(is_forward_scan);
  return Status::OK();
}

Status PgGateApiImpl::ExecSelect(PgStatement *handle, const PgExecParameters *exec_params) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_SELECT)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgDmlRead*>(handle)->Exec(exec_params);
}

// Insert ------------------------------------------------------------------------------------------

Status PgGateApiImpl::NewInsert(const PgObjectId& table_id,
                            const bool is_single_row_txn,
                            PgStatement **handle) {
  *handle = nullptr;
  auto stmt = std::make_shared<PgInsert>(pg_session_, table_id, is_single_row_txn);
  RETURN_NOT_OK(stmt->Prepare());
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::ExecInsert(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_INSERT)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgInsert*>(handle)->Exec();
}

Status PgGateApiImpl::InsertStmtSetUpsertMode(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_INSERT)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  down_cast<PgInsert*>(handle)->SetUpsertMode();

  return Status::OK();
}

Status PgGateApiImpl::InsertStmtSetWriteTime(PgStatement *handle, const uint64_t write_time) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_INSERT)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  RETURN_NOT_OK(down_cast<PgInsert*>(handle)->SetWriteTime(write_time));
  return Status::OK();
}

// Update ------------------------------------------------------------------------------------------

Status PgGateApiImpl::NewUpdate(const PgObjectId& table_id,
                            const bool is_single_row_txn,
                            PgStatement **handle) {
  *handle = nullptr;
  auto stmt = std::make_shared<PgUpdate>(pg_session_, table_id, is_single_row_txn);
  RETURN_NOT_OK(stmt->Prepare());
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::ExecUpdate(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_UPDATE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgUpdate*>(handle)->Exec();
}

// Delete ------------------------------------------------------------------------------------------

Status PgGateApiImpl::NewDelete(const PgObjectId& table_id,
                            const bool is_single_row_txn,
                            PgStatement **handle) {
  *handle = nullptr;
  auto stmt = std::make_shared<PgDelete>(pg_session_, table_id, is_single_row_txn);
  RETURN_NOT_OK(stmt->Prepare());
  RETURN_NOT_OK(AddToCurrentMemctx(stmt, handle));
  return Status::OK();
}

Status PgGateApiImpl::ExecDelete(PgStatement *handle) {
  if (!PgStatement::IsValidStmt(handle, StmtOp::STMT_DELETE)) {
    // Invalid handle.
    return STATUS(InvalidArgument, "Invalid statement handle");
  }
  return down_cast<PgDelete*>(handle)->Exec();
}

Status PgGateApiImpl::BeginTransaction() {
  pg_session_->InvalidateForeignKeyReferenceCache();
  return pg_txn_handler_->BeginTransaction();
}

Status PgGateApiImpl::RestartTransaction() {
  pg_session_->InvalidateForeignKeyReferenceCache();
  return pg_txn_handler_->RestartTransaction();
}

Status PgGateApiImpl::CommitTransaction() {
  pg_session_->InvalidateForeignKeyReferenceCache();
  return pg_txn_handler_->CommitTransaction();
}

Status PgGateApiImpl::AbortTransaction() {
  pg_session_->InvalidateForeignKeyReferenceCache();
  return pg_txn_handler_->AbortTransaction();
}

Status PgGateApiImpl::SetTransactionIsolationLevel(int isolation) {
  return pg_txn_handler_->SetIsolationLevel(isolation);
}

Status PgGateApiImpl::SetTransactionReadOnly(bool read_only) {
  return pg_txn_handler_->SetReadOnly(read_only);
}

Status PgGateApiImpl::SetTransactionDeferrable(bool deferrable) {
  return pg_txn_handler_->SetDeferrable(deferrable);
}

Status PgGateApiImpl::EnterSeparateDdlTxnMode() {
  // Flush all buffered operations as ddl txn use its own transaction session.
  RETURN_NOT_OK(pg_session_->FlushBufferedOperations());
  return pg_txn_handler_->EnterSeparateDdlTxnMode();
}

Status PgGateApiImpl::ExitSeparateDdlTxnMode(bool success) {
  // Flush all buffered operations as ddl txn use its own transaction session.
  if (success) {
    RETURN_NOT_OK(pg_session_->FlushBufferedOperations());
  } else {
    pg_session_->DropBufferedOperations();
  }

  return pg_txn_handler_->ExitSeparateDdlTxnMode(success);
}

}  // namespace gate
}  // namespace k2pg
