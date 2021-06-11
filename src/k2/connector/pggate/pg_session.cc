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

#include "pggate/pg_op.h"
#include "pggate/pg_session.h"

namespace k2pg {
namespace gate {

using namespace std::chrono;
using namespace k2pg::sql;

RowIdentifier::RowIdentifier(const std::string& table_id, const std::string row_id) :
  table_id_(table_id), row_id_(row_id) {
}

const string& RowIdentifier::row_id() const {
  return row_id_;
}

const string& RowIdentifier::table_id() const {
  return table_id_;
}

bool operator==(const RowIdentifier& k1, const RowIdentifier& k2) {
  return k1.table_id() == k2.table_id() && k1.row_id() == k2.row_id();
}

size_t hash_value(const RowIdentifier& key) {
  size_t hash = 0;
  boost::hash_combine(hash, key.table_id());
  boost::hash_combine(hash, key.row_id());
  return hash;
}

PgSession::PgSession(
    std::shared_ptr<SqlCatalogClient> catalog_client,
    std::shared_ptr<K2Adapter> k2_adapter,
    const string& database_name,
    std::shared_ptr<PgTxnHandler> pg_txn_handler,
    const K2PgCallbacks& pg_callbacks)
    : catalog_client_(catalog_client),
      k2_adapter_(k2_adapter),
      pg_txn_handler_(pg_txn_handler),
      pg_callbacks_(pg_callbacks),
      client_id_("K2PG") {
    ConnectDatabase(database_name);
}

PgSession::~PgSession() {
}

Status PgSession::InitPrimaryCluster()
{
  return catalog_client_->InitPrimaryCluster();
}

Status PgSession::FinishInitDB()
{
  return catalog_client_->FinishInitDB();
}

Status PgSession::ConnectDatabase(const string& database_name) {
  connected_database_ = database_name;
  RETURN_NOT_OK(catalog_client_->UseDatabase(database_name));
  return Status::OK();
}

Status PgSession::CreateDatabase(const string& database_name,
                                 const PgOid database_oid,
                                 const PgOid source_database_oid,
                                 const PgOid next_oid) {
  return catalog_client_->CreateDatabase(database_name,
                                  PgObjectId::GetDatabaseUuid(database_oid),
                                  database_oid,
                                  source_database_oid != kPgInvalidOid ? PgObjectId::GetDatabaseUuid(source_database_oid) : "",
                                  "" /* creator_role_name */,
                                  next_oid);
}

Status PgSession::DropDatabase(const string& database_name, PgOid database_oid) {
  RETURN_NOT_OK(catalog_client_->DeleteDatabase(database_name,
                                         PgObjectId::GetDatabaseUuid(database_oid)));
  RETURN_NOT_OK(DeleteDBSequences(database_oid));
  return Status::OK();
}

Status PgSession::RenameDatabase(const std::string& database_name, PgOid database_oid, std::optional<std::string> rename_to) {
  // TODO: add implementation
  return STATUS(NotSupported, "RenameDatabase not supported");
}

Status PgSession::CreateTable(
    const std::string& database_id,
    const std::string& database_name,
    const std::string& table_name,
    const PgObjectId& table_object_id,
    PgSchema& schema,
    bool is_pg_catalog_table,
    bool is_shared_table,
    bool if_not_exist) {
  return catalog_client_->CreateTable(database_name, table_name, table_object_id, schema,
    is_pg_catalog_table, is_shared_table, if_not_exist);
}

Status PgSession::CreateIndexTable(
    const std::string& database_id,
    const std::string& database_name,
    const std::string& table_name,
    const PgObjectId& table_object_id,
    const PgObjectId& base_table_object_id,
    PgSchema& schema,
    bool is_unique_index,
    bool skip_index_backfill,
    bool is_pg_catalog_table,
    bool is_shared_table,
    bool if_not_exist) {
  return catalog_client_->CreateIndexTable(database_name, table_name, table_object_id, base_table_object_id, schema,
    is_unique_index, skip_index_backfill, is_pg_catalog_table, is_shared_table, if_not_exist);
}

Status PgSession::DropTable(const PgObjectId& table_object_id) {
  return catalog_client_->DeleteTable(table_object_id.GetDatabaseOid(), table_object_id.GetObjectOid());
}

Status PgSession::DropIndex(const PgObjectId& index_object_id, PgOid *base_table_oid, bool wait) {
  return catalog_client_->DeleteIndexTable(index_object_id.GetDatabaseOid(), index_object_id.GetObjectOid(), base_table_oid);
}

Status PgSession::ReserveOids(const PgOid database_oid,
                              const PgOid next_oid,
                              const uint32_t count,
                              PgOid *begin_oid,
                              PgOid *end_oid) {
  return catalog_client_->ReservePgOids(database_oid, next_oid, count,
                                   begin_oid, end_oid);
}

Status PgSession::GetCatalogMasterVersion(uint64_t *catalog_version) {
  return catalog_client_->GetCatalogVersion(catalog_version);
}

// Sequence -----------------------------------------------------------------------------------------

Status PgSession::CreateSequencesDataTable() {
  // TODO: add implementation
  return Status::OK();
}

Status PgSession::InsertSequenceTuple(int64_t db_oid,
                                      int64_t seq_oid,
                                      uint64_t ysql_catalog_version,
                                      int64_t last_val,
                                      bool is_called) {
  // TODO: add implementation
  return Status::OK();
}

Status PgSession::UpdateSequenceTuple(int64_t db_oid,
                                      int64_t seq_oid,
                                      uint64_t ysql_catalog_version,
                                      int64_t last_val,
                                      bool is_called,
                                      std::optional<int64_t> expected_last_val,
                                      std::optional<bool> expected_is_called,
                                      bool* skipped) {
  // TODO: add implementation
  return Status::OK();
}

Status PgSession::ReadSequenceTuple(int64_t db_oid,
                                    int64_t seq_oid,
                                    uint64_t ysql_catalog_version,
                                    int64_t *last_val,
                                    bool *is_called) {
  // TODO: add implementation
  return Status::OK();
}

Status PgSession::DeleteSequenceTuple(int64_t db_oid, int64_t seq_oid) {
  // TODO: add implementation
  return Status::OK();
}

Status PgSession::DeleteDBSequences(int64_t db_oid) {
  // TODO: add implementation
  return Status::OK();
}

void PgSession::InvalidateTableCache(const PgObjectId& table_obj_id) {
  std::string pg_table_uuid = table_obj_id.GetTableUuid();
  table_cache_.erase(pg_table_uuid);
}

Status PgSession::HandleResponse(PgOpTemplate& op, const PgObjectId& relation_id) {
  if (op.succeeded()) {
    return Status::OK();
  }

  auto& response = op.response();
  if (response.pg_error_code != 0) {
    // TODO: handle pg error code
  }

  if (response.txn_error_code != 0) {
    // TODO: handle txn error code
  }

  Status s;
  // TODO: add errors to s
  return s;
}

bool PgSession::ShouldHandleTransactionally(const PgOpTemplate& op) {
  return op.IsTransactional() && !K2PgIsInitDbModeEnvVarSet();
}

Result<CBFuture<Status>> PgSession::RunAsync(const std::shared_ptr<PgOpTemplate>* op,
                                           size_t ops_count,
                                           const PgObjectId& relation_id,
                                           uint64_t* read_time) {
  DCHECK_GT(ops_count, 0);

  if (!ShouldHandleTransactionally(**op)) {
    InvalidateForeignKeyReferenceCache();
  }

  if (ops_count == 1) {
    // run a single operation
    return k2_adapter_->Exec(pg_txn_handler_->GetTxn(), *op);
  } else {  // ops_count > 1
    // run multiple operations in a batch
    std::vector<std::shared_ptr<PgOpTemplate>> ops;
    for (auto end = op + ops_count; op != end; ++op) {
      ops.push_back(*op);
    }
    return k2_adapter_->BatchExec(pg_txn_handler_->GetTxn(), ops);
  }
}

Result<std::shared_ptr<PgTableDesc>> PgSession::LoadTable(const PgObjectId& table_object_id) {
  std::string t_table_uuid = table_object_id.GetTableUuid();
  K2LOG_D(log::pg, "Loading table descriptor for {}, uuid={}", table_object_id, t_table_uuid);
  std::shared_ptr<TableInfo> table;

  auto cached_table = table_cache_.find(t_table_uuid);
  if (cached_table == table_cache_.end()) {
    K2LOG_D(log::pg, "Table cache MISS: {}", table_object_id);
    Status s = catalog_client_->OpenTable(table_object_id.GetDatabaseOid(), table_object_id.GetObjectOid(), &table);
    if (!s.ok()) {
      K2LOG_E(log::pg, "LoadTable: Server returns an error: {}", s);
      return STATUS_FORMAT(NotFound, "Error loading table with oid {} in database with oid {}: {}",
                           table_object_id.GetObjectOid(), table_object_id.GetDatabaseOid(), s.ToUserMessage());
    }
    table_cache_[t_table_uuid] = table;
  } else {
    K2LOG_D(log::pg, "Table cache HIT: {}", table_object_id);
    table = cached_table->second;
  }

  std::string t_table_id = table_object_id.GetTableId();
  // check if the t_table_id is for a table or an index
  if (table->table_id().compare(t_table_id) == 0) {
    // a table
    return std::make_shared<PgTableDesc>(table);
  }

  // an index
  const auto itr = table->secondary_indexes().find(t_table_id);
  if (itr == table->secondary_indexes().end()) {
    K2LOG_E(log::pg, "Cannot find index with id {}", t_table_id);
    return STATUS_FORMAT(NotFound, "Cannot find index {} in database {}",
                         t_table_id, table->database_id());
  }

  return std::make_shared<PgTableDesc>(itr->second, table->database_id());
}

Result<bool> PgSession::IsInitDbDone() {
  bool isDone = false;
  RETURN_NOT_OK(catalog_client_->IsInitDbDone(&isDone));
  return isDone;
}

Result<uint64_t> PgSession::GetSharedCatalogVersion() {
  // It is the same as K2PgGetCatalogMasterVersion() for now since we use local catalog manager for the timebeing
  uint64_t catalog_version;
  RETURN_NOT_OK(catalog_client_->GetCatalogVersion(&catalog_version));
  return catalog_version;
}

bool operator==(const PgForeignKeyReference& k1, const PgForeignKeyReference& k2) {
  return k1.table_oid == k2.table_oid &&
      k1.ybctid == k2.ybctid;
}

size_t hash_value(const PgForeignKeyReference& key) {
  size_t hash = 0;
  boost::hash_combine(hash, key.table_oid);
  boost::hash_combine(hash, key.ybctid);
  return hash;
}

bool PgSession::ForeignKeyReferenceExists(uint32_t table_oid, std::string&& ybctid) {
  PgForeignKeyReference reference{table_oid, std::move(ybctid)};
  return fk_reference_cache_.find(reference) != fk_reference_cache_.end();
}

Status PgSession::CacheForeignKeyReference(uint32_t table_oid, std::string&& ybctid) {
  PgForeignKeyReference reference{table_oid, std::move(ybctid)};
  fk_reference_cache_.emplace(reference);
  return Status::OK();
}

Status PgSession::DeleteForeignKeyReference(uint32_t table_oid, std::string&& ybctid) {
  PgForeignKeyReference reference{table_oid, std::move(ybctid)};
  fk_reference_cache_.erase(reference);
  return Status::OK();
}

Result<IndexPermissions> PgSession::WaitUntilIndexPermissionsAtLeast(
    const PgObjectId& table_object_id,
    const PgObjectId& index_object_id,
    const IndexPermissions& target_index_permissions) {
  // TODO: add implementation
  return IndexPermissions::INDEX_PERM_NOT_USED;
}

Status PgSession::AsyncUpdateIndexPermissions(const PgObjectId& indexed_table_object_id) {
  // TODO: add implementation
  return Status::OK();
}

}  // namespace gate
}  // namespace k2pg
