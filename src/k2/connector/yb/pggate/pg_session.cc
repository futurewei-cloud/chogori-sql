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

#include "yb/common/port.h"
#include "yb/pggate/pg_op.h"
#include "yb/pggate/pg_session.h"

namespace k2pg {
namespace gate {

using namespace std::chrono;

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
    scoped_refptr<SqlCatalogClient> catalog_client,        
    scoped_refptr<K2Adapter> k2_adapter,
    const string& database_name,
    scoped_refptr<PgTxnHandler> pg_txn_handler,
    const YBCPgCallbacks& pg_callbacks)
    : catalog_client_(catalog_client),
      k2_adapter_(k2_adapter),
      pg_txn_handler_(pg_txn_handler),
      pg_callbacks_(pg_callbacks),
      client_id_("K2PG") {
    ConnectDatabase(database_name);
}

PgSession::~PgSession() {
}

Status PgSession::ConnectDatabase(const string& database_name) {
  connected_database_ = database_name;
  return Status::OK();
}

Status PgSession::CreateDatabase(const string& database_name,
                                 const PgOid database_oid,
                                 const PgOid source_database_oid,
                                 const PgOid next_oid) {
  return catalog_client_->CreateNamespace(database_name,
                                  "" /* creator_role_name */,
                                  GetPgsqlNamespaceId(database_oid),
                                  source_database_oid != kPgInvalidOid
                                  ? GetPgsqlNamespaceId(source_database_oid) : "",
                                  next_oid);
}

Status PgSession::DropDatabase(const string& database_name, PgOid database_oid) {
  RETURN_NOT_OK(catalog_client_->DeleteNamespace(database_name,
                                         GetPgsqlNamespaceId(database_oid)));
  RETURN_NOT_OK(DeleteDBSequences(database_oid));
  return Status::OK();
}

Status PgSession::RenameDatabase(const std::string& database_name, PgOid database_oid, std::optional<std::string> rename_to) {
  // TODO: add implementation
  return Status::OK(); 
}

Status PgSession::CreateTable(
    std::string& namespace_id, 
    std::string& namespace_name, 
    std::string& table_name, 
    const PgObjectId& table_id, 
    PgSchema& schema, 
    bool is_pg_catalog_table, 
    bool is_shared_table, 
    bool if_not_exist) {
  return catalog_client_->CreateTable(namespace_name, table_name, table_id, schema,
    is_pg_catalog_table, is_shared_table, if_not_exist);
}

Status PgSession::CreateIndexTable(std::string& namespace_id, std::string& namespace_name, std::string& table_name, const PgObjectId& table_id, 
    const PgObjectId& base_table_id, PgSchema& schema, bool is_unique_index, bool skip_index_backfill,
    bool is_pg_catalog_table, bool is_shared_table, bool if_not_exist) {
  return catalog_client_->CreateIndexTable(namespace_name, table_name, table_id, base_table_id, schema,
    is_unique_index, skip_index_backfill, is_pg_catalog_table, is_shared_table, if_not_exist);
}

Status PgSession::DropTable(const PgObjectId& table_id) {
  return catalog_client_->DeleteTable(table_id.database_oid, table_id.object_oid);
}

Status PgSession::DropIndex(const PgObjectId& index_id, PgOid *base_table_oid, bool wait) {
  return catalog_client_->DeleteIndexTable(index_id.database_oid, index_id.object_oid, base_table_oid); 
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

void PgSession::InvalidateTableCache(const PgObjectId& table_id) {
  const TableId yb_table_id = table_id.GetYBTableId();
  table_cache_.erase(yb_table_id);
}

void PgSession::StartOperationsBuffering() {
  DCHECK(!buffering_enabled_);
  DCHECK(buffered_keys_.empty());
  buffering_enabled_ = true;
}

Status PgSession::StopOperationsBuffering() {
  DCHECK(buffering_enabled_);
  buffering_enabled_ = false;
  return FlushBufferedOperationsImpl();
}

Status PgSession::ResetOperationsBuffering() {
  SCHECK(buffered_keys_.empty(),
         IllegalState,
         Format("Pending operations are not expected, $0 found", buffered_keys_.size()));
  buffering_enabled_ = false;
  return Status::OK();
}

Status PgSession::FlushBufferedOperations() {
  return FlushBufferedOperationsImpl();
}

void PgSession::DropBufferedOperations() {
  VLOG_IF(1, !buffered_keys_.empty())
          << "Dropping " << buffered_keys_.size() << " pending operations";
  buffered_keys_.clear();
  buffered_ops_.clear();
  buffered_txn_ops_.clear();
}

Status PgSession::FlushBufferedOperationsImpl() {
  auto ops = std::move(buffered_ops_);
  auto txn_ops = std::move(buffered_txn_ops_);
  buffered_keys_.clear();
  buffered_ops_.clear();
  buffered_txn_ops_.clear();
  if (!ops.empty()) {
    RETURN_NOT_OK(FlushBufferedOperationsImpl(ops, false /* transactional */));
  }
  if (!txn_ops.empty()) {
    // No transactional operations are expected in the initdb mode.
    DCHECK(!YBCIsInitDbModeEnvVarSet());
    RETURN_NOT_OK(FlushBufferedOperationsImpl(txn_ops, true /* transactional */));
  }
  return Status::OK();
}

Status PgSession::FlushBufferedOperationsImpl(const PgsqlOpBuffer& ops, bool transactional) {
  DCHECK(ops.size() > 0 && ops.size() <= default_session_max_batch_size);

  for (auto buffered_op : ops) {
    const auto& op = buffered_op.operation;
    DCHECK_EQ(ShouldHandleTransactionally(*op), transactional)
        << "Table name: " << op->getTable()->table_name()
        << ", table is transactional: "
        << op->IsTransactional()
        << ", initdb mode: " << YBCIsInitDbModeEnvVarSet();

    std::shared_ptr<K23SITxn> k23SITxn = GetTxnHandler(transactional, op->read_only());    
    RETURN_NOT_OK(k2_adapter_->Apply(op, k23SITxn));
  }
  const auto status = k2_adapter_->FlushFuture().get();

  // TODO: need to combine errors from the batch to the status if they exist

  for (const auto& buffered_op : ops) {
    RETURN_NOT_OK(HandleResponse(*buffered_op.operation, buffered_op.relation_id));
  } 

  return Status::OK();
}

Status PgSession::HandleResponse(const PgOpTemplate& op, const PgObjectId& relation_id) {
  if (op.succeeded()) {
    return Status::OK();
  }

  const auto& response = op.response();
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
  return op.IsTransactional() && !YBCIsInitDbModeEnvVarSet();
}

PgSessionAsyncRunResult::PgSessionAsyncRunResult(std::future<Status> future_status,
                                                 scoped_refptr<K2Adapter> client)
    :  future_status_(std::move(future_status)),
       client_(client) {
}

Status PgSessionAsyncRunResult::GetStatus() {
  DCHECK(InProgress());
  auto status = future_status_.get();
  future_status_ = std::future<Status>();
  // TODO: populate errors to status if client supports batch operations
  return status;
}

bool PgSessionAsyncRunResult::InProgress() const {
  return future_status_.valid();
}

PgSession::RunHelper::RunHelper(scoped_refptr<PgSession> pg_session, scoped_refptr<K2Adapter> client, bool transactional)
    :  pg_session_(pg_session),
       client_(client),
       transactional_(transactional),
       buffered_ops_(transactional_ ? pg_session_->buffered_txn_ops_
                                    : pg_session_->buffered_ops_) {
  if (!transactional_) {
    pg_session_->InvalidateForeignKeyReferenceCache();
  }
}

Status PgSession::RunHelper::Apply(std::shared_ptr<PgOpTemplate> op,
                                   const PgObjectId& relation_id,
                                   uint64_t* read_time,
                                   bool force_non_bufferable) {
  auto& buffered_keys = pg_session_->buffered_keys_;
  if (pg_session_->buffering_enabled_ && !force_non_bufferable &&
      op->type() == PgOpTemplate::Type::WRITE) {
    const auto& wop = down_cast<PgWriteOpTemplate*>(op.get());
    std::string row_id = client_->GetRowId(wop->request());
    std::string table_id = wop->request()->table_name;
    // Check for buffered operation related to same row.
    if (PREDICT_FALSE(!buffered_keys.insert(RowIdentifier(table_id, row_id)).second)) {
      RETURN_NOT_OK(pg_session_->FlushBufferedOperationsImpl());
      buffered_keys.insert(RowIdentifier(table_id, row_id));
    }
    buffered_ops_.push_back({std::move(op), relation_id});
    // Flush buffers in case limit of operations in single batch exceeded.
    return PREDICT_TRUE(buffered_keys.size() < default_session_max_batch_size)
        ? Status::OK()
        : pg_session_->FlushBufferedOperationsImpl();
  }

  // Flush all buffered operations (if any) before performing non-bufferable operation
  if (!buffered_keys.empty()) {
    RETURN_NOT_OK(pg_session_->FlushBufferedOperationsImpl());
  }

  // TODO: ybc has the logic to check if needs_pessimistic_locking here by looking at row_mark_type
  // in the request, but K2 SKV does not support pessimistic locking, should we simply skip that logic?

  std::shared_ptr<K23SITxn> k23SITxn = pg_session_->GetTxnHandler(transactional_, op->read_only());
  return client_->Apply(std::move(op), k23SITxn);
}

Result<PgSessionAsyncRunResult> PgSession::RunHelper::Flush() {
  auto future_status = MakeFuture<Status>([this](auto callback) {
      client_->FlushAsync([callback](const Status& status) { callback(status); });
  });
  return PgSessionAsyncRunResult(std::move(future_status), client_);  
}

Result<PgTableDesc::ScopedRefPtr> PgSession::LoadTable(const PgObjectId& table_id) {
 VLOG(3) << "Loading table descriptor for " << table_id;
  const TableId yb_table_id = table_id.GetYBTableId();
  shared_ptr<TableInfo> table;

  auto cached_table = table_cache_.find(yb_table_id);
  if (cached_table == table_cache_.end()) {
    VLOG(4) << "Table cache MISS: " << table_id;
    Status s = catalog_client_->OpenTable(table_id.database_oid, table_id.object_oid, &table);
    if (!s.ok()) {
      VLOG(3) << "LoadTable: Server returns an error: " << s;
      return STATUS_FORMAT(NotFound, "Error loading table with oid $0 in database with oid $1: $2",
                           table_id.object_oid, table_id.database_oid, s.ToUserMessage());
    }
    table_cache_[yb_table_id] = table;
  } else {
    VLOG(4) << "Table cache HIT: " << table_id;
    table = cached_table->second;
  }

  return make_scoped_refptr<PgTableDesc>(table);
}

Result<bool> PgSession::IsInitDbDone() {
  bool isDone = false;
  RETURN_NOT_OK(catalog_client_->IsInitDbDone(&isDone));
  return isDone;
}

Result<uint64_t> PgSession::GetSharedCatalogVersion() {
  // It is the same as YBCPgGetCatalogMasterVersion() for now since we use local catalog manager for the timebeing
  uint64_t catalog_version;
  RETURN_NOT_OK(catalog_client_->GetCatalogVersion(&catalog_version));
  return catalog_version;
}

bool operator==(const PgForeignKeyReference& k1, const PgForeignKeyReference& k2) {
  return k1.table_id == k2.table_id &&
      k1.ybctid == k2.ybctid;
}

size_t hash_value(const PgForeignKeyReference& key) {
  size_t hash = 0;
  boost::hash_combine(hash, key.table_id);
  boost::hash_combine(hash, key.ybctid);
  return hash;
}

bool PgSession::ForeignKeyReferenceExists(uint32_t table_id, std::string&& ybctid) {
  PgForeignKeyReference reference = {table_id, std::move(ybctid)};
  return fk_reference_cache_.find(reference) != fk_reference_cache_.end();
}

Status PgSession::CacheForeignKeyReference(uint32_t table_id, std::string&& ybctid) {
  PgForeignKeyReference reference = {table_id, std::move(ybctid)};
  fk_reference_cache_.emplace(reference);
  return Status::OK();
}

Status PgSession::DeleteForeignKeyReference(uint32_t table_id, std::string&& ybctid) {
  PgForeignKeyReference reference = {table_id, std::move(ybctid)};
  fk_reference_cache_.erase(reference);
  return Status::OK();
}

std::shared_ptr<K23SITxn> PgSession::GetTxnHandler(bool transactional, bool read_only) {
  return pg_txn_handler_->GetNewTransactionIfNecessary(read_only);
}

Result<IndexPermissions> PgSession::WaitUntilIndexPermissionsAtLeast(
    const PgObjectId& table_id,
    const PgObjectId& index_id,
    const IndexPermissions& target_index_permissions) {
  // TODO: add implementation
  return IndexPermissions::INDEX_PERM_NOT_USED;
}

Status PgSession::AsyncUpdateIndexPermissions(const PgObjectId& indexed_table_id) {
  // TODO: add implementation
  return Status::OK();
}

}  // namespace gate
}  // namespace k2pg
