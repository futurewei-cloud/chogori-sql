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

#include "k2session.h"

namespace k2 {
namespace gate {

RowIdentifier::RowIdentifier(const DocWriteOp& op, K2Client* k2_client) :
  table_id_(&op.request().table_id) {
  auto& request = op.request();
  if (request.ybctid_column_value) {
    // ybctid_ = &request.ybctid_column_value->binary_value();
  } else {
    // calculate the doc key from k2 client
//    ybctid_holder_ = k2_client->getDocKey(request);
    ybctid_ = nullptr;
  }
}

const string& RowIdentifier::ybctid() const {
  return ybctid_ ? *ybctid_ : ybctid_holder_;
}

const string& RowIdentifier::table_id() const {
  return *table_id_;
}

bool operator==(const RowIdentifier& k1, const RowIdentifier& k2) {
  return k1.table_id() == k2.table_id() && k1.ybctid() == k2.ybctid();
}

size_t hash_value(const RowIdentifier& key) {
  size_t hash = 0;
  boost::hash_combine(hash, key.table_id());
  boost::hash_combine(hash, key.ybctid());
  return hash;
}

K2Session::K2Session(
    K2Client* k2_client,
    const string& database_name,
    const YBCPgCallbacks& pg_callbacks)
    : k2_client_(k2_client),
      pg_callbacks_(pg_callbacks) {
    ConnectDatabase(database_name);
}

K2Session::~K2Session() {
}

Status K2Session::ConnectDatabase(const string& database_name) {
  connected_database_ = database_name;
  return Status::OK();
}

Status K2Session::CreateDatabase(const string& database_name,
                                 const PgOid database_oid,
                                 const PgOid source_database_oid,
                                 const PgOid next_oid) {
  return k2_client_->CreateNamespace(database_name,
                                  "" /* creator_role_name */,
                                  GetPgsqlNamespaceId(database_oid),
                                  source_database_oid != kPgInvalidOid
                                  ? GetPgsqlNamespaceId(source_database_oid) : "",
                                  next_oid);
}

Status K2Session::DropDatabase(const string& database_name, PgOid database_oid) {
  RETURN_NOT_OK(k2_client_->DeleteNamespace(database_name,
                                         GetPgsqlNamespaceId(database_oid)));
  // TODO: enable the following code once adding sequence support                                       
  // RETURN_NOT_OK(DeleteDBSequences(database_oid));
  return Status::OK();
}

Status K2Session::CreateTable(NamespaceId& namespace_id, NamespaceName& namespace_name, TableName& table_name, const PgObjectId& table_id, 
    Schema& schema, std::vector<std::string>& range_columns, std::vector<std::vector<SqlValue>>& split_rows, 
    bool is_pg_catalog_table, bool is_shared_table, bool if_not_exist) {
   return k2_client_->CreateTable(namespace_id, namespace_name, table_name, table_id, schema, range_columns, split_rows, 
    is_pg_catalog_table, is_shared_table, if_not_exist);
}

Status K2Session::DropTable(const PgObjectId& table_id) {
  return k2_client_->DeleteTable(table_id.GetYBTableId());
}

Status K2Session::ReserveOids(const PgOid database_oid,
                              const PgOid next_oid,
                              const uint32_t count,
                              PgOid *begin_oid,
                              PgOid *end_oid) {
  return k2_client_->ReservePgsqlOids(GetPgsqlNamespaceId(database_oid), next_oid, count,
                                   begin_oid, end_oid);
}

Status K2Session::GetCatalogMasterVersion(uint64_t *version) {
  return k2_client_->GetYsqlCatalogMasterVersion(version);
}

void K2Session::InvalidateTableCache(const PgObjectId& table_id) {
  const TableId yb_table_id = table_id.GetYBTableId();
  table_cache_.erase(yb_table_id);
}

void K2Session::StartOperationsBuffering() {
  DCHECK(!buffering_enabled_);
  DCHECK(buffered_keys_.empty());
  buffering_enabled_ = true;
}

Status K2Session::StopOperationsBuffering() {
  DCHECK(buffering_enabled_);
  buffering_enabled_ = false;
  return FlushBufferedOperationsImpl();
}

Status K2Session::ResetOperationsBuffering() {
  SCHECK(buffered_keys_.empty(),
         IllegalState,
         Format("Pending operations are not expected, $0 found", buffered_keys_.size()));
  buffering_enabled_ = false;
  return Status::OK();
}

Status K2Session::FlushBufferedOperations() {
  return FlushBufferedOperationsImpl();
}

void K2Session::DropBufferedOperations() {
  VLOG_IF(1, !buffered_keys_.empty())
          << "Dropping " << buffered_keys_.size() << " pending operations";
  buffered_keys_.clear();
  buffered_ops_.clear();
  buffered_txn_ops_.clear();
}

Status K2Session::FlushBufferedOperationsImpl() {
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
//    DCHECK(!YBCIsInitDbModeEnvVarSet());
    RETURN_NOT_OK(FlushBufferedOperationsImpl(txn_ops, true /* transactional */));
  }
  return Status::OK();
}

Status K2Session::FlushBufferedOperationsImpl(const PgsqlOpBuffer& ops, bool transactional) {

/*   
  DCHECK(ops.size() > 0 && ops.size() <= FLAGS_ysql_session_max_batch_size);
  auto session = VERIFY_RESULT(GetSession(transactional, false));
  if (session != session_.get()) {
    DCHECK(transactional);
    session->SetInTxnLimit(HybridTime(clock_->Now().ToUint64()));
  }

  for (auto buffered_op : ops) {
    const auto& op = buffered_op.operation;
    DCHECK_EQ(ShouldHandleTransactionally(*op), transactional)
        << "Table name: " << op->table()->name().ToString()
        << ", table is transactional: "
        << op->table()->schema().table_properties().is_transactional()
        << ", initdb mode: " << YBCIsInitDbModeEnvVarSet();
    RETURN_NOT_OK(session->Apply(op));
  }
  const auto status = session->FlushFuture().get();
  RETURN_NOT_OK(CombineErrorsToStatus(session->GetPendingErrors(), status));

  for (const auto& buffered_op : ops) {
    RETURN_NOT_OK(HandleResponse(*buffered_op.operation, buffered_op.relation_id));
  } */

  return Status::OK();
}

Status K2Session::HandleResponse(const DocOp& op, const PgObjectId& relation_id) {
/*   if (op.succeeded()) {
    return Status::OK();
  }
  const auto& response = op.response();
  YBPgErrorCode pg_error_code = YBPgErrorCode::YB_PG_INTERNAL_ERROR;
  if (response.has_pg_error_code()) {
    pg_error_code = static_cast<YBPgErrorCode>(response.pg_error_code());
  }

  TransactionErrorCode txn_error_code = TransactionErrorCode::kNone;
  if (response.txn_error_code != 0) {
    txn_error_code = static_cast<TransactionErrorCode>(response.txn_error_code());
  }

  Status s;
  if (response.status() == PgsqlResponsePB::PGSQL_STATUS_DUPLICATE_KEY_ERROR) {
    char constraint_name[0xFF];
    constraint_name[sizeof(constraint_name) - 1] = 0;
    pg_callbacks_.FetchUniqueConstraintName(relation_id.object_oid,
                                            constraint_name,
                                            sizeof(constraint_name) - 1);
    s = STATUS(
        AlreadyPresent,
        Format("duplicate key value violates unique constraint \"$0\"", Slice(constraint_name)),
        Slice(),
        PgsqlError(YBPgErrorCode::YB_PG_UNIQUE_VIOLATION));
  } else {
    s = STATUS(QLError, op.response().error_message(), Slice(),
               PgsqlError(pg_error_code));
  }
  s = s.CloneAndAddErrorCode(TransactionError(txn_error_code));
  return s; */
  return Status::OK();
}

Result<K2TableDesc::ScopedRefPtr> K2Session::LoadTable(const PgObjectId& table_id) {
  // TODO: add implementation                                   
  return nullptr;
}

Result<bool> K2Session::IsInitDbDone() {
  // TODO: add implementation                                   
  return false;
}

Result<uint64_t> K2Session::GetSharedCatalogVersion() {
  // TODO: add implementation
  return 0;
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

bool K2Session::ForeignKeyReferenceExists(uint32_t table_id, std::string&& ybctid) {
  PgForeignKeyReference reference = {table_id, std::move(ybctid)};
  return fk_reference_cache_.find(reference) != fk_reference_cache_.end();
}

Status K2Session::CacheForeignKeyReference(uint32_t table_id, std::string&& ybctid) {
  PgForeignKeyReference reference = {table_id, std::move(ybctid)};
  fk_reference_cache_.emplace(reference);
  return Status::OK();
}

Status K2Session::DeleteForeignKeyReference(uint32_t table_id, std::string&& ybctid) {
  PgForeignKeyReference reference = {table_id, std::move(ybctid)};
  fk_reference_cache_.erase(reference);
  return Status::OK();
}

}  // namespace gate
}  // namespace k2
