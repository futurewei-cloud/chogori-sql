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

#ifndef CHOGORI_GATE_PG_SESSION_H
#define CHOGORI_GATE_PG_SESSION_H

#include <optional>
#include <unordered_set>

#include "yb/common/concurrent/ref_counted.h"
#include "yb/common/oid_generator.h"
#include "yb/common/sys/monotime.h"
#include "yb/entities/entity_ids.h"
#include "yb/entities/index.h"
#include "yb/entities/schema.h"
#include "yb/pggate/pg_env.h"
#include "yb/pggate/pg_tabledesc.h"
#include "yb/pggate/pg_op_api.h"
#include "yb/pggate/k2_adapter.h"
#include "yb/pggate/pg_gate_api.h"
#include "yb/pggate/pg_txn_handler.h"
#include "yb/pggate/sql_catalog_client.h"

namespace k2pg {
namespace gate {

using yb::RefCountedThreadSafe;
using namespace k2pg::sql;
using yb::Status;

static const int default_session_max_batch_size = 5;

struct BufferableOperation {
  std::shared_ptr<PgOpTemplate> operation;
  // Postgres's relation id. Required to resolve constraint name in case
  // operation will fail with PGSQL_STATUS_DUPLICATE_KEY_ERROR.
  PgObjectId relation_id;
};

typedef std::vector<BufferableOperation> PgsqlOpBuffer;

// This class provides access to run operation's result by reading std::future<Status>
// and analyzing possible pending errors of k2 client object in GetStatus() method.
// If GetStatus() method will not be called, possible errors in k2 client object will be preserved.
class PgSessionAsyncRunResult {
 public:
  PgSessionAsyncRunResult() = default;
  PgSessionAsyncRunResult(std::future<Status> future_status,
                          scoped_refptr<K2Adapter> client);
  CHECKED_STATUS GetStatus();
  bool InProgress() const;

 private:
  std::future<Status> future_status_;
  scoped_refptr<K2Adapter> client_;
};

struct PgForeignKeyReference {
  const uint32_t table_id;
  const std::string ybctid;

  PgForeignKeyReference(uint32_t i_table_id, std::string &&i_ybctid)
      : table_id(i_table_id), ybctid(i_ybctid) {
  }

  std::string ToString() const {
    return Format("{ table_id: $0 ybctid: $1 }",
                  table_id, ybctid);
  }
};

class RowIdentifier {
 public:
  explicit RowIdentifier(const std::string& table_id, const std::string row_id);
  inline const std::string& table_id() const;
  inline const std::string& row_id() const;

 private:
  std::string table_id_;
  std::string row_id_;
};

// This class is not thread-safe as it is mostly used by a single-threaded PostgreSQL backend
// process.
class PgSession : public RefCountedThreadSafe<PgSession> {
 public:
  // Public types.
  typedef scoped_refptr<PgSession> ScopedRefPtr;

  // Constructors.
  PgSession(scoped_refptr<SqlCatalogClient> catalog_client,
            scoped_refptr<K2Adapter> k2_adapter,
            const string& database_name,
            scoped_refptr<PgTxnHandler> pg_txn_handler,
            const YBCPgCallbacks& pg_callbacks);

  virtual ~PgSession();

  CHECKED_STATUS ConnectDatabase(const std::string& database_name);

  CHECKED_STATUS CreateDatabase(const std::string& database_name,
                                PgOid database_oid,
                                PgOid source_database_oid,
                                PgOid next_oid);

  CHECKED_STATUS DropDatabase(const std::string& database_name, PgOid database_oid);

  CHECKED_STATUS RenameDatabase(const std::string& database_name, PgOid database_oid, std::optional<std::string> rename_to);

  CHECKED_STATUS CreateTable(NamespaceId& namespace_id, NamespaceName& namespace_name, TableName& table_name, const PgObjectId& table_id, 
    PgSchema& schema, bool is_pg_catalog_table, bool is_shared_table, bool if_not_exist);

  CHECKED_STATUS CreateIndexTable(NamespaceId& namespace_id, NamespaceName& namespace_name, TableName& table_name, const PgObjectId& table_id, 
    const PgObjectId& base_table_id, PgSchema& schema, bool is_unique_index, bool skip_index_backfill,
    bool is_pg_catalog_table, bool is_shared_table, bool if_not_exist);

  CHECKED_STATUS DropTable(const PgObjectId& table_id);

  CHECKED_STATUS DropIndex(const PgObjectId& index_id, PgOid *base_table_oid, bool wait = true);

  CHECKED_STATUS ReserveOids(PgOid database_oid,
                             PgOid nexte_oid,
                             uint32_t count,
                             PgOid *begin_oid,
                             PgOid *end_oid);

  CHECKED_STATUS GetCatalogMasterVersion(uint64_t *version);

  // API for sequences data operations.
  CHECKED_STATUS CreateSequencesDataTable();

  CHECKED_STATUS InsertSequenceTuple(int64_t db_oid,
                                     int64_t seq_oid,
                                     uint64_t ysql_catalog_version,
                                     int64_t last_val,
                                     bool is_called);

  CHECKED_STATUS UpdateSequenceTuple(int64_t db_oid,
                                     int64_t seq_oid,
                                     uint64_t ysql_catalog_version,
                                     int64_t last_val,
                                     bool is_called,
                                     std::optional<int64_t> expected_last_val,
                                     std::optional<bool> expected_is_called,
                                     bool* skipped);

  CHECKED_STATUS ReadSequenceTuple(int64_t db_oid,
                                   int64_t seq_oid,
                                   uint64_t ysql_catalog_version,
                                   int64_t *last_val,
                                   bool *is_called);

  CHECKED_STATUS DeleteSequenceTuple(int64_t db_oid, int64_t seq_oid);

  CHECKED_STATUS DeleteDBSequences(int64_t db_oid);

  // Access functions for connected database.
  const char* connected_dbname() const {
    return connected_database_.c_str();
  }

  const string& connected_database() const {
    return connected_database_;
  }
  void set_connected_database(const std::string& database) {
    connected_database_ = database;
  }
  void reset_connected_database() {
    connected_database_ = "";
  }

  void InvalidateCache() {
    table_cache_.clear();
  }

  void InvalidateForeignKeyReferenceCache() {
    fk_reference_cache_.clear();
  }

  Result<PgTableDesc::ScopedRefPtr> LoadTable(const PgObjectId& table_id);

  void InvalidateTableCache(const PgObjectId& table_id);
      
  // Check if initdb has already been run before. Needed to make initdb idempotent.
  Result<bool> IsInitDbDone();   

  // Returns the local catalog version stored in shared memory, or an error if
  // the shared memory has not been initialized (e.g. in initdb).
  Result<uint64_t> GetSharedCatalogVersion();

  const string& GetClientId() const {
    return client_id_;
  }

  int64_t GetNextStmtId() {
    // TODO: add more complext stmt id generation logic
    return stmt_id_++;
  }

  void SetTimeout(const int timeout_ms) {
      timeout_ = MonoDelta::FromMilliseconds(timeout_ms);
  }

  // Returns true if the row referenced by ybctid exists in FK reference cache (Used for caching
  // foreign key checks).
  bool ForeignKeyReferenceExists(uint32_t table_id, std::string&& ybctid);

  // Adds the row referenced by ybctid to FK reference cache.
  CHECKED_STATUS CacheForeignKeyReference(uint32_t table_id, std::string&& ybctid);

  // Deletes the row referenced by ybctid from FK reference cache.
  CHECKED_STATUS DeleteForeignKeyReference(uint32_t table_id, std::string&& ybctid);

  // Start operation buffering. Buffering must not be in progress.
  void StartOperationsBuffering();
  // Flush all pending buffered operation and stop further buffering.
  // Buffering must be in progress.
  CHECKED_STATUS StopOperationsBuffering();
  // Stop further buffering. Buffering may be in any state,
  // but pending buffered operations are not allowed.
  CHECKED_STATUS ResetOperationsBuffering();

  // Flush all pending buffered operations. Buffering mode remain unchanged.
  CHECKED_STATUS FlushBufferedOperations();
  // Drop all pending buffered operations. Buffering mode remain unchanged.
  void DropBufferedOperations();

  // Run (apply + flush) the given operation to read and write database content.
  // Template is used here to handle all kind of derived operations
  // (shared_ptr<PgReadOpTemplate>, shared_ptr<PgWriteOpTemplate>)
  // without implicitly conversion to shared_ptr<PgReadOpTemplate>.
  // Conversion to shared_ptr<PgOpTemplate> will be done later and result will re-used with move.
  template<class Op>
  Result<PgSessionAsyncRunResult> RunAsync(const std::shared_ptr<Op>& op,
                                           const PgObjectId& relation_id,
                                           uint64_t* read_time,
                                           bool force_non_bufferable) {
    return RunAsync(&op, 1, relation_id, read_time, force_non_bufferable);
  }

  // Run (apply + flush) list of given operations to read and write database content.
  template<class Op>
  Result<PgSessionAsyncRunResult> RunAsync(const std::vector<std::shared_ptr<Op>>& ops,
                                           const PgObjectId& relation_id,
                                           uint64_t* read_time,
                                           bool force_non_bufferable) {
    DCHECK(!ops.empty());
    return RunAsync(ops.data(), ops.size(), relation_id, read_time, force_non_bufferable);
  }

  // Run multiple operations.
  template<class Op>
  Result<PgSessionAsyncRunResult> RunAsync(const std::shared_ptr<Op>* op,
                                           size_t ops_count,
                                           const PgObjectId& relation_id,
                                           uint64_t* read_time,
                                           bool force_non_bufferable) {
    DCHECK_GT(ops_count, 0);
    RunHelper runner(this, k2_adapter_, ShouldHandleTransactionally(**op));
    for (auto end = op + ops_count; op != end; ++op) {
      RETURN_NOT_OK(runner.Apply(*op, relation_id, read_time, force_non_bufferable));
    }
    return runner.Flush();
  }

  CHECKED_STATUS HandleResponse(const PgOpTemplate& op, const PgObjectId& relation_id);

  // Returns the appropriate session to use, in most cases the one used by the current transaction.
  // read_only - whether this is being done in the context of a read-only operation.
  std::shared_ptr<K23SITxn> GetTxnHandler(bool transactional, bool read_onl);

  Result<IndexPermissions> WaitUntilIndexPermissionsAtLeast(
      const PgObjectId& table_id,
      const PgObjectId& index_id,
      const IndexPermissions& target_index_permissions);

  CHECKED_STATUS AsyncUpdateIndexPermissions(const PgObjectId& indexed_table_id);

  private:
  CHECKED_STATUS FlushBufferedOperationsImpl();

  CHECKED_STATUS FlushBufferedOperationsImpl(const PgsqlOpBuffer& ops, bool transactional);

  // Helper class to run multiple operations on single session.
  // This class allows to keep implementation of RunAsync template method simple
  // without moving its implementation details into header file.
  class RunHelper {
   public:
    RunHelper(scoped_refptr<PgSession> pg_session, scoped_refptr<K2Adapter> client, bool transactional);
    CHECKED_STATUS Apply(std::shared_ptr<PgOpTemplate> op,
                         const PgObjectId& relation_id,
                         uint64_t* read_time,
                         bool force_non_bufferable);
    Result<PgSessionAsyncRunResult> Flush();

   private:
    scoped_refptr<PgSession> pg_session_;
    scoped_refptr<K2Adapter> client_;
    bool transactional_;
    PgsqlOpBuffer& buffered_ops_;
  };

  // Flush buffered write operations from the given buffer.
  Status FlushBufferedWriteOperations(PgsqlOpBuffer* write_ops, bool transactional);

  // Whether we should use transactional or non-transactional session.
  bool ShouldHandleTransactionally(const PgOpTemplate& op);

  // Connected database.
  std::string connected_database_;

  // Execution status.
  Status status_;
  string errmsg_;

    // Rowid generator.
  ObjectIdGenerator rowid_generator_;

  scoped_refptr<SqlCatalogClient> catalog_client_;

  scoped_refptr<K2Adapter> k2_adapter_;

  // A transaction handler allowing to begin/abort/commit transactions.
  scoped_refptr<PgTxnHandler> pg_txn_handler_;

  std::unordered_map<TableId, std::shared_ptr<TableInfo>> table_cache_;
  std::unordered_set<PgForeignKeyReference, boost::hash<PgForeignKeyReference>> fk_reference_cache_;

  // Should write operations be buffered?
  bool buffering_enabled_ = false;
  PgsqlOpBuffer buffered_ops_;
  PgsqlOpBuffer buffered_txn_ops_;
  std::unordered_set<RowIdentifier, boost::hash<RowIdentifier>> buffered_keys_;

  const YBCPgCallbacks& pg_callbacks_;

  // TODO: pass client/user id from pg?
  string client_id_;

  std::atomic<int64_t> stmt_id_ = 1;

  MonoDelta timeout_;
};

}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_PG_SESSION_H