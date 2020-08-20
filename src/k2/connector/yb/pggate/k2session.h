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

#ifndef CHOGORI_GATE_SESSION_H
#define CHOGORI_GATE_SESSION_H

#include <unordered_set>

#include "yb/common/concurrent/ref_counted.h"
#include "yb/common/oid_generator.h"
#include "yb/common/sys/monotime.h"
#include "yb/entities/table.h"
#include "yb/pggate/pg_env.h"
#include "yb/pggate/k2client.h"

namespace k2 {
namespace gate {

using yb::RefCountedThreadSafe;
using namespace k2::sql;

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

// This class is not thread-safe as it is mostly used by a single-threaded PostgreSQL backend
// process.
class K2Session : public RefCountedThreadSafe<K2Session> {
 public:
  // Public types.
  typedef scoped_refptr<K2Session> ScopedRefPtr;

  // Constructors.
  K2Session(K2Client* k2_client,
            const string& database_name,
            const YBCPgCallbacks& pg_callbacks);

  virtual ~K2Session();

  CHECKED_STATUS ConnectDatabase(const std::string& database_name);

  CHECKED_STATUS CreateDatabase(const std::string& database_name,
                                PgOid database_oid,
                                PgOid source_database_oid,
                                PgOid next_oid);

  CHECKED_STATUS DropDatabase(const std::string& database_name, PgOid database_oid);

  CHECKED_STATUS CreateTable(NamespaceId& namespace_id, NamespaceName& namespace_name, TableName& table_name, const PgObjectId& table_id, 
    Schema& schema, std::vector<std::string>& range_columns, std::vector<std::vector<SqlValue>>& split_rows, 
    bool is_pg_catalog_table, bool is_shared_table, bool if_not_exist);

  CHECKED_STATUS DropTable(const PgObjectId& table_id);

  CHECKED_STATUS ReserveOids(PgOid database_oid,
                             PgOid nexte_oid,
                             uint32_t count,
                             PgOid *begin_oid,
                             PgOid *end_oid);

  CHECKED_STATUS GetCatalogMasterVersion(uint64_t *version);

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

  Result<TableInfo::ScopedRefPtr> LoadTable(const PgObjectId& table_id);

  void InvalidateTableCache(const PgObjectId& table_id);
      
  // Check if initdb has already been run before. Needed to make initdb idempotent.
  Result<bool> IsInitDbDone();   

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

  private:
    // Connected database.
  std::string connected_database_;

  // Execution status.
  Status status_;
  string errmsg_;

    // Rowid generator.
  ObjectIdGenerator rowid_generator_;

  K2Client* const k2_client_;

  std::unordered_map<TableId, std::shared_ptr<TableInfo>> table_cache_;
  std::unordered_set<PgForeignKeyReference, boost::hash<PgForeignKeyReference>> fk_reference_cache_;

  // Should write operations be buffered?
  bool buffering_enabled_ = false;

  const YBCPgCallbacks& pg_callbacks_;

  MonoDelta timeout_;
};

}  // namespace gate
}  // namespace k2

#endif //CHOGORI_GATE_SESSION_H