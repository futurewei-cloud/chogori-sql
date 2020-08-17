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

void K2Session::InvalidateTableCache(const PgObjectId& table_id) {
  const TableId yb_table_id = table_id.GetYBTableId();
  table_cache_.erase(yb_table_id);
}

}  // namespace gate
}  // namespace k2
