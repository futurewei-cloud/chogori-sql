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

#include "yb/pggate/sql_catalog_client.h"

namespace k2pg {
namespace sql {

Status SqlCatalogClient::IsInitDbDone(bool* isDone) {                                  
  return catalog_manager_->IsInitDbDone(isDone);
}

Status SqlCatalogClient::CreateNamespace(const std::string& namespace_name,
                                 const std::string& creator_role_name,
                                 const std::string& namespace_id,
                                 const std::string& source_namespace_id,
                                 const std::optional<uint32_t>& next_pg_oid) {
  // TODO: add implementation                                   
  return Status::OK();
}

Status SqlCatalogClient::DeleteNamespace(const std::string& namespace_name,
                                 const std::string& namespace_id) {
  // TODO: add implementation                                   
  return Status::OK();
}

Status SqlCatalogClient::CreateTable(NamespaceId& namespace_id, NamespaceName& namespace_name, TableName& table_name, const PgObjectId& table_id, 
    PgSchema& schema, std::vector<std::string>& range_columns, std::vector<std::vector<SqlValue>>& split_rows, 
    bool is_pg_catalog_table, bool is_shared_table, bool if_not_exist) {

  // TODO: add implementation                                   
  return Status::OK();
}

Status SqlCatalogClient::DeleteTable(const string& table_id, bool wait) {
  // TODO: add implementation                                   
  return Status::OK();
}
    
Status OpenTable(const TableId& table_id, std::shared_ptr<TableInfo>* table) {
  // TODO: add implementation                                   
  return Status::OK();
}

Status SqlCatalogClient::ReservePgsqlOids(const std::string& namespace_id,
                                  const uint32_t next_oid, const uint32_t count,
                                  uint32_t* begin_oid, uint32_t* end_oid) {
  // TODO: add implementation                                   
  return Status::OK();
}

Status SqlCatalogClient::GetCatalogVersion(uint64_t *catalog_version) {
  *catalog_version = catalog_manager_->GetCatalogVersion();
  return Status::OK();
}    
 
}  // namespace sql
}  // namespace k2pg