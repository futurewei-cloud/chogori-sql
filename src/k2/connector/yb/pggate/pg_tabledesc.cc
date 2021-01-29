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

#include "yb/pggate/pg_tabledesc.h"

#include "yb/pggate/catalog/collection_util.h"

namespace k2pg {
namespace gate {

using k2pg::sql::Schema;
using k2pg::sql::ColumnSchema;
using k2pg::sql::PgSystemAttrNum;
using k2pg::sql::catalog::CollectionUtil;

PgTableDesc::PgTableDesc(std::shared_ptr<TableInfo> pg_table) : is_index_(false),
    namespace_id_(pg_table->namespace_id()), table_id_(pg_table->table_id()), schema_version_(pg_table->schema().version()),
    transactional_(pg_table->schema().table_properties().is_transactional()),
    hash_column_num_(pg_table->schema().num_hash_key_columns()), key_column_num_(pg_table->schema().num_key_columns())
{
  // create PgTableDesc from a table
  const auto& schema = pg_table->schema();
  const int num_columns = schema.num_columns();
  columns_.resize(num_columns);
  for (size_t idx = 0; idx < num_columns; idx++) {
    // Find the column descriptor.
    const auto& col = schema.column(idx);

    // create map by attr_num instead of the default id
    ColumnDesc *desc = columns_[idx].desc();
    desc->Init(idx,
               schema.column_id(idx),
               col.name(),
               idx < schema.num_hash_key_columns(),
               idx < schema.num_key_columns(),
               col.order() /* attr_num */,
               col.type(),
               col.sorting_type());
    attr_num_map_[col.order()] = idx;
    K2LOG_V(log::pg, "Table attr_num_map: [{}]= {}, for id={}, name={}",
       col.order(), idx, schema.column_id(idx), col.name());
  }

  // Create virtual columns.
  column_ybctid_.Init(PgSystemAttrNum::kYBTupleId);

  collection_name_ = CollectionUtil::GetCollectionName(namespace_id_, pg_table->is_shared_table());

  K2LOG_D(log::pg, "PgTableDesc table_id={}, ns_id={}, schema_version={}, hash_columns={}, key_columns={}, columns={}, transactional={}",
    table_id_, namespace_id_, schema_version_, hash_column_num_, key_column_num_, columns_.size(), transactional_);
}

PgTableDesc::PgTableDesc(const IndexInfo& index_info, const std::string& namespace_id, bool is_transactional) : is_index_(true),
    namespace_id_(namespace_id), table_id_(index_info.table_id()), schema_version_(index_info.version()), transactional_(is_transactional),
    hash_column_num_(index_info.hash_column_count()), key_column_num_(index_info.key_column_count())
{
  // create PgTableDesc from an index
  const int num_columns = index_info.num_columns();
  columns_.resize(num_columns);
  for (size_t idx = 0; idx < num_columns; idx++) {
    // Find the column descriptor.
    const auto& col = index_info.column(idx);

    // create map by attr_num instead of the default id
    ColumnDesc *desc = columns_[idx].desc();
    desc->Init(idx,
               col.column_id,
               col.column_name,
               col.is_hash,
               col.is_hash || col.is_range,
               col.order /* attr_num */,
               SQLType::Create(col.type),
               col.sorting_type);
    attr_num_map_[col.order] = idx;
    K2LOG_V(log::pg, "Table attr_num_map: [{}]= {}, for id={}, name={}", col.order, idx, col.column_id, col.column_name);
  }

  // Create virtual columns.
  column_ybctid_.Init(PgSystemAttrNum::kYBTupleId);

  collection_name_ = CollectionUtil::GetCollectionName(namespace_id_, index_info.is_shared());

  K2LOG_D(log::pg, "PgTableDesc table_id={}, ns_id={}, schema_version={}, hash_columns={}, key_columns={}, columns={}, transactional={}",
    table_id_, namespace_id_, schema_version_, hash_column_num_, key_column_num_, columns_.size(), transactional_);
}

Result<PgColumn *> PgTableDesc::FindColumn(int attr_num) {
  // Find virtual columns.
  if (attr_num == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
    return &column_ybctid_;
  }

  // Find physical column.
  const auto itr = attr_num_map_.find(attr_num);
  if (itr != attr_num_map_.end()) {
    return &columns_[itr->second];
  }

  return STATUS_FORMAT(InvalidArgument, "Invalid column number $0", attr_num);
}

Status PgTableDesc::GetColumnInfo(int16_t attr_number, bool *is_primary, bool *is_hash) const {
  const auto itr = attr_num_map_.find(attr_number);
  if (itr != attr_num_map_.end()) {
    const ColumnDesc* desc = columns_[itr->second].desc();
    *is_primary = desc->is_primary();
    *is_hash = desc->is_hash();
  } else {
    *is_primary = false;
    *is_hash = false;
  }
  return Status::OK();
}

std::unique_ptr<PgReadOpTemplate> PgTableDesc::NewPgsqlSelect(const string& client_id, int64_t stmt_id) {
  std::unique_ptr<PgReadOpTemplate> op = std::make_unique<PgReadOpTemplate>();
  std::shared_ptr<SqlOpReadRequest> req = op->request();
  req->client_id = client_id;
  req->collection_name = collection_name_;
  req->table_id = table_id_;
  req->schema_version = schema_version_;
  req->stmt_id = stmt_id;

  return op;
}

std::unique_ptr<PgWriteOpTemplate> PgTableDesc::NewPgsqlOpWrite(SqlOpWriteRequest::StmtType stmt_type, const string& client_id, int64_t stmt_id) {
  std::unique_ptr<PgWriteOpTemplate> op = std::make_unique<PgWriteOpTemplate>();
  std::shared_ptr<SqlOpWriteRequest> req = op->request();
  req->client_id = client_id;
  req->collection_name = collection_name_;
  req->table_id = table_id_;
  req->schema_version = schema_version_;
  req->stmt_id = stmt_id;
  req->stmt_type = stmt_type;

  return op;
}

std::unique_ptr<PgWriteOpTemplate> PgTableDesc::NewPgsqlInsert(const string& client_id, int64_t stmt_id) {
  return NewPgsqlOpWrite(SqlOpWriteRequest::StmtType::PGSQL_INSERT, client_id, stmt_id);
}

std::unique_ptr<PgWriteOpTemplate> PgTableDesc::NewPgsqlUpdate(const string& client_id, int64_t stmt_id) {
  return NewPgsqlOpWrite(SqlOpWriteRequest::StmtType::PGSQL_UPDATE, client_id, stmt_id);
}

std::unique_ptr<PgWriteOpTemplate> PgTableDesc::NewPgsqlDelete(const string& client_id, int64_t stmt_id) {
  return NewPgsqlOpWrite(SqlOpWriteRequest::StmtType::PGSQL_DELETE, client_id, stmt_id);
}

}  // namespace gate
}  // namespace k2pg
