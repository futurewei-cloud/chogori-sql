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

#include "yb/pggate/pg_ddl.h"
#include "yb/pggate/pg_gate_typedefs.h"

namespace k2pg {
namespace gate {

using std::make_shared;
using std::shared_ptr;
using std::string;
using namespace std::literals;  // NOLINT
using namespace yb;
using namespace k2pg::sql;

//--------------------------------------------------------------------------------------------------
// PgCreateDatabase
//--------------------------------------------------------------------------------------------------

PgCreateDatabase::PgCreateDatabase(PgSession::ScopedRefPtr pg_session,
                                   const std::string& database_name,
                                   const PgOid database_oid,
                                   const PgOid source_database_oid,
                                   const PgOid next_oid)
    : PgDdl(std::move(pg_session)),
      database_name_(database_name),
      database_oid_(database_oid),
      source_database_oid_(source_database_oid),
      next_oid_(next_oid) {
}

PgCreateDatabase::~PgCreateDatabase() {
}

Status PgCreateDatabase::Exec() {
  return pg_session_->CreateDatabase(database_name_, database_oid_, source_database_oid_, next_oid_);
}

PgDropDatabase::PgDropDatabase(PgSession::ScopedRefPtr pg_session,
                               const std::string& database_name,
                               PgOid database_oid)
    : PgDdl(pg_session),
      database_name_(database_name),
      database_oid_(database_oid) {
}

PgDropDatabase::~PgDropDatabase() {
}

Status PgDropDatabase::Exec() {
  return pg_session_->DropDatabase(database_name_, database_oid_);
}

PgAlterDatabase::PgAlterDatabase(PgSession::ScopedRefPtr pg_session,
                               const std::string& database_name,
                               PgOid database_oid)
    : PgDdl(pg_session),
      database_name_(database_name),
      database_oid_(database_oid) {
}

PgAlterDatabase::~PgAlterDatabase() {
}

Status PgAlterDatabase::Exec() {
  return pg_session_->RenameDatabase(database_name_, database_oid_, rename_to_);
}

Status PgAlterDatabase::RenameDatabase(const std::string& new_name) {
  rename_to_ = new_name;  
  return Status::OK();
}

//--------------------------------------------------------------------------------------------------
// PgCreateTable
//--------------------------------------------------------------------------------------------------

PgCreateTable::PgCreateTable(PgSession::ScopedRefPtr pg_session,
                             const std::string& database_name,
                             const std::string& schema_name,
                             const std::string& table_name,
                             const PgObjectId& table_id,
                             bool is_shared_table,
                             bool if_not_exist,
                             bool add_primary_key)
    : PgDdl(pg_session),
      namespace_id_(GetPgsqlNamespaceId(table_id.database_oid)),
      namespace_name_(database_name),
      table_name_(table_name),
      table_id_(table_id),
      is_pg_catalog_table_(schema_name.compare("pg_catalog") == 0 ||
                           schema_name.compare("information_schema") == 0),
      is_shared_table_(is_shared_table),
      if_not_exist_(if_not_exist) {
  // Add internal primary key column to a Postgres table without a user-specified primary key.
  if (add_primary_key) {
    // For regular user table, ybrowid should be a hash key because ybrowid is a random uuid.
    //
    // TODO: need to double check if we want to support the internal primary key column feature
    //
    bool is_hash = !(is_pg_catalog_table_);
    CHECK_OK(AddColumn("ybrowid", static_cast<int32_t>(PgSystemAttrNum::kYBRowId),
                       YB_YQL_DATA_TYPE_BINARY, is_hash, true /* is_range */));
  }
}

Status PgCreateTable::AddColumnImpl(const std::string& attr_name,
                                    int attr_num,
                                    int attr_ybtype,
                                    bool is_hash,
                                    bool is_range,
                                    ColumnSchema::SortingType sorting_type) {
  shared_ptr<SQLType> data_type = SQLType::Create(static_cast<DataType>(attr_ybtype));
  bool is_nullable = true;
  if (is_hash) {
    if (!range_columns_.empty()) {
      return STATUS(InvalidArgument, "Hash column not allowed after an ASC/DESC column");
    }
    if (sorting_type != ColumnSchema::SortingType::kNotSpecified) {
      return STATUS(InvalidArgument, "Hash column can't have sorting order");
    }
    // key should not be null
    is_nullable = false;
  } else if (is_range) {
    range_columns_.emplace_back(attr_name);
    // key should not be null
    is_nullable = false;
  }

  schema_builder_.AddColumn(attr_name, data_type, is_nullable, is_range || is_hash, attr_num, sorting_type);
  return Status::OK();
}

size_t PgCreateTable::PrimaryKeyRangeColumnCount() const {
  return range_columns_.size();
}

Status PgCreateTable::AddSplitRow(int num_cols, YBCPgTypeEntity **types, uint64_t *data) {
  const auto key_column_count = PrimaryKeyRangeColumnCount();
  SCHECK(num_cols && num_cols <= key_column_count,
      InvalidArgument,
      "Split points cannot be more than number of primary key columns");

  std::vector<SqlValue> row;
  row.reserve(key_column_count);
  for (size_t i = 0; i < key_column_count; ++i) {
    SqlValue sql_value(types[i], data[i], false);
    row.push_back(std::move(sql_value));
  }

  split_rows_.push_back(std::move(row));
  return Status::OK();
}

Result<std::vector<std::string>> PgCreateTable::BuildSplitRows(const PgSchema& schema) {
  std::vector<std::string> rows;
  rows.reserve(split_rows_.size());
  // TODO: should not be used by K2 SQL, remove this

  return rows;
}

Status PgCreateTable::Exec() {
  TableProperties table_properties;
  // always use transaction for create table
  table_properties.SetTransactional(true);
  schema_builder_.SetTableProperties(table_properties);

  // Construct schema.
  PgSchema schema = schema_builder_.Build();

  // Create table.
  const Status s = pg_session_->CreateTable(namespace_id_, namespace_name_, table_name_, table_id_, schema,
    is_pg_catalog_table_, is_shared_table_, if_not_exist_);
  if (PREDICT_FALSE(!s.ok())) {
    if (s.IsAlreadyPresent()) {
      if (if_not_exist_) {
        return Status::OK();
      }
      return STATUS(InvalidArgument, "Duplicate table");
    }
    if (s.IsNotFound()) {
      return STATUS(InvalidArgument, "Database not found", namespace_name_);
    }
    return STATUS_FORMAT(
        InvalidArgument, "Invalid table definition: $0",
        s.ToString(false /* include_file_and_line */, false /* include_code */));
  }

  return Status::OK();
}

//--------------------------------------------------------------------------------------------------
// PgDropTable
//--------------------------------------------------------------------------------------------------

PgDropTable::PgDropTable(PgSession::ScopedRefPtr pg_session,
                         const PgObjectId& table_id,
                         bool if_exist)
    : PgDdl(pg_session),
      table_id_(table_id),
      if_exist_(if_exist) {
}

PgDropTable::~PgDropTable() {
}

Status PgDropTable::Exec() {
  Status s = pg_session_->DropTable(table_id_);
  pg_session_->InvalidateTableCache(table_id_);
  if (s.ok() || (s.IsNotFound() && if_exist_)) {
    return Status::OK();
  }
  return s;
}

//--------------------------------------------------------------------------------------------------
// PgAlterTable
//--------------------------------------------------------------------------------------------------

PgAlterTable::PgAlterTable(PgSession::ScopedRefPtr pg_session,
                           const PgObjectId& table_id)
    : PgDdl(pg_session),
      table_id_(table_id) {
}

PgAlterTable::~PgAlterTable() {
}

Status PgAlterTable::AddColumn(const std::string& name,
                               const YBCPgTypeEntity *attr_type,
                               int order,
                               bool is_not_null) {
  ColumnSchema colSchema(name, static_cast<DataType>(attr_type->yb_type), is_not_null, false, false, order, ColumnSchema::SortingType::kNotSpecified);
  // TODO: add implementation
  return Status::OK();
}

Status PgAlterTable::RenameColumn(const std::string& old_name, const std::string& new_name) {
   // TODO: add implementation
 return Status::OK();
}

Status PgAlterTable::DropColumn(const std::string& name) {
  // TODO: add implementation
  return Status::OK();
}

Status PgAlterTable::RenameTable(const std::string& db_name, const std::string& new_name) {
  // TODO: add implementation
  return Status::OK();
}

Status PgAlterTable::Exec() {
  pg_session_->InvalidateTableCache(table_id_);
  // TODO: add implementation
  return Status::OK();
}

//--------------------------------------------------------------------------------------------------
// PgCreateIndex
//--------------------------------------------------------------------------------------------------

PgCreateIndex::PgCreateIndex(PgSession::ScopedRefPtr pg_session,
                             const std::string& database_name,
                             const std::string& schema_name,
                             const std::string& index_name,
                             const PgObjectId& index_id,
                             const PgObjectId& base_table_id,
                             bool is_shared_index,
                             bool is_unique_index,
                             const bool skip_index_backfill,
                             bool if_not_exist)
    : PgCreateTable(pg_session, database_name, schema_name, index_name, index_id,
                    is_shared_index, if_not_exist, false /* add_primary_key */),
      base_table_id_(base_table_id),
      is_unique_index_(is_unique_index),
      skip_index_backfill_(skip_index_backfill) {
}

size_t PgCreateIndex::PrimaryKeyRangeColumnCount() const {
  return ybbasectid_added_ ? primary_key_range_column_count_
                           : PgCreateTable::PrimaryKeyRangeColumnCount();
}

Status PgCreateIndex::AddYBbasectidColumn() {
  primary_key_range_column_count_ = PgCreateTable::PrimaryKeyRangeColumnCount();
  // Add YBUniqueIdxKeySuffix column to store key suffix for handling multiple NULL values in column
  // with unique index.
  // Value of this column is set to ybctid (same as ybbasectid) for index row in case index
  // is unique and at least one of its key column is NULL.
  // In all other case value of this column is NULL.
  if (is_unique_index_) {
    RETURN_NOT_OK(
        PgCreateTable::AddColumnImpl("ybuniqueidxkeysuffix",
                                     to_underlying(PgSystemAttrNum::kYBUniqueIdxKeySuffix),
                                     YB_YQL_DATA_TYPE_BINARY,
                                     false /* is_hash */,
                                     true /* is_range */));
  }

  // Add ybbasectid column to store the ybctid of the rows in the indexed table. It should be added
  // at the end of the primary key of the index, i.e. either before any non-primary-key column if
  // any or before exec() below.
  RETURN_NOT_OK(PgCreateTable::AddColumnImpl("ybidxbasectid",
                                             to_underlying(PgSystemAttrNum::kYBIdxBaseTupleId),
                                             YB_YQL_DATA_TYPE_BINARY,
                                             false /* is_hash */,
                                             !is_unique_index_ /* is_range */));
  ybbasectid_added_ = true;
  return Status::OK();
}

Status PgCreateIndex::AddColumnImpl(const std::string& attr_name,
                                    int attr_num,
                                    int attr_ybtype,
                                    bool is_hash,
                                    bool is_range,
                                    ColumnSchema::SortingType sorting_type) {
  if (!is_hash && !is_range && !ybbasectid_added_) {
    RETURN_NOT_OK(AddYBbasectidColumn());
  }

  return PgCreateTable::AddColumnImpl(attr_name, attr_num, attr_ybtype,
      is_hash, is_range, sorting_type);
}

Status PgCreateIndex::Exec() {
  if (!ybbasectid_added_) {
    RETURN_NOT_OK(AddYBbasectidColumn());
  }
   
  TableProperties table_properties;
  // always use transaction for create table
  table_properties.SetTransactional(true);
  schema_builder_.SetTableProperties(table_properties);

  // Construct schema.
  PgSchema schema = schema_builder_.Build();

  // Create table.
  const Status s = pg_session_->CreateIndexTable(namespace_id_, namespace_name_, table_name_, table_id_, base_table_id_, schema,
    is_unique_index_, skip_index_backfill_, is_pg_catalog_table_, is_shared_table_, if_not_exist_);
  if (PREDICT_FALSE(!s.ok())) {
    if (s.IsAlreadyPresent()) {
      if (if_not_exist_) {
        return Status::OK();
      }
      return STATUS(InvalidArgument, "Duplicate index table");
    }
    if (s.IsNotFound()) {
      return STATUS(InvalidArgument, "Database not found", namespace_name_);
    }
    return STATUS_FORMAT(
        InvalidArgument, "Invalid index table definition: $0",
        s.ToString(false /* include_file_and_line */, false /* include_code */));
  }

  pg_session_->InvalidateTableCache(base_table_id_);
  return Status::OK(); 
}

//--------------------------------------------------------------------------------------------------
// PgDropIndex
//--------------------------------------------------------------------------------------------------

PgDropIndex::PgDropIndex(PgSession::ScopedRefPtr pg_session,
                         const PgObjectId& index_id,
                         bool if_exist)
    : PgDropTable(pg_session, index_id, if_exist) {
}

PgDropIndex::~PgDropIndex() {
}

Status PgDropIndex::Exec() {
  PgOid *base_table_oid;
  Status s = pg_session_->DropIndex(table_id_, base_table_oid);
  PgObjectId base_table_id(table_id_.database_oid, *base_table_oid);

  pg_session_->InvalidateTableCache(table_id_);
  pg_session_->InvalidateTableCache(base_table_id);
  if (s.ok() || (s.IsNotFound() && if_exist_)) {
    return Status::OK();
  }
  return s;
}

}  // namespace gate
}  // namespace k2pg
