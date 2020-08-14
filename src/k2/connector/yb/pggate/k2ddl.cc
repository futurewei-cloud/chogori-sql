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

#include "yb/pggate/k2ddl.h"
#include "yb/pggate/ybc_pg_typedefs.h"

namespace k2 {
namespace gate {

using std::make_shared;
using std::shared_ptr;
using std::string;
using namespace std::literals;  // NOLINT
using namespace yb;
using namespace k2::sql;

//--------------------------------------------------------------------------------------------------
// K2CreateDatabase
//--------------------------------------------------------------------------------------------------

K2CreateDatabase::K2CreateDatabase(K2Session::ScopedRefPtr k2_session,
                                   const char *database_name,
                                   const PgOid database_oid,
                                   const PgOid source_database_oid,
                                   const PgOid next_oid)
    : K2Ddl(std::move(k2_session)),
      database_name_(database_name),
      database_oid_(database_oid),
      source_database_oid_(source_database_oid),
      next_oid_(next_oid) {
}

K2CreateDatabase::~K2CreateDatabase() {
}

Status K2CreateDatabase::Exec() {
  return k2_session_->CreateDatabase(database_name_, database_oid_, source_database_oid_, next_oid_);
}

K2DropDatabase::K2DropDatabase(K2Session::ScopedRefPtr k2_session,
                               const char *database_name,
                               PgOid database_oid)
    : K2Ddl(k2_session),
      database_name_(database_name),
      database_oid_(database_oid) {
}

K2DropDatabase::~K2DropDatabase() {
}

Status K2DropDatabase::Exec() {
  return k2_session_->DropDatabase(database_name_, database_oid_);
}

//--------------------------------------------------------------------------------------------------
// K2CreateTable
//--------------------------------------------------------------------------------------------------

K2CreateTable::K2CreateTable(K2Session::ScopedRefPtr k2_session,
                             const char *database_name,
                             const char *schema_name,
                             const char *table_name,
                             const PgObjectId& table_id,
                             bool is_shared_table,
                             bool if_not_exist,
                             bool add_primary_key)
    : K2Ddl(k2_session),
      namespace_id_(GetPgsqlNamespaceId(table_id.database_oid)),
      namespace_name_(database_name),
      table_name_(table_name),
      table_id_(table_id),
      num_tablets_(-1),
      is_pg_catalog_table_(strcmp(schema_name, "pg_catalog") == 0 ||
                           strcmp(schema_name, "information_schema") == 0),
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

Status K2CreateTable::AddColumnImpl(const char *attr_name,
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

size_t K2CreateTable::PrimaryKeyRangeColumnCount() const {
  return range_columns_.size();
}

Status K2CreateTable::AddSplitRow(int num_cols, YBCPgTypeEntity **types, uint64_t *data) {
  const auto key_column_count = PrimaryKeyRangeColumnCount();
  SCHECK(num_cols && num_cols <= key_column_count,
      InvalidArgument,
      "Split points cannot be more than number of primary key columns");

  std::vector<SqlValue> row;
  row.reserve(key_column_count);
  for (size_t i = 0; i < key_column_count; ++i) {
    SqlValue sql_value(types[i], data[i], false);
/*     if (i < num_cols) {
      PgConstant point(types[i], data[i], false);
      RETURN_NOT_OK(point.Eval(&ql_value));
    } */
    row.push_back(std::move(ql_value));
  }

  split_rows_.push_back(std::move(row));
  return Status::OK();
}

Result<std::vector<std::string>> K2CreateTable::BuildSplitRows(const Schema& schema) {
  std::vector<std::string> rows;
  rows.reserve(split_rows_.size());
  docdb::DocKey prev_doc_key;
  for (const auto& row : split_rows_) {
    SCHECK_EQ(
        row.size(), PrimaryKeyRangeColumnCount(),
        IllegalState, "Number of split row values must be equal to number of primary key columns");
    std::vector<docdb::PrimitiveValue> range_components;
    range_components.reserve(row.size());
    bool compare_columns = true;
    for (const auto& row_value : row) {
      const auto column_index = range_components.size();
      range_components.push_back(row_value.value_case() == QLValuePB::VALUE_NOT_SET
        ? docdb::PrimitiveValue(docdb::ValueType::kLowest)
        : docdb::PrimitiveValue::FromQLValuePB(
            row_value,
            schema.Column(schema.FindColumn(range_columns_[column_index])).sorting_type()));

      // Validate that split rows honor column ordering.
      if (compare_columns && !prev_doc_key.empty()) {
        const auto& prev_value = prev_doc_key.range_group()[column_index];
        const auto compare = prev_value.CompareTo(range_components.back());
        if (compare > 0) {
          return STATUS(InvalidArgument, "Split rows ordering does not match column ordering");
        } else if (compare < 0) {
          // Don't need to compare further columns
          compare_columns = false;
        }
      }
    }
    prev_doc_key = docdb::DocKey(std::move(range_components));
    const auto keybytes = prev_doc_key.Encode();

    // Validate that there are no duplicate split rows.
    if (rows.size() > 0 && keybytes.AsSlice() == Slice(rows.back())) {
      return STATUS(InvalidArgument, "Cannot have duplicate split rows");
    }
    rows.push_back(keybytes.ToStringBuffer());
  }
  return rows;
}

Status K2CreateTable::Exec() {
  // Construct schema.
  client::YBSchema schema;

  TableProperties table_properties;
  const char* pg_txn_enabled_env_var = getenv("YB_PG_TRANSACTIONS_ENABLED");
  const bool transactional =
      !pg_txn_enabled_env_var || strcmp(pg_txn_enabled_env_var, "1") == 0;
  LOG(INFO) << Format(
      "K2CreateTable: creating a $0 table: $1",
      transactional ? "transactional" : "non-transactional", table_name_.ToString());
  if (transactional) {
    table_properties.SetTransactional(true);
    schema_builder_.SetTableProperties(table_properties);
  }

  RETURN_NOT_OK(schema_builder_.Build(&schema));
  std::vector<std::string> split_rows = VERIFY_RESULT(BuildSplitRows(schema));

  // Create table.
  shared_ptr<client::YBTableCreator> table_creator(pg_session_->NewTableCreator());
  table_creator->table_name(table_name_).table_type(client::YBTableType::PGSQL_TABLE_TYPE)
                .table_id(table_id_.GetYBTableId())
                .num_tablets(num_tablets_)
                .schema(&schema)
                .colocated(colocated_);
  if (is_pg_catalog_table_) {
    table_creator->is_pg_catalog_table();
  }
  if (is_shared_table_) {
    table_creator->is_pg_shared_table();
  }
  if (hash_schema_) {
    table_creator->hash_schema(*hash_schema_);
  } else if (!is_pg_catalog_table_) {
    table_creator->set_range_partition_columns(range_columns_, split_rows);
  }

  // For index, set indexed (base) table id.
  if (indexed_table_id()) {
    table_creator->indexed_table_id(indexed_table_id()->GetYBTableId());
    if (is_unique_index()) {
      table_creator->is_unique_index(true);
    }
    if (skip_index_backfill()) {
      table_creator->skip_index_backfill(true);
    } else if (!FLAGS_ysql_disable_index_backfill) {
      // For online index backfill, don't wait for backfill to finish because waiting on index
      // permissions is done anyway.
      table_creator->wait(false);
    }
  }

  const Status s = table_creator->Create();
  if (PREDICT_FALSE(!s.ok())) {
    if (s.IsAlreadyPresent()) {
      if (if_not_exist_) {
        return Status::OK();
      }
      return STATUS(InvalidArgument, "Duplicate table");
    }
    if (s.IsNotFound()) {
      return STATUS(InvalidArgument, "Database not found", table_name_.namespace_name());
    }
    return STATUS_FORMAT(
        InvalidArgument, "Invalid table definition: $0",
        s.ToString(false /* include_file_and_line */, false /* include_code */));
  }

  return Status::OK();
}

//--------------------------------------------------------------------------------------------------
// K2DropTable
//--------------------------------------------------------------------------------------------------

K2DropTable::K2DropTable(K2Session::ScopedRefPtr k2_session,
                         const PgObjectId& table_id,
                         bool if_exist)
    : K2Ddl(k2_session),
      table_id_(table_id),
      if_exist_(if_exist) {
}

K2DropTable::~K2DropTable() {
}

Status K2DropTable::Exec() {
  Status s = k2_session_->DropTable(table_id_);
  k2_session_->InvalidateTableCache(table_id_);
  if (s.ok() || (s.IsNotFound() && if_exist_)) {
    return Status::OK();
  }
  return s;
}


}  // namespace gate
}  // namespace k2
