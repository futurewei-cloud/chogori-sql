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

namespace k2 {
namespace gate {

PgTableDesc::PgTableDesc(std::shared_ptr<TableInfo> pg_table) : table_(pg_table) {
  const auto& schema = pg_table->schema();
  const int num_columns = schema.num_columns();
  columns_.resize(num_columns);
  for (size_t idx = 0; idx < num_columns; idx++) {
    // Find the column descriptor.
    const auto& col = schema.column(idx);

    // TODO(neil) Considering index columns by attr_num instead of ID.
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
  }

  // Create virtual columns.
  column_ybctid_.Init(PgSystemAttrNum::kYBTupleId);
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
    *is_hash = desc->is_partition();
  } else {
    *is_primary = false;
    *is_hash = false;
  }
  return Status::OK();
}

bool PgTableDesc::IsTransactional() const {
  return table_->schema().table_properties().is_transactional();
}

int PgTableDesc::GetPartitionCount() const {
  // TODO: add logic for k2 storage partition count calculation
  return 1;
}

const TableIdentifier& PgTableDesc::table_name() const {
  return table_->table_identifier();
}

const size_t PgTableDesc::num_hash_key_columns() const {
  return table_->schema().num_hash_key_columns();
}

const size_t PgTableDesc::num_key_columns() const {
  return table_->schema().num_key_columns();
}

const size_t PgTableDesc::num_columns() const {
  return table_->schema().num_columns();
}

std::unique_ptr<SqlOpReadCall> PgTableDesc::NewPgsqlSelect() {
  // TODO: add implementation
  return nullptr;
}

std::unique_ptr<SqlOpWriteCall> PgTableDesc::NewPgsqlInsert() {
  // TODO: add implementation
  return nullptr;
}

std::unique_ptr<SqlOpWriteCall> PgTableDesc::NewPgsqlUpdate() {
  // TODO: add implementation
  return nullptr;
}

std::unique_ptr<SqlOpWriteCall> PgTableDesc::NewPgsqlDelete() {
  // TODO: add implementation
  return nullptr;
}

}  // namespace gate
}  // namespace k2
