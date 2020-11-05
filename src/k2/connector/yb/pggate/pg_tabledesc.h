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

#ifndef CHOGORI_GATE_PG_TABLE_DESC_H
#define CHOGORI_GATE_PG_TABLE_DESC_H

#include "yb/entities/table.h"
#include "yb/pggate/pg_column.h"
#include "yb/pggate/pg_op_api.h"

namespace k2pg {
namespace gate {

using namespace yb;
using namespace k2pg::sql;

// This class can be used to describe any reference of a column.
class PgTableDesc {
 public:
  explicit PgTableDesc(std::shared_ptr<TableInfo> pg_table);

  const TableIdentifier& table_name() const;

  const std::shared_ptr<TableInfo> table() const {
    return table_;
  }

  static int ToPgAttrNum(const string &attr_name, int attr_num);

  std::vector<PgColumn>& columns() {
    return columns_;
  }

  const size_t num_hash_key_columns() const;
  const size_t num_key_columns() const;
  const size_t num_columns() const;

  // Methods to initialize the templates for different SQL operations
  std::unique_ptr<PgReadOpTemplate> NewPgsqlSelect(const string& client_id, int64_t stmt_id);
  std::unique_ptr<PgWriteOpTemplate> NewPgsqlInsert(const string& client_id, int64_t stmt_id);
  std::unique_ptr<PgWriteOpTemplate> NewPgsqlUpdate(const string& client_id, int64_t stmt_id);
  std::unique_ptr<PgWriteOpTemplate> NewPgsqlDelete(const string& client_id, int64_t stmt_id);

  // Find the column given the postgres attr number.
  Result<PgColumn *> FindColumn(int attr_num);

  CHECKED_STATUS GetColumnInfo(int16_t attr_number, bool *is_primary, bool *is_hash) const;

  bool IsTransactional() const;

  int GetPartitionCount() const;
  
  protected:   
  std::unique_ptr<PgWriteOpTemplate> NewPgsqlOpWrite(SqlOpWriteRequest::StmtType stmt_type, const string& client_id, int64_t stmt_id);

  private:
  std::shared_ptr<TableInfo> table_;

  std::vector<PgColumn> columns_;
  std::unordered_map<int, size_t> attr_num_map_; // Attr number to column index map.

  // Hidden columns.
  PgColumn column_ybctid_;
};

}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_PG_TABLE_DESC_H
