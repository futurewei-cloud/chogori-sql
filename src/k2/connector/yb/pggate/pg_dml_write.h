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

#ifndef CHOGORI_GATE_PG_DML_WRITE_H
#define CHOGORI_GATE_PG_DML_WRITE_H

#include "yb/pggate/pg_dml.h"

namespace k2pg {
namespace gate {

//--------------------------------------------------------------------------------------------------
// DML WRITE - Insert, Update, Delete.
//--------------------------------------------------------------------------------------------------

class PgDmlWrite : public PgDml {
 public:
  // Abstract class without constructors.
  virtual ~PgDmlWrite();

  // Prepare write operations.
  virtual CHECKED_STATUS Prepare();

  // Setup internal structures for binding values during prepare.
  void PrepareColumns();

  // force_non_bufferable flag indicates this operation should not be buffered.
  CHECKED_STATUS Exec(bool force_non_bufferable = false);

  void SetIsSystemCatalogChange() {
      ysql_catalog_change_ = true;
  }

  void SetCatalogCacheVersion(const uint64_t catalog_cache_version) override {
    ysql_catalog_version_ = catalog_cache_version;
  }

  int32_t GetRowsAffectedCount() {
    return rows_affected_count_;
  }

  CHECKED_STATUS SetWriteTime(const uint64_t write_time);

 protected:
  // Constructor.
  PgDmlWrite(PgSession::ScopedRefPtr pg_session,
             const PgObjectId& table_id,
             bool is_single_row_txn = false);
 
  // Allocate write request.
  void AllocWriteRequest();

  // Allocate column expression.
  std::shared_ptr<SqlOpExpr> AllocColumnBindVar(PgColumn *col) override;

  // Allocate target for selected or returned expressions.
  std::shared_ptr<SqlOpExpr> AllocTargetVar() override;

  // Allocate column expression.
  std::shared_ptr<SqlOpExpr> AllocColumnAssignVar(PgColumn *col) override;

  // Delete allocated target for columns that have no bind-values.
  CHECKED_STATUS DeleteEmptyPrimaryBinds();

  std::shared_ptr<SqlOpWriteRequest> write_req_ = nullptr;

  bool is_single_row_txn_ = false; // default.

  int32_t rows_affected_count_ = 0;

  bool ysql_catalog_change_ = false;

  uint64_t ysql_catalog_version_ = 0;

  private:
  virtual std::unique_ptr<PgWriteOpTemplate> AllocWriteOperation() const = 0;
};

}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_PG_DML_WRITE_H    