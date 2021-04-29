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

#ifndef CHOGORI_GATE_PG_DML_READ_H
#define CHOGORI_GATE_PG_DML_READ_H

#include "pggate/pg_dml.h"
#include "k2_log.h"

namespace k2pg {
namespace gate {
//--------------------------------------------------------------------------------------------------
// DML_READ
//--------------------------------------------------------------------------------------------------
// Scan Scenarios:
//
// 1. SequentialScan or PrimaryIndexScan (class PgSelect)
//    - We does not have a separate table for PrimaryIndex.
//    - The target table descriptor, where data is read and returned, is the main table.
//    - The binding table descriptor, whose column is bound to values, is also the main table.
//
// 2. IndexOnlyScan (Class PgSelectIndex)
//    - This special case is optimized where data is read from index table.
//    - The target table descriptor, where data is read and returned, is the index table.
//    - The binding table descriptor, whose column is bound to values, is also the index table.
//
// 3. IndexScan SysTable / UserTable (Class PgSelect and Nested PgSelectIndex)
//    - We will use the binds to query base-ybctid in the index table, which is then used
//      to query data from the main table.
//    - The target table descriptor, where data is read and returned, is the main table.
//    - The binding table descriptor, whose column is bound to values, is the index table.

class PgDmlRead : public PgDml {

 public:
  typedef std::shared_ptr<PgDmlRead> SharedPtr;

  // Constructors.
  PgDmlRead(std::shared_ptr<PgSession> pg_session, const PgObjectId& table_object_id,
           const PgObjectId& index_object_id, const PgPrepareParameters *prepare_params);
  virtual ~PgDmlRead();

  StmtOp stmt_op() const override { return StmtOp::STMT_SELECT; }

  virtual CHECKED_STATUS Prepare() = 0;

  // Allocate binds.
  virtual void PrepareBinds();

  // Set forward (or backward) scan.
  void SetForwardScan(const bool is_forward_scan);

  // Bind a column with an EQUALS condition.
  CHECKED_STATUS BindColumnCondEq(int attnum, PgExpr *attr_value);

  // Bind a range column with a BETWEEN condition.
  CHECKED_STATUS BindColumnCondBetween(int attr_num, PgExpr *attr_value, PgExpr *attr_value_end);

  // Bind a column with an IN condition.
  CHECKED_STATUS BindColumnCondIn(int attnum, int n_attr_values, PgExpr **attr_values);

  // Bind the range conditions
  CHECKED_STATUS BindRangeConds(PgExpr *range_conds);

  // Bind the where conditions
  CHECKED_STATUS BindWhereConds(PgExpr *where_conds);

  // Execute.
  virtual CHECKED_STATUS Exec(const PgExecParameters *exec_params);

  void SetCatalogCacheVersion(const uint64_t catalog_cache_version) override {
    DCHECK_NOTNULL(read_req_)->catalog_version = catalog_cache_version;
  }

  protected:
  CHECKED_STATUS PopulateAttrName(PgExpr *pg_expr);

   // Allocate column variable.
  std::shared_ptr<SqlOpExpr> AllocColumnBindVar(PgColumn *col) override;
  std::shared_ptr<SqlOpCondition> AllocColumnBindConditionExprVar(PgColumn *col);

  // get target vectors
  std::vector<PgExpr *>& GetTargets() override;

  // Allocate column expression.
  std::shared_ptr<SqlOpExpr> AllocColumnAssignVar(PgColumn *col) override;

  // Detect and set columns that have no bind-values.
  CHECKED_STATUS SetUnboundPrimaryBinds();

  // References read request from template operation.
  std::shared_ptr<SqlOpReadRequest> read_req_ = nullptr;
};

}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_PG_DML_READ_H
