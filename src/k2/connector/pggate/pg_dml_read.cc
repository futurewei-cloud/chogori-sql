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

#include "pggate/pg_dml_read.h"
#include "pggate/pg_select.h"

namespace k2pg {
namespace gate {

PgDmlRead::PgDmlRead(std::shared_ptr<PgSession> pg_session, const PgObjectId& table_object_id,
                     const PgObjectId& index_object_id, const PgPrepareParameters *prepare_params)
    : PgDml(pg_session, table_object_id, index_object_id, prepare_params) {
}

PgDmlRead::~PgDmlRead() {
}

void PgDmlRead::PrepareBinds() {
  if (!bind_desc_) {
    // This statement doesn't have bindings.
    return;
  }

  for (PgColumn &col : bind_desc_->columns()) {
    col.AllocKeyBind(read_req_);
  }
}

void PgDmlRead::SetForwardScan(const bool is_forward_scan) {
   if (secondary_index_query_) {
    return secondary_index_query_->SetForwardScan(is_forward_scan);
  }
  read_req_->is_forward_scan = is_forward_scan;
}

// Allocate column variable.
std::shared_ptr<BindVariable> PgDmlRead::AllocColumnBindVar(PgColumn *col) {
  return col->AllocBind(read_req_);
}

std::vector<PgExpr *>& PgDmlRead::GetTargets() {
  return read_req_->targets;
}

// Allocate column expression.
std::shared_ptr<BindVariable> PgDmlRead::AllocColumnAssignVar(PgColumn *col) {
  // SELECT statement should not have an assign expression (SET clause).
  K2ASSERT(log::pg, false, "Pure virtual function is being call");
  return nullptr;
}

Status PgDmlRead::SetUnboundPrimaryBinds() {
  if (secondary_index_query_) {
    RETURN_NOT_OK(secondary_index_query_->SetUnboundPrimaryBinds());
  }

  if (!bind_desc_) {
    // This query does not have any allocated columns.
    read_req_->key_column_values.clear();
    return Status::OK();
  }

  return Status::OK();
}

Status PgDmlRead::BindColumnCondEq(int attr_num, PgExpr *attr_value) {
  K2LOG_D(log::pg, "Binding column {} for EQUAL condition", attr_num);
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumnCondEq(attr_num, attr_value);
  }

  // Find column.
  PgColumn *col = VERIFY_RESULT(bind_desc_->FindColumn(attr_num));

  if (attr_value != NULL) {
    PgOperator *top_expr;
    if (col->is_primary()) {
      // bind to range_conds
      if (read_req_->range_conds == NULL) {
        std::unique_ptr<PgExpr> range_expr = std::make_unique<PgOperator>("and", nullptr);
        read_req_->range_conds = range_expr.get();
        AddExpr(std::move(range_expr));
      }
      top_expr = static_cast<PgOperator *>(read_req_->range_conds);
    } else {
      // bind to where_conds
      if (read_req_->where_conds == NULL) {
        std::unique_ptr<PgExpr> where_expr = std::make_unique<PgOperator>("and", nullptr);
        read_req_->where_conds = where_expr.get();
        AddExpr(std::move(where_expr));
      }
      top_expr = static_cast<PgOperator *>(read_req_->where_conds);
    }

    PgOperator eq_opr("=", nullptr);
    PgColumnRef col(attr_num, attr_value->type_entity(), attr_value->type_attrs());
    eq_opr.AppendArg(&col);
    eq_opr.AppendArg(attr_value);
    top_expr->AppendArg(&eq_opr);
  }

  if (attr_num == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
    CHECK(attr_value->is_constant()) << "Column ybctid must be bound to constant";
    K2LOG_D(log::pg, "kYBTupleId was bound and ybctid_bind_ is set as true");
    ybctid_bind_ = true;
  }

  return Status::OK();
}

Status PgDmlRead::BindColumnCondBetween(int attr_num, PgExpr *attr_value, PgExpr *attr_value_end) {
  K2LOG_D(log::pg, "Binding column {} for BETWEEN condition", attr_num);
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumnCondBetween(attr_num, attr_value, attr_value_end);
  }

  DCHECK(attr_num != static_cast<int>(PgSystemAttrNum::kYBTupleId))
    << "Operator BETWEEN cannot be applied to ROWID";

  // Find column.
  PgColumn *col = VERIFY_RESULT(bind_desc_->FindColumn(attr_num));

  CHECK(!col->desc()->is_hash()) << "This method cannot be used for binding hash column!";

  K2ASSERT(log::pg, attr_value != NULL && attr_value_end != NULL, "Between operator should not have NULL values");

  PgOperator *top_expr;
  if (col->is_primary()) {
      // bind to range_conds
      if (read_req_->range_conds == NULL) {
        std::unique_ptr<PgExpr> range_expr = std::make_unique<PgOperator>("and", nullptr);
        read_req_->range_conds = range_expr.get();
        AddExpr(std::move(range_expr));
      }
      top_expr = static_cast<PgOperator *>(read_req_->range_conds);
  } else {
      // bind to where_conds
      if (read_req_->where_conds == NULL) {
        std::unique_ptr<PgExpr> where_expr = std::make_unique<PgOperator>("and", nullptr);
        read_req_->where_conds = where_expr.get();
        AddExpr(std::move(where_expr));
      }
      top_expr = static_cast<PgOperator *>(read_req_->where_conds);
  }

  PgOperator opr1(">=", nullptr);
  PgColumnRef col1(attr_num, attr_value->type_entity(), attr_value->type_attrs());
  opr1.AppendArg(&col1);
  opr1.AppendArg(attr_value);
  top_expr->AppendArg(&opr1);

  PgOperator opr2("<=", nullptr);
  PgColumnRef col2(attr_num, attr_value_end->type_entity(), attr_value_end->type_attrs());
  opr2.AppendArg(&col2);
  opr2.AppendArg(attr_value_end);
  top_expr->AppendArg(&opr2);

  return Status::OK();
}

Status PgDmlRead::BindColumnCondIn(int attr_num, int n_attr_values, PgExpr **attr_values) {
  K2LOG_D(log::pg, "Binding column {} for IN condition", attr_num);
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumnCondIn(attr_num, n_attr_values, attr_values);
  }

  DCHECK(attr_num != static_cast<int>(PgSystemAttrNum::kYBTupleId))
    << "Operator IN cannot be applied to ROWID";

  throw std::logic_error("K2 does not support set operator yet");
}

Status PgDmlRead::PopulateAttrName(PgExpr *pg_expr) {
  switch(pg_expr->opcode()) {
    case PgExpr::Opcode::PG_EXPR_COLREF: {
      PgColumnRef* col_ref = (PgColumnRef *)(pg_expr);
      DCHECK(col_ref->attr_num() != static_cast<int>(PgSystemAttrNum::kYBTupleId)) << "PgColumnRef cannot be applied to ROWID";
      PgColumn *col = VERIFY_RESULT(bind_desc_->FindColumn(col_ref->attr_num()));
      col_ref->set_attr_name(col->attr_name());
      K2LOG_D(log::pg, "Set column name as {} for {}", col_ref->attr_name(), col_ref->attr_num());
    } break;
    case PgExpr::Opcode::PG_EXPR_CONSTANT:
      break;
    default: {
      PgOperator* pg_opr = (PgOperator *)(pg_expr);
      for (auto arg : pg_opr->getArgs()) {
        PopulateAttrName(arg);
      }
    } break;
  }
  return Status::OK();
}

Status PgDmlRead::BindRangeConds(PgExpr *range_conds) {
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindRangeConds(range_conds);
  }

  RETURN_NOT_OK(PopulateAttrName(range_conds));
  read_req_->range_conds = range_conds;
  return Status::OK();
}

Status PgDmlRead::BindWhereConds(PgExpr *where_conds) {
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindWhereConds(where_conds);
  }

  RETURN_NOT_OK(PopulateAttrName(where_conds));
  read_req_->where_conds = where_conds;
  return Status::OK();
}

Status PgDmlRead::Exec(const PgExecParameters *exec_params) {
  // Initialize sql operator.
  if (sql_op_) {
    sql_op_->ExecuteInit(exec_params);
  }

  // Detect and set key columns that are not bound to any values.
  RETURN_NOT_OK(SetUnboundPrimaryBinds());

  // First, process the secondary index request.
  bool has_ybctid = VERIFY_RESULT(ProcessSecondaryIndexRequest(exec_params));

  if (!has_ybctid && secondary_index_query_ && secondary_index_query_->has_sql_op()) {
    // No ybctid is found from the IndexScan. Instruct "sql_op_" to abandon the execution and not
    // querying any data from storage server.
    sql_op_->AbandonExecution();
  } else {
    // Update bind values for constants and placeholders.
    RETURN_NOT_OK(UpdateBindVars());

    // Execute select statement and prefetching data from DocDB.
    // Note: For SysTable, sql_op_ === null, IndexScan doesn't send separate request.
    if (sql_op_) {
      K2LOG_D(log::pg, "PgDmlRead executing sql_op_, secondary_index_query_ is null? 1 or 0 {}", (!secondary_index_query_ ? 1 :0));
      SCHECK_EQ(VERIFY_RESULT(sql_op_->Execute()), RequestSent::kTrue, IllegalState,
                "YSQL read operation was not sent");
    }
  }

  return Status::OK();
}

}  // namespace gate
}  // namespace k2pg
