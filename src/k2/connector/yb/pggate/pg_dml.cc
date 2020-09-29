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

#include "yb/pggate/pg_dml.h"
#include "yb/pggate/pg_select.h"
namespace k2pg {
namespace gate {

using namespace yb;
using namespace k2pg::sql;

PgDml::PgDml(PgSession::ScopedRefPtr pg_session, const PgObjectId& table_id)
    : PgStatement(std::move(pg_session)), table_id_(table_id) {
}

PgDml::PgDml(PgSession::ScopedRefPtr pg_session,
             const PgObjectId& table_id,
             const PgObjectId& index_id,
             const PgPrepareParameters *prepare_params)
    : PgDml(pg_session, table_id) {

  if (prepare_params) {
    prepare_params_ = *prepare_params;
    // Primary index does not have its own data table.
    if (prepare_params_.use_secondary_index) {
      index_id_ = index_id;
    }
  }
}

PgDml::~PgDml() {
}

Status PgDml::AppendTarget(PgExpr *target) {
  // Except for base_ctid, all targets should be appended to this DML.
  if (target_desc_ && (prepare_params_.index_only_scan || !target->is_ybbasetid())) {
    RETURN_NOT_OK(AppendTargetDoc(target));
  } else {
    // Append base_ctid to the index_query.
    RETURN_NOT_OK(secondary_index_query_->AppendTargetDoc(target));
  }

  return Status::OK();
}

Status PgDml::AppendTargetDoc(PgExpr *target) {
  // Append to targets_.
  targets_.push_back(target);

  // Allocate associated doc expression.
  SqlOpExpr *expr_pb = AllocTargetDoc();

  // Prepare expression. Except for constants and place_holders, all other expressions can be
  // evaluate just one time during prepare.
  RETURN_NOT_OK(PrepareForRead(target, expr_pb));

  // Link the given expression "attr_value" with the allocated protobuf. Note that except for
  // constants and place_holders, all other expressions can be setup just one time during prepare.
  // Example:
  // - Bind values for a target of SELECT
  //   SELECT AVG(col + ?) FROM a_table;
  expr_binds_[expr_pb] = target;
  return Status::OK();
}

Status PgDml::PrepareColumnForRead(int attr_num, SqlOpExpr *target_pb,
                                   const PgColumn **col) {
  *col = nullptr;

  // Find column from targeted table.
  PgColumn *pg_col = VERIFY_RESULT(target_desc_->FindColumn(attr_num));

  if (target_pb)
    target_pb->setColumnId(pg_col->id());

  // Mark non-virtual column reference for Doc API.
  if (!pg_col->is_virtual_column()) {
    pg_col->set_read_requested(true);
  }

  *col = pg_col;
  return Status::OK();
}

Status PgDml::PrepareColumnForWrite(PgColumn *pg_col, SqlOpExpr *assign_pb) {
  assign_pb->setColumnId(pg_col->id());

  // Mark non-virtual column reference for DocDB.
  if (!pg_col->is_virtual_column()) {
    pg_col->set_write_requested(true);
  }

  return Status::OK();
}

void PgDml::ColumnRefsToDoc(SqlOpColumnRefs *column_refs) {
  column_refs->ids.clear();
  for (const PgColumn& col : target_desc_->columns()) {
    if (col.read_requested() || col.write_requested()) {
      column_refs->ids.push_back(col.id());
    }
  }
}

//--------------------------------------------------------------------------------------------------

Status PgDml::BindColumn(int attr_num, PgExpr *attr_value) {
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumn(attr_num, attr_value);
  }

  // Find column to bind.
  PgColumn *col = VERIFY_RESULT(bind_desc_->FindColumn(attr_num));

  // Check datatype.
  //SCHECK_EQ(col->internal_type(), attr_value->internal_type(), Corruption,
  //          "Attribute value type does not match column type");

  // Alloc the doc expression.
  SqlOpExpr *bind_pb = col->bind_pb();
  if (bind_pb == nullptr) {
    bind_pb = AllocColumnBindDoc(col);
  } else {
    if (expr_binds_.find(bind_pb) != expr_binds_.end()) {
      LOG(WARNING) << strings::Substitute("Column $0 is already bound to another value.", attr_num);
    }
  }

  // Link the expression and doc api. During execution, expr will write result to the doc api.
  RETURN_NOT_OK(PrepareForRead(attr_value, bind_pb));

  // Link the given expression "attr_value" with the allocated doc api. Note that except for
  // constants and place_holders, all other expressions can be setup just one time during prepare.
  // Examples:
  // - Bind values for primary columns in where clause.
  //     WHERE hash = ?
  // - Bind values for a column in INSERT statement.
  //     INSERT INTO a_table(hash, key, col) VALUES(?, ?, ?)
  expr_binds_[bind_pb] = attr_value;
  if (attr_num == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
    CHECK(attr_value->is_constant()) << "Column ybctid must be bound to constant";
    ybctid_bind_ = true;
  }

  return Status::OK();
}

Status PgDml::UpdateBindDocs() {
  for (const auto &entry : expr_binds_) {
    SqlOpExpr *expr_pb = entry.first;
    PgExpr *attr_value = entry.second;
    RETURN_NOT_OK(Eval(attr_value, expr_pb));
  }

  return Status::OK();
}

//--------------------------------------------------------------------------------------------------

Status PgDml::BindTable() {
  bind_table_ = true;
  return Status::OK();
}

//--------------------------------------------------------------------------------------------------

Status PgDml::AssignColumn(int attr_num, PgExpr *attr_value) {
  // Find column from targeted table.
  PgColumn *col = VERIFY_RESULT(target_desc_->FindColumn(attr_num));

  // Check datatype.
//  SCHECK_EQ(col->internal_type(), attr_value->internal_type(), Corruption,
 //           "Attribute value type does not match column type");

  // Alloc the doc expression.
  SqlOpExpr *assign_pb = col->assign_pb();
  if (assign_pb == nullptr) {
    assign_pb = AllocColumnAssignDoc(col);
  } else {
    if (expr_assigns_.find(assign_pb) != expr_assigns_.end()) {
      return STATUS_SUBSTITUTE(InvalidArgument,
                               "Column $0 is already assigned to another value", attr_num);
    }
  }

  // Link the expression and doc api. During execution, expr will write result to the pb.
  // - Prepare the left hand side for write.
  // - Prepare the right hand side for read. Currently, the right hand side is always constant.
  RETURN_NOT_OK(PrepareColumnForWrite(col, assign_pb));
  RETURN_NOT_OK(PrepareForRead(attr_value, assign_pb));

  // Link the given expression "attr_value" with the allocated doc api. Note that except for
  // constants and place_holders, all other expressions can be setup just one time during prepare.
  // Examples:
  // - Setup rhs values for SET column = assign_pb in UPDATE statement.
  //     UPDATE a_table SET col = assign_expr;
  expr_assigns_[assign_pb] = attr_value;

  return Status::OK();
}

Status PgDml::UpdateAssignDocs() {
  // Process the column binds for two cases.
  // For performance reasons, we might evaluate these expressions together with bind values in YB.
  for (const auto &entry : expr_assigns_) {
    SqlOpExpr *expr_pb = entry.first;
    PgExpr *attr_value = entry.second;
    RETURN_NOT_OK(Eval(attr_value, expr_pb));
  }

  return Status::OK();
}

Status PgDml::ClearBinds() {
  return STATUS(NotSupported, "Clearing binds for prepared statement is not yet implemented");
}

Status PgDml::Fetch(int32_t natts, uint64_t *values, bool *isnulls, PgSysColumns *syscols, bool *has_data) {
  // Each isnulls and values correspond (in order) to columns from the table schema.
  // Initialize to nulls for any columns not present in result.
  if (isnulls) {
    memset(isnulls, true, natts * sizeof(bool));
  }
  if (syscols) {
    memset(syscols, 0, sizeof(PgSysColumns));
  }

  // Keep reading until we either reach the end or get some rows.
  *has_data = true;
  PgTuple pg_tuple(values, isnulls, syscols);
  while (!VERIFY_RESULT(GetNextRow(&pg_tuple))) {
    if (!VERIFY_RESULT(FetchDataFromServer())) {
      // Stop processing as server returns no more rows.
      *has_data = false;
      return Status::OK();
    }
  }

  return Status::OK();
}

Result<bool> PgDml::FetchDataFromServer() {
  // Get the rowsets from doc-operator.
  RETURN_NOT_OK(doc_op_->GetResult(&rowsets_));

  // Check if EOF is reached.
  if (rowsets_.empty()) {
    // Process the secondary index to find the next WHERE condition.
    //   DML(Table) WHERE ybctid IN (SELECT base_ybctid FROM IndexTable),
    //   The nested query would return many rows each of which yields different result-set.
    if (!VERIFY_RESULT(ProcessSecondaryIndexRequest(nullptr))) {
      // Return EOF as the nested subquery does not have any more data.
      return false;
    }

    // Execute doc_op_ again for the new set of WHERE condition from the nested query.
    SCHECK_EQ(VERIFY_RESULT(doc_op_->Execute()), RequestSent::kTrue, IllegalState,
              "SQL read operation was not sent");

    // Get the rowsets from doc-operator.
    RETURN_NOT_OK(doc_op_->GetResult(&rowsets_));
  }

  return true;
}

Result<bool> PgDml::ProcessSecondaryIndexRequest(const PgExecParameters *exec_params) {
  if (!secondary_index_query_) {
    // Secondary INDEX is not used in this request.
    return false;
  }

  // Execute query in PgGate.
  // If index query is not yet executed, run it.
  if (!secondary_index_query_->is_executed()) {
    secondary_index_query_->set_is_executed(true);
    RETURN_NOT_OK(secondary_index_query_->Exec(exec_params));
  }

  // Not processing index request if it does not require its own doc operator.
  //
  // When INDEX is used for system catalog (colocated table), the index subquery does not have its
  // own operator. The request is combined with 'this' outer SELECT using 'index_request' attribute.
  //   (PgDocOp)doc_op_->(YBPgsqlReadOp)read_op_->(PgsqlReadRequestPB)read_request_::index_request
  if (!secondary_index_query_->has_doc_op()) {
    return false;
  }

  // When INDEX has its own doc_op, execute it to fetch next batch of ybctids which is then used
  // to read data from the main table.
  const vector<Slice> *ybctids;
  if (!VERIFY_RESULT(secondary_index_query_->FetchYbctidBatch(&ybctids))) {
    // No more rows of ybctids.
    return false;
  }

  // Update request with the new batch of ybctids to fetch the next batch of rows.
  RETURN_NOT_OK(doc_op_->PopulateDmlByYbctidOps(ybctids));
  return true;
}

Result<bool> PgDml::GetNextRow(PgTuple *pg_tuple) {
  for (auto rowset_iter = rowsets_.begin(); rowset_iter != rowsets_.end();) {
    // Check if the rowset has any data.
    auto& rowset = *rowset_iter;
    if (rowset.is_eof()) {
      rowset_iter = rowsets_.erase(rowset_iter);
      continue;
    }

    // If this rowset has the next row of the index order, load it. Otherwise, continue looking for
    // the next row in the order.
    //
    // NOTE:
    //   DML <Table> WHERE ybctid IN (SELECT base_ybctid FROM <Index> ORDER BY <Index Range>)
    // The nested subquery should return rows in indexing order, but the ybctids are then grouped
    // by hash-code for BATCH-DML-REQUEST, so the response here are out-of-order.
    if (rowset.NextRowOrder() <= current_row_order_) {
      // Write row to postgres tuple.
      int64_t row_order = -1;
      RETURN_NOT_OK(rowset.WritePgTuple(targets_, pg_tuple, &row_order));
      SCHECK(row_order == -1 || row_order == current_row_order_, InternalError,
             "The resulting row are not arranged in indexing order");

      // Found the current row. Move cursor to next row.
      current_row_order_++;
      return true;
    }

    rowset_iter++;
  }

  return false;
}

Result<string> PgDml::BuildYBTupleId(const PgAttrValueDescriptor *attrs, int32_t nattrs) {
  // TODO: get Doc Key from DOC API client
  return nullptr;
}

bool PgDml::has_aggregate_targets() {
  int num_aggregate_targets = 0;
  for (const auto& target : targets_) {
    if (target->is_aggregate())
      num_aggregate_targets++;
  }

  CHECK(num_aggregate_targets == 0 || num_aggregate_targets == targets_.size())
    << "Some, but not all, targets are aggregate expressions.";

  return num_aggregate_targets > 0;
}

Status PgDml::PrepareForRead(PgExpr *target, SqlOpExpr *expr_pb) {
  // TODO: add logic for different PgExpr types
  return Status::OK();
}

Status PgDml::Eval(PgExpr *target, SqlOpExpr *expr_pb) {
  // TODO: add logic for different PgExpr types
  return Status::OK();
}

PgDmlRead::PgDmlRead(PgSession::ScopedRefPtr pg_session, const PgObjectId& table_id,
                     const PgObjectId& index_id, const PgPrepareParameters *prepare_params)
    : PgDml(std::move(pg_session), table_id, index_id, prepare_params) {
}

PgDmlRead::~PgDmlRead() {
}

void PgDmlRead::PrepareBinds() {
  if (!bind_desc_) {
    // This statement doesn't have bindings.
    return;
  }

  for (PgColumn &col : bind_desc_->columns()) {
    col.AllocPrimaryBind(read_req_);
  }
}

void PgDmlRead::SetForwardScan(const bool is_forward_scan) {
   if (secondary_index_query_) {
    return secondary_index_query_->SetForwardScan(is_forward_scan);
  }
  read_req_->is_forward_scan = is_forward_scan;
}

// Allocate column doc.
SqlOpExpr *PgDmlRead::AllocColumnBindDoc(PgColumn *col) {
  return col->AllocBind(read_req_);
}

SqlOpCondition *PgDmlRead::AllocColumnBindConditionExprDoc(PgColumn *col) {
  return col->AllocBindConditionExpr(read_req_);
}

// Allocate protobuf for target.
SqlOpExpr *PgDmlRead::AllocTargetDoc() {
  SqlOpExpr* target = new SqlOpExpr();
  read_req_->targets.push_back(target);
  return target;
}

// Allocate column expression.
SqlOpExpr *PgDmlRead::AllocColumnAssignDoc(PgColumn *col) {
  // SELECT statement should not have an assign expression (SET clause).
  LOG(FATAL) << "Pure virtual function is being call";
  return nullptr;
}

void PgDmlRead::SetColumnRefs() {
   if (secondary_index_query_) {
    DCHECK(!has_aggregate_targets()) << "Aggregate pushdown should not happen with index";
  }
  read_req_->is_aggregate = has_aggregate_targets();
  if (read_req_->column_refs == nullptr) {
    read_req_->column_refs = new SqlOpColumnRefs();
  }
  ColumnRefsToDoc(read_req_->column_refs); 
}

Status PgDmlRead::DeleteEmptyPrimaryBinds() {
  if (secondary_index_query_) {
    RETURN_NOT_OK(secondary_index_query_->DeleteEmptyPrimaryBinds());
  }

  if (!bind_desc_) {
    // This query does not have any binds.
    read_req_->partition_column_values.clear();
    read_req_->range_column_values.clear();
    return Status::OK();
  }

  // NOTE: ybctid is a system column and not processed as bind.
  bool miss_partition_columns = false;
  bool has_partition_columns = false;

  for (size_t i = 0; i < bind_desc_->num_hash_key_columns(); i++) {
    PgColumn& col = bind_desc_->columns()[i];
    SqlOpExpr* expr = col.bind_pb();
    // TODO: fix the condition logic here after we add condition to doc expr
    if (expr_binds_.find(expr) == expr_binds_.end() && expr->getType() != SqlOpExpr::ExprType::CONDITION) {
      // For IN clause on hash_column, expr->has_condition() returns 'true'.
      miss_partition_columns = true;
    } else {
      has_partition_columns = true;
    }
  }

  if (miss_partition_columns) {
    VLOG(1) << "Full scan is needed";
    read_req_->partition_column_values.clear();
    read_req_->range_column_values.clear();
  }

  if (has_partition_columns && miss_partition_columns) {
    return STATUS(InvalidArgument, "Partition key must be fully specified");
  }

  bool miss_range_columns = false;
  size_t num_bound_range_columns = 0;

  for (size_t i = bind_desc_->num_hash_key_columns(); i < bind_desc_->num_key_columns(); i++) {
    PgColumn &col = bind_desc_->columns()[i];
    if (expr_binds_.find(col.bind_pb()) == expr_binds_.end()) {
      miss_range_columns = true;
    } else if (miss_range_columns) {
      return STATUS(InvalidArgument,
                    "Unspecified range key column must be at the end of the range key");
    } else {
      num_bound_range_columns++;
    }
  }

  auto range_column_values = read_req_->range_column_values;
  // TODO: double check if the logic is correct here
  range_column_values.erase(range_column_values.begin() + num_bound_range_columns, range_column_values.end());
  return Status::OK();
}

Status PgDmlRead::BindColumnCondEq(int attr_num, PgExpr *attr_value) {
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumnCondEq(attr_num, attr_value);
  }

  // Find column.
  PgColumn *col = VERIFY_RESULT(bind_desc_->FindColumn(attr_num));

  // Check datatype.
  // if (attr_value) {
  //   SCHECK_EQ(col->internal_type(), attr_value->internal_type(), Corruption,
  //             "Attribute value type does not match column type");
  // }

  // Alloc the doc api.
  SqlOpCondition *condition_expr_doc = AllocColumnBindConditionExprDoc(col);

  if (attr_value != nullptr) {
    condition_expr_doc->setOp(PgExpr::Opcode::PG_EXPR_EQ);

    SqlOpExpr* op1_doc = new SqlOpExpr(SqlOpExpr::ExprType::COLUMN_ID, col->id());
    condition_expr_doc->addOperand(op1_doc);

    SqlOpExpr* op2_doc = new SqlOpExpr();
    condition_expr_doc->addOperand(op2_doc);

    // read value from attr_value, which should be a PgConstant
    RETURN_NOT_OK(Eval(attr_value, op2_doc));
  }

  if (attr_num == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
    CHECK(attr_value->is_constant()) << "Column ybctid must be bound to constant";
    ybctid_bind_ = true;
  }
  
  return Status::OK();
}

Status PgDmlRead::BindColumnCondBetween(int attr_num, PgExpr *attr_value, PgExpr *attr_value_end) {
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumnCondBetween(attr_num, attr_value, attr_value_end);
  }

  DCHECK(attr_num != static_cast<int>(PgSystemAttrNum::kYBTupleId))
    << "Operator BETWEEN cannot be applied to ROWID";

  // Find column.
  PgColumn *col = VERIFY_RESULT(bind_desc_->FindColumn(attr_num));

  // Check datatype.
  // if (attr_value) {
  //   SCHECK_EQ(col->internal_type(), attr_value->internal_type(), Corruption,
  //             "Attribute value type does not match column type");
  // }

  // if (attr_value_end) {
  //   SCHECK_EQ(col->internal_type(), attr_value_end->internal_type(), Corruption,
  //             "Attribute value type does not match column type");
  // }

  CHECK(!col->desc()->is_partition()) << "This method cannot be used for binding partition column!";

  // Alloc the doc condition
  SqlOpCondition *condition_expr_pb = AllocColumnBindConditionExprDoc(col);

  if (attr_value != nullptr) {
    if (attr_value_end != nullptr) {
      condition_expr_pb->setOp(PgExpr::Opcode::PG_EXPR_BETWEEN);
      SqlOpExpr *op1_doc = new SqlOpExpr(SqlOpExpr::ExprType::COLUMN_ID, col->id());
      condition_expr_pb->addOperand(op1_doc);

      SqlOpExpr *op2_doc = new SqlOpExpr();
      condition_expr_pb->addOperand(op2_doc);
      SqlOpExpr *op3_doc = new SqlOpExpr();
      condition_expr_pb->addOperand(op3_doc);

      RETURN_NOT_OK(Eval(attr_value, op2_doc));
      RETURN_NOT_OK(Eval(attr_value_end, op3_doc));
    } else {
      condition_expr_pb->setOp(PgExpr::Opcode::PG_EXPR_GE);
      SqlOpExpr *op1_doc = new SqlOpExpr(SqlOpExpr::ExprType::COLUMN_ID, col->id());
      condition_expr_pb->addOperand(op1_doc);

      SqlOpExpr *op2_doc = new SqlOpExpr();
      condition_expr_pb->addOperand(op2_doc);

      RETURN_NOT_OK(Eval(attr_value, op2_doc));
    }
  } else {
    if (attr_value_end != nullptr) {
      condition_expr_pb->setOp(PgExpr::Opcode::PG_EXPR_LE);
      SqlOpExpr *op1_doc = new SqlOpExpr(SqlOpExpr::ExprType::COLUMN_ID, col->id());
      condition_expr_pb->addOperand(op1_doc);

      SqlOpExpr *op2_doc = new SqlOpExpr();
      condition_expr_pb->addOperand(op2_doc);

      RETURN_NOT_OK(Eval(attr_value_end, op2_doc));
    } else {
      // Unreachable.
    }
  }  
  
  return Status::OK();
}

Status PgDmlRead::BindColumnCondIn(int attr_num, int n_attr_values, PgExpr **attr_values) {
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumnCondIn(attr_num, n_attr_values, attr_values);
  }

  DCHECK(attr_num != static_cast<int>(PgSystemAttrNum::kYBTupleId))
    << "Operator IN cannot be applied to ROWID";

  // Find column.
  PgColumn *col = VERIFY_RESULT(bind_desc_->FindColumn(attr_num));

  // Check datatype.
  // TODO(neil) Current code combine TEXT and BINARY datatypes into ONE representation.  Once that
  // is fixed, we can remove the special if() check for BINARY type.
  // if (col->internal_type() != InternalType::kBinaryValue) {
  //   for (int i = 0; i < n_attr_values; i++) {
  //     if (attr_values[i]) {
  //       SCHECK_EQ(col->internal_type(), attr_values[i]->internal_type(), Corruption,
  //           "Attribute value type does not match column type");
  //     }
  //   }
  // }

  if (col->desc()->is_partition()) {
    // Alloc the doc expression.
    SqlOpExpr* bind_pb = col->bind_pb();
    if (bind_pb == nullptr) {
      bind_pb = AllocColumnBindDoc(col);
    } else {
      if (expr_binds_.find(bind_pb) != expr_binds_.end()) {
        LOG(WARNING) << strings::Substitute("Column $0 is already bound to another value.",
                                            attr_num);
      }
    }

    SqlOpCondition* doc_condition = new SqlOpCondition();
    doc_condition->setOp(PgExpr::Opcode::PG_EXPR_IN);
    SqlOpExpr *op1_doc = new SqlOpExpr(SqlOpExpr::ExprType::COLUMN_ID, col->id());
    doc_condition->addOperand(op1_doc);
    bind_pb->setCondition(doc_condition);

    // There's no "list of expressions" field so we simulate it with an artificial nested OR
    // with repeated operands, one per bind expression.
    // This is only used for operation unrolling in pg_doc_op and is not understood by DocDB.
    // TODO: double check if we need this logic for K2 storage as well
    SqlOpCondition* nested_condition = new SqlOpCondition();
    nested_condition->setOp(PgExpr::Opcode::PG_EXPR_OR);
    SqlOpExpr *op2_doc = new SqlOpExpr(nested_condition);
    doc_condition->addOperand(op2_doc);

    for (int i = 0; i < n_attr_values; i++) {
      SqlOpExpr* attr_doc = new SqlOpExpr();
      nested_condition->addOperand(attr_doc);

      // Link the expression and doc expression. During execution, expr will write result to the doc object.
      RETURN_NOT_OK(PrepareForRead(attr_values[i], attr_doc));

      expr_binds_[attr_doc] = attr_values[i];

      if (attr_num == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
        CHECK(attr_values[i]->is_constant()) << "Column ybctid must be bound to constant";
        ybctid_bind_ = true;
      }
    }
  } else {
    // Alloc the doc condition
    SqlOpCondition* doc_condition = AllocColumnBindConditionExprDoc(col);
    doc_condition->setOp(PgExpr::Opcode::PG_EXPR_IN);
    SqlOpExpr *op1_doc = new SqlOpExpr(SqlOpExpr::ExprType::COLUMN_ID, col->id());
    doc_condition->addOperand(op1_doc);
    SqlOpExpr *op2_doc = new SqlOpExpr();
    doc_condition->addOperand(op2_doc);

    for (int i = 0; i < n_attr_values; i++) {
      // Link the given expression "attr_value" with the allocated doc object.
      // Note that except for constants and place_holders, all other expressions can be setup
      // just one time during prepare.
      // Examples:
      // - Bind values for primary columns in where clause.
      //     WHERE hash = ?
      // - Bind values for a column in INSERT statement.
      //     INSERT INTO a_table(hash, key, col) VALUES(?, ?, ?)

      if (attr_values[i]) {
        SqlOpExpr* attr_val = new SqlOpExpr();
        RETURN_NOT_OK(Eval(attr_values[i], attr_val));
        op2_doc->addListValue(attr_val->getValue());
      }

      if (attr_num == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
        CHECK(attr_values[i]->is_constant()) << "Column ybctid must be bound to constant";
        ybctid_bind_ = true;
      }
    }
  }
  
  return Status::OK();
}

Status PgDmlRead::Exec(const PgExecParameters *exec_params) {
  // Initialize doc operator.
  if (doc_op_) {
    doc_op_->ExecuteInit(exec_params);
  }

  // Set column references in request and whether query is aggregate.
  SetColumnRefs();

  // Delete key columns that are not bound to any values.
  RETURN_NOT_OK(DeleteEmptyPrimaryBinds());

  // First, process the secondary index request.
  bool has_ybctid = VERIFY_RESULT(ProcessSecondaryIndexRequest(exec_params));

  if (!has_ybctid && secondary_index_query_ && secondary_index_query_->has_doc_op()) {
    // No ybctid is found from the IndexScan. Instruct "doc_op_" to abandon the execution and not
    // querying any data from storage server.
    //
    // Note: For system catalog (colocated table), the secondary_index_query_ won't send a separate
    // scan read request to DocDB.  For this case, the index request is embedded inside the SELECT
    // request (PgsqlReadRequestPB::index_request).
    doc_op_->AbandonExecution();

  } else {
    // Update bind values for constants and placeholders.
    RETURN_NOT_OK(UpdateBindDocs());

    // Execute select statement and prefetching data from DocDB.
    // Note: For SysTable, doc_op_ === null, IndexScan doesn't send separate request.
    if (doc_op_) {
      SCHECK_EQ(VERIFY_RESULT(doc_op_->Execute()), RequestSent::kTrue, IllegalState,
                "YSQL read operation was not sent");
    }
  }
    
  return Status::OK();
}

PgDmlWrite::PgDmlWrite(PgSession::ScopedRefPtr pg_session,
                       const PgObjectId& table_id,
                       const bool is_single_row_txn)
    : PgDml(std::move(pg_session), table_id), is_single_row_txn_(is_single_row_txn) {
}

PgDmlWrite::~PgDmlWrite() {
}

Status PgDmlWrite::Prepare() {
  // Setup descriptors for target and bind columns.
  target_desc_ = bind_desc_ = VERIFY_RESULT(pg_session_->LoadTable(table_id_));

  // Allocate either INSERT, UPDATE, or DELETE request.
  AllocWriteRequest();
  PrepareColumns();

  return Status::OK();
}

void PgDmlWrite::PrepareColumns() {
  // Because DocDB API requires that primary columns must be listed in their created-order,
  // the slots for primary column bind expressions are allocated here in correct order.
  // TODO: verify this is true for K2 storage
  for (PgColumn &col : target_desc_->columns()) {
    col.AllocPrimaryBind(write_req_);
  }
}

Status PgDmlWrite::Exec(bool force_non_bufferable) {
  // Delete allocated binds that are not associated with a value.
  // YBClient interface enforce us to allocate binds for primary key columns in their indexing
  // order, so we have to allocate these binds before associating them with values. When the values
  // are not assigned, these allocated binds must be deleted.
  RETURN_NOT_OK(DeleteEmptyPrimaryBinds());

  // First update doc request with new bind values.
  RETURN_NOT_OK(UpdateBindDocs());
  RETURN_NOT_OK(UpdateAssignDocs());

  if (write_req_->ybctid_column_value != nullptr) {
    SqlOpExpr *expr_doc = write_req_->ybctid_column_value;
    CHECK(expr_doc->isValueType() && expr_doc->getValue() != nullptr && expr_doc->getValue()->isBinaryValue())
      << "YBCTID must be of BINARY datatype";
  }

  // Initialize doc operator.
  doc_op_->ExecuteInit(nullptr);

  // Set column references in protobuf.
  ColumnRefsToDoc(write_req_->column_refs);

  // Execute the statement. If the request has been sent, get the result and handle any rows
  // returned.
  if (VERIFY_RESULT(doc_op_->Execute(force_non_bufferable)) == RequestSent::kTrue) {
    RETURN_NOT_OK(doc_op_->GetResult(&rowsets_));

    // Save the number of rows affected by the op.
    rows_affected_count_ = VERIFY_RESULT(doc_op_->GetRowsAffectedCount());
  }
  
  return Status::OK();
}

Status PgDmlWrite::DeleteEmptyPrimaryBinds() {
  // Iterate primary-key columns and remove the binds without values.
  bool missing_primary_key = false;

  // Either ybctid or primary key must be present.
  if (!ybctid_bind_) {
    // Remove empty binds from partition list.
    auto partition_iter = write_req_->partition_column_values.begin();
    while (partition_iter != write_req_->partition_column_values.end()) {
      if (expr_binds_.find(*partition_iter) == expr_binds_.end()) {
        missing_primary_key = true;
        partition_iter = write_req_->partition_column_values.erase(partition_iter);
      } else {
        partition_iter++;
      }
    }

    // Remove empty binds from range list.
    auto range_iter = write_req_->range_column_values.begin();
    while (range_iter != write_req_->range_column_values.end()) {
      if (expr_binds_.find(*range_iter) == expr_binds_.end()) {
        missing_primary_key = true;
        range_iter = write_req_->range_column_values.erase(range_iter);
      } else {
        range_iter++;
      }
    }
  } else {
    write_req_->partition_column_values.clear();
    write_req_->range_column_values.clear();
  }

  // Check for missing key.  This is okay when binding the whole table (for colocated truncate).
  if (missing_primary_key && !bind_table_) {
    return STATUS(InvalidArgument, "Primary key must be fully specified for modifying table");
  }

  return Status::OK();
}

void PgDmlWrite::AllocWriteRequest() {
  auto wop = AllocWriteOperation();
  DCHECK(wop);
  wop->set_is_single_row_txn(is_single_row_txn_);
  write_req_ = &wop->request();
  doc_op_ = std::make_shared<PgWriteOp>(pg_session_, target_desc_, table_id_, std::move(wop));
}

SqlOpExpr *PgDmlWrite::AllocColumnBindDoc(PgColumn *col) {
  return col->AllocBind(write_req_);
}

SqlOpExpr *PgDmlWrite::AllocColumnAssignDoc(PgColumn *col) {
  return col->AllocAssign(write_req_);
}

SqlOpExpr *PgDmlWrite::AllocTargetDoc() {
  SqlOpExpr *target_doc = new SqlOpExpr();
  write_req_->targets.push_back(target_doc);
  return target_doc;
}

Status PgDmlWrite::SetWriteTime(const uint64_t write_time) {
  SCHECK(doc_op_.get() != nullptr, RuntimeError, "expected doc_op_ to be initialized");
  down_cast<PgWriteOp*>(doc_op_.get())->SetWriteTime(write_time);
  return Status::OK();
}

}  // namespace gate
}  // namespace k2pg
