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
    RETURN_NOT_OK(AppendTargetVar(target));
  } else {
    // Append base_ctid to the index_query.
    RETURN_NOT_OK(secondary_index_query_->AppendTargetVar(target));
  }

  return Status::OK();
}

Status PgDml::AppendTargetVar(PgExpr *target) {
  // Append to targets_.
  targets_.push_back(target);

  // Allocate associated expression.
  std::shared_ptr<SqlOpExpr> expr_var = AllocTargetVar();

  // Prepare expression. Except for constants and place_holders, all other expressions can be
  // evaluate just one time during prepare.
  RETURN_NOT_OK(PrepareExpression(target, expr_var));

  // Link the given expression "attr_value" with the allocated protobuf. Note that except for
  // constants and place_holders, all other expressions can be setup just one time during prepare.
  // Example:
  // - Bind values for a target of SELECT
  //   SELECT AVG(col + ?) FROM a_table;
  expr_binds_[expr_var] = target;
  return Status::OK();
}

Status PgDml::PrepareColumnForRead(int attr_num, std::shared_ptr<SqlOpExpr> target_var) 
{
  // Find column from targeted table.
  PgColumn *pg_col = VERIFY_RESULT(target_desc_->FindColumn(attr_num));

  if (target_var)
    target_var->setColumnId(pg_col->id());

  // Mark non-virtual column for writing
  if (!pg_col->is_virtual_column()) {
    pg_col->set_read_requested(true);
  }

  return Status::OK();
}

Status PgDml::PrepareColumnForWrite(PgColumn *pg_col, std::shared_ptr<SqlOpExpr> assign_var) {
  assign_var->setColumnId(pg_col->id());

  // Mark non-virtual column for writing.
  if (!pg_col->is_virtual_column()) {
    pg_col->set_write_requested(true);
  }

  return Status::OK();
}

//--------------------------------------------------------------------------------------------------

Status PgDml::BindColumn(int attr_num, PgExpr *attr_value) {
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumn(attr_num, attr_value);
  }

  // Find column to bind.
  PgColumn *col = VERIFY_RESULT(bind_desc_->FindColumn(attr_num));

  // Alloc the expression variable.
  std::shared_ptr<SqlOpExpr> bind_var = col->bind_var();
  if (bind_var == nullptr) {
    bind_var = AllocColumnBindVar(col);
  } else {
    if (expr_binds_.find(bind_var) != expr_binds_.end()) {
      LOG(WARNING) << strings::Substitute("Column $0 is already bound to another value.", attr_num);
    }
  }

  // Link the expression and expression variable for SKV. During execution, expr will write result to the storage api.
  RETURN_NOT_OK(PrepareExpression(attr_value, bind_var));

  // Link the given expression "attr_value" with the allocated doc api. Note that except for
  // constants and place_holders, all other expressions can be setup just one time during prepare.
  // Examples:
  // - Bind values for primary columns in where clause.
  //     WHERE hash = ?
  // - Bind values for a column in INSERT statement.
  //     INSERT INTO a_table(hash, key, col) VALUES(?, ?, ?)
  expr_binds_[bind_var] = attr_value;
  if (attr_num == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
    // YBC logic uses a virtual column ybctid as a row id in a string format
    // we need to follow the logic unless we change the logic inside PG
    CHECK(attr_value->is_constant()) << "Column ybctid must be bound to constant";
    ybctid_bind_ = true;
  }

  return Status::OK();
}

Status PgDml::UpdateBindVars() {
  for (const auto &entry : expr_binds_) {
    std::shared_ptr<SqlOpExpr> expr_var = entry.first;
    PgExpr *attr_value = entry.second;
    RETURN_NOT_OK(PrepareExpression(attr_value, expr_var));
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

  // Alloc the expression.
  std::shared_ptr<SqlOpExpr> assign_var = col->assign_var();
  if (assign_var == nullptr) {
    assign_var = AllocColumnAssignVar(col);
  } else {
    if (expr_assigns_.find(assign_var) != expr_assigns_.end()) {
      return STATUS_SUBSTITUTE(InvalidArgument,
                               "Column $0 is already assigned to another value", attr_num);
    }
  }

  // Link the expression and the expression variable in request. During execution, expr will write result to the request.
  // - Prepare the left hand side for write.
  // - Prepare the right hand side for read. Currently, the right hand side is always constant.
  RETURN_NOT_OK(PrepareColumnForWrite(col, assign_var));
  RETURN_NOT_OK(PrepareExpression(attr_value, assign_var));

  // Link the given expression "attr_value" with the allocated variable. Note that except for
  // constants and place_holders, all other expressions can be setup just one time during prepare.
  // Examples:
  // - Setup rhs values for SET column = assign_var in UPDATE statement.
  //     UPDATE a_table SET col = assign_expr;
  expr_assigns_[assign_var] = attr_value;

  return Status::OK();
}

Status PgDml::UpdateAssignVars() {
  for (const auto &entry : expr_assigns_) {
    std::shared_ptr<SqlOpExpr> expr_var = entry.first;
    PgExpr *attr_value = entry.second;
    RETURN_NOT_OK(PrepareExpression(attr_value, expr_var));
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
  // Get the rowsets from sql operator.
  RETURN_NOT_OK(sql_op_->GetResult(&rowsets_));

  // Check if EOF is reached.
  if (rowsets_.empty()) {
    // Process the secondary index to find the next WHERE condition.
    //   DML(Table) WHERE ybctid IN (SELECT base_ybctid FROM IndexTable),
    //   The nested query would return many rows each of which yields different result-set.
    if (!VERIFY_RESULT(ProcessSecondaryIndexRequest(nullptr))) {
      // Return EOF as the nested subquery does not have any more data.
      return false;
    }

    // Execute sql_op_ again for the new set of WHERE condition from the nested query.
    SCHECK_EQ(VERIFY_RESULT(sql_op_->Execute()), RequestSent::kTrue, IllegalState,
              "SQL read operation was not sent");

    // Get the rowsets from sql operator.
    RETURN_NOT_OK(sql_op_->GetResult(&rowsets_));
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

  // When INDEX has its own sql_op_, execute it to fetch next batch of ybctids which is then used
  // to read data from the main table.
  std::vector<Slice> ybctids;
  if (!VERIFY_RESULT(secondary_index_query_->FetchRowIdBatch(ybctids))) {
    // No more rows of ybctids.
    return false;
  }

  // Update request with the new batch of ybctids to fetch the next batch of rows.
  RETURN_NOT_OK(sql_op_->PopulateDmlByRowIdOps(ybctids));
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
  // TODO: generate the row id by calling K2 Adapter to use SKV client to 
  // generate the id in string format from the primary keys
  throw new std::logic_error("Not implemented yet");
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

Status PgDml::PrepareExpression(PgExpr *target, std::shared_ptr<SqlOpExpr> expr_var) {
  if (target->is_colref()) {
    // PgColumnRef
    PgColumnRef *col_ref = static_cast<PgColumnRef *>(target);
    PrepareColumnForRead(col_ref->attr_num(), expr_var);
  } else if (target->is_constant()) {
    // PgConstant
    PgConstant *col_const = static_cast<PgConstant *>(target);
    // the PgExpr *target is accessed by PG as well, need a copy of the value for SKV so that
    // we don't accidentally delete the value that is needed by PG when free the value owned by shared_ptrs.
    std::shared_ptr<SqlValue> col_val(col_const->getValue()->Clone());
    expr_var->setValue(col_val);
  } else {
    // PgOperator 
    // we only consider logic expressions for now
    // TODO: add aggregation function support once SKV supports that
    if (target->is_logic_expr()) {
      std::shared_ptr<SqlOpCondition> op_cond = std::make_shared<SqlOpCondition>();
      PgOperator *op_var = static_cast<PgOperator *>(target);
      const std::vector<PgExpr*> & args = op_var->getArgs();
      for (PgExpr *arg : args) {
        std::shared_ptr<SqlOpExpr> arg_expr = std::make_shared<SqlOpExpr>();
        PrepareExpression(arg, arg_expr);
        op_cond->addOperand(arg_expr);
      }
      expr_var->setCondition(op_cond);
    }
  }

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
std::shared_ptr<SqlOpExpr> PgDmlRead::AllocColumnBindVar(PgColumn *col) {
  return col->AllocBind(read_req_);
}

std::shared_ptr<SqlOpCondition> PgDmlRead::AllocColumnBindConditionExprVar(PgColumn *col) {
  return col->AllocBindConditionExpr(read_req_);
}

// Allocate variable for target.
std::shared_ptr<SqlOpExpr> PgDmlRead::AllocTargetVar() {
  std::shared_ptr<SqlOpExpr> target = std::make_shared<SqlOpExpr>();
  read_req_->targets.push_back(target);
  return target;
}

// Allocate column expression.
std::shared_ptr<SqlOpExpr> PgDmlRead::AllocColumnAssignVar(PgColumn *col) {
  // SELECT statement should not have an assign expression (SET clause).
  LOG(FATAL) << "Pure virtual function is being call";
  return nullptr;
}

Status PgDmlRead::DeleteEmptyPrimaryBinds() {
  if (secondary_index_query_) {
    RETURN_NOT_OK(secondary_index_query_->DeleteEmptyPrimaryBinds());
  }

  if (!bind_desc_) {
    // This query does not have any bindings.
    read_req_->key_column_values.clear();
    return Status::OK();
  }

  bool miss_key_columns = false;
  for (size_t i = 0; i < bind_desc_->num_key_columns(); i++) {
    PgColumn& col = bind_desc_->columns()[i];
    std::shared_ptr<SqlOpExpr> expr = col.bind_var();
    // Not find the binding in the expr_binds_ map
    if (expr_binds_.find(expr) == expr_binds_.end()) {
      miss_key_columns = true;
      break;
    } 
  }

  // if one of the primary keys is not bound, we need to use full scan without passing key values
  if (miss_key_columns) {
    VLOG(1) << "Full scan is needed";
    read_req_->key_column_values.clear();
  }

  return Status::OK();
}

Status PgDmlRead::BindColumnCondEq(int attr_num, PgExpr *attr_value) {
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumnCondEq(attr_num, attr_value);
  }

  // Find column.
  PgColumn *col = VERIFY_RESULT(bind_desc_->FindColumn(attr_num));

  // Alloc the expression
  std::shared_ptr<SqlOpCondition> condition_expr_var = AllocColumnBindConditionExprVar(col);

  if (attr_value != nullptr) {
    condition_expr_var->setOp(PgExpr::Opcode::PG_EXPR_EQ);

    std::shared_ptr<SqlOpExpr> op1_var = std::make_shared<SqlOpExpr>(SqlOpExpr::ExprType::COLUMN_ID, col->id());
    condition_expr_var->addOperand(op1_var);

    std::shared_ptr<SqlOpExpr> op2_var = std::make_shared<SqlOpExpr>();
    condition_expr_var->addOperand(op2_var);

    // read value from attr_value, which should be a PgConstant
    RETURN_NOT_OK(PrepareExpression(attr_value, op2_var));
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

  CHECK(!col->desc()->is_partition()) << "This method cannot be used for binding partition column!";

  // Alloc the doc condition
  std::shared_ptr<SqlOpCondition> condition_expr_var = AllocColumnBindConditionExprVar(col);

  if (attr_value != nullptr) {
    if (attr_value_end != nullptr) {
      condition_expr_var->setOp(PgExpr::Opcode::PG_EXPR_BETWEEN);
      std::shared_ptr<SqlOpExpr> op1_var = std::make_shared<SqlOpExpr>(SqlOpExpr::ExprType::COLUMN_ID, col->id());
      condition_expr_var->addOperand(op1_var);

      std::shared_ptr<SqlOpExpr> op2_var = std::make_shared<SqlOpExpr>();
      condition_expr_var->addOperand(op2_var);
      std::shared_ptr<SqlOpExpr> op3_var = std::make_shared<SqlOpExpr>();
      condition_expr_var->addOperand(op3_var);

      RETURN_NOT_OK(PrepareExpression(attr_value, op2_var));
      RETURN_NOT_OK(PrepareExpression(attr_value_end, op3_var));
    } else {
      condition_expr_var->setOp(PgExpr::Opcode::PG_EXPR_GE);
      std::shared_ptr<SqlOpExpr> op1_var = std::make_shared<SqlOpExpr>(SqlOpExpr::ExprType::COLUMN_ID, col->id());
      condition_expr_var->addOperand(op1_var);

      std::shared_ptr<SqlOpExpr> op2_var = std::make_shared<SqlOpExpr>();
      condition_expr_var->addOperand(op2_var);

      RETURN_NOT_OK(PrepareExpression(attr_value, op2_var));
    }
  } else {
    if (attr_value_end != nullptr) {
      condition_expr_var->setOp(PgExpr::Opcode::PG_EXPR_LE);
      std::shared_ptr<SqlOpExpr> op1_var = std::make_shared<SqlOpExpr>(SqlOpExpr::ExprType::COLUMN_ID, col->id());
      condition_expr_var->addOperand(op1_var);

      std::shared_ptr<SqlOpExpr> op2_var = std::make_shared<SqlOpExpr>();
      condition_expr_var->addOperand(op2_var);

      RETURN_NOT_OK(PrepareExpression(attr_value_end, op2_var));
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

  if (col->desc()->is_partition()) {
    // Alloc the expression variable.
    std::shared_ptr<SqlOpExpr> bind_var = col->bind_var();
    if (bind_var == nullptr) {
      bind_var = AllocColumnBindVar(col);
    } else {
      if (expr_binds_.find(bind_var) != expr_binds_.end()) {
        LOG(WARNING) << strings::Substitute("Column $0 is already bound to another value.",
                                            attr_num);
      }
    }

    std::shared_ptr<SqlOpCondition> doc_condition = std::make_shared<SqlOpCondition>();
    doc_condition->setOp(PgExpr::Opcode::PG_EXPR_IN);
    std::shared_ptr<SqlOpExpr> op1_var = std::make_shared<SqlOpExpr>(SqlOpExpr::ExprType::COLUMN_ID, col->id());
    doc_condition->addOperand(op1_var);
    bind_var->setCondition(doc_condition);

    // There's no "list of expressions" field so we simulate it with an artificial nested OR
    // with repeated operands, one per bind expression.
    // This is only used for operation unrolling in pg_op_.
    std::shared_ptr<SqlOpCondition> nested_condition = std::make_shared<SqlOpCondition>();
    nested_condition->setOp(PgExpr::Opcode::PG_EXPR_OR);
    std::shared_ptr<SqlOpExpr> op2_var = std::make_shared<SqlOpExpr>(nested_condition);
    doc_condition->addOperand(op2_var);

    for (int i = 0; i < n_attr_values; i++) {
      std::shared_ptr<SqlOpExpr> attr_var = std::make_shared<SqlOpExpr>();
      nested_condition->addOperand(attr_var);

      RETURN_NOT_OK(PrepareExpression(attr_values[i], attr_var));

      expr_binds_[attr_var] = attr_values[i];

      if (attr_num == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
        CHECK(attr_values[i]->is_constant()) << "Column ybctid must be bound to constant";
        ybctid_bind_ = true;
      }
    }
  } else {
    // Alloc the condition variable
    std::shared_ptr<SqlOpCondition> doc_condition = AllocColumnBindConditionExprVar(col);
    doc_condition->setOp(PgExpr::Opcode::PG_EXPR_IN);
    std::shared_ptr<SqlOpExpr> op1_var = std::make_shared<SqlOpExpr>(SqlOpExpr::ExprType::COLUMN_ID, col->id());
    doc_condition->addOperand(op1_var);
    std::shared_ptr<SqlOpExpr> op2_var = std::make_shared<SqlOpExpr>();
    doc_condition->addOperand(op2_var);

    for (int i = 0; i < n_attr_values; i++) {
      // Link the given expression "attr_value" with the allocated request variable.
      // Note that except for constants and place_holders, all other expressions can be setup
      // just one time during prepare.
      // Examples:
      // - Bind values for primary columns in where clause.
      //     WHERE hash = ?
      // - Bind values for a column in INSERT statement.
      //     INSERT INTO a_table(hash, key, col) VALUES(?, ?, ?)

      if (attr_values[i]) {
        std::shared_ptr<SqlOpExpr> attr_val = std::make_shared<SqlOpExpr>();
        RETURN_NOT_OK(PrepareExpression(attr_values[i], attr_val));
        op2_var->addListValue(attr_val->getValue());
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
  // Initialize sql operator.
  if (sql_op_) {
    sql_op_->ExecuteInit(exec_params);
  }

  // Delete key columns that are not bound to any values.
  RETURN_NOT_OK(DeleteEmptyPrimaryBinds());

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
      SCHECK_EQ(VERIFY_RESULT(sql_op_->Execute()), RequestSent::kTrue, IllegalState,
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
  // Assume that the storage layer requires that primary columns must be listed in their created-order,
  // the slots for primary column bind expressions are allocated here in correct order.
  for (PgColumn &col : target_desc_->columns()) {
    col.AllocKeyBind(write_req_);
  }
}

Status PgDmlWrite::Exec(bool force_non_bufferable) {
  // Delete allocated binds that are not associated with a value.
  RETURN_NOT_OK(DeleteEmptyPrimaryBinds());

  // First update request with new bind values.
  RETURN_NOT_OK(UpdateBindVars());
  RETURN_NOT_OK(UpdateAssignVars());

  if (write_req_->ybctid_column_value != nullptr) {
    std::shared_ptr<SqlOpExpr> expr_var = write_req_->ybctid_column_value;
    CHECK(expr_var->isValueType() && expr_var->getValue() != nullptr && expr_var->getValue()->isBinaryValue())
      << "YBCTID must be of BINARY datatype";
  }

  // Initialize sql operator.
  sql_op_->ExecuteInit(nullptr);

  // Execute the statement. If the request has been sent, get the result and handle any rows
  // returned.
  if (VERIFY_RESULT(sql_op_->Execute(force_non_bufferable)) == RequestSent::kTrue) {
    RETURN_NOT_OK(sql_op_->GetResult(&rowsets_));

    // Save the number of rows affected by the op.
    rows_affected_count_ = VERIFY_RESULT(sql_op_->GetRowsAffectedCount());
  }
  
  return Status::OK();
}

Status PgDmlWrite::DeleteEmptyPrimaryBinds() {
  // Iterate primary-key columns and remove the binds without values.
  bool missing_primary_key = false;

  // Either ybctid or primary key must be present.
  if (!ybctid_bind_) {
    // Remove empty binds from key list.
    auto key_iter = write_req_->key_column_values.begin();
    while (key_iter != write_req_->key_column_values.end()) {
      if (expr_binds_.find(*key_iter) == expr_binds_.end()) {
        missing_primary_key = true;
        key_iter = write_req_->key_column_values.erase(key_iter);
      } else {
        key_iter++;
      }
    }
  } else {
    write_req_->key_column_values.clear();
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
  write_req_ = wop->request();
  sql_op_ = std::make_shared<PgWriteOp>(pg_session_, target_desc_, table_id_, std::move(wop));
}

std::shared_ptr<SqlOpExpr> PgDmlWrite::AllocColumnBindVar(PgColumn *col) {
  return col->AllocBind(write_req_);
}

std::shared_ptr<SqlOpExpr> PgDmlWrite::AllocColumnAssignVar(PgColumn *col) {
  return col->AllocAssign(write_req_);
}

std::shared_ptr<SqlOpExpr> PgDmlWrite::AllocTargetVar() {
  std::shared_ptr<SqlOpExpr> target_var = std::make_shared<SqlOpExpr>();
  write_req_->targets.push_back(target_var);
  return target_var;
}

Status PgDmlWrite::SetWriteTime(const uint64_t write_time) {
  SCHECK(sql_op_.get() != nullptr, RuntimeError, "expected sql_op_ to be initialized");
  down_cast<PgWriteOp*>(sql_op_.get())->SetWriteTime(write_time);
  return Status::OK();
}

}  // namespace gate
}  // namespace k2pg
