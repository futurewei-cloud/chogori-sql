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
#include "yb/common/enums.h"

namespace k2pg {
namespace gate {

using namespace k2pg::sql;
using yb::to_underlying;

PgDml::PgDml(std::shared_ptr<PgSession> pg_session, const PgObjectId& table_id)
    : PgStatement(std::move(pg_session)), table_id_(table_id) {
}

PgDml::PgDml(std::shared_ptr<PgSession> pg_session,
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
  K2DEBUG("Append target " << target->ToString());
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
  if (!target->is_colref()) {
      return STATUS(InternalError, "Unexpected expression, only column refs supported in SKV");
  }
  // update the name mapping for the targets
  targets_by_name_[((PgColumnRef*)target)->attr_name()] = target;

  return Status::OK();
}

Status PgDml::PrepareColumnForRead(int attr_num, std::shared_ptr<SqlOpExpr> target_var)
{
  // Find column from targeted table.
  PgColumn *pg_col = VERIFY_RESULT(target_desc_->FindColumn(attr_num));

  if (target_var)
    target_var->setColumnId(pg_col->id(), pg_col->attr_name());

  // Mark non-virtual column for writing
  if (!pg_col->is_virtual_column()) {
    pg_col->set_read_requested(true);
  }

  return Status::OK();
}

Status PgDml::PrepareColumnForWrite(PgColumn *pg_col, std::shared_ptr<SqlOpExpr> assign_var) {
  assign_var->setColumnId(pg_col->id(), pg_col->attr_name());

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
  K2DEBUG("Bind column with attr_num: " << attr_num << ", name: " << col->attr_name() << ", value" << k2::escape(attr_value->ToString()));

  // Alloc the expression variable.
  std::shared_ptr<SqlOpExpr> bind_var = col->bind_var();
  if (bind_var == nullptr) {
    bind_var = AllocColumnBindVar(col);
  } else {
    if (expr_binds_.find(bind_var) != expr_binds_.end()) {
      K2WARN("Column " << attr_num << " is already bound to another value.");
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
    K2DEBUG("kYBTupleId was bound and ybctid_bind_ is set as true");
    ybctid_bind_ = true;
  }

  return Status::OK();
}

Status PgDml::UpdateBindVars() {
  K2DEBUG("Updating bind variables");
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
  K2DEBUG("Assign column with attr_num: " << attr_num << ", name: " << col->attr_name() << ", value" << k2::escape(attr_value->ToString()));

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
  K2DEBUG("Updating assigned PgExpr to SqlOpExpr");
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
  K2DEBUG("Fetching tuple");
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
  K2DEBUG("Fetch data from SKV");
  // Get the rowsets from sql operator.
  RETURN_NOT_OK(sql_op_->GetResult(&rowsets_));

  // Check if EOF is reached.
  if (rowsets_.empty()) {
    K2DEBUG("Result set is empty");
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

  K2DEBUG("Processing secondary index request");
  // Execute query in PgGate.
  // If index query is not yet executed, run it.
  if (!secondary_index_query_->is_executed()) {
    secondary_index_query_->set_is_executed(true);
    RETURN_NOT_OK(secondary_index_query_->Exec(exec_params));
  }

  // When INDEX has its own sql_op_, execute it to fetch next batch of ybctids which is then used
  // to read data from the main table.
  std::vector<std::string> ybctids;
  if (!VERIFY_RESULT(secondary_index_query_->FetchRowIdBatch(ybctids))) {
    // No more rows of ybctids.
    return false;
  }

  // Update request with the new batch of ybctids to fetch the next batch of rows.
  RETURN_NOT_OK(sql_op_->PopulateDmlByRowIdOps(ybctids));
  return true;
}

Result<bool> PgDml::GetNextRow(PgTuple *pg_tuple) {
  K2DEBUG("Get next row of pg tuple");
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
      RETURN_NOT_OK(rowset.WritePgTuple(targets_, targets_by_name_, pg_tuple, &row_order));
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
  vector<std::shared_ptr<SqlValue>> values;
  auto attrs_end = attrs + nattrs;
  for (const auto& c : target_desc_->columns()) {
    for (auto attr = attrs; attr != attrs_end; ++attr) {
      if (attr->attr_num == c.attr_num()) {
        if (!c.desc()->is_primary()) {
          return STATUS_SUBSTITUTE(InvalidArgument, "Attribute number $0 not a primary attribute",
                                   attr->attr_num);
        }

        if (attr->attr_num == to_underlying(PgSystemAttrNum::kYBRowId)) {
          // generate new rowid for kYBRowId column when no primary keys are defined
          std::string row_id = std::move(pg_session()->GenerateNewRowid());
          Slice s(row_id.data(), row_id.size());
          std::shared_ptr<SqlValue> value = std::make_shared<SqlValue>(s);
          values.push_back(value);
        } else {
          std::shared_ptr<SqlValue> value = std::make_shared<SqlValue>(attr->type_entity, attr->datum, false);
          values.push_back(value);
        }
      }
    }
  }
  // secondary index query does not have bind_desc_
  std::shared_ptr<PgTableDesc> table_schema = (bind_desc_ == nullptr) ? target_desc_ : bind_desc_;
  return pg_session_->GetRowId(table_schema->namespace_id(), table_schema->table_id(), table_schema->SchemaVersion(), values);
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
    PgColumn *col = VERIFY_RESULT(target_desc_->FindColumn(col_ref->attr_num()));
    col_ref->set_attr_name(col->attr_name());
    PrepareColumnForRead(col_ref->attr_num(), expr_var);
  } else if (target->is_constant()) {
    // PgConstant
    PgConstant *col_const = static_cast<PgConstant *>(target);
    // the PgExpr *target is accessed by PG as well, need a copy of the value for SKV so that
    // we don't accidentally delete the value that is needed by PG when free the value owned by shared_ptrs.
    auto col_val = std::make_shared<SqlValue>(*(col_const->getValue()));
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
  K2DEBUG("Finished binding PgExpr " << target->ToString() << " to " << expr_var->ToString());

  return Status::OK();
}

}  // namespace gate
}  // namespace k2pg
