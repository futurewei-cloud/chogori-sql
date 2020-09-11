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

#include "yb/pggate/k2dml.h"

namespace k2 {
namespace gate {

using namespace yb;
using namespace k2::sql;

K2Dml::K2Dml(K2Session::ScopedRefPtr k2_session, const PgObjectId& table_id)
    : K2Statement(std::move(k2_session)), table_id_(table_id) {
}

K2Dml::K2Dml(K2Session::ScopedRefPtr k2_session,
             const PgObjectId& table_id,
             const PgObjectId& index_id,
             const PgPrepareParameters *prepare_params)
    : K2Dml(k2_session, table_id) {

  if (prepare_params) {
    prepare_params_ = *prepare_params;
    // Primary index does not have its own data table.
    if (prepare_params_.use_secondary_index) {
      index_id_ = index_id;
    }
  }
}

K2Dml::~K2Dml() {
}

Status K2Dml::AppendTarget(PgExpr *target) {
  // Except for base_ctid, all targets should be appended to this DML.
  if (target_desc_ && (prepare_params_.index_only_scan || !target->is_ybbasetid())) {
    RETURN_NOT_OK(AppendTargetPB(target));
  } else {
    // Append base_ctid to the index_query.
//    RETURN_NOT_OK(secondary_index_query_->AppendTargetPB(target));
  }

  return Status::OK();
}

Status K2Dml::AppendTargetPB(PgExpr *target) {
  // Append to targets_.
  targets_.push_back(target);

  // Allocate associated protobuf.
  PgExpr *expr_pb = AllocTargetPB();

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

Status K2Dml::PrepareColumnForRead(int attr_num, PgExpr *target_pb,
                                   const K2Column **col) {
  *col = nullptr;

  // Find column from targeted table.
  K2Column *pg_col = VERIFY_RESULT(target_desc_->FindColumn(attr_num));

  // Prepare protobuf to send to DocDB.
  //if (target_pb)
  //  target_pb->set_column_id(pg_col->id());

  // Mark non-virtual column reference for DocDB.
  if (!pg_col->is_virtual_column()) {
    pg_col->set_read_requested(true);
  }

  *col = pg_col;
  return Status::OK();
}

Status K2Dml::PrepareColumnForWrite(K2Column *pg_col, PgExpr *assign_pb) {
  // Prepare protobuf to send to DocDB.
  // assign_pb->set_column_id(pg_col->id());

  // Mark non-virtual column reference for DocDB.
  if (!pg_col->is_virtual_column()) {
    pg_col->set_write_requested(true);
  }

  return Status::OK();
}

void K2Dml::ColumnRefsToPB(PgColumnRef *column_refs) {
//  column_refs->Clear();
  for (const K2Column& col : target_desc_->columns()) {
    if (col.read_requested() || col.write_requested()) {
//      column_refs->add_ids(col.id());
    }
  }
}

//--------------------------------------------------------------------------------------------------

Status K2Dml::BindColumn(int attr_num, PgExpr *attr_value) {
/*   if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumn(attr_num, attr_value);
  } */

  // Find column to bind.
  K2Column *col = VERIFY_RESULT(bind_desc_->FindColumn(attr_num));

  // Check datatype.
  //SCHECK_EQ(col->internal_type(), attr_value->internal_type(), Corruption,
  //          "Attribute value type does not match column type");

  // Alloc the protobuf.
  PgExpr *bind_pb = col->bind_pb();
  if (bind_pb == nullptr) {
    bind_pb = AllocColumnBindPB(col);
  } else {
    if (expr_binds_.find(bind_pb) != expr_binds_.end()) {
      LOG(WARNING) << strings::Substitute("Column $0 is already bound to another value.", attr_num);
    }
  }

  // Link the expression and protobuf. During execution, expr will write result to the pb.
  RETURN_NOT_OK(PrepareForRead(attr_value, bind_pb));

  // Link the given expression "attr_value" with the allocated protobuf. Note that except for
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

Status K2Dml::UpdateBindPBs() {
  for (const auto &entry : expr_binds_) {
    PgExpr *expr_pb = entry.first;
    PgExpr *attr_value = entry.second;
    RETURN_NOT_OK(Eval(attr_value, expr_pb));
  }

  return Status::OK();
}

//--------------------------------------------------------------------------------------------------

Status K2Dml::BindTable() {
  bind_table_ = true;
  return Status::OK();
}

//--------------------------------------------------------------------------------------------------

Status K2Dml::AssignColumn(int attr_num, PgExpr *attr_value) {
  // Find column from targeted table.
  K2Column *col = VERIFY_RESULT(target_desc_->FindColumn(attr_num));

  // Check datatype.
//  SCHECK_EQ(col->internal_type(), attr_value->internal_type(), Corruption,
 //           "Attribute value type does not match column type");

  // Alloc the protobuf.
  PgExpr *assign_pb = col->assign_pb();
  if (assign_pb == nullptr) {
    assign_pb = AllocColumnAssignPB(col);
  } else {
    if (expr_assigns_.find(assign_pb) != expr_assigns_.end()) {
      return STATUS_SUBSTITUTE(InvalidArgument,
                               "Column $0 is already assigned to another value", attr_num);
    }
  }

  // Link the expression and protobuf. During execution, expr will write result to the pb.
  // - Prepare the left hand side for write.
  // - Prepare the right hand side for read. Currently, the right hand side is always constant.
  RETURN_NOT_OK(PrepareColumnForWrite(col, assign_pb));
  RETURN_NOT_OK(PrepareForRead(attr_value, assign_pb));

  // Link the given expression "attr_value" with the allocated protobuf. Note that except for
  // constants and place_holders, all other expressions can be setup just one time during prepare.
  // Examples:
  // - Setup rhs values for SET column = assign_pb in UPDATE statement.
  //     UPDATE a_table SET col = assign_expr;
  expr_assigns_[assign_pb] = attr_value;

  return Status::OK();
}

Status K2Dml::UpdateAssignPBs() {
  // Process the column binds for two cases.
  // For performance reasons, we might evaluate these expressions together with bind values in YB.
  for (const auto &entry : expr_assigns_) {
    PgExpr *expr_pb = entry.first;
    PgExpr *attr_value = entry.second;
    RETURN_NOT_OK(Eval(attr_value, expr_pb));
  }

  return Status::OK();
}

Status K2Dml::ClearBinds() {
  return STATUS(NotSupported, "Clearing binds for prepared statement is not yet implemented");
}

Status K2Dml::Fetch(int32_t natts, uint64_t *values, bool *isnulls, PgSysColumns *syscols, bool *has_data) {
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

Result<bool> K2Dml::FetchDataFromServer() {
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
              "YSQL read operation was not sent");

    // Get the rowsets from doc-operator.
    RETURN_NOT_OK(doc_op_->GetResult(&rowsets_));
  }

  return true;
}

Result<bool> K2Dml::ProcessSecondaryIndexRequest(const PgExecParameters *exec_params) {
  // TODO: add implementation
  
  return true;
}

Result<bool> K2Dml::GetNextRow(PgTuple *pg_tuple) {
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

Result<string> K2Dml::BuildYBTupleId(const PgAttrValueDescriptor *attrs, int32_t nattrs) {
  // get Doc Key from DOC API client
  return nullptr;
}

bool K2Dml::has_aggregate_targets() {
  int num_aggregate_targets = 0;
  for (const auto& target : targets_) {
    if (target->is_aggregate())
      num_aggregate_targets++;
  }

  CHECK(num_aggregate_targets == 0 || num_aggregate_targets == targets_.size())
    << "Some, but not all, targets are aggregate expressions.";

  return num_aggregate_targets > 0;
}

Status K2Dml::PrepareForRead(PgExpr *target, PgExpr *expr_pb) {
  // TODO: add logic for different PgExpr types
  return Status::OK();
}

Status K2Dml::Eval(PgExpr *target, PgExpr *expr_pb) {
  // TODO: add logic for different PgExpr types
  return Status::OK();
}

K2DmlRead::K2DmlRead(K2Session::ScopedRefPtr k2_session, const PgObjectId& table_id,
                     const PgObjectId& index_id, const PgPrepareParameters *prepare_params)
    : K2Dml(std::move(k2_session), table_id, index_id, prepare_params) {
}

K2DmlRead::~K2DmlRead() {
}

void K2DmlRead::PrepareBinds() {
  if (!bind_desc_) {
    // This statement doesn't have bindings.
    return;
  }

  // TODO: add column processing
}

void K2DmlRead::SetForwardScan(const bool is_forward_scan) {
  // TODO:: add logic for secondary index scan
  is_forward_scan_ = is_forward_scan;
}

void K2DmlRead::SetColumnRefs() {
  // TODO: add implementation 
}

Status K2DmlRead::DeleteEmptyPrimaryBinds() {
  // TODO: add implementation 
  return Status::OK();
}

Status K2DmlRead::BindColumnCondEq(int attnum, PgExpr *attr_value) {
  // TODO: add implementation 
  return Status::OK();
}

Status K2DmlRead::BindColumnCondBetween(int attr_num, PgExpr *attr_value, PgExpr *attr_value_end) {
  // TODO: add implementation 
  return Status::OK();
}

Status K2DmlRead::BindColumnCondIn(int attnum, int n_attr_values, PgExpr **attr_values) {
  // TODO: add implementation 
  return Status::OK();
}

Status K2DmlRead::Exec(const PgExecParameters *exec_params) {
  // TODO: add implementation 
  return Status::OK();
}

K2DmlWrite::K2DmlWrite(K2Session::ScopedRefPtr k2_session,
                       const PgObjectId& table_id,
                       const bool is_single_row_txn)
    : K2Dml(std::move(k2_session), table_id), is_single_row_txn_(is_single_row_txn) {
}

K2DmlWrite::~K2DmlWrite() {
}

Status K2DmlWrite::Prepare() {
  // TODO: add implementation
  return Status::OK();
}

Status K2DmlWrite::Exec(bool force_non_bufferable) {
  // TODO: add implementation
  return Status::OK();
}


}  // namespace gate
}  // namespace k2
