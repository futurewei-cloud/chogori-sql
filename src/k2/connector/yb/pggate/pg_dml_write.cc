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

#include "yb/pggate/pg_dml_write.h"

namespace k2pg {
namespace gate {

PgDmlWrite::PgDmlWrite(std::shared_ptr<PgSession> pg_session,
                       const PgObjectId& table_id,
                       const bool is_single_row_txn)
    : PgDml(pg_session, table_id), is_single_row_txn_(is_single_row_txn) {
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
    if (col.attr_num() == yb::to_underlying(PgSystemAttrNum::kYBRowId)) {
      // generate new rowid for kYBRowId column when no primary keys are defined
      std::string row_id = std::move(pg_session()->GenerateNewRowid());
      K2DEBUG("Generated new row id " << k2::escape(row_id));
      col.AllocKeyBindForRowId(write_req_, row_id);
      // set the row_id bind
      row_id_bind_.emplace(col.bind_var());
    } else {
      col.AllocKeyBind(write_req_);
    }
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
  // always use regular binding for INSERT
  if (stmt_op() == StmtOp::STMT_INSERT || !ybctid_bind_) {
    K2DEBUG("Checking missing primary keys for regular key binding, stmt op: " << stmt_op() << ", key_column_values size: " << write_req_->key_column_values.size());
    // Remove empty binds from key list.
    auto key_iter = write_req_->key_column_values.begin();
    while (key_iter != write_req_->key_column_values.end()) {
      if (row_id_bind_.has_value() && row_id_bind_.value() == (*key_iter)) {
        K2DEBUG("Found RowId column: " << (*(*key_iter).get()));
        key_iter++;
      } else {
        if (expr_binds_.find(*key_iter) == expr_binds_.end()) {
          missing_primary_key = true;
          K2DEBUG("Missing primary key: " << (*(*key_iter).get()));
          key_iter = write_req_->key_column_values.erase(key_iter);
        } else {
          key_iter++;
        }
      }
    }
  } else {
    K2DEBUG("Clearing key column values for ybctid binding");
    write_req_->key_column_values.clear();
  }

  K2DEBUG("Deleting empty primary binds and found missing primary key: " << missing_primary_key);
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

