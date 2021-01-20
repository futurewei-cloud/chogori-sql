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

#include "yb/pggate/pg_column.h"
#include "k2_includes.h"

namespace k2pg
{
  namespace gate
  {

    void PgColumn::Init(PgSystemAttrNum attr_num)
    {
      switch (attr_num)
      {
      case PgSystemAttrNum::kSelfItemPointer:
      case PgSystemAttrNum::kObjectId:
      case PgSystemAttrNum::kMinTransactionId:
      case PgSystemAttrNum::kMinCommandId:
      case PgSystemAttrNum::kMaxTransactionId:
      case PgSystemAttrNum::kMaxCommandId:
      case PgSystemAttrNum::kTableOid:
      case PgSystemAttrNum::kYBRowId:
      case PgSystemAttrNum::kYBIdxBaseTupleId:
      case PgSystemAttrNum::kYBUniqueIdxKeySuffix:
        break;

      case PgSystemAttrNum::kYBTupleId:
      {
        int idx = static_cast<int>(PgSystemAttrNum::kYBTupleId);
        desc_.Init(idx,
                   idx,
                   "ybctid",
                   false,
                   false,
                   idx,
                   SQLType::Create(DataType::BINARY),
                   ColumnSchema::SortingType::kNotSpecified);
        return;
      }
      }
      K2LOG_E(log::pg, "Invalid attribute number for hidden column");
    }

    bool PgColumn::is_virtual_column()
    {
      // Currently only ybctid is a virtual column.
      return attr_num() == static_cast<int>(PgSystemAttrNum::kYBTupleId);
    }

    std::shared_ptr<SqlOpExpr> PgColumn::AllocKeyBind(std::shared_ptr<SqlOpWriteRequest> write_req)
    {
      if (is_primary() && bind_var_ == nullptr)
      {
        K2LOG_D(log::pg, "Allocating key binding SqlOpExpr for column name: {}, order: {}, for write request", attr_name(), attr_num());
        bind_var_ = std::make_shared<SqlOpExpr>();
        write_req->key_column_values.push_back(bind_var_);
      }

      return bind_var_;
    }

    std::shared_ptr<SqlOpExpr> PgColumn::AllocKeyBindForRowId(std::shared_ptr<SqlOpWriteRequest> write_req, std::string row_id) {
      if (is_primary() && attr_num() == static_cast<int>(PgSystemAttrNum::kYBRowId)) {
        std::shared_ptr<SqlValue> value = std::make_shared<SqlValue>(std::move(row_id));
        bind_var_ = std::make_shared<SqlOpExpr>(SqlOpExpr::ExprType::VALUE, value);
        K2LOG_D(log::pg, "Allocating row id key binding SqlOpExpr {} for column name: {}, order: {} for write request",
            *bind_var_.get(), attr_name(), attr_num());
        write_req->key_column_values.push_back(bind_var_);
      }
      return bind_var_;
    }

    std::shared_ptr<SqlOpExpr> PgColumn::AllocBind(std::shared_ptr<SqlOpWriteRequest> write_req)
    {
      if (bind_var_ == nullptr)
      {
        K2ASSERT(log::pg, !desc_.is_hash() && !desc_.is_primary(),
            "Binds for primary columns should have already been allocated by AllocKeyBind()");

        K2LOG_D(log::pg, "Allocating binding SqlOpExpr for column name: {}, order: {}, for write request", attr_name(), attr_num());
        if (id() == static_cast<int>(PgSystemAttrNum::kYBTupleId))
        {
          if (write_req->ybctid_column_value == nullptr)
          {
            bind_var_ = std::make_shared<SqlOpExpr>();
            write_req->ybctid_column_value = bind_var_;
          }
        }
        else
        {
          ColumnValue col;
          col.column_id = id();
          col.expr = std::make_shared<SqlOpExpr>();
          bind_var_ = col.expr;
          write_req->column_values.push_back(col);
        }
      }

      return bind_var_;
    }

    std::shared_ptr<SqlOpExpr> PgColumn::AllocAssign(std::shared_ptr<SqlOpWriteRequest> write_req)
    {
      if (assign_var_ == nullptr)
      {
        K2LOG_D(log::pg, "Allocating assign SqlOpExpr for column name: {}, order: {}, for write request", attr_name(), attr_num());
        ColumnValue col;
        col.column_id = id();
        col.expr = std::make_shared<SqlOpExpr>();
        assign_var_ = col.expr;
        write_req->column_new_values.push_back(col);
      }

      return assign_var_;
    }

    std::shared_ptr<SqlOpExpr> PgColumn::AllocKeyBind(std::shared_ptr<SqlOpReadRequest> read_req)
    {
      if (is_primary() && bind_var_ == nullptr)
      {
        K2LOG_D(log::pg, "Allocating key binding SqlOpExpr for column name: {}, order: {}, for read request", attr_name(), attr_num());
        bind_var_ = std::make_shared<SqlOpExpr>();
        read_req->key_column_values.push_back(bind_var_);
      }

      return bind_var_;
    }

    //--------------------------------------------------------------------------------------------------

    std::shared_ptr<SqlOpExpr> PgColumn::AllocBind(std::shared_ptr<SqlOpReadRequest> read_req)
    {
      if (bind_var_ == nullptr)
      {
        K2ASSERT(log::pg, !desc_.is_hash() && !desc_.is_primary(),
            "Binds for primary columns should have already been allocated by AllocKeyBind()");

        K2LOG_D(log::pg, "Allocating binding SqlOpExpr for column name: {}, order: {}, for read request", attr_name(), attr_num());
        if (id() == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
          bind_var_ = std::make_shared<SqlOpExpr>();
          read_req->ybctid_column_values.push_back(bind_var_);
        } else {
          K2LOG_E(log::pg, "Binds for other columns are not allowed");
        }
      }

      return bind_var_;
    }

    std::shared_ptr<SqlOpCondition> PgColumn::AllocBindConditionExpr(std::shared_ptr<SqlOpReadRequest> read_req)
    {
      K2LOG_D(log::pg, "Allocating binding SqlOpCondition for column name: {}, order: {}, for read request", attr_name(), attr_num());
      if (bind_condition_expr_var_ == nullptr)
      {
        if (read_req->condition_expr == nullptr) {
          read_req->condition_expr = std::make_shared<SqlOpCondition>();
          read_req->condition_expr->setOp(PgExpr::Opcode::PG_EXPR_AND);
        }
        bind_condition_expr_var_ = read_req->condition_expr;
      }

      std::shared_ptr<SqlOpCondition> new_condition = std::make_shared<SqlOpCondition>();
      std::shared_ptr<SqlOpExpr> new_operand = std::make_shared<SqlOpExpr>(new_condition);
      bind_condition_expr_var_->addOperand(new_operand);
      return new_operand->getCondition();
    }

  } // namespace gate
} // namespace k2pg
