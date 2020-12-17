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
      LOG(FATAL) << "Invalid attribute number for hidden column";
    }

    bool PgColumn::is_virtual_column()
    {
      // Currently only ybctid is a virtual column.
      return attr_num() == static_cast<int>(PgSystemAttrNum::kYBTupleId);
    }

    std::shared_ptr<SqlOpExpr> PgColumn::AllocKeyBind(std::shared_ptr<SqlOpWriteRequest> write_req)
    {
      if (bind_var_ == nullptr)
      {
        DCHECK(desc_.is_partition() || desc_.is_primary())
            << "Only primary columns are allocated by AllocKeyBind()";

        bind_var_ = std::make_shared<SqlOpExpr>();
        write_req->key_column_values.push_back(bind_var_);
      }

      return bind_var_;
    }

    std::shared_ptr<SqlOpExpr> PgColumn::AllocBind(std::shared_ptr<SqlOpWriteRequest> write_req)
    {
      if (bind_var_ == nullptr)
      {
        DCHECK(!desc_.is_partition() && !desc_.is_primary())
            << "Binds for primary columns should have already been allocated by AllocKeyBind()";

        if (id() == static_cast<int>(PgSystemAttrNum::kYBTupleId))
        {
          bind_var_ = write_req->ybctid_column_value;
          if (bind_var_ == nullptr)
          {
            bind_var_ = std::make_shared<SqlOpExpr>();
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
      if (bind_var_ == nullptr)
      {
        DCHECK(desc_.is_partition() || desc_.is_primary())
            << "Only primary columns are allocated by AllocKeyBind()";

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
        DCHECK(!desc_.is_partition() && !desc_.is_primary())
            << "Binds for primary columns should have already been allocated by AllocKeyBind()";

        if (id() == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
          bind_var_ = read_req->ybctid_column_value;
          if (bind_var_ == nullptr)
          {
            bind_var_ = std::make_shared<SqlOpExpr>();
          }
        } else {
          DLOG(FATAL) << "Binds for other columns are not allowed";
        }
      }

      return bind_var_;
    }

    std::shared_ptr<SqlOpCondition> PgColumn::AllocBindConditionExpr(std::shared_ptr<SqlOpReadRequest> read_req)
    {
      if (bind_condition_expr_var_ == nullptr)
      {
        bind_condition_expr_var_ = read_req->condition_expr;
        if (bind_condition_expr_var_ == nullptr)
        {
          bind_condition_expr_var_ = std::make_shared<SqlOpCondition>();
        }
        bind_condition_expr_var_->setOp(PgExpr::Opcode::PG_EXPR_AND);
        return bind_condition_expr_var_;
      }

      std::shared_ptr<SqlOpCondition> new_condition = std::make_shared<SqlOpCondition>();
      std::shared_ptr<SqlOpExpr> new_operand = std::make_shared<SqlOpExpr>(new_condition);
      bind_condition_expr_var_->addOperand(new_operand);
      return new_operand->getCondition();
    }

  } // namespace gate
} // namespace k2pg