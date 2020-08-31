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

#include "yb/pggate/k2column.h"

namespace k2 {
namespace gate {

void K2Column::Init(PgSystemAttrNum attr_num) {
  switch (attr_num) {
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

    case PgSystemAttrNum::kYBTupleId: {
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

bool K2Column::is_virtual_column() {
  // Currently only ybctid is a virtual column.
  return attr_num() == static_cast<int>(PgSystemAttrNum::kYBTupleId);
}

PgExpr *K2Column::AllocPrimaryBind(DocWriteRequest *write_req) {
  if (desc_.is_partition()) {
    write_req->partition_column_values.push_back(bind_pb_);
  } else if (desc_.is_primary()) {
    write_req->range_column_values.push_back(bind_pb_);
  }
  return bind_pb_;
}

PgExpr *K2Column::AllocBind(DocWriteRequest *write_req) {
  if (bind_pb_ == nullptr) {
    DCHECK(!desc_.is_partition() && !desc_.is_primary())
      << "Binds for primary columns should have already been allocated by AllocPrimaryBindPB()";

    if (id() == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
      bind_pb_ = write_req->ybctid_column_value;
    } else {        
      ColumnValue col;
      col.column_id = id();
      // TODO: initialize PgExpr
      col.expr = nullptr;
      bind_pb_ = col.expr;
      write_req->column_values.push_back(col);
    }
  }
  return bind_pb_;
}

PgExpr *K2Column::AllocAssign(DocWriteRequest *write_req) {
  if (assign_pb_ == nullptr) {
    ColumnValue col;
    col.column_id = id();
    // TODO: initialize PgExpr
    col.expr = nullptr;
    assign_pb_ = col.expr;    
    write_req->column_new_values.push_back(col);
  }
  return assign_pb_;
}

PgExpr *K2Column::AllocPrimaryBind(DocReadRequest *read_req) {
   if (desc_.is_partition()) {
    read_req->partition_column_values.push_back(bind_pb_);
  } else if (desc_.is_primary()) {
    read_req->range_column_values.push_back(bind_pb_);  
  }
  return bind_pb_; 
}

//--------------------------------------------------------------------------------------------------

PgExpr *K2Column::AllocBind(DocReadRequest *read_req) {
  if (desc_.is_partition()) {
    read_req->partition_column_values.push_back(bind_pb_);
  } else if (desc_.is_primary()) {
    read_req->range_column_values.push_back(bind_pb_);  
  }
  return bind_pb_;
}

PgExpr *K2Column::AllocBindConditionExpr(DocReadRequest *read_req) {
  if (bind_pb_ == nullptr) {
    DCHECK(!desc_.is_partition() && !desc_.is_primary())
      << "Binds for primary columns should have already been allocated by AllocPrimaryBind()";

    if (id() == static_cast<int>(PgSystemAttrNum::kYBTupleId)) {
      bind_pb_ = read_req->ybctid_column_value;
    } else {
      DLOG(FATAL) << "Binds for other columns are not allowed";
    }
  }
  return bind_pb_;
}

}  // namespace gate
}  // namespace k2