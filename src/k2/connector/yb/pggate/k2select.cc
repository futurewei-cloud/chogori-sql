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

#include "yb/pggate/k2select.h"

namespace k2 {
namespace gate {

using std::make_shared;

K2Select::K2Select(K2Session::ScopedRefPtr k2_session, const PgObjectId& table_id,
                   const PgObjectId& index_id, const PgPrepareParameters *prepare_params)
    : K2DmlRead(k2_session, table_id, index_id, prepare_params) {
}

K2Select::~K2Select() {
}

Status K2Select::Prepare() {
  // Prepare target and bind descriptors.
  if (!prepare_params_.use_secondary_index) {
    target_desc_ = bind_desc_ = VERIFY_RESULT(k2_session_->LoadTable(table_id_));
  } else {
    target_desc_ = VERIFY_RESULT(k2_session_->LoadTable(table_id_));
    bind_desc_ = nullptr;

    // Create secondary index query.
    secondary_index_query_ =
      make_scoped_refptr<K2SelectIndex>(k2_session_, table_id_, index_id_, &prepare_params_);
  }

  // Allocate READ requests to send to Doc api.
  auto read_op = target_desc_->NewPgsqlSelect();
  read_req_ = &read_op->request();
  auto doc_op = make_shared<K2DocReadOp>(k2_session_, target_desc_, std::move(read_op));

  // Prepare the index selection if this operation is using the index.
  RETURN_NOT_OK(PrepareSecondaryIndex());

  // Prepare binds for the request.
  PrepareBinds();

  // Preparation complete.
  doc_op_ = doc_op;  
  
  return Status::OK();
}

Status K2Select::PrepareSecondaryIndex() {
  if (!secondary_index_query_) {
    // This DML statement is not using secondary index.
    return Status::OK();
  }

  // Prepare the index operation to read ybctids from the index table. There are two different
  // scenarios on how ybctids are requested.
  // - Due to an optimization in Doc, for colocated tables (both system and user colocated), index
  //   request is sent as a part of the actual read request using doc field
  //   "DocReadRequest::index_request"
  //
  //   For this case, "index_request" is allocated here and passed to K2SelectIndex node to
  //   fill in with bind-values when necessary.
  //
  // - For regular tables, the index subquery will send separate request to tablet servers collect
  //   batches of ybctids which is then used by 'this' outer select to query actual data.
  DocReadRequest *index_req = nullptr;
  if (prepare_params_.querying_colocated_table) {
    // Allocate "index_request" and pass to K2SelectIndex.
    index_req = read_req_->index_request;
  }

  // Prepare subquery. When index_req is not null, it is part of 'this' SELECT request. When it
  // is nullptr, the subquery will create its own doc_op to run a separate read request.
  return secondary_index_query_->PrepareSubquery(index_req); 
}

K2SelectIndex::K2SelectIndex(K2Session::ScopedRefPtr k2_session,
                             const PgObjectId& table_id,
                             const PgObjectId& index_id,
                             const PgPrepareParameters *prepare_params)
    : K2DmlRead(k2_session, table_id, index_id, prepare_params) {
}

K2SelectIndex::~K2SelectIndex() {
}

Status K2SelectIndex::Prepare() {
  // We get here only if this is an IndexOnly scan.
  CHECK(prepare_params_.index_only_scan) << "Unexpected IndexOnly scan type";
  return PrepareQuery(nullptr);
}

Status K2SelectIndex::PrepareSubquery(DocReadRequest *read_req) {
  // We get here if this is an SecondaryIndex scan.
  CHECK(prepare_params_.use_secondary_index && !prepare_params_.index_only_scan)
    << "Unexpected Index scan type";
  return PrepareQuery(read_req);
}

Status K2SelectIndex::PrepareQuery(DocReadRequest *read_req) {
  // Setup target and bind descriptor.
  target_desc_ = bind_desc_ = VERIFY_RESULT(k2_session_->LoadTable(index_id_));

  // Allocate READ requests to send to Doc api.
  if (read_req) {
    // For (system and user) colocated tables, SelectIndex is a part of Select and being sent
    // together with the SELECT doc request. A read doc_op and request is not needed in this
    // case.
    DSCHECK(prepare_params_.querying_colocated_table, InvalidArgument, "Read request invalid");
    read_req_ = read_req;
    read_req_->table_id = index_id_.GetYBTableId();
    doc_op_ = nullptr;
  } else {
    auto read_op = target_desc_->NewPgsqlSelect();
    read_req_ = &read_op->request();
    doc_op_ = make_shared<K2DocReadOp>(k2_session_, target_desc_, std::move(read_op));
  }

  // Prepare index key columns.
  PrepareBinds();
  return Status::OK();
}

Result<bool> K2SelectIndex::FetchYbctidBatch(const vector<Slice> **ybctids) {
  // Keep reading until we get one batch of ybctids or EOF.
  while (!VERIFY_RESULT(GetNextYbctidBatch())) {
    if (!VERIFY_RESULT(FetchDataFromServer())) {
      // Server returns no more rows.
      *ybctids = nullptr;
      return false;
    }
  }

  // Got the next batch of ybctids.
  DCHECK(!rowsets_.empty());
  *ybctids = &rowsets_.front().ybctids();
  return true;
}

Result<bool> K2SelectIndex::GetNextYbctidBatch() {
  for (auto rowset_iter = rowsets_.begin(); rowset_iter != rowsets_.end();) {
    if (rowset_iter->is_eof()) {
      rowset_iter = rowsets_.erase(rowset_iter);
    } else {
      // Write all found rows to ybctid array.
      RETURN_NOT_OK(rowset_iter->ProcessSystemColumns());
      return true;
    }
  }

  return false;
}

}  // namespace gate
}  // namespace k2