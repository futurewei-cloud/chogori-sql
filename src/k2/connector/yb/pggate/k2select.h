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

#ifndef CHOGORI_GATE_DML_SELECT_H
#define CHOGORI_GATE_DML_SELECT_H

#include "yb/pggate/k2dml.h"

namespace k2 {
namespace gate {

using namespace yb;
using namespace k2::sql;

//--------------------------------------------------------------------------------------------------
// SELECT
//--------------------------------------------------------------------------------------------------

class K2Select : public K2DmlRead {
 public:
  // Public types.
  typedef scoped_refptr<K2Select> ScopedRefPtr;

  // Constructors.
  K2Select(K2Session::ScopedRefPtr k2_session, const PgObjectId& table_id,
           const PgObjectId& index_id, const PgPrepareParameters *prepare_params);

  virtual ~K2Select();

  // Prepare query before execution.
  virtual CHECKED_STATUS Prepare();

  // Prepare secondary index if that index is used by this query.
  CHECKED_STATUS PrepareSecondaryIndex();
};

//--------------------------------------------------------------------------------------------------
// SELECT FROM Secondary Index Table
//--------------------------------------------------------------------------------------------------

class K2SelectIndex : public K2DmlRead {
 public:
  // Public types.
  typedef scoped_refptr<K2SelectIndex> ScopedRefPtr;
  typedef std::shared_ptr<K2SelectIndex> SharedPtr;

  // Constructors.
  K2SelectIndex(K2Session::ScopedRefPtr k2_session,
                const PgObjectId& table_id,
                const PgObjectId& index_id,
                const PgPrepareParameters *prepare_params);
  virtual ~K2SelectIndex();

  // Prepare query for secondary index. This function is called when Postgres layer is accessing
  // the IndexTable directy (IndexOnlyScan).
  CHECKED_STATUS Prepare();

  // Prepare NESTED query for secondary index. This function is called when Postgres layer is
  // accessing the IndexTable via an outer select (Sequential or primary scans)
 // CHECKED_STATUS PrepareSubquery(PgsqlReadRequestPB *read_req);

 // CHECKED_STATUS PrepareQuery(PgsqlReadRequestPB *read_req);

  // The output parameter "ybctids" are pointer to the data buffer in "ybctid_batch_".
  Result<bool> FetchYbctidBatch(const vector<Slice> **ybctids);

  // Get next batch of ybctids from either PgGate::cache or server.
  Result<bool> GetNextYbctidBatch();

  void set_is_executed(bool value) {
    is_executed_ = value;
  }

  bool is_executed() {
    return is_executed_;
  }

 private:
  // Collect ybctids from IndexTable.
  CHECKED_STATUS FetchYbctids();

  // This secondary query should be executed just one time.
  bool is_executed_ = false;
};

}  // namespace gate
}  // namespace k2

#endif //CHOGORI_GATE_DML_SELECT_H

