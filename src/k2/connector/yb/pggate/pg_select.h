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

#include "yb/pggate/pg_dml_read.h"

namespace k2pg {
namespace gate {

using yb::Status;
using yb::Slice;

//--------------------------------------------------------------------------------------------------
// SELECT
//--------------------------------------------------------------------------------------------------

class PgSelect : public PgDmlRead {
 public:
  // Constructors.
  PgSelect(std::shared_ptr<PgSession> pg_session, const PgObjectId& table_id,
           const PgObjectId& index_id, const PgPrepareParameters *prepare_params);

  virtual ~PgSelect();

  // Prepare query before execution.
  virtual CHECKED_STATUS Prepare();

  // Prepare secondary index if that index is used by this query.
  CHECKED_STATUS PrepareSecondaryIndex();
};

//--------------------------------------------------------------------------------------------------
// SELECT FROM Secondary Index Table
//--------------------------------------------------------------------------------------------------

class PgSelectIndex : public PgDmlRead {
 public:
  typedef std::shared_ptr<PgSelectIndex> SharedPtr;

  // Constructors.
  PgSelectIndex(std::shared_ptr<PgSession> pg_session,
                const PgObjectId& table_id,
                const PgObjectId& index_id,
                const PgPrepareParameters *prepare_params);
  virtual ~PgSelectIndex();

  // Prepare query for secondary index. This function is called when Postgres layer is accessing
  // the IndexTable directy (IndexOnlyScan).
  CHECKED_STATUS Prepare();

  // Prepare NESTED query for secondary index. This function is called when Postgres layer is
  // accessing the IndexTable via an outer select (Sequential or primary scans)
  CHECKED_STATUS PrepareSubquery(std::shared_ptr<SqlOpReadRequest> read_req);

  CHECKED_STATUS PrepareQuery(std::shared_ptr<SqlOpReadRequest> read_req);

  // The output parameter "ybidxbasectid" of index rows
  Result<bool> FetchBaseRowIdBatch(std::vector<std::string>& baseRowIds);

  void set_is_executed(bool value) {
    is_executed_ = value;
  }

  bool is_executed() {
    return is_executed_;
  }

 private:

   // check if we have local cached BaseRowId result.
  Result<bool> HasBaseRowIdBatch();

  // This secondary query should be executed just one time.
  bool is_executed_ = false;
};

}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_DML_SELECT_H

