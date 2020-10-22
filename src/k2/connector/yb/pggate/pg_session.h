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

#ifndef CHOGORI_GATE_PG_SESSION_H
#define CHOGORI_GATE_PG_SESSION_H

#include <optional>
#include <unordered_set>

#include "yb/common/concurrent/ref_counted.h"
#include "yb/common/oid_generator.h"
#include "yb/common/sys/monotime.h"
#include "yb/entities/entity_ids.h"
#include "yb/entities/index.h"
#include "yb/entities/schema.h"

#include "yb/pggate/pg_env.h"
#include "yb/pggate/pg_op_api.h"
#include "yb/pggate/pg_gate_api.h"

namespace k2pg {
namespace gate {

using yb::RefCountedThreadSafe;
using namespace k2pg::sql;
using yb::Status;

// This class provides access to run operation's result by reading std::future<Status>
// and analyzing possible pending errors of k2 client object in GetStatus() method.
// If GetStatus() method will not be called, possible errors in k2 client object will be preserved.
class PgSessionAsyncRunResult {
 public:
  PgSessionAsyncRunResult() = default;
  PgSessionAsyncRunResult(std::future<Status> future_status);
  CHECKED_STATUS GetStatus();
  bool InProgress() const;

 private:
  std::future<Status> future_status_;
};

// This class is not thread-safe as it is mostly used by a single-threaded PostgreSQL backend
// process.
class PgSession : public RefCountedThreadSafe<PgSession> {
 public:
  // Public types.
  typedef scoped_refptr<PgSession> ScopedRefPtr;

  // Constructors.
  PgSession(const string& database_name,
            const YBCPgCallbacks& pg_callbacks);

  virtual ~PgSession();

  // Run (apply + flush) the given operation to read and write database content.
  // Template is used here to handle all kind of derived operations
  // (shared_ptr<PgReadOpTemplate>, shared_ptr<PgWriteOpTemplate>)
  // without implicitly conversion to shared_ptr<PgReadOpTemplate>.
  // Conversion to shared_ptr<PgOpTemplate> will be done later and result will re-used with move.
  template<class Op>
  Result<PgSessionAsyncRunResult> RunAsync(const std::shared_ptr<Op>& op,
                                           const PgObjectId& relation_id,
                                           uint64_t* read_time,
                                           bool force_non_bufferable) {
    return RunAsync(&op, 1, relation_id, read_time, force_non_bufferable);
  }

  // Run (apply + flush) list of given operations to read and write database content.
  template<class Op>
  Result<PgSessionAsyncRunResult> RunAsync(const std::vector<std::shared_ptr<Op>>& ops,
                                           const PgObjectId& relation_id,
                                           uint64_t* read_time,
                                           bool force_non_bufferable) {
    DCHECK(!ops.empty());
    return RunAsync(ops.data(), ops.size(), relation_id, read_time, force_non_bufferable);
  }

  // Run multiple operations.
  template<class Op>
  Result<PgSessionAsyncRunResult> RunAsync(const std::shared_ptr<Op>* op,
                                           size_t ops_count,
                                           const PgObjectId& relation_id,
                                           uint64_t* read_time,
                                           bool force_non_bufferable) {
    // TODO: implementation                                         
    PgSessionAsyncRunResult result;
    return result;
  }

  CHECKED_STATUS HandleResponse(const PgOpTemplate& op, const PgObjectId& relation_id);

  private:  
  // Connected database.
  std::string connected_database_;
  
  const YBCPgCallbacks& pg_callbacks_;
};

}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_PG_SESSION_H