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

#include "yb/pggate/pg_memctx.h"

namespace k2pg {
namespace gate {

PgMemctx::PgMemctx() {
}

PgMemctx::~PgMemctx() {
}

namespace {
  // Table of memory contexts.
  // - Although defined in K2Sql, this table is owned and managed by Postgres process.
  //   Other processes cannot control "PgMemctx" to avoid memory violations.
  // - Table "postgres_process_memctxs" is to help releasing the references to PgStatement when
  //   Postgres Process (a C program) is exiting.
  // - Transaction layer and Postgres BOTH hold references to PgStatement objects, and those
  //   PgGate objects wouldn't be destroyed unless both layers release their references or both
  //   layers are terminated.
  std::unordered_map<PgMemctx *, PgMemctx::SharedPtr> postgres_process_memctxs;
} // namespace

PgMemctx *PgMemctx::Create() {
  auto memctx = std::make_shared<PgMemctx>();
  postgres_process_memctxs[memctx.get()] = memctx;
  return memctx.get();
}

Status PgMemctx::Destroy(PgMemctx *handle) {
  if (handle) {
    SCHECK(postgres_process_memctxs.find(handle) != postgres_process_memctxs.end(),
           InternalError, "Invalid memory context handle");
    postgres_process_memctxs.erase(handle);
  }
  return Status::OK();
}

Status PgMemctx::Reset(PgMemctx *handle) {
  if (handle) {
    SCHECK(postgres_process_memctxs.find(handle) != postgres_process_memctxs.end(),
           InternalError, "Invalid memory context handle");
    handle->Clear();
  }
  return Status::OK();
}

void PgMemctx::Clear() {
  // The safest option is to retain all K2SQL statement objects.
  // - Clear the table descriptors from cache. We can just reload them when requested.
  // - Clear the "stmts_" for now. However, if this causes issue, keep "stmts_" vector around.
  //
  // PgGate and its contexts are between Postgres and K2SQL lower layers, and because these
  // layers might be still operating on the raw pointer or reference to "stmts_" after Postgres's
  // cancellation, there's a chance we might have an unexpected issue.
  tabledesc_map_.clear();
  stmts_.clear();
}

void PgMemctx::Cache(const std::shared_ptr<PgStatement> &stmt) {
  // Hold the stmt until the context is released.
  stmts_.push_back(stmt);
}

void PgMemctx::Cache(size_t hash_id, const std::shared_ptr<PgTableDesc> &table_desc) {
  // Add table descriptor to table.
  tabledesc_map_[hash_id] = table_desc;
}

void PgMemctx::GetCache(size_t hash_id, PgTableDesc **handle) {
  // Read table descriptor to table.
  const auto iter = tabledesc_map_.find(hash_id);
  if (iter == tabledesc_map_.end()) {
    *handle = nullptr;
  } else {
    *handle = iter->second.get();
  }
}

}  // namespace gate
}  // namespace k2pg
