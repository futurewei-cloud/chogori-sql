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

#ifndef CHOGORI_GATE_MEMCTX_H
#define CHOGORI_GATE_MEMCTX_H

#include <memory>
#include <string>

#include "yb/common/status.h"
#include "yb/pggate/pg_tabledesc.h"
#include "yb/pggate/pg_statement.h"

namespace k2 {
namespace gate {
// This is the K2 counterpart of Postgres's MemoryContext.
// K2SQL memory context hold one reference count to PgGate objects such as PgStatement.
// When Postgres process complete execution, it would release the reference count by destroying
// the K2SQL memory context.
//
// - Each K2SQL Memctx will be associated with a Postgres MemoryContext.
// - K2SQL Memctx will be initialized to NULL and later created on its first use.
// - When Postgres MemoryContext is destroyed, K2SQL Memctx will be destroyed.
// - When Postgres MemoryContext allocates K2SQL object, that K2SQL object will belong to the
//   associated K2SQL Memctx. The object is automatically destroyed when K2SQL Memctx is destroyed.
class PgMemctx {
 public:
  typedef std::shared_ptr<PgMemctx> SharedPtr;

  // Constructor and destructor.
  PgMemctx();
  virtual ~PgMemctx();

  // API: Create(), Destroy(), and Reset()
  // - Because Postgres process own K2SQL memory context, only Postgres processes should call
  //   these functions to manage K2SQL memory context.
  // - When Postgres process (a C Program) is exiting, it assumes that all associated memories
  //   are destroyed and will not call Destroy() to free K2SQL memory context. As a result,
  //   PgGate must release the remain K2SQL memory contexts itself. Create(), Destroy(), and
  //   Reset() API uses a global variable for that purpose.  When Postgres processes exit, the
  //   global destructor will free all K2SQL memory contexts.

  // Create K2SQL memory context that will be owned by Postgres process.
  static PgMemctx *Create();

  // Destroy K2SQL memory context that is owned by Postgres process.
  static CHECKED_STATUS Destroy(PgMemctx *handle);

  // Clear the content of K2SQL memory context that is owned by Postgres process.
  // Postgres has Reset() option where it clears the allocated memory for the current context but
  // keeps all the allocated memory for the child contexts.
  static CHECKED_STATUS Reset(PgMemctx *handle);

  // Cache the statement in the memory context to be destroyed later on.
  void Cache(const PgStatement::ScopedRefPtr &stmt);

  // Cache the table descriptor in the memory context to be destroyed later on.
  void Cache(size_t hash_id, const PgTableDesc::ScopedRefPtr &table_desc);

  // Read the table descriptor from cache.
  void GetCache(size_t hash_id, PgTableDesc **handle);

 private:
  // NOTE:
  // - In Postgres, the objects in the outer context can references to the objects of the nested
  //   context but not vice versa, so it is safe to clear objects of outer context.
  // - In K2SQL, the above abstraction must be followed, but I am not yet sure that we did.
  //   For now we destroy the K2SQL objects in the current context also as we should. However,
  //   if the objects in nested context might still have referenced to the objects of the outer
  //   memctx, we can delay the PgStatement objects' destruction.
  void Clear();

  // All statements that are allocated with this memory context.
  std::vector<PgStatement::ScopedRefPtr> stmts_;

  // All table descriptors that are allocated with this memory context.
  std::unordered_map<size_t, PgTableDesc::ScopedRefPtr> tabledesc_map_;
};

}  // namespace gate
}  // namespace k2

#endif //CHOGORI_GATE_MEMCTX_H