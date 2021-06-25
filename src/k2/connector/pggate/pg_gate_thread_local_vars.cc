//--------------------------------------------------------------------------------------------------
// Copyright (c) YugaByte, Inc.
// Portions Copyright (c) 2021 Futurewei Cloud
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
//--------------------------------------------------------------------------------------------------


#include "pggate/pg_gate_thread_local_vars.h"

#include <setjmp.h>
#include <unordered_map>

#include "common/status.h"

namespace k2pg {
namespace gate {

/*
 * This code does not need to know anything about the value internals.
 * TODO we could use opaque types instead of void* for additional type safety.
 */
thread_local void *thread_local_memory_context_ = NULL;
thread_local void *pg_strtok_ptr = NULL;
thread_local void *jump_buffer = NULL;
thread_local const void *err_msg = NULL;

//-----------------------------------------------------------------------------
// Memory context.
//-----------------------------------------------------------------------------

void* PgSetThreadLocalCurrentMemoryContext(void *memctx) {
  void *old = thread_local_memory_context_;
  thread_local_memory_context_ = memctx;
  return old;
}

void* PgGetThreadLocalCurrentMemoryContext() {
  return thread_local_memory_context_;
}

void PgResetCurrentMemCtxThreadLocalVars() {
  pg_strtok_ptr = NULL;
  jump_buffer = NULL;
  err_msg = NULL;
}

//-----------------------------------------------------------------------------
// Error reporting.
//-----------------------------------------------------------------------------

void* PgSetThreadLocalJumpBuffer(void* new_buffer) {
    void *old_buffer = jump_buffer;
    jump_buffer = new_buffer;
    return old_buffer;
}

void* PgGetThreadLocalJumpBuffer() {
    return jump_buffer;
}

void PgSetThreadLocalErrMsg(const void* new_msg) {
    err_msg = new_msg;
}

const void* PgGetThreadLocalErrMsg() {
    return err_msg;
}

//-----------------------------------------------------------------------------
// Expression processing.
//-----------------------------------------------------------------------------

void* PgGetThreadLocalStrTokPtr() {
  return pg_strtok_ptr;
}

void PgSetThreadLocalStrTokPtr(char *new_pg_strtok_ptr) {
  pg_strtok_ptr = new_pg_strtok_ptr;
}

}  // namespace gate
}  // namespace k2pg
