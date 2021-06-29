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

// This file contains thread-local equivalents for (some) PG global variables
// to allow (some components of) the PG/PSQL layer to run in a multi-threaded
// environment.
//
#pragma once

#include "pggate/pg_gate_typedefs.h"

namespace k2pg {
namespace gate {

//-----------------------------------------------------------------------------
// Memory context.
//-----------------------------------------------------------------------------

/* CurrentMemoryContext (from palloc.h/mcxt.c) */
void* PgGetThreadLocalCurrentMemoryContext();
void* PgSetThreadLocalCurrentMemoryContext(void *memctx);

/*
 * Reset all variables that would be allocated within the CurrentMemoryContext.
 * These should not be used anyway, but just keeping things tidy.
 * To be used when calling MemoryContextReset() for the CurrentMemoryContext.
 */
void PgResetCurrentMemCtxThreadLocalVars();

//-----------------------------------------------------------------------------
// Error reporting.
//-----------------------------------------------------------------------------

/*
 * Jump buffer used for error reporting (with sigjmp/longjmp) in multithread
 * context.
 * TODO Currently not yet the same as the standard PG/PSQL sigjmp_buf exception
 * stack because we use simplified error reporting when multi-threaded.
 */
void* PgSetThreadLocalJumpBuffer(void* new_buffer);
void* PgGetThreadLocalJumpBuffer();

/*
 * Save/get the error message. Needs a separate function because it will be
 * generated separately in errmsg when using ereport (instead of elog).
 */
void PgSetThreadLocalErrMsg(const void* new_msg);
const void* PgGetThreadLocalErrMsg();

//-----------------------------------------------------------------------------
// Expression processing.
//-----------------------------------------------------------------------------

/*
 * pg_strtok_ptr (from read.c)
 * TODO Technically this does not need to be global but refactoring the parsing
 * code to pass it as a parameter is tedious due to the notational overhead.
 */
void* PgGetThreadLocalStrTokPtr();
void PgSetThreadLocalStrTokPtr(char *new_pg_strtok_ptr);

} // namespace gate
} // namespace k2pg
