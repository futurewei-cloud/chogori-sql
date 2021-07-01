// Copyright (c) YugaByte, Inc.
// Portions Copyright (c) 2021 Futurewei Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// Unless required by applicable law or agreed to in writing, software distributed under the License
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

// Utilities for implementations of C wrappers around K2PG C++ code. This file is not intended
// to be included
#pragma once

#ifndef __cplusplus
#error "This header can only be included in C++ code"
#endif

#include <cstddef>
#include <string>

#include "k2pg_util.h"
#include "status.h"

namespace k2pg {

// Convert our C++ status to K2PgStatus, which can be returned to PostgreSQL C code.
K2PgStatus ToK2PgStatus(const Status& status);
K2PgStatus ToK2PgStatus(Status&& status);
void FreeK2PgStatus(K2PgStatus status);

void K2PgSetPAllocFn(K2PgPAllocFn pg_palloc_fn);
void* K2PgPAlloc(size_t size);

void K2PgSetCStringToTextWithLenFn(K2PgCStringToTextWithLenFn fn);
void* K2PgCStringToTextWithLen(const char* c, int size);

// K2PgStatus definition for Some common Status.

K2PgStatus K2PgStatusNotSupport(const std::string& feature_name);

// Duplicate the given string in memory allocated using PostgreSQL's palloc.
const char* K2PgPAllocStdString(const std::string& s);

} // namespace k2pg
