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

#include "k2pg-internal.h"

namespace k2pg {

namespace {
K2PgPAllocFn g_palloc_fn = nullptr;
K2PgCStringToTextWithLenFn g_cstring_to_text_with_len_fn = nullptr;
}  // anonymous namespace

void K2PgSetPAllocFn(K2PgPAllocFn palloc_fn) {
  CHECK_NOTNULL(palloc_fn);
  g_palloc_fn = palloc_fn;
}

void* K2PgPAlloc(size_t size) {
  CHECK_NOTNULL(g_palloc_fn);
  return g_palloc_fn(size);
}

void K2PgSetCStringToTextWithLenFn(K2PgCStringToTextWithLenFn fn) {
  CHECK_NOTNULL(fn);
  g_cstring_to_text_with_len_fn = fn;
}

void* K2PgCStringToTextWithLen(const char* c, int size) {
  CHECK_NOTNULL(g_cstring_to_text_with_len_fn);
  return g_cstring_to_text_with_len_fn(c, size);
}

K2PgStatus ToK2PgStatus(const Status& status) {
  return status.RetainStruct();
}

K2PgStatus ToK2PgStatus(Status&& status) {
  return status.DetachStruct();
}

void FreeK2PgStatus(K2PgStatus status) {
  // Create Status object that receives control over provided status, so it will be destoyed with
  // k2pg_status.
  Status k2pg_status(status, false);
}

K2PgStatus K2PgStatusNotSupport(const std::string& feature_name) {
  if (feature_name.empty()) {
    return ToK2PgStatus(STATUS(NotSupported, "Feature is not supported"));
  } else {
    return ToK2PgStatus(STATUS_FORMAT(NotSupported, "Feature '{}' not supported", feature_name));
  }
}

const char* K2PgPAllocStdString(const std::string& s) {
  const size_t len = s.size();
  char* result = reinterpret_cast<char*>(K2PgPAlloc(len + 1));
  memcpy(result, s.c_str(), len);
  result[len] = 0;
  return result;
}

} // namespace k2pg
