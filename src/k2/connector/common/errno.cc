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
// Portions Copyright (c) 2021 Futurewei Cloud
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

#include "errno.h"

#include "status.h"

namespace k2pg {

void ErrnoToCString(int err, char *buf, size_t buf_len) {
  CHECK_GT(buf_len, 0);
#if !defined(__GLIBC__) || \
  ((_POSIX_C_SOURCE >= 200112 || _XOPEN_SOURCE >= 600) && !defined(_GNU_SOURCE))
  // Using POSIX version 'int strerror_r(...)'.
  int ret = strerror_r(err, buf, buf_len);
  if (ret && ret != ERANGE && ret != EINVAL) {
    strncpy(buf, "unknown error", buf_len);
    buf[buf_len - 1] = '\0';
  }
#else
  // Using GLIBC version
  char* ret = strerror_r(err, buf, buf_len);
  if (ret != buf) {
    strncpy(buf, ret, buf_len);
    buf[buf_len - 1] = '\0';
  }
#endif
}

static const std::string kErrnoCategoryName = "system error";

static StatusCategoryRegisterer errno_category_registerer(
    StatusCategoryDescription::Make<ErrnoTag>(&kErrnoCategoryName));

} // namespace k2pg
