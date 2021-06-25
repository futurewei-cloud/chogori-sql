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
#pragma once

#include <string>

#include "status.h"

namespace k2pg {

void ErrnoToCString(int err, char *buf, size_t buf_len);

// Return a string representing an errno.
inline std::string ErrnoToString(int err) {
  char buf[512];
  ErrnoToCString(err, buf, sizeof(buf));
  return std::string(buf);
}

struct ErrnoTag : IntegralErrorTag<int32_t> {
  // This category id is part of the wire protocol and should not be changed once released.
  static constexpr uint8_t kCategory = 1;

  static std::string ToMessage(Value value) {
    return ErrnoToString(value);
  }
};

typedef StatusErrorCodeImpl<ErrnoTag> Errno;

} // namespace k2pg
