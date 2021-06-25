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
//
#pragma once

#include "status.h"
#include "k2pg_errcodes.h"

namespace k2pg {

struct PgsqlErrorTag : IntegralErrorTag<K2PgErrorCode> {
  // It is part of the wire protocol and should not be changed once released.
  static constexpr uint8_t kCategory = 6;

  static std::string ToMessage(Value value) {
    return ToString(value);
  }

  static std::string DecodeToString(const uint8_t* source) {
    return ToMessage(Decode(source));
  }

};

typedef StatusErrorCodeImpl<PgsqlErrorTag> PgsqlError;

} // namespace k2pg
