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

#include "common/result.h"

namespace k2pg {

Result<int64_t> CheckedStoll(Slice slice);

template <class Int>
Result<Int> CheckedStoInt(Slice slice) {
  auto long_value = CheckedStoll(slice);
  RETURN_NOT_OK(long_value);
  auto result = static_cast<Int>(*long_value);
  if (result != *long_value) {
    return STATUS_FORMAT(InvalidArgument,
                         "result is out of range: [{}, {}]",
                         std::numeric_limits<Int>::min(),
                         std::numeric_limits<Int>::max());
  }
  return result;
}

inline Result<int32_t> CheckedStoi(Slice slice) {
  return CheckedStoInt<int32_t>(slice);
}

Result<long double> CheckedStold(Slice slice);

} // namespace k2pg
