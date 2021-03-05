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

#include <mutex>
#include <string>

#include "common/strings/escaping.h"
#include "common/cast.h"
#include "common/oid_generator.h"
#include "common/concurrent/thread.h"

namespace yb {

string ObjectIdGenerator::Next(const bool binary_id) {

  // Use the thread id to select a random oid generator.
  const int idx = yb::Thread::UniqueThreadId() % kNumOidGenerators;
  std::unique_lock<LockType> lck(oid_lock_[idx]);
  boost::uuids::uuid oid = oid_generator_[idx]();
  lck.unlock();

  return binary_id ? string(util::to_char_ptr(oid.data), sizeof(oid.data))
                   : b2a_hex(util::to_char_ptr(oid.data), sizeof(oid.data));
}

} // namespace yb
