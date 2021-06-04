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

#include <thread>

#include <glog/logging.h>

#include "thread_restrictions.h"

#ifdef ENABLE_THREAD_RESTRICTIONS

namespace k2pg {

namespace {
  thread_local bool io_allowed = true;
  thread_local bool wait_allowed = true;
} // anonymous namespace

bool ThreadRestrictions::SetIOAllowed(bool allowed) {
  bool previous_allowed = io_allowed;
  io_allowed = allowed;
  return previous_allowed;
}

void ThreadRestrictions::AssertIOAllowed() {
  CHECK(io_allowed)
    << "Function marked as IO-only was called from a thread that "
    << "disallows IO!  If this thread really should be allowed to "
    << "make IO calls, adjust the call to "
    << "ThreadRestrictions::SetIOAllowed() in this thread's "
    << "startup. "
    << std::this_thread::get_id();
}

bool ThreadRestrictions::SetWaitAllowed(bool allowed) {
  bool previous_allowed = wait_allowed;
  wait_allowed = allowed;
  return previous_allowed;
}

bool ThreadRestrictions::IsWaitAllowed() {
  return wait_allowed;
}

void ThreadRestrictions::AssertWaitAllowed() {
  CHECK(wait_allowed)
    << "Waiting is not allowed to be used on this thread to prevent "
    << "server-wide latency aberrations and deadlocks. "
    << std::this_thread::get_id();
}

} // namespace k2pg

#endif
