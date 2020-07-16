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
#ifndef YB_UTIL_INIT_H
#define YB_UTIL_INIT_H

#include "macros.h"
#include "status.h"

namespace yb {

extern const char* kTopLevelDataDirName;

// Return a NotSupported Status if the current CPU does not support the CPU flags
// required for YB.
CHECKED_STATUS CheckCPUFlags();

// Returns an IllegalState Status if we cannot create the dir structure for logging.
//CHECKED_STATUS SetupLogDir(const std::string& server_type);

// Initialize YB, checking that the platform we are running on is supported, etc.
// Issues a FATAL log message if we fail to init.
// argv0 is passed to InitGoogleLoggingSafe.
//CHECKED_STATUS InitYB(const std::string &server_type, const char* argv0);

} // namespace yb
#endif /* YB_UTIL_INIT_H */
