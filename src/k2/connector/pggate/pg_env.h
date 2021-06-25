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

// TODO(neil) PgEnv defines the interface for the environment where PostgreSQL engine is running.
// Although postgres libraries might handle most of the environment variables, YugaByte libraries
// might need to deal with some of them. This class is provided for that reason.  This class can
// be removed if YugaByte layers, especially DocDB, do not handle any custom values for ENV.

#pragma once

#include <memory>
#include <string>

namespace k2pg {
namespace gate {

class PgEnv {
 public:
  // Public types and constants.
  typedef std::unique_ptr<PgEnv> UniPtr;
  typedef std::unique_ptr<const PgEnv> UniPtrConst;

  typedef std::shared_ptr<PgEnv> SharedPtr;
  typedef std::shared_ptr<const PgEnv> SharedPtrConst;

  // Constructor.
  PgEnv() { }
  virtual ~PgEnv() { }
};

}  // namespace gate
}  // namespace k2pg
