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

#include "pgsql_error.h"

namespace k2pg {
namespace {

const std::string kPgsqlErrorCategoryName = "pgsql error";

StatusCategoryRegisterer pgsql_error_category_registerer(
    StatusCategoryDescription::Make<PgsqlErrorTag>(&kPgsqlErrorCategoryName));

} // namespace
} // namespace k2pg
