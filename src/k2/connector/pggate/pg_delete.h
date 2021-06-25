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

#pragma once

#include "pggate/pg_dml_write.h"

namespace k2pg {
namespace gate {

class PgDelete : public PgDmlWrite {
 public:
  // Constructors.
  PgDelete(std::shared_ptr<PgSession> pg_session, const PgObjectId& table_object_id, bool is_single_row_txn)
      : PgDmlWrite(pg_session, table_object_id, is_single_row_txn) {}

  StmtOp stmt_op() const override { return StmtOp::STMT_DELETE; }

  private:
  std::unique_ptr<PgWriteOpTemplate> AllocWriteOperation() const override {
    return target_desc_->NewPgsqlDelete(client_id_, stmt_id_);
  }
};

}  // namespace gate
}  // namespace k2pg
