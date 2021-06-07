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
// Copyright(c) 2020 Futurewei Cloud
//
// Permission is hereby granted,
//        free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

#pragma once

#include <memory>
#include <string>
#include <list>

#include "entities/expr.h"
#include "common/status.h"
#include "pggate/pg_session.h"

namespace k2pg {
namespace gate {

using std::string;
using k2pg::sql::PgExpr;
using k2pg::Status;

// Statement types.
enum StmtOp {
  STMT_NOOP = 0,
  STMT_CREATE_DATABASE,
  STMT_DROP_DATABASE,
  STMT_CREATE_SCHEMA,
  STMT_DROP_SCHEMA,
  STMT_CREATE_TABLE,
  STMT_DROP_TABLE,
  STMT_TRUNCATE_TABLE,
  STMT_CREATE_INDEX,
  STMT_DROP_INDEX,
  STMT_ALTER_TABLE,
  STMT_INSERT,
  STMT_UPDATE,
  STMT_DELETE,
  STMT_TRUNCATE,
  STMT_SELECT,
  STMT_ALTER_DATABASE,
};

class PgStatement {
 public:

  //------------------------------------------------------------------------------------------------
  // Constructors.
  // pg_session is the session that this statement belongs to. If PostgreSQL cancels the session
  // while statement is running, pg_session::sharedptr can still be accessed without crashing.
  explicit PgStatement(std::shared_ptr<PgSession> pg_session);
  virtual ~PgStatement();

  const std::shared_ptr<PgSession>& pg_session() {
    return pg_session_;
  }

  // Statement type.
  virtual StmtOp stmt_op() const = 0;

  //------------------------------------------------------------------------------------------------
  static bool IsValidStmt(PgStatement* stmt, StmtOp op) {
    return (stmt != nullptr && stmt->stmt_op() == op);
  }

  //------------------------------------------------------------------------------------------------
  // Add expressions that are belong to this statement.
  void AddExpr(std::unique_ptr<PgExpr> expr);

  //------------------------------------------------------------------------------------------------
  // Clear all values and expressions that were bound to the given statement.
  virtual CHECKED_STATUS ClearBinds() = 0;

  int64_t stmt_id() const {
    return stmt_id_;
  }

 protected:
  // PgSession that this statement belongs to.
  std::shared_ptr<PgSession> pg_session_;

  // Execution status.
  Status status_;
  string errmsg_;

  // Expression list to be destroyed as soon as the statement is removed from the API.
  std::list<std::unique_ptr<PgExpr>> exprs_;

  string client_id_;

  int64_t stmt_id_;
};

}  // namespace gate
}  // namespace k2pg
