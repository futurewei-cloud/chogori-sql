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

#include <string>
#include <vector>

#include "entities/entity_ids.h"
#include "entities/schema.h"
#include "entities/type.h"
#include "entities/value.h"
#include "entities/expr.h"
#include "pggate/pg_gate_typedefs.h"
#include "pggate/pg_env.h"
#include "pggate/pg_tuple.h"
#include "pggate/pg_column.h"
#include "pggate/pg_statement.h"
#include "pggate/pg_op.h"
#include "k2_log.h"

namespace k2pg {
namespace gate {

using k2pg::Status;
using k2pg::sql::PgExpr;
using k2pg::sql::PgObjectId;
using k2pg::sql::PgOid;

class PgSelectIndex;

class PgDml : public PgStatement {
 public:

  virtual ~PgDml();

  // Append a target in SELECT or RETURNING.
  CHECKED_STATUS AppendTarget(PgExpr* target);

  // Prepare column for both ends.
  // - Prepare request to communicate with storage.
  // - Prepare PgExpr to send data back to Postgres layer.
  CHECKED_STATUS PrepareColumnForRead(int attr_num, std::shared_ptr<BindVariable> target_var);

  CHECKED_STATUS PrepareColumnForWrite(PgColumn *pg_col, std::shared_ptr<BindVariable> assign_var);

  // Bind a column with an expression.
  // - For a secondary-index-scan, this bind specify the value of the secondary key which is used to
  //   query a row.
  // - For a primary-index-scan, this bind specify the value of the keys of the table.
  virtual CHECKED_STATUS BindColumn(int attnum, PgExpr* attr_value);

  // Bind the whole table.
  CHECKED_STATUS BindTable();

  // Assign an expression to a column.
  CHECKED_STATUS AssignColumn(int attnum,  PgExpr* attr_value);

  // This function is not yet working and might not be needed.
  virtual CHECKED_STATUS ClearBinds();

  // Process the secondary index request if it is nested within this statement.
  Result<bool> ProcessSecondaryIndexRequest(const PgExecParameters *exec_params);

  // Fetch a row and return it to Postgres layer.
  CHECKED_STATUS Fetch(int32_t natts,
                       uint64_t *values,
                       bool *isnulls,
                       PgSysColumns *syscols,
                       bool *has_data);

  // Returns TRUE if K2 SKV replies with more data.
  Result<bool> FetchDataFromServer();

  // Returns TRUE if desired row is found.
  Result<bool> GetNextRow(PgTuple *pg_tuple);

  // Build tuple id (k2pgctid) of the given Postgres tuple.
  Result<std::string> BuildPgTupleId(const PgAttrValueDescriptor *attrs, int32_t nattrs);

  virtual void SetCatalogCacheVersion(uint64_t catalog_cache_version) = 0;

  bool has_aggregate_targets();

  // Note: Seems has_sql_op means this DML is normal SQL dml statement (against user database objects)
  // otherwise, it is bootstrap dml statement(used e.g. for creating system catalogs during InitDB)
  bool has_sql_op() {
    return sql_op_ != nullptr;
  }

  protected:
  // Method members.
  // Constructor.
  PgDml(std::shared_ptr<PgSession> pg_session, const PgObjectId& table_object_id);
  PgDml(std::shared_ptr<PgSession> pg_session,
        const PgObjectId& table_object_id,
        const PgObjectId& index_object_id,
        const PgPrepareParameters *prepare_params);

  // Get target vector
  virtual std::vector<PgExpr *>& GetTargets() = 0;

  // Allocate doc expression for expression whose value is bounded to a column.
  virtual std::shared_ptr<BindVariable> AllocColumnBindVar(PgColumn *col) = 0;

  // Allocate doc expression for expression whose value is assigned to a column (SET clause).
  virtual std::shared_ptr<BindVariable> AllocColumnAssignVar(PgColumn *col) = 0;

  // Specify target of the query in request.
  CHECKED_STATUS AppendTargetVar(PgExpr *target);

  // Update bind values.
  CHECKED_STATUS UpdateBindVars();

  // Update set values.
  CHECKED_STATUS UpdateAssignVars();

  // set up binding variable based on PgExpr
  CHECKED_STATUS PrepareExpression(PgExpr *target, std::shared_ptr<BindVariable> expr_var);

  // -----------------------------------------------------------------------------------------------
  // Data members that define the DML statement.

  // Table identifiers
  // - table_object_id_ identifies the table to read data from.
  // - index_object_id_ identifies the index to be used for scanning.
  //
  // Example for query on table_object_id_ using index_object_id_.
  //   SELECT FROM "table_object_id_"
  //     WHERE k2pgctid IN (SELECT base_k2pgctid FROM "index_object_id_" WHERE matched-index-binds)
  //
  // - Postgres will create PgSelect(table_object_id_) { nested PgSelectIndex (index_object_id_) }
  // - When bind functions are called, it bind user-values to columns in PgSelectIndex as these
  //   binds will be used to find base_k2pgctid from the IndexTable.
  // - When AddTargets() is called, the target is added to PgSelect as data will be reading from
  //   table_object_id_ using the found base_k2pgctid from index_object_id_.
  PgObjectId table_object_id_;
  PgObjectId index_object_id_;

  // Targets of statements (Output parameter).
  // - "target_desc_" is the table descriptor where data will be read from.
  // - "targets_" are either selected or returned expressions by DML statements.
  std::shared_ptr<PgTableDesc> target_desc_;

  // reverted this back to still use raw pointer since the all PgExprs have already been
  // stored in its parent class PgStatement as follows
  //
  // std::list<PgExpr::SharedPtr> exprs_;
  //
  // they would be released once the statement is finished.
  // As a result, we don't need double effort here
  std::vector<PgExpr *> targets_;
  // helper map over the above vector, maps targets by their attribute names.
  std::unordered_map<string, PgExpr*> targets_by_name_;

  // bind_desc_ is the descriptor of the table whose key columns' values will be specified by the
  // the DML statement being executed.
  // - For primary key binding, "bind_desc_" is the descriptor of the main table as we don't have
  //   a separated primary-index table.
  // - For secondary key binding, "bind_desc_" is the descriptor of the secondary index table.
  //   The bound values will be used to read base_k2pgctid which is then used to read actual data
  //   from the main table.
  std::shared_ptr<PgTableDesc> bind_desc_;

  // Prepare control parameters.
  PgPrepareParameters prepare_params_ = { kInvalidOid /* index_oid */,
                                          false /* index_only_scan */,
                                          false /* use_secondary_index */,
                                          false /* querying_colocated_table */ };

  // -----------------------------------------------------------------------------------------------
  // Data members for generated request.
  // NOTE:
  // - Where clause processing data is not supported yet.
  // - Some request structure are also set up in PgColumn class.

  // Column associated values (expressions) to be used by DML statements.
  // - When expression are constructed, we bind them with their associated request variables.
  // - These expressions might not yet have values for place_holders or literals.
  // - During execution, the place_holder values are updated, and the statement request variable need to
  //   be updated accordingly.
  //
  // * Bind values are used to identify the selected rows to be operated on.
  // * Set values are used to hold columns' new values in the selected rows.
  bool k2pgctid_bind_ = false;
  std::unordered_map<std::shared_ptr<BindVariable>, PgExpr*> expr_binds_;
  std::unordered_map<std::shared_ptr<BindVariable>, PgExpr*> expr_assigns_;
  std::optional<std::shared_ptr<BindVariable>> row_id_bind_ = std::nullopt;

  // Used for colocated TRUNCATE that doesn't bind any columns.
  // We don't support it for now and just keep it here as a place holder
  bool bind_table_ = false;

  //------------------------------------------------------------------------------------------------
  // Data members for navigating the output / result-set from either selected or returned targets.
  std::list<PgOpResult> rowsets_;
  int64_t current_row_order_ = 0;

  // DML Operator.
  PgOp::SharedPtr sql_op_;

  // -----------------------------------------------------------------------------------------------
  // Data members for nested query: This is used for an optimization in PgGate.
  //
  // - Each DML operation can be understood as
  //     Read / Write TABLE WHERE k2pgctid IN (SELECT k2pgctid from INDEX).
  // - In most cases, the Postgres layer processes the subquery "SELECT k2pgctid from INDEX".
  // - Under certain conditions, to optimize the performance, the PgGate layer might operate on
  //   the INDEX subquery itself.
  std::shared_ptr<PgSelectIndex> secondary_index_query_;
};

}  // namespace gate
}  // namespace k2pg
