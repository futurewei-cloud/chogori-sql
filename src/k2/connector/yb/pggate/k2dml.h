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

#ifndef CHOGORI_GATE_DML_H
#define CHOGORI_GATE_DML_H

#include <string>
#include <vector>

#include "yb/entities/entity_ids.h"
#include "yb/entities/schema.h"
#include "yb/entities/type.h"
#include "yb/entities/value.h"
#include "yb/entities/expr.h"
#include "yb/pggate/ybc_pg_typedefs.h"
#include "yb/pggate/pg_env.h"
#include "yb/pggate/pg_tuple.h"
#include "yb/pggate/k2column.h"
#include "yb/pggate/k2statement.h"
#include "yb/pggate/k2docop.h"

namespace k2 {
namespace gate {

using namespace yb;
using namespace k2::sql;

class K2Dml : public K2Statement {
 public:

  virtual ~K2Dml();

  // Append a target in SELECT or RETURNING.
  CHECKED_STATUS AppendTarget(PgExpr *target);

  // Prepare column for both ends.
  // - Prepare protobuf to communicate with DocDB.
  // - Prepare PgExpr to send data back to Postgres layer.
  CHECKED_STATUS PrepareColumnForRead(int attr_num, PgExpr *target_pb, const K2Column **col);
  CHECKED_STATUS PrepareColumnForWrite(K2Column *pg_col, PgExpr *assign_pb);

  // Bind a column with an expression.
  // - For a secondary-index-scan, this bind specify the value of the secondary key which is used to
  //   query a row.
  // - For a primary-index-scan, this bind specify the value of the keys of the table.
  virtual CHECKED_STATUS BindColumn(int attnum, PgExpr *attr_value);

  // Bind the whole table.
  CHECKED_STATUS BindTable();

  // Assign an expression to a column.
  CHECKED_STATUS AssignColumn(int attnum,  PgExpr *attr_value);

  // This function is not yet working and might not be needed.
  virtual CHECKED_STATUS ClearBinds();

  // Fetch a row and return it to Postgres layer.
  CHECKED_STATUS Fetch(int32_t natts,
                       uint64_t *values,
                       bool *isnulls,
                       PgSysColumns *syscols,
                       bool *has_data);

  // Returns TRUE if K2 doc api replies with more data.
  Result<bool> FetchDataFromServer();

  // Returns TRUE if desired row is found.
  Result<bool> GetNextRow(PgTuple *pg_tuple);

  // Build tuple id (ybctid) of the given Postgres tuple.
  Result<std::string> BuildYBTupleId(const PgAttrValueDescriptor *attrs, int32_t nattrs);

  virtual void SetCatalogCacheVersion(uint64_t catalog_cache_version) = 0;

  bool has_aggregate_targets();

  protected:
  // Method members.
  // Constructor.
  K2Dml(K2Session::ScopedRefPtr k2_session, const PgObjectId& table_id);
  K2Dml(K2Session::ScopedRefPtr k2_session,
        const PgObjectId& table_id,
        const PgObjectId& index_id,
        const PgPrepareParameters *prepare_params);

  // Allocate protobuf for a SELECTed expression.
  virtual PgExpr *AllocTargetPB() = 0;

  // Allocate protobuf for expression whose value is bounded to a column.
  virtual PgExpr *AllocColumnBindPB(K2Column *col) = 0;

  // Allocate protobuf for expression whose value is assigned to a column (SET clause).
  virtual PgExpr *AllocColumnAssignPB(K2Column *col) = 0;

  // Specify target of the query in protobuf request.
  CHECKED_STATUS AppendTargetPB(PgExpr *target);

  // Update bind values.
  CHECKED_STATUS UpdateBindPBs();

  // Update set values.
  CHECKED_STATUS UpdateAssignPBs();

  // Indicate in the protobuf what columns must be read before the statement is processed.
  void ColumnRefsToPB(PgColumnRef *column_refs);

  CHECKED_STATUS PrepareForRead(PgExpr *target, PgExpr *expr_pb);

  CHECKED_STATUS Eval(PgExpr *target, PgExpr *expr_pb);

  // -----------------------------------------------------------------------------------------------
  // Data members that define the DML statement.

  // Table identifiers
  // - table_id_ identifies the table to read data from.
  // - index_id_ identifies the index to be used for scanning.
  //
  // Example for query on table_id_ using index_id_.
  //   SELECT FROM "table_id_"
  //     WHERE ybctid IN (SELECT base_ybctid FROM "index_id_" WHERE matched-index-binds)
  //
  // - Postgres will create PgSelect(table_id_) { nested PgSelectIndex (index_id_) }
  // - When bind functions are called, it bind user-values to columns in PgSelectIndex as these
  //   binds will be used to find base_ybctid from the IndexTable.
  // - When AddTargets() is called, the target is added to PgSlect as data will be reading from
  //   table_id_ using the found base_ybctid from index_id_.
  PgObjectId table_id_;
  PgObjectId index_id_;

  // Targets of statements (Output parameter).
  // - "target_desc_" is the table descriptor where data will be read from.
  // - "targets_" are either selected or returned expressions by DML statements.
  K2TableDesc::ScopedRefPtr target_desc_;
  std::vector<PgExpr*> targets_;

  // bind_desc_ is the descriptor of the table whose key columns' values will be specified by the
  // the DML statement being executed.
  // - For primary key binding, "bind_desc_" is the descriptor of the main table as we don't have
  //   a separated primary-index table.
  // - For secondary key binding, "bind_desc_" is the descriptor of teh secondary index table.
  //   The bound values will be used to read base_ybctid which is then used to read actual data
  //   from the main table.
  K2TableDesc::ScopedRefPtr bind_desc_;

  // Prepare control parameters.
  PgPrepareParameters prepare_params_ = { kInvalidOid /* index_oid */,
                                          false /* index_only_scan */,
                                          false /* use_secondary_index */,
                                          false /* querying_colocated_table */ };

  // -----------------------------------------------------------------------------------------------
  // Data members for generated protobuf.
  // NOTE:
  // - Where clause processing data is not supported yet.
  // - Some protobuf structure are also set up in PgColumn class.

  // Column associated values (expressions) to be used by DML statements.
  // - When expression are constructed, we bind them with their associated protobuf.
  // - These expressions might not yet have values for place_holders or literals.
  // - During execution, the place_holder values are updated, and the statement protobuf need to
  //   be updated accordingly.
  //
  // * Bind values are used to identify the selected rows to be operated on.
  // * Set values are used to hold columns' new values in the selected rows.
  bool ybctid_bind_ = false;
  std::unordered_map<PgExpr*, PgExpr*> expr_binds_;
  std::unordered_map<PgExpr*, PgExpr*> expr_assigns_;

  // Used for colocated TRUNCATE that doesn't bind any columns.
  bool bind_table_ = false;

  //------------------------------------------------------------------------------------------------
  // Data members for navigating the output / result-set from either seleted or returned targets.
  std::list<K2DocResult> rowsets_;
  int64_t current_row_order_ = 0;

  //------------------------------------------------------------------------------------------------
  // Hashed and range values/components used to compute the tuple id.
  //
  // These members are populated by the AddYBTupleIdColumn function and the tuple id is retrieved
  // using the GetYBTupleId function.
  //
  // These members are not used internally by the statement and are simply a utility for computing
  // the tuple id (ybctid).
};

//--------------------------------------------------------------------------------------------------
// DML_READ
//--------------------------------------------------------------------------------------------------
// Scan Scenarios:
//
// 1. SequentialScan or PrimaryIndexScan (class PgSelect)
//    - YugaByte does not have a separate table for PrimaryIndex.
//    - The target table descriptor, where data is read and returned, is the main table.
//    - The binding table descriptor, whose column is bound to values, is also the main table.
//
// 2. IndexOnlyScan (Class PgSelectIndex)
//    - This special case is optimized where data is read from index table.
//    - The target table descriptor, where data is read and returned, is the index table.
//    - The binding table descriptor, whose column is bound to values, is also the index table.
//
// 3. IndexScan SysTable / UserTable (Class PgSelect and Nested PgSelectIndex)
//    - YugaByte will use the binds to query base-ybctid in the index table, which is then used
//      to query data from the main table.
//    - The target table descriptor, where data is read and returned, is the main table.
//    - The binding table descriptor, whose column is bound to values, is the index table.

class K2DmlRead : public K2Dml {
 public:
  // Public types.
  typedef scoped_refptr<K2DmlRead> ScopedRefPtr;
  typedef std::shared_ptr<K2DmlRead> SharedPtr;

  // Constructors.
  K2DmlRead(K2Session::ScopedRefPtr k2_session, const PgObjectId& table_id,
           const PgObjectId& index_id, const PgPrepareParameters *prepare_params);
  virtual ~K2DmlRead();

  StmtOp stmt_op() const override { return StmtOp::STMT_SELECT; }

  virtual CHECKED_STATUS Prepare() = 0;

  // Allocate binds.
  virtual void PrepareBinds();

  // Set forward (or backward) scan.
  void SetForwardScan(const bool is_forward_scan);

  // Bind a column with an EQUALS condition.
  CHECKED_STATUS BindColumnCondEq(int attnum, PgExpr *attr_value);

  // Bind a range column with a BETWEEN condition.
  CHECKED_STATUS BindColumnCondBetween(int attr_num, PgExpr *attr_value, PgExpr *attr_value_end);

  // Bind a column with an IN condition.
  CHECKED_STATUS BindColumnCondIn(int attnum, int n_attr_values, PgExpr **attr_values);

  // Execute.
  virtual CHECKED_STATUS Exec(const PgExecParameters *exec_params);

  protected:
  // Add column refs to protobuf read request.
  void SetColumnRefs();

  // Delete allocated target for columns that have no bind-values.
  CHECKED_STATUS DeleteEmptyPrimaryBinds();

  private:
  bool is_forward_scan_;
};

//--------------------------------------------------------------------------------------------------
// DML WRITE - Insert, Update, Delete.
//--------------------------------------------------------------------------------------------------

class K2DmlWrite : public K2Dml {
 public:
  // Abstract class without constructors.
  virtual ~K2DmlWrite();

  // Prepare write operations.
  virtual CHECKED_STATUS Prepare();

  // Setup internal structures for binding values during prepare.
  void PrepareColumns();

  // force_non_bufferable flag indicates this operation should not be buffered.
  CHECKED_STATUS Exec(bool force_non_bufferable = false);

  void SetIsSystemCatalogChange() {
      ysql_catalog_change_ = true;
  }

  void SetCatalogCacheVersion(const uint64_t catalog_cache_version) override {
    ysql_catalog_version_ = catalog_cache_version;
  }

  int32_t GetRowsAffectedCount() {
    return rows_affected_count_;
  }

 protected:
  // Constructor.
  K2DmlWrite(K2Session::ScopedRefPtr K2_session,
             const PgObjectId& table_id,
             bool is_single_row_txn = false);

  // Delete allocated target for columns that have no bind-values.
  CHECKED_STATUS DeleteEmptyPrimaryBinds();

  bool is_single_row_txn_ = false; // default.

  int32_t rows_affected_count_ = 0;

  bool ysql_catalog_change_ = false;

  uint64_t ysql_catalog_version_ = 0;
};

}  // namespace gate
}  // namespace k2

#endif //CHOGORI_GATE_DML_H