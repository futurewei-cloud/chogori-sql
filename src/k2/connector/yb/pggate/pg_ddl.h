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

#ifndef CHOGORI_GATE_DDL_H
#define CHOGORI_GATE_DDL_H

#include <string>
#include <vector>

#include "yb/entities/entity_ids.h"
#include "yb/entities/schema.h"
#include "yb/entities/type.h"
#include "yb/entities/value.h"
#include "yb/pggate/pg_env.h"
#include "yb/pggate/pg_statement.h"

namespace k2pg {
namespace gate {

using namespace yb;
using namespace k2pg::sql;

class PgDdl : public PgStatement {
 public:
  explicit PgDdl(PgSession::ScopedRefPtr pg_session) : PgStatement(pg_session) {
  }

  virtual CHECKED_STATUS ClearBinds() {
    return STATUS(InvalidArgument, "This statement cannot be bound to any values");
  }
};

//--------------------------------------------------------------------------------------------------
// CREATE DATABASE
//--------------------------------------------------------------------------------------------------

class PgCreateDatabase : public PgDdl {
 public:
  // Public types.
  typedef scoped_refptr<PgCreateDatabase> ScopedRefPtr;
  typedef scoped_refptr<const PgCreateDatabase> ScopedRefPtrConst;

  typedef std::unique_ptr<PgCreateDatabase> UniPtr;
  typedef std::unique_ptr<const PgCreateDatabase> UniPtrConst;

  // Constructors.
  PgCreateDatabase(PgSession::ScopedRefPtr pg_session,
                   const char *database_name,
                   PgOid database_oid,
                   PgOid source_database_oid,
                   PgOid next_oid);
  virtual ~PgCreateDatabase();

  StmtOp stmt_op() const override { return StmtOp::STMT_CREATE_DATABASE; }

  // Execute.
  CHECKED_STATUS Exec();

 private:
  const char *database_name_;
  const PgOid database_oid_;
  const PgOid source_database_oid_;
  const PgOid next_oid_;
};

class PgDropDatabase : public PgDdl {
 public:
  // Public types.
  typedef scoped_refptr<PgDropDatabase> ScopedRefPtr;
  typedef scoped_refptr<const PgDropDatabase> ScopedRefPtrConst;

  typedef std::unique_ptr<PgDropDatabase> UniPtr;
  typedef std::unique_ptr<const PgDropDatabase> UniPtrConst;

  // Constructors.
  PgDropDatabase(PgSession::ScopedRefPtr pg_session, const char *database_name, PgOid database_oid);
  virtual ~PgDropDatabase();

  StmtOp stmt_op() const override { return StmtOp::STMT_DROP_DATABASE; }

  // Execute.
  CHECKED_STATUS Exec();

 private:
  const char *database_name_;
  const PgOid database_oid_;
};

//--------------------------------------------------------------------------------------------------
// CREATE TABLE
//--------------------------------------------------------------------------------------------------

class PgCreateTable : public PgDdl {
 public:
  // Public types.
  typedef scoped_refptr<PgCreateTable> ScopedRefPtr;
  typedef scoped_refptr<const PgCreateTable> ScopedRefPtrConst;

  typedef std::unique_ptr<PgCreateTable> UniPtr;
  typedef std::unique_ptr<const PgCreateTable> UniPtrConst;

  // Constructors.
  PgCreateTable(PgSession::ScopedRefPtr pg_session,
                const char *database_name,
                const char *schema_name,
                const char *table_name,
                const PgObjectId& table_id,
                bool is_shared_table,
                bool if_not_exist,
                bool add_primary_key);

  StmtOp stmt_op() const override { return StmtOp::STMT_CREATE_TABLE; }

  // For K2CreateIndex: the indexed (base) table id and if this is a unique index.
  virtual std::optional<PgObjectId> indexed_table_id() const { return std::nullopt; }
  virtual bool is_unique_index() const { return false; }
  virtual const bool skip_index_backfill() const { return false; }

  CHECKED_STATUS AddColumn(const char *attr_name,
                           int attr_num,
                           int attr_ybtype,
                           bool is_hash,
                           bool is_range,
                           ColumnSchema::SortingType sorting_type =
                              ColumnSchema::SortingType::kNotSpecified) {
    return AddColumnImpl(attr_name, attr_num, attr_ybtype, is_hash, is_range, sorting_type);
  }

  CHECKED_STATUS AddColumn(const char *attr_name,
                           int attr_num,
                           const YBCPgTypeEntity *attr_type,
                           bool is_hash,
                           bool is_range,
                           ColumnSchema::SortingType sorting_type =
                               ColumnSchema::SortingType::kNotSpecified) {
    return AddColumnImpl(attr_name, attr_num, attr_type->yb_type, is_hash, is_range, sorting_type);
  }

  CHECKED_STATUS AddSplitRow(int num_cols, YBCPgTypeEntity **types, uint64_t *data);

  // Execute.
  virtual CHECKED_STATUS Exec();

 protected:
  virtual CHECKED_STATUS AddColumnImpl(const char *attr_name,
                                       int attr_num,
                                       int attr_ybtype,
                                       bool is_hash,
                                       bool is_range,
                                       ColumnSchema::SortingType sorting_type =
                                           ColumnSchema::SortingType::kNotSpecified);

  virtual size_t PrimaryKeyRangeColumnCount() const;

 private:
  Result<std::vector<std::string>> BuildSplitRows(const PgSchema& schema);

  NamespaceId namespace_id_;
  NamespaceName namespace_name_;
  TableName table_name_;
  const PgObjectId table_id_;
  bool is_pg_catalog_table_;
  bool is_shared_table_;
  bool if_not_exist_;
  // XXX: we don't support PG hash schema since we manage hash internally inside k2 storage
  //boost::optional<YBHashSchema> hash_schema_;
  std::vector<std::string> range_columns_;
  std::vector<std::vector<SqlValue>> split_rows_; // Split rows for range tables
  SchemaBuilder schema_builder_;
};

class PgDropTable: public PgDdl {
 public:
  // Public types.
  typedef scoped_refptr<PgDropTable> ScopedRefPtr;
  typedef scoped_refptr<const PgDropTable> ScopedRefPtrConst;

  typedef std::unique_ptr<PgDropTable> UniPtr;
  typedef std::unique_ptr<const PgDropTable> UniPtrConst;

  // Constructors.
  PgDropTable(PgSession::ScopedRefPtr pg_session, const PgObjectId& table_id, bool if_exist);
  virtual ~PgDropTable();

  StmtOp stmt_op() const override { return StmtOp::STMT_DROP_TABLE; }

  // Execute.
  CHECKED_STATUS Exec();

 protected:
  const PgObjectId table_id_;
  bool if_exist_;
};

}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_DDL_H