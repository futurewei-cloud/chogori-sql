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
#include "pggate/pg_env.h"
#include "pggate/pg_statement.h"
#include "k2_log.h"

namespace k2pg {
namespace gate {

using k2pg::Status;
using k2pg::sql::ColumnSchema;
using k2pg::sql::SchemaBuilder;
using k2pg::sql::PgObjectId;
using k2pg::sql::PgOid;

class PgDdl : public PgStatement {
 public:
  explicit PgDdl(std::shared_ptr<PgSession> pg_session) : PgStatement(pg_session) {
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
  // Constructors.
  PgCreateDatabase(std::shared_ptr<PgSession> pg_session,
                   const std::string& database_name,
                   PgOid database_oid,
                   PgOid source_database_oid,
                   PgOid next_oid);
  virtual ~PgCreateDatabase();

  StmtOp stmt_op() const override { return StmtOp::STMT_CREATE_DATABASE; }

  // Execute.
  CHECKED_STATUS Exec();

 private:
  const std::string database_name_;
  const PgOid database_oid_;
  const PgOid source_database_oid_;
  const PgOid next_oid_;
};

class PgDropDatabase : public PgDdl {
 public:
  // Constructors.
  PgDropDatabase(std::shared_ptr<PgSession> pg_session, const std::string& database_name, PgOid database_oid);
  virtual ~PgDropDatabase();

  StmtOp stmt_op() const override { return StmtOp::STMT_DROP_DATABASE; }

  // Execute.
  CHECKED_STATUS Exec();

 private:
  const std::string database_name_;
  const PgOid database_oid_;
};

class PgAlterDatabase : public PgDdl {
 public:
  // Constructors.
  PgAlterDatabase(std::shared_ptr<PgSession> pg_session,
                  const std::string& database_name,
                  PgOid database_oid);
  virtual ~PgAlterDatabase();

  StmtOp stmt_op() const override { return StmtOp::STMT_ALTER_DATABASE; }

  CHECKED_STATUS RenameDatabase(const std::string& newname);

  // Execute.
  CHECKED_STATUS Exec();

 private:
  const std::string database_name_;
  const PgOid database_oid_;
  std::optional<std::string> rename_to_;
};

//--------------------------------------------------------------------------------------------------
// CREATE TABLE
//--------------------------------------------------------------------------------------------------

class PgCreateTable : public PgDdl {
 public:
  // Constructors.
  PgCreateTable(std::shared_ptr<PgSession> pg_session,
                const std::string& database_name,
                const std::string& schema_name,
                const std::string& table_name,
                const PgObjectId& table_object_id,
                bool is_shared_table,
                bool if_not_exist,
                bool add_primary_key);

  StmtOp stmt_op() const override { return StmtOp::STMT_CREATE_TABLE; }

  // For PgCreateIndex: the indexed (base) table id and if this is a unique index.
  virtual std::optional<const PgObjectId> indexed_table_object_id() const { return std::nullopt; }
  virtual bool is_unique_index() const { return false; }
  virtual const bool skip_index_backfill() const { return false; }

  CHECKED_STATUS AddColumn(const std::string& attr_name,
                           int attr_num,
                           int attr_ybtype,
                           bool is_hash,
                           bool is_range,
                           ColumnSchema::SortingType sorting_type =
                              ColumnSchema::SortingType::kNotSpecified) {
    return AddColumnImpl(attr_name, attr_num, attr_ybtype, is_hash, is_range, sorting_type);
  }

  CHECKED_STATUS AddColumn(const std::string& attr_name,
                           int attr_num,
                           const YBCPgTypeEntity *attr_type,
                           bool is_hash,
                           bool is_range,
                           ColumnSchema::SortingType sorting_type =
                               ColumnSchema::SortingType::kNotSpecified) {
    return AddColumnImpl(attr_name, attr_num, attr_type->k2pg_type, is_hash, is_range, sorting_type);
  }

  // Execute.
  virtual CHECKED_STATUS Exec();

 protected:
  virtual CHECKED_STATUS AddColumnImpl(const std::string& attr_name,
                                       int attr_num,
                                       int attr_ybtype,
                                       bool is_hash,
                                       bool is_range,
                                       ColumnSchema::SortingType sorting_type =
                                           ColumnSchema::SortingType::kNotSpecified);

  virtual size_t PrimaryKeyRangeColumnCount() const;

 protected:
  const std::string database_id_;
  const std::string database_name_;
  const std::string table_name_;
  const PgObjectId table_object_id_;
  bool is_pg_catalog_table_;
  bool is_shared_table_;
  bool if_not_exist_;
  // TODO: add hash schema
  std::vector<std::string> range_columns_;
  SchemaBuilder schema_builder_;
};

class PgDropTable: public PgDdl {
 public:
  // Constructors.
  PgDropTable(std::shared_ptr<PgSession> pg_session, const PgObjectId& table_object_id, bool if_exist);
  virtual ~PgDropTable();

  StmtOp stmt_op() const override { return StmtOp::STMT_DROP_TABLE; }

  // Execute.
  CHECKED_STATUS Exec();

 protected:
  const PgObjectId table_object_id_;
  bool if_exist_;
};

//--------------------------------------------------------------------------------------------------
// ALTER TABLE
//--------------------------------------------------------------------------------------------------

class PgAlterTable : public PgDdl {
 public:
  // Constructors.
  PgAlterTable(std::shared_ptr<PgSession> pg_session,
               const PgObjectId& table_object_id);

  CHECKED_STATUS AddColumn(const std::string& name,
                           const YBCPgTypeEntity *attr_type,
                           int order,
                           bool is_not_null);

  CHECKED_STATUS RenameColumn(const std::string& old_name, const std::string& new_name);

  CHECKED_STATUS DropColumn(const std::string& name);

  CHECKED_STATUS RenameTable(const std::string& db_name, const std::string& new_name);

  CHECKED_STATUS Exec();

  virtual ~PgAlterTable();

  StmtOp stmt_op() const override { return StmtOp::STMT_ALTER_TABLE; }

  private:
  const PgObjectId table_object_id_;
};

class PgCreateIndex : public PgCreateTable {
 public:
  // Constructors.
  PgCreateIndex(std::shared_ptr<PgSession> pg_session,
                const std::string& database_name,
                const std::string& schema_name,
                const std::string& index_name,
                const PgObjectId& index_object_id,
                const PgObjectId& base_table_object_id,
                bool is_shared_index,
                bool is_unique_index,
                const bool skip_index_backfill,
                bool if_not_exist);

  StmtOp stmt_op() const override { return StmtOp::STMT_CREATE_INDEX; }

  std::optional<const PgObjectId> indexed_table_object_id() const override {
    return base_table_object_id_;
  }

  bool is_unique_index() const override {
    return is_unique_index_;
  }

  const bool skip_index_backfill() const override {
    return skip_index_backfill_;
  }

  // Execute.
  CHECKED_STATUS Exec() override;

 protected:
  CHECKED_STATUS AddColumnImpl(const std::string& attr_name,
                               int attr_num,
                               int attr_ybtype,
                               bool is_hash,
                               bool is_range,
                               ColumnSchema::SortingType sorting_type) override;

 private:
  size_t PrimaryKeyRangeColumnCount() const override;

  CHECKED_STATUS AddYBbasectidColumn();

  const PgObjectId base_table_object_id_;
  bool is_unique_index_ = false;
  bool skip_index_backfill_ = false;
  bool ybbasectid_added_ = false;
  size_t primary_key_range_column_count_ = 0;
};

class PgDropIndex : public PgDropTable {
 public:
  // Constructors.
  PgDropIndex(std::shared_ptr<PgSession> pg_session, const PgObjectId& index_object_id, bool if_exist);
  virtual ~PgDropIndex();

  StmtOp stmt_op() const override { return StmtOp::STMT_DROP_INDEX; }

  // Execute.
  CHECKED_STATUS Exec();
};

}  // namespace gate
}  // namespace k2pg
