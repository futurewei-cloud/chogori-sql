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

#include "entities/entity_ids.h"
#include "entities/type.h"
#include "entities/expr.h"
#include "entities/schema.h"
#include "pggate/pg_op_api.h"
#include "k2_log.h"

namespace k2pg {
namespace gate {

using k2pg::sql::SQLType;
using k2pg::sql::ColumnSchema;
using k2pg::sql::PgSystemAttrNum;

// This class can be used to describe any reference of a column.
class ColumnDesc {
 public:
  typedef std::shared_ptr<ColumnDesc> SharedPtr;

  ColumnDesc() : sql_type_(SQLType::Create(DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA)) {
  }

  void Init(int index,
            int id,
            string name,
            bool is_hash,
            bool is_primary,
            int32_t attr_num,
            const std::shared_ptr<SQLType>& sql_type,
            ColumnSchema::SortingType sorting_type) {
    index_ = index,
    id_ = id;
    name_ = name;
    is_hash_ = is_hash;
    is_primary_ = is_primary;
    attr_num_ = attr_num;
    sql_type_ = sql_type;
    sorting_type_ = sorting_type;
  }

  bool IsInitialized() const {
    return (index_ >= 0);
  }

  int index() const {
    return index_;
  }

  int id() const {
    return id_;
  }

  const string& name() const {
    return name_;
  }

  bool is_hash() const {
    return is_hash_;
  }

  bool is_primary() const {
    return is_primary_;
  }

  int32_t attr_num() const {
    return attr_num_;
  }

  std::shared_ptr<SQLType> sql_type() const {
    return sql_type_;
  }

  ColumnSchema::SortingType sorting_type() const {
    return sorting_type_;
  }

 private:
  int index_ = -1;
  int id_ = -1;
  string name_;
  bool is_hash_ = false;
  bool is_primary_ = false;
  int32_t attr_num_ = -1;
  std::shared_ptr<SQLType> sql_type_;
  ColumnSchema::SortingType sorting_type_ = ColumnSchema::SortingType::kNotSpecified;
};

class PgColumn {
 public:
  // Constructor & Destructor.
  PgColumn() {
  }

  virtual ~PgColumn() {
  }

  // Initialize hidden columns.
  void Init(PgSystemAttrNum attr_num);

  // Bindings for write requests.
  std::shared_ptr<BindVariable> AllocKeyBind(std::shared_ptr<SqlOpWriteRequest> write_req);
  std::shared_ptr<BindVariable> AllocKeyBindForRowId(PgStatement *stmt, std::shared_ptr<SqlOpWriteRequest> write_req, std::string row_id);
  std::shared_ptr<BindVariable> AllocBind(std::shared_ptr<SqlOpWriteRequest> write_req);

  // Bindings for read requests.
  std::shared_ptr<BindVariable> AllocKeyBind(std::shared_ptr<SqlOpReadRequest> write_req);
  std::shared_ptr<BindVariable> AllocBind(std::shared_ptr<SqlOpReadRequest> read_req);

  // Assign values for write requests.
  std::shared_ptr<BindVariable> AllocAssign(std::shared_ptr<SqlOpWriteRequest> write_req);

  ColumnDesc *desc() {
    return &desc_;
  }

  const ColumnDesc *desc() const {
    return &desc_;
  }

  const string& attr_name() const {
    return desc_.name();
  }

  bool is_primary() const {
    return desc_.is_primary();
  }

  std::shared_ptr<BindVariable> bind_var() {
    return bind_var_;
  }

  std::shared_ptr<BindVariable> assign_var() {
    return assign_var_;
  }

  int32_t attr_num() const {
    return desc_.attr_num();
  }

  int index() const {
    return desc_.index();
  }

  int id() const {
    return desc_.id();
  }

  bool read_requested() const {
    return read_requested_;
  }

  void set_read_requested(const bool value) {
    read_requested_ = value;
  }

  bool write_requested() const {
    return write_requested_;
  }

  void set_write_requested(const bool value) {
    write_requested_ = value;
  }

  bool is_system_column() {
    return attr_num() < 0;
  }

  bool is_virtual_column();

  private:
  ColumnDesc desc_;

  // Input binds. For now these are just literal values of the columns.
  // - In storage API, for primary columns, their associated values in expression list must
  //   strictly follow the order that was specified by CREATE TABLE statement while Postgres DML
  //   statements will not follow this order. Therefore, we reserve the spaces in request
  //   structures for associated expressions of the primary columns in the specified order.
  // - During DML execution, the reserved expression spaces will be filled with actual values.
  // - The data-member "primary_exprs" is to map column id with the reserved expression spaces.
  std::shared_ptr<BindVariable> bind_var_ = nullptr;

  // new-values of a column in the tuple.
  std::shared_ptr<BindVariable> assign_var_ = nullptr;

  // Wether or not this column must be read from DB for the SQL request.
  bool read_requested_ = false;

  // Wether or not this column will be written for the request.
  bool write_requested_ = false;
};

}  // namespace gate
}  // namespace k2pg
