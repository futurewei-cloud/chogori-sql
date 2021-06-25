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

#include "pggate/pg_column.h"
#include "pggate/pg_gate_typedefs.h"
#include "pggate/pg_statement.h"
#include "k2_includes.h"

namespace k2pg
{
  namespace gate
  {
    using sql::PgConstant;

    void PgColumn::Init(PgSystemAttrNum attr_num)
    {
      switch (attr_num)
      {
      case PgSystemAttrNum::kSelfItemPointer:
      case PgSystemAttrNum::kObjectId:
      case PgSystemAttrNum::kMinTransactionId:
      case PgSystemAttrNum::kMinCommandId:
      case PgSystemAttrNum::kMaxTransactionId:
      case PgSystemAttrNum::kMaxCommandId:
      case PgSystemAttrNum::kTableOid:
      case PgSystemAttrNum::kPgRowId:
      case PgSystemAttrNum::kPgIdxBaseTupleId:
      case PgSystemAttrNum::kPgUniqueIdxKeySuffix:
        break;

      case PgSystemAttrNum::kPgTupleId:
      {
        int idx = static_cast<int>(PgSystemAttrNum::kPgTupleId);
        desc_.Init(idx,
                   idx,
                   "k2pgctid",
                   false,
                   false,
                   idx,
                   SQLType::Create(DataType::K2SQL_DATA_TYPE_BINARY),
                   ColumnSchema::SortingType::kNotSpecified);
        return;
      }
      }
      K2LOG_E(log::pg, "Invalid attribute number for hidden column");
    }

    bool PgColumn::is_virtual_column()
    {
      // Currently only k2pgctid is a virtual column.
      return attr_num() == static_cast<int>(PgSystemAttrNum::kPgTupleId);
    }

    std::shared_ptr<BindVariable> PgColumn::AllocKeyBind(std::shared_ptr<SqlOpWriteRequest> write_req)
    {
      if (is_primary() && bind_var_ == nullptr)
      {
        K2LOG_V(log::pg, "Allocating key binding variable for column name: {}, order: {}, for write request", attr_name(), attr_num());
        bind_var_ = std::make_shared<BindVariable>(index());
        write_req->key_column_values.push_back(bind_var_);
      }

      return bind_var_;
    }

    std::shared_ptr<BindVariable> PgColumn::AllocKeyBindForRowId(PgStatement *stmt, std::shared_ptr<SqlOpWriteRequest> write_req, std::string row_id) {
      if (is_primary() && attr_num() == static_cast<int>(PgSystemAttrNum::kPgRowId)) {
        const K2PgTypeEntity *string_type = K2PgFindTypeEntity(STRING_TYPE_OID);
        std::unique_ptr<PgConstant> pg_const = std::make_unique<PgConstant>(string_type, SqlValue(row_id));
        bind_var_ = std::make_shared<BindVariable>(index(), pg_const.get());
        // the PgConstant's life cycle in the bind_var should be managed by the statement
        stmt->AddExpr(std::move(pg_const));
        K2LOG_D(log::pg, "Allocating row id key binding variable {} for column name: {}, order: {} for write request",
            *bind_var_.get(), attr_name(), attr_num());
        write_req->key_column_values.push_back(bind_var_);
      }
      return bind_var_;
    }

    std::shared_ptr<BindVariable> PgColumn::AllocBind(std::shared_ptr<SqlOpWriteRequest> write_req)
    {
      if (bind_var_ == nullptr)
      {
        K2ASSERT(log::pg, !desc_.is_hash() && !desc_.is_primary(),
            "Binds for primary columns should have already been allocated by AllocKeyBind()");

        K2LOG_V(log::pg, "Allocating binding variable for column name: {}, order: {}, for write request", attr_name(), attr_num());
        if (id() == static_cast<int>(PgSystemAttrNum::kPgTupleId))
        {
          if (write_req->k2pgctid_column_value == nullptr)
          {
            bind_var_ = std::make_shared<BindVariable>(index());
            write_req->k2pgctid_column_value = bind_var_;
          }
        }
        else
        {
          bind_var_ = std::make_shared<BindVariable>(index());
          write_req->column_values.push_back(bind_var_ );
        }
      }

      return bind_var_;
    }

    std::shared_ptr<BindVariable> PgColumn::AllocAssign(std::shared_ptr<SqlOpWriteRequest> write_req)
    {
      if (assign_var_ == nullptr)
      {
        K2LOG_V(log::pg, "Allocating assign variable for column name: {}, order: {}, for write request", attr_name(), attr_num());
        assign_var_ = std::make_shared<BindVariable>(index());
        write_req->column_new_values.push_back(assign_var_);
      }

      return assign_var_;
    }

    std::shared_ptr<BindVariable> PgColumn::AllocKeyBind(std::shared_ptr<SqlOpReadRequest> read_req)
    {
      if (is_primary() && bind_var_ == nullptr)
      {
        K2LOG_V(log::pg, "Allocating key binding variable for column name: {}, order: {}, for read request", attr_name(), attr_num());
        bind_var_ = std::make_shared<BindVariable>(index());
        read_req->key_column_values.push_back(bind_var_);
      }

      return bind_var_;
    }

    // PG binds each column by a separate PG gate API and thus, we need to bind to one column at a time
    std::shared_ptr<BindVariable> PgColumn::AllocBind(std::shared_ptr<SqlOpReadRequest> read_req)
    {
      if (bind_var_ == nullptr)
      {
        K2ASSERT(log::pg, !desc_.is_hash() && !desc_.is_primary(),
            "Binds for primary columns should have already been allocated by AllocKeyBind()");

        K2LOG_V(log::pg, "Allocating binding variable for column name: {}, order: {}, for read request", attr_name(), attr_num());
        if (id() == static_cast<int>(PgSystemAttrNum::kPgTupleId)) {
          bind_var_ = std::make_shared<BindVariable>(index());
          read_req->k2pgctid_column_values.push_back(bind_var_);
        } else {
          K2LOG_E(log::pg, "Binds for other columns are not allowed");
        }
      }

      return bind_var_;
    }
  } // namespace gate
} // namespace k2pg
