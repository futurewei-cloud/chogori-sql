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

#include <unordered_map>

#include "entities/expr.h"

namespace k2pg {
namespace sql {

using std::string;
using std::make_shared;
using k2pg::Status;

//--------------------------------------------------------------------------------------------------
// Mapping Postgres operator names to K2 PG gate opcodes.
// When constructing expresions, Postgres layer will pass the operator name.
const std::unordered_map<string, PgExpr::Opcode> kOperatorNames = {
  { "!", PgExpr::Opcode::PG_EXPR_NOT },
  { "not", PgExpr::Opcode::PG_EXPR_NOT },
  { "=", PgExpr::Opcode::PG_EXPR_EQ },
  { "<>", PgExpr::Opcode::PG_EXPR_NE },
  { "!=", PgExpr::Opcode::PG_EXPR_NE },
  { ">", PgExpr::Opcode::PG_EXPR_GT },
  { ">=", PgExpr::Opcode::PG_EXPR_GE },
  { "<", PgExpr::Opcode::PG_EXPR_LT },
  { "<=", PgExpr::Opcode::PG_EXPR_LE },

  { "and", PgExpr::Opcode::PG_EXPR_AND },
  { "or", PgExpr::Opcode::PG_EXPR_OR },
  { "in", PgExpr::Opcode::PG_EXPR_IN },
  { "between", PgExpr::Opcode::PG_EXPR_BETWEEN },

  { "avg", PgExpr::Opcode::PG_EXPR_AVG },
  { "sum", PgExpr::Opcode::PG_EXPR_SUM },
  { "count", PgExpr::Opcode::PG_EXPR_COUNT },
  { "max", PgExpr::Opcode::PG_EXPR_MAX },
  { "min", PgExpr::Opcode::PG_EXPR_MIN },
  { "eval_expr_call", PgExpr::Opcode::PG_EXPR_EVAL_EXPR_CALL }
};

PgExpr::PgExpr(Opcode opcode, const K2PgTypeEntity *type_entity)
    : opcode_(opcode), type_entity_(type_entity) , type_attrs_({0}) {
  DCHECK(type_entity_) << "Datatype of result must be specified for expression";
  DCHECK(type_entity_->k2pg_type != K2SQL_DATA_TYPE_NOT_SUPPORTED &&
         type_entity_->k2pg_type != K2SQL_DATA_TYPE_UNKNOWN_DATA &&
         type_entity_->k2pg_type != K2SQL_DATA_TYPE_NULL_VALUE_TYPE)
    << "Invalid datatype for YSQL expressions";
  DCHECK(type_entity_->datum_to_k2pg) << "Conversion from datum to K2 Sql format not defined";
  DCHECK(type_entity_->k2pg_to_datum) << "Conversion from K2 Sql to datum format not defined";
}

PgExpr::PgExpr(Opcode opcode, const K2PgTypeEntity *type_entity, const PgTypeAttrs *type_attrs)
    : opcode_(opcode), type_entity_(type_entity), type_attrs_(*type_attrs) {
  DCHECK(type_entity_) << "Datatype of result must be specified for expression";
  DCHECK(type_entity_->k2pg_type != K2SQL_DATA_TYPE_NOT_SUPPORTED &&
         type_entity_->k2pg_type != K2SQL_DATA_TYPE_UNKNOWN_DATA &&
         type_entity_->k2pg_type != K2SQL_DATA_TYPE_NULL_VALUE_TYPE)
    << "Invalid datatype for YSQL expressions";
  DCHECK(type_entity_->datum_to_k2pg) << "Conversion from datum to K2 Sql format not defined";
  DCHECK(type_entity_->k2pg_to_datum) << "Conversion from K2 Sql to datum format not defined";
}

PgExpr::PgExpr(const char *opname, const K2PgTypeEntity *type_entity)
    : PgExpr(NameToOpcode(opname), type_entity) {
}

PgExpr::~PgExpr() {
}

Status PgExpr::CheckOperatorName(const char *name) {
  auto iter = kOperatorNames.find(name);
  if (iter == kOperatorNames.end()) {
    return STATUS_FORMAT(InvalidArgument, "Wrong operator name: {}", name);
  }
  return Status::OK();
}

PgExpr::Opcode PgExpr::NameToOpcode(const char *name) {
  auto iter = kOperatorNames.find(name);
  DCHECK(iter != kOperatorNames.end()) << "Wrong operator name: " << name;
  return iter->second;
}

PgConstant::PgConstant(const K2PgTypeEntity *type_entity, uint64_t datum, bool is_null,
    PgExpr::Opcode opcode)
    : PgExpr(opcode, type_entity), value_(type_entity, datum, is_null) {
}

PgConstant::PgConstant(const K2PgTypeEntity *type_entity, SqlValue value) : PgExpr(PgExpr::Opcode::PG_EXPR_CONSTANT, type_entity), value_(value) {

}

PgConstant::~PgConstant() {
}

void PgConstant::UpdateConstant(int8_t value, bool is_null) {
    value_.set_int8_value(value, is_null);
}

void PgConstant::UpdateConstant(int16_t value, bool is_null) {
    value_.set_int16_value(value, is_null);
}

void PgConstant::UpdateConstant(int32_t value, bool is_null) {
    value_.set_int32_value(value, is_null);
}

void PgConstant::UpdateConstant(int64_t value, bool is_null) {
    value_.set_int64_value(value, is_null);
}

void PgConstant::UpdateConstant(float value, bool is_null) {
    value_.set_float_value(value, is_null);
}

void PgConstant::UpdateConstant(double value, bool is_null) {
    value_.set_double_value(value, is_null);
}

void PgConstant::UpdateConstant(const char *value, bool is_null) {
    value_.set_string_value(value, is_null);
}

void PgConstant::UpdateConstant(const char *value, size_t bytes, bool is_null) {
    value_.set_binary_value(value, bytes, is_null);
}

PgColumnRef::PgColumnRef(int attr_num,
                         const K2PgTypeEntity *type_entity,
                         const PgTypeAttrs *type_attrs)
    : PgExpr(PgExpr::Opcode::PG_EXPR_COLREF, type_entity, type_attrs), attr_num_(attr_num) {
}

PgColumnRef::~PgColumnRef() {
}

bool PgColumnRef::is_ybbasetid() const {
  return attr_num_ == static_cast<int>(PgSystemAttrNum::kPgIdxBaseTupleId);
}

PgOperator::PgOperator(const char *opname, const K2PgTypeEntity *type_entity)
  : PgExpr(opname, type_entity), opname_(opname) {
}

PgOperator::~PgOperator() {
}

void PgOperator::AppendArg(PgExpr *arg) {
  args_.push_back(arg);
}

}  // namespace sql
}  // namespace k2pg
