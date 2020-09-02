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

#ifndef CHOGORI_SQL_EXPR_H
#define CHOGORI_SQL_EXPR_H

#include <memory>
#include <vector>

#include "yb/common/status.h"
#include "yb/entities/entity_ids.h"
#include "yb/entities/value.h"

namespace k2 {
namespace sql {

using namespace yb;

class PgExpr {
    public:
    enum class Opcode {
        PG_EXPR_CONSTANT,
        PG_EXPR_COLREF,
        PG_EXPR_VARIABLE,

        // The logical expression for defining the conditions when we support WHERE clause.
        PG_EXPR_NOT,
        PG_EXPR_EQ,
        PG_EXPR_NE,
        PG_EXPR_GE,
        PG_EXPR_GT,
        PG_EXPR_LE,
        PG_EXPR_LT,

        // Aggregate functions.
        PG_EXPR_AVG,
        PG_EXPR_SUM,
        PG_EXPR_COUNT,
        PG_EXPR_MAX,
        PG_EXPR_MIN,

        // Serialized YSQL/PG Expr node.
        PG_EXPR_EVAL_EXPR_CALL,

        PG_EXPR_GENERATE_ROWID,
    };
    
    typedef std::shared_ptr<PgExpr> SharedPtr;

    explicit PgExpr(Opcode opcode, const YBCPgTypeEntity *type_entity);

    explicit PgExpr(Opcode opcode, const YBCPgTypeEntity *type_entity, const PgTypeAttrs *type_attrs);

    explicit PgExpr(const char *opname, const YBCPgTypeEntity *type_entity);

    virtual ~PgExpr();

    Opcode opcode() const {
        return opcode_;
    }

    bool is_constant() const {
        return opcode_ == Opcode::PG_EXPR_CONSTANT;
    }

    bool is_colref() const {
        return opcode_ == Opcode::PG_EXPR_COLREF;
    }

    bool is_aggregate() const {
        // Only return true for pushdown supported aggregates.
        return (opcode_ == Opcode::PG_EXPR_SUM ||
                opcode_ == Opcode::PG_EXPR_COUNT ||
                opcode_ == Opcode::PG_EXPR_MAX ||
                opcode_ == Opcode::PG_EXPR_MIN);
    }

    virtual bool is_ybbasetid() const {
        return false;
    }

    const PgTypeEntity *type_entity() const {
        return type_entity_;
    }

    const PgTypeAttrs& type_attrs() const {
        return type_attrs_;
    }

    // Find opcode.
    static CHECKED_STATUS CheckOperatorName(const char *name);
    static Opcode NameToOpcode(const char *name);

    protected:
    Opcode opcode_;
    const PgTypeEntity *type_entity_;
    const PgTypeAttrs type_attrs_;
};

class PgConstant : public PgExpr {
 public:
  // Public types.
  typedef std::shared_ptr<PgConstant> SharedPtr;
  // Constructor.
  explicit PgConstant(const YBCPgTypeEntity *type_entity, uint64_t datum, bool is_null,
      PgExpr::Opcode opcode = PgExpr::Opcode::PG_EXPR_CONSTANT);

  // Destructor.
  virtual ~PgConstant();

  // Update numeric.
  void UpdateConstant(int8_t value, bool is_null);
  void UpdateConstant(int16_t value, bool is_null);
  void UpdateConstant(int32_t value, bool is_null);
  void UpdateConstant(int64_t value, bool is_null);
  void UpdateConstant(float value, bool is_null);
  void UpdateConstant(double value, bool is_null);

  // Update text.
  void UpdateConstant(const char *value, bool is_null);
  void UpdateConstant(const char *value, size_t bytes, bool is_null);

  private:
  SqlValue value_;
};

class PgColumnRef : public PgExpr {
 public:
  // Public types.
  typedef std::shared_ptr<PgColumnRef> SharedPtr;
  explicit PgColumnRef(int attr_num,
                       const PgTypeEntity *type_entity,
                       const PgTypeAttrs *type_attrs);
  virtual ~PgColumnRef();

  int attr_num() const {
    return attr_num_;
  }

  bool is_ybbasetid() const override;

 private:
  int attr_num_;
};

class PgOperator : public PgExpr {
 public:
  // Public types.
  typedef std::shared_ptr<PgOperator> SharedPtr;

  // Constructor.
  explicit PgOperator(const char *name, const YBCPgTypeEntity *type_entity);
  virtual ~PgOperator();

  // Append arguments.
  void AppendArg(PgExpr *arg);

  private:
  const string opname_;
  std::vector<PgExpr*> args_;
};

}  // namespace sql
}  // namespace k2

#endif //CHOGORI_SQL_EXPR_H