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
#include <vector>

#include "common/status.h"
#include "entities/entity_ids.h"
#include "entities/value.h"

namespace k2pg {
namespace sql {

class PgExpr {
    public:
    enum class Opcode {
        PG_EXPR_CONSTANT,
        PG_EXPR_COLREF,

        // The logical expression for defining the conditions when we support WHERE clause.
        PG_EXPR_NOT,
        PG_EXPR_EQ,
        PG_EXPR_NE,
        PG_EXPR_GE,
        PG_EXPR_GT,
        PG_EXPR_LE,
        PG_EXPR_LT,

        // exists
        PG_EXPR_EXISTS,

        // Logic operators that take two or more operands.
        PG_EXPR_AND,
        PG_EXPR_OR,
        PG_EXPR_IN,
        PG_EXPR_BETWEEN,

        // Aggregate functions.
        PG_EXPR_AVG,
        PG_EXPR_SUM,
        PG_EXPR_COUNT,
        PG_EXPR_MAX,
        PG_EXPR_MIN,

        // built-in functions
        PG_EXPR_EVAL_EXPR_CALL,
    };

    friend std::ostream& operator<<(std::ostream& os, const Opcode& opcode) {
        switch(opcode) {
            case Opcode::PG_EXPR_CONSTANT: return os << "PG_EXPR_CONSTANT";
            case Opcode::PG_EXPR_COLREF: return os << "PG_EXPR_COLREF";
            case Opcode::PG_EXPR_NOT: return os << "PG_EXPR_NOT";
            case Opcode::PG_EXPR_EQ: return os << "PG_EXPR_EQ";
            case Opcode::PG_EXPR_NE: return os << "PG_EXPR_NE";
            case Opcode::PG_EXPR_GE: return os << "PG_EXPR_GE";
            case Opcode::PG_EXPR_GT: return os << "PG_EXPR_GT";
            case Opcode::PG_EXPR_LE: return os << "PG_EXPR_LE";
            case Opcode::PG_EXPR_LT: return os << "PG_EXPR_LT";
            case Opcode::PG_EXPR_EXISTS: return os << "PG_EXPR_EXISTS";
            case Opcode::PG_EXPR_AND: return os << "PG_EXPR_AND";
            case Opcode::PG_EXPR_OR: return os << "PG_EXPR_OR";
            case Opcode::PG_EXPR_IN: return os << "PG_EXPR_IN";
            case Opcode::PG_EXPR_BETWEEN: return os << "PG_EXPR_BETWEEN";
            case Opcode::PG_EXPR_AVG: return os << "PG_EXPR_AVG";
            case Opcode::PG_EXPR_SUM: return os << "PG_EXPR_SUM";
            case Opcode::PG_EXPR_COUNT: return os << "PG_EXPR_COUNT";
            case Opcode::PG_EXPR_MAX: return os << "PG_EXPR_MAX";
            case Opcode::PG_EXPR_MIN: return os << "PG_EXPR_MIN";
            case Opcode::PG_EXPR_EVAL_EXPR_CALL: return os << "PG_EXPR_EVAL_EXPR_CALL";
            default: return os << "UNKNOWN";
        }
    }

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

    bool is_logic_expr() const {
        return (opcode_ == Opcode::PG_EXPR_NOT ||
                opcode_ == Opcode::PG_EXPR_EQ ||
                opcode_ == Opcode::PG_EXPR_NE ||
                opcode_ == Opcode::PG_EXPR_GE ||
                opcode_ == Opcode::PG_EXPR_GT ||
                opcode_ == Opcode::PG_EXPR_LE ||
                opcode_ == Opcode::PG_EXPR_LT);
    }

    virtual bool is_ybbasetid() const {
        return false;
    }

    const PgTypeEntity *type_entity() const {
        return type_entity_;
    }

    const PgTypeAttrs *type_attrs() const {
        return &type_attrs_;
    }

    // Find opcode.
    static CHECKED_STATUS CheckOperatorName(const char *name);
    static Opcode NameToOpcode(const char *name);

    friend std::ostream& operator<<(std::ostream& os, const PgExpr& expr) {
      os << "(PgExpr: Opcode: " << expr.opcode_ << ", type: " << expr.type_entity_->yb_type << ")";
      return os;
    }

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

  explicit PgConstant(const YBCPgTypeEntity *type_entity, SqlValue value);

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

  SqlValue* getValue() {
      return &value_;
  }

  friend std::ostream& operator<<(std::ostream& os, const PgConstant& expr) {
      os << "(PgConst: " << expr.value_ << ")";
      return os;
  }

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

  void set_attr_name(const std::string& name) {
    attr_name_ = name;
  }

  const std::string& attr_name() const {
      return attr_name_;
  }

  int attr_num() const {
    return attr_num_;
  }

  bool is_ybbasetid() const override;

  friend std::ostream& operator<<(std::ostream& os, const PgColumnRef& expr) {
      os << "(PgColumnRef: attr_name: " << expr.attr_name_ << ", attr_num: " << expr.attr_num_ << ")";
      return os;
  }

 private:
  int attr_num_;
  std::string attr_name_;
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

  const std::vector<PgExpr*> & getArgs() const {
      return args_;
  }

  friend std::ostream& operator<<(std::ostream& os, const PgOperator& expr) {
      os << "(PgOperator: opcode: " << expr.opcode_ << ", opname: " << expr.opname_ << ", args_num: " << expr.args_.size() << ", args:[";
      for (PgExpr* arg : expr.args_) {
        os << (*arg) << ",";
      }
      os << "])";
      return os;
  }

  private:
  const std::string opname_;
  std::vector<PgExpr*> args_;
};

}  // namespace sql
}  // namespace k2pg
