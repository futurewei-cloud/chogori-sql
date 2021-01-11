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

#include "yb/pggate/pg_op_api.h"
#include "yb/common/endian.h"

namespace k2pg {
namespace gate {
    std::string SqlOpExpr::ToString() {
        std::ostringstream os;
        os << "(SqlOpExpr: type: " << type_ << ", expr: ";
        switch(type_) {
            case ExprType::VALUE: {
                os << (value_ == nullptr ? "NULL" : value_->ToString());
            } break;
            case ExprType::LIST_VALUES: {
                os << "[";
                for (auto value : values_) {
                    os << (value == nullptr ? "NULL" : value->ToString()) << ", ";
                }
                os << "]";
            } break;
            case ExprType::COLUMN_ID: {
                os << id_;
            } break;
            case ExprType::BIND_ID: {
                os << id_;
            } break;
            case ExprType::ALIAS_ID: {
                os << id_;
            } break;
            case ExprType::CONDITION: {
                os << (condition_ == nullptr ? "NULL" : condition_->ToString());
            } break;
            default: os << "UNKNOWN";
        }
        os << ")";
        return os.str();
    }

    std::string SqlOpCondition::ToString() {
        std::ostringstream os;
        os << "(SqlOpCondition: op: ";
        switch(op_) {
            case PgExpr::Opcode::PG_EXPR_CONSTANT: {
                os << "PG_EXPR_CONSTANT";
            } break;
            case PgExpr::Opcode::PG_EXPR_COLREF: {
                os << "PG_EXPR_COLREF";
            } break;
            case PgExpr::Opcode::PG_EXPR_VARIABLE: {
                os << "PG_EXPR_VARIABLE";
            } break;
            case PgExpr::Opcode::PG_EXPR_NOT: {
                os << "PG_EXPR_NOT";
            } break;
            case PgExpr::Opcode::PG_EXPR_EQ: {
                os << "PG_EXPR_EQ";
            } break;
            case PgExpr::Opcode::PG_EXPR_NE: {
                os << "PG_EXPR_NE";
            } break;
            case PgExpr::Opcode::PG_EXPR_GE: {
                os << "PG_EXPR_GE";
            } break;
            case PgExpr::Opcode::PG_EXPR_GT: {
                os << "PG_EXPR_GT";
            } break;
            case PgExpr::Opcode::PG_EXPR_LE: {
                os << "PG_EXPR_LE";
            } break;
            case PgExpr::Opcode::PG_EXPR_LT: {
                os << "PG_EXPR_LT";
            } break;
            case PgExpr::Opcode::PG_EXPR_EXISTS: {
                os << "PG_EXPR_EXISTS";
            } break;
            case PgExpr::Opcode::PG_EXPR_AND: {
                os << "PG_EXPR_AND";
            } break;
            case PgExpr::Opcode::PG_EXPR_OR: {
                os << "PG_EXPR_OR";
            } break;
            case PgExpr::Opcode::PG_EXPR_IN: {
                os << "PG_EXPR_IN";
            } break;
            case PgExpr::Opcode::PG_EXPR_BETWEEN: {
                os << "PG_EXPR_BETWEEN";
            } break;
            case PgExpr::Opcode::PG_EXPR_AVG: {
                os << "PG_EXPR_AVG";
            } break;
            case PgExpr::Opcode::PG_EXPR_SUM: {
                os << "PG_EXPR_SUM";
            } break;
            case PgExpr::Opcode::PG_EXPR_COUNT: {
                os << "PG_EXPR_COUNT";
            } break;
            case PgExpr::Opcode::PG_EXPR_MAX: {
                os << "PG_EXPR_MAX";
            } break;
            case PgExpr::Opcode::PG_EXPR_MIN: {
                os << "PG_EXPR_MIN";
            } break;
            case PgExpr::Opcode::PG_EXPR_EVAL_EXPR_CALL: {
                os << "PG_EXPR_EVAL_EXPR_CALL";
            } break;
            case PgExpr::Opcode::PG_EXPR_GENERATE_ROWID: {
                os << "PG_EXPR_GENERATE_ROWID";
            } break;
            default: break;
        }
        os << ", operands: [";
        for (auto operand : operands_) {
            os << (operand == nullptr ? "NULL" : operand->ToString()) << ", ";
        }
        os << "])";
        return os.str();
    }

    std::unique_ptr<SqlOpReadRequest> SqlOpReadRequest::clone() {
       std::unique_ptr<SqlOpReadRequest> newRequest = std::make_unique<SqlOpReadRequest>();
       newRequest->client_id = client_id;
       newRequest->stmt_id = stmt_id;
       newRequest->namespace_id = namespace_id;
       newRequest->table_id = table_id;
       newRequest->schema_version = schema_version;
       newRequest->key_column_values = key_column_values;
       newRequest->ybctid_column_value = ybctid_column_value;
       newRequest->targets = targets;
       newRequest->where_expr = where_expr;
       newRequest->condition_expr = condition_expr;
       newRequest->is_forward_scan = is_forward_scan;
       newRequest->distinct = distinct;
       newRequest->is_aggregate = is_aggregate;
       newRequest->limit = limit;
       newRequest->paging_state = paging_state;
       newRequest->return_paging_state = return_paging_state;
       newRequest->catalog_version = catalog_version;
       newRequest->row_mark_type = row_mark_type;
       return newRequest;
    }

    std::unique_ptr<SqlOpWriteRequest> SqlOpWriteRequest::clone() {
       return std::make_unique<SqlOpWriteRequest>(*this);
    }

    PgOpTemplate::PgOpTemplate() {
    }

    PgOpTemplate::~PgOpTemplate() {}

    PgWriteOpTemplate::PgWriteOpTemplate()
            : PgOpTemplate(), write_request_(new SqlOpWriteRequest()) {
    }

    PgWriteOpTemplate::~PgWriteOpTemplate() {}

    bool PgWriteOpTemplate::IsTransactional() const {
        return !is_single_row_txn_;
    }

    std::string PgWriteOpTemplate::ToString() const {
        return "PGSQL WRITE: " + write_request_->stmt_id;
    }

    std::unique_ptr<PgWriteOpTemplate> PgWriteOpTemplate::DeepCopy() {
        std::unique_ptr<PgWriteOpTemplate> result = std::make_unique<PgWriteOpTemplate>();
        result->set_active(is_active());
        result->write_request_ = write_request_->clone();
        result->is_single_row_txn_ = is_single_row_txn_;
        return result;
    }

    PgReadOpTemplate::PgReadOpTemplate()
        : PgOpTemplate(), read_request_(new SqlOpReadRequest()) {
    }

    std::string PgReadOpTemplate::ToString() const {
        return "PGSQL READ: " + read_request_->stmt_id;
    }

    std::unique_ptr<PgReadOpTemplate> PgReadOpTemplate::DeepCopy() {
        std::unique_ptr<PgReadOpTemplate> result = std::make_unique<PgReadOpTemplate>();
        result->set_active(is_active());
        result->read_request_ = read_request_->clone();
        return result;
    }
}  // namespace gate
}  // namespace k2pg
