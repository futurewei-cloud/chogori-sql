//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
#include "k2_adapter.h"

#include <unordered_map>

#include <seastar/core/memory.hh>
#include <seastar/core/resource.hh>

#include "pg_gate_defaults.h"

namespace k2pg {
namespace gate {

using k2::K2TxnOptions;

Status K2Adapter::Init() {
    K2LOG_I(log::k2Adapter, "Initialize adapter");

    return Status::OK();
}

Status K2Adapter::Shutdown() {
    // TODO: add implementation
    K2LOG_I(log::k2Adapter, "Shutdown adapter");
    return Status::OK();
}

std::string K2Adapter::GetRowIdFromReadRecord(k2::dto::SKVRecord& record) {
    k2::dto::SKVRecord key_record = record.getSKVKeyRecord();
    return SerializeSKVRecordToString(key_record);
}

// helper method to convert a PgExpr to K2 expression
k2::dto::expression::Expression K2Adapter::ToK2Expression(PgExpr* pg_expr) {
    switch(pg_expr->opcode()) {
        case PgExpr::Opcode::PG_EXPR_EQ:
        case PgExpr::Opcode::PG_EXPR_GE:
        case PgExpr::Opcode::PG_EXPR_GT:
        case PgExpr::Opcode::PG_EXPR_LE:
        case PgExpr::Opcode::PG_EXPR_LT:
        {
            return ToK2BinaryLogicOperator(static_cast<PgOperator *>(pg_expr));
        } break;
        case PgExpr::Opcode::PG_EXPR_AND: {
            PgOperator* pg_opr = static_cast<PgOperator *>(pg_expr);
            K2LOG_D(log::k2Adapter, "Converting PgOperator {}", *pg_opr);
            std::vector<PgExpr*> args;
            for (auto arg : pg_opr->getArgs()) {
                args.emplace_back(arg);
            }
            return ToK2AndOrOperator(k2::dto::expression::Operation::AND, args);
        } break;
        case PgExpr::Opcode::PG_EXPR_BETWEEN:
            return ToK2BetweenOperator(static_cast<PgOperator *>(pg_expr));
            break;
        // don't support OR for now
        case PgExpr::Opcode::PG_EXPR_OR:
        // don't support NOT for now
        case PgExpr::Opcode::PG_EXPR_NOT:
        case PgExpr::Opcode::PG_EXPR_NE:
        // don't support constant and column reference at the top level
        case PgExpr::Opcode::PG_EXPR_CONSTANT:
        case PgExpr::Opcode::PG_EXPR_COLREF:
        // don't support set operator in K2 yet
        case PgExpr::Opcode::PG_EXPR_IN:
        // don't support aggregation in K2 yet
        case PgExpr::Opcode::PG_EXPR_AVG:
        case PgExpr::Opcode::PG_EXPR_SUM:
        case PgExpr::Opcode::PG_EXPR_COUNT:
        case PgExpr::Opcode::PG_EXPR_MAX:
        case PgExpr::Opcode::PG_EXPR_MIN:
        // don't support built-in func call yet
        case PgExpr::Opcode::PG_EXPR_EVAL_EXPR_CALL:
        default: {
            std::stringstream oss;
            oss << "Unsupported PgExpr " << pg_expr->opcode();
            throw std::invalid_argument(oss.str());
        } break;
    }
}

k2::dto::expression::Expression K2Adapter::ToK2AndOrOperator(k2::dto::expression::Operation op, std::vector<PgExpr*> args) {
    if (args.size() == 2) {
        std::vector<k2::dto::expression::Expression> exprs;
        exprs.emplace_back(ToK2Expression(args.back()));
        args.pop_back();
        exprs.emplace_back(ToK2Expression(args.back()));
        args.pop_back();
        return k2::dto::expression::makeExpression(op, {}, std::move(exprs));
    } else if (args.size() > 2) {
        PgExpr* pg_expr = args.back();
        args.pop_back();
        std::vector<k2::dto::expression::Expression> exprs;
        exprs.emplace_back(ToK2Expression(pg_expr));
        exprs.emplace_back(ToK2AndOrOperator(op, args));
        return k2::dto::expression::makeExpression(op, {}, std::move(exprs));
    } else {
        k2::dto::expression::Expression expr = ToK2Expression(args.back());
        args.pop_back();
        return expr;
    }
}

k2::dto::expression::Expression K2Adapter::ToK2BinaryLogicOperator(PgOperator* pg_opr) {
    K2LOG_D(log::k2Adapter, "Converting PgOperator {}", *pg_opr);
    auto& args = pg_opr->getArgs();
    K2ASSERT(log::k2Adapter, args.size() == 2, "Binary logic operator must have two arguments");
    if (!args[0]->is_colref()) {
        std::stringstream oss;
        oss << "First argument should be column reference, but actually is " << args[0]->opcode();
        throw std::invalid_argument(oss.str());
    }
    if (!args[1]->is_constant()) {
        // only consider value here
        std::stringstream oss;
        oss << "Second argument should be a value, but actually is " << args[1]->opcode();
        throw std::invalid_argument(oss.str());
    }

    PgColumnRef* ref = static_cast<PgColumnRef *>(args[0]);
    PgConstant* val = static_cast<PgConstant *>(args[1]);

    if (pg_opr->opcode() == PgExpr::Opcode::PG_EXPR_EQ) {
        // special handing for a NULL value
        if (val->getValue()->IsNull()) {
            std::vector<k2::dto::expression::Value> values;
            values.emplace_back(ToK2ColumnRef(ref));
            return k2::dto::expression::makeExpression(k2::dto::expression::Operation::IS_NULL, std::move(values), {});
        } else {
            std::vector<k2::dto::expression::Value> values;
            values.emplace_back(ToK2ColumnRef(ref));
            values.emplace_back(ToK2Value(val));
            return k2::dto::expression::makeExpression(ToK2OperationType(pg_opr), std::move(values), {});
        }
    } else {
        if (val->getValue()->IsNull()) {
            // NULL value should not be handled here
            std::stringstream oss;
            oss << "NULL value should not be handled by operator " << pg_opr->opcode();
            throw std::invalid_argument(oss.str());
        }
        std::vector<k2::dto::expression::Value> values;
        values.emplace_back(ToK2ColumnRef(ref));
        values.emplace_back(ToK2Value(val));
        return k2::dto::expression::makeExpression(ToK2OperationType(pg_opr), std::move(values), {});
    }
}

k2::dto::expression::Expression K2Adapter::ToK2BetweenOperator(PgOperator* pg_opr) {
    K2LOG_D(log::k2Adapter, "Converting PgOperator {}", *pg_opr);
    auto& args = pg_opr->getArgs();
    if (args.size() != 3) {
       throw std::invalid_argument("Between operator should have 3 arguments, but actually has " + args.size());
    }
    if (!args[0]->is_colref()) {
        std::stringstream oss;
        oss << "First argument should be column reference, but actually is " << args[0]->opcode();
        throw std::invalid_argument(oss.str());
    }

    if (!args[1]->is_constant() || !args[2]->is_constant()) {
        std::stringstream oss;
        oss << "Second and third arguments should be value, but actually are " << args[1]->opcode() << " and " << args[2]->opcode();
        throw std::invalid_argument(oss.str());
    }

    PgConstant* val1 = static_cast<PgConstant *>(args[1]);
    PgConstant* val2 = static_cast<PgConstant *>(args[2]);

    K2ASSERT(log::k2Adapter, !val1->getValue()->IsNull() && !val2->getValue()->IsNull(), "Between operator should not have null values");

    PgConstant* lower = val1;
    PgConstant* higher = val2;
    if (val1->getValue()->Compare(val2->getValue()) > 0) {
        lower = val2;
        higher = val1;
    }
    // BETWEEN is AND(LTE, GTE)
    std::vector<k2::dto::expression::Value> lower_values;
    lower_values.emplace_back(ToK2ColumnRef((PgColumnRef *)(args[0])));
    lower_values.emplace_back(ToK2Value(lower));
    k2::dto::expression::Expression lower_expr = k2::dto::expression::makeExpression(k2::dto::expression::Operation::GTE, std::move(lower_values), {});
    std::vector<k2::dto::expression::Value> higher_values;
    higher_values.emplace_back(ToK2ColumnRef((PgColumnRef *)(args[0])));
    higher_values.emplace_back(ToK2Value(higher));
    k2::dto::expression::Expression higher_expr = k2::dto::expression::makeExpression(k2::dto::expression::Operation::LTE, std::move(higher_values), {});
    std::vector<k2::dto::expression::Expression> exprs;
    exprs.emplace_back(std::move(lower_expr));
    exprs.emplace_back(std::move(higher_expr));
    return k2::dto::expression::makeExpression(k2::dto::expression::Operation::AND, {}, std::move(exprs));
}

k2::dto::expression::Operation K2Adapter::ToK2OperationType(PgExpr* pg_expr) {
    k2::dto::expression::Operation opr_type = k2::dto::expression::Operation::UNKNOWN;
    switch(pg_expr->opcode()) {
        case PgExpr::Opcode::PG_EXPR_NOT:
            opr_type = k2::dto::expression::Operation::NOT;
            break;
        case PgExpr::Opcode::PG_EXPR_EQ:
            opr_type = k2::dto::expression::Operation::EQ;
            break;
        case PgExpr::Opcode::PG_EXPR_NE:
            opr_type = k2::dto::expression::Operation::NOT;
            break;
        case PgExpr::Opcode::PG_EXPR_GE:
            opr_type = k2::dto::expression::Operation::GTE;
            break;
        case PgExpr::Opcode::PG_EXPR_GT:
            opr_type = k2::dto::expression::Operation::GT;
            break;
        case PgExpr::Opcode::PG_EXPR_LE:
            opr_type = k2::dto::expression::Operation::LTE;
            break;
        case PgExpr::Opcode::PG_EXPR_LT:
            opr_type = k2::dto::expression::Operation::LT;
            break;
        case PgExpr::Opcode::PG_EXPR_AND:
            opr_type = k2::dto::expression::Operation::AND;
            break;
        case PgExpr::Opcode::PG_EXPR_OR:
            opr_type = k2::dto::expression::Operation::OR;
            break;
        default:
            K2LOG_W(log::k2Adapter, "Unsupported PgExpr type {}", pg_expr->opcode());
            break;
    }

    return opr_type;
}

k2::dto::expression::Value K2Adapter::ToK2Value(PgConstant* pg_const) {
    if (pg_const->getValue()->IsNull()) {
        // NULL value should not be handled here
        throw std::invalid_argument("NULL value should be handled differently");
    }

    switch(pg_const->getValue()->type_) {
        case SqlValue::ValueType::BOOL: {
            bool val = pg_const->getValue()->data_.bool_val_;
            return k2::dto::expression::makeValueLiteral<bool>(std::move(val));
        }
        case SqlValue::ValueType::INT: {
            int64_t val = pg_const->getValue()->data_.int_val_;
            return k2::dto::expression::makeValueLiteral<int64_t>(std::move(val));
        }
        case SqlValue::ValueType::FLOAT: {
            float val = pg_const->getValue()->data_.float_val_;
            return k2::dto::expression::makeValueLiteral<float>(std::move(val));
        }
        case SqlValue::ValueType::DOUBLE: {
            double val = pg_const->getValue()->data_.double_val_;
            return k2::dto::expression::makeValueLiteral<double>(std::move(val));
        }
        case SqlValue::ValueType::SLICE:
            return k2::dto::expression::makeValueLiteral<k2::String>(k2::String(pg_const->getValue()->data_.slice_val_));
        default:
            throw std::invalid_argument("Unknown SqlValue type: " + pg_const->getValue()->type_);
    }
}

k2::dto::expression::Value K2Adapter::ToK2ColumnRef(PgColumnRef* pg_colref) {
    K2LOG_D(log::k2Adapter, "Setting column reference {}", pg_colref->attr_name());
    return k2::dto::expression::makeValueReference(pg_colref->attr_name());
}

Status K2Adapter::HandleRangeConditions(PgExpr *range_conds, std::vector<PgExpr *>& leftover_exprs, k2::dto::SKVRecord& start, k2::dto::SKVRecord& end) {
    if (range_conds == NULL) {
        return Status::OK();
    }

    if (range_conds->opcode() != PgExpr::Opcode::PG_EXPR_AND) {
        const char* msg = "Only AND top-level condition is supported in range expression";
        K2LOG_E(log::k2Adapter, "{}", msg);
        return STATUS(InvalidCommand, msg);
    }

    PgOperator * pg_opr = static_cast<PgOperator *>(range_conds);
    if (pg_opr->getArgs().empty()) {
        K2LOG_D(log::k2Adapter, "Child conditions are empty");
        return Status::OK();
    }

    std::vector<k2::dto::SchemaField> fields = start.schema->fields;
    std::unordered_map<std::string, int> field_map;
    for (int i = SKV_FIELD_OFFSET; i < fields.size(); i++) {
        field_map[fields[i].name] = i;
    }

    // make sure that the record cursor in the correct start position
    start.seekField(SKV_FIELD_OFFSET);
    end.seekField(SKV_FIELD_OFFSET);

    int start_idx = SKV_FIELD_OFFSET - 1;
    bool didBranch = false;
    for (auto& pg_expr : pg_opr->getArgs()) {
         if (didBranch) {
            // there was a branch in the processing of previous condition and we cannot continue.
            // Ideally, this shouldn't happen if the query parser did its job well.
            // This is not an error, and so we can still process the request. PG would down-filter the result set after
            K2LOG_W(log::k2Adapter, "Condition branched at previous key field. Use the condition as filter condition");
            leftover_exprs.emplace_back(pg_expr);
            continue; // keep going so that we log all skipped expressions;
        }
        switch(pg_expr->opcode()) {
            case PgExpr::Opcode::PG_EXPR_EQ: {
                auto& args = static_cast<PgOperator *>(pg_expr)->getArgs();
                if (!args[0]->is_colref()) {
                    std::stringstream oss;
                    oss << "First argument should be column reference, but actually is " << args[0]->opcode();
                    throw std::invalid_argument(oss.str());
                }
                if (!args[1]->is_constant()) {
                    // only consider value here
                    // TODO:: apply NOT to other types of expressions
                    std::stringstream oss;
                    oss << "Second argument should be value, but actually is " << args[1]->opcode();
                    throw std::invalid_argument(oss.str());
                }
                PgColumnRef* col_ref = static_cast<PgColumnRef *>(args[0]);
                PgConstant* val = static_cast<PgConstant *>(args[1]);
                int cur_idx = field_map[col_ref->attr_name()];
                if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                    start_idx = cur_idx;
                    K2Adapter::SerializeValueToSKVRecord(*(val->getValue()), start);
                    K2Adapter::SerializeValueToSKVRecord(*(val->getValue()), end);
                } else {
                    didBranch = true;
                    leftover_exprs.emplace_back(pg_expr);
                }
            } break;
            case PgExpr::Opcode::PG_EXPR_GE:
            case PgExpr::Opcode::PG_EXPR_GT: {
                auto& args = static_cast<PgOperator *>(pg_expr)->getArgs();
                if (!args[0]->is_colref()) {
                    std::stringstream oss;
                    oss << "First argument should be column reference, but actually is " << args[0]->opcode();
                    throw std::invalid_argument(oss.str());
                }
                if (!args[1]->is_constant()) {
                    // only consider value here
                    // TODO:: apply NOT to other types of expressions
                    std::stringstream oss;
                    oss << "Second argument should be value, but actually is " << args[1]->opcode();
                    throw std::invalid_argument(oss.str());
                }
                PgColumnRef* col_ref = static_cast<PgColumnRef *>(args[0]);
                PgConstant* val = static_cast<PgConstant *>(args[1]);
                if (val->getValue()->IsInteger()) {
                    int cur_idx = field_map[col_ref->attr_name()];
                    if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                        start_idx = cur_idx;
                        K2Adapter::SerializeValueToSKVRecord(*(val->getValue()), start);
                    } else {
                        didBranch = true;
                    }
                } else {
                    didBranch = true;
                }
                // always push the comparison operator to K2 as discussed
                leftover_exprs.emplace_back(pg_expr);
            } break;
            case PgExpr::Opcode::PG_EXPR_LT: {
                auto& args = static_cast<PgOperator *>(pg_expr)->getArgs();
                if (!args[0]->is_colref()) {
                    std::stringstream oss;
                    oss << "First argument should be column reference, but actually is " << args[0]->opcode();
                    throw std::invalid_argument(oss.str());
                }
                if (!args[1]->is_constant()) {
                    // only consider value here
                    // TODO:: apply NOT to other types of expressions
                    std::stringstream oss;
                    oss << "Second argument should be value, but actually is " << args[1]->opcode();
                    throw std::invalid_argument(oss.str());
                }
                PgColumnRef* col_ref = static_cast<PgColumnRef *>(args[0]);
                PgConstant* val = static_cast<PgConstant *>(args[1]);
                if (val->getValue()->IsInteger()) {
                    int cur_idx = field_map[col_ref->attr_name()];
                    if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                        start_idx = cur_idx;
                        K2Adapter::SerializeValueToSKVRecord(*(val->getValue()), end);
                    } else {
                        didBranch = true;
                    }
                } else {
                    didBranch = true;
                }
                // always push the comparison operator to K2 as discussed
                leftover_exprs.emplace_back(pg_expr);
            } break;
            case PgExpr::Opcode::PG_EXPR_LE: {
                auto& args = static_cast<PgOperator *>(pg_expr)->getArgs();
                if (!args[0]->is_colref()) {
                    std::stringstream oss;
                    oss << "First argument should be column reference, but actually is " << args[0]->opcode();
                    throw std::invalid_argument(oss.str());
                }
                if (!args[1]->is_constant()) {
                    // only consider value here
                    // TODO:: apply NOT to other types of expressions
                    std::stringstream oss;
                    oss << "Second argument should be value, but actually is " << args[1]->opcode();
                    throw std::invalid_argument(oss.str());
                }
                PgColumnRef* col_ref = static_cast<PgColumnRef *>(args[0]);
                PgConstant* val = static_cast<PgConstant *>(args[1]);
                if (val->getValue()->IsInteger()) {
                    int cur_idx = field_map[col_ref->attr_name()];
                    if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                        start_idx = cur_idx;
                        if (val->getValue()->IsMaxInteger()) {
                            // do not set the range if the value is maximum
                            didBranch = true;
                        } else {
                            K2Adapter::SerializeValueToSKVRecord(val->getValue()->UpperBound(), end);
                        }
                    } else {
                        didBranch = true;
                    }
                } else {
                    didBranch = true;
                }
                leftover_exprs.emplace_back(pg_expr);
            } break;
            case PgExpr::Opcode::PG_EXPR_BETWEEN: {
                auto& args = static_cast<PgOperator *>(pg_expr)->getArgs();
                if (args.size() != 3) {
                    throw std::invalid_argument("Between operator should have 3 arguments, but actually has " + args.size());
                }
                if (!args[0]->is_colref()) {
                    std::stringstream oss;
                    oss << "First argument should be column reference, but actually is " << args[0]->opcode();
                    throw std::invalid_argument(oss.str());
                }
                if (!args[1]->is_constant() || !args[2]->is_constant()) {
                    // only consider value here
                    // TODO:: apply NOT to other types of expressions
                    std::stringstream oss;
                    oss << "Second and third arguments should be value, but actually are " << args[1]->opcode() << " and " << args[2]->opcode();
                    throw std::invalid_argument(oss.str());
                }
                PgColumnRef* col_ref = static_cast<PgColumnRef *>(args[0]);
                PgConstant* val1 = static_cast<PgConstant *>(args[1]);
                PgConstant* val2 = static_cast<PgConstant *>(args[2]);

                K2ASSERT(log::k2Adapter, !val1->getValue()->IsNull() && !val2->getValue()->IsNull(), "Between operator should not have null values");
                if (val1->getValue()->IsInteger() && val2->getValue()->IsInteger()) {
                    PgConstant* lower = val1;
                    PgConstant* higher = val2;
                    if (val1->getValue()->Compare(val2->getValue()) > 0) {
                        lower = val2;
                        higher = val1;
                    }
                    int cur_idx = field_map[col_ref->attr_name()];
                    if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                        start_idx = cur_idx;
                        K2Adapter::SerializeValueToSKVRecord(*(lower->getValue()), start);
                        K2Adapter::SerializeValueToSKVRecord(*(higher->getValue()), end);
                    } else {
                        didBranch = true;
                    }
                } else {
                    didBranch = true;
                }
                leftover_exprs.emplace_back(pg_expr);
            } break;
            default: {
                const char* msg = "Expression Condition must be one of [BETWEEN, EQ, GE, LE]";
                K2LOG_W(log::k2Adapter, "{}", msg);
                didBranch = true;
                leftover_exprs.emplace_back(pg_expr);
            } break;
        }
    }

    return Status::OK();
}

// this helper method processes the given leaf condition, and sets the bounds start/end accordingly.
// didBranch: output param. If this condition causes a branch (e.g. it has some sort of inequality),
// then we set the didBranch output param so that the top-level processor knows that we can't build
// more of the key prefix.
// lastColId: is an input/output param. We check against it to make sure we haven't seen the current field yet,
// and we set it to the current field if we are about to process it.
// startValues, endValues: store values that are passed in out of order so that they can be later sorted
// and serialized to SKV in order
Status handleLeafCondition(std::shared_ptr<SqlOpCondition> cond,
                           k2::dto::SKVRecord& start, k2::dto::SKVRecord& end,
                           bool& didBranch, int& lastColId,
                           std::vector<std::pair<int, std::shared_ptr<SqlOpExpr>>>& startValues,
                           std::vector<std::pair<int, std::shared_ptr<SqlOpExpr>>>& endValues) {
    auto& ops = cond->getOperands();
    // we expect 2 nested expressions except for BETWEEN which has 3
    int expectedExpressions = cond->getOp() == PgExpr::Opcode::PG_EXPR_BETWEEN ? 3: 2;
    if (ops.size() != expectedExpressions) {
        const char* msg = "leaf condition wrong number of operands";
        K2LOG_E(log::k2Adapter, "{}, got={}, expected={}", msg, ops.size(), expectedExpressions);
        return STATUS(InvalidCommand, msg);
    }
    // first operand is col reference expression
    if (ops[0]->getType() != SqlOpExpr::ExprType::COLUMN_ID) {
        const char* msg = "1st expression in leaf condition must be a column reference";
        K2LOG_E(log::k2Adapter, "{}, got {}", msg, ops[0]->getType());
        return STATUS(InvalidCommand, msg);
    }

    int colId = ops[0]->getId() - 1; // Converting from 1-based attribute number to column ID
    if (colId < 0) {
        const char* msg = "column reference is for a system field";
        K2LOG_E(log::k2Adapter, "{}, got={}", msg, colId);
        return STATUS(InvalidCommand, msg);
    }

    // make sure we're working on a prefix of the key
    if (colId <= lastColId) {
        const char* msg = "column reference in leaf condition is for an already processed field";
        K2LOG_E(log::k2Adapter, "{}, got={}, lastColId={}", msg, colId, lastColId);
        return STATUS(InvalidCommand, msg);
    }

    int skvId = colId + K2Adapter::SKV_FIELD_OFFSET;
    bool saveForLater = false;
    if (start.getFieldCursor() != skvId || end.getFieldCursor() != skvId) {
        const char* msg = "column reference in leaf condition refers to non-consecutive field";
        K2LOG_D(log::k2Adapter, "{}, name={}, got={}, start={}, end={}", msg, ops[0]->getName(), skvId, start.getFieldCursor(), end.getFieldCursor());
        saveForLater = true;
    } else {
        // update the output param
        lastColId = colId;
    }

    switch (cond->getOp()) {
        case PgExpr::Opcode::PG_EXPR_EQ: {
            if (ops[1]->getType() != SqlOpExpr::ExprType::VALUE) {
                const char* msg = "2nd expression in EQ leaf condition must be a value";
                K2LOG_E(log::k2Adapter, "{}, got={}", msg, ops[1]->getType());
                return STATUS(InvalidCommand, msg);
            }
            // non-branching case. Set the value here in both start and end keys as we're limiting both bounds
            if (saveForLater) {
                startValues.push_back(std::make_pair(colId, ops[1]));
                endValues.push_back(std::make_pair(colId, ops[1]));
                break;
            }

            K2Adapter::SerializeValueToSKVRecord(*(ops[1]->getValue()), start);
            K2Adapter::SerializeValueToSKVRecord(*(ops[1]->getValue()), end);
            break;
        }
        case PgExpr::Opcode::PG_EXPR_GE: {
            if (ops[1]->getType() != SqlOpExpr::ExprType::VALUE) {
                const char* msg = "2nd expression in GE leaf condition must be a value";
                K2LOG_E(log::k2Adapter, "{}, got={}", msg, ops[1]->getType());
                return STATUS(InvalidCommand, msg);
            }
            // branching case. we can only set the lower bound
            didBranch = true;

            if (saveForLater) {
                startValues.push_back(std::make_pair(colId, ops[1]));
                break;
            }

            K2Adapter::SerializeValueToSKVRecord(*(ops[1]->getValue()), start);
            break;
        }
        case PgExpr::Opcode::PG_EXPR_LE: {
            if (ops[1]->getType() != SqlOpExpr::ExprType::VALUE) {
                const char* msg = "2nd expression in LE leaf condition must be a value";
                K2LOG_E(log::k2Adapter, "{}, got={}", msg, ops[1]->getType());
                return STATUS(InvalidCommand, msg);
            }
            // branching case. we can only set the upper bound
            didBranch = true;

            if (saveForLater) {
                endValues.push_back(std::make_pair(colId, ops[1]));
                break;
            }

            K2Adapter::SerializeValueToSKVRecord(*(ops[1]->getValue()), end);
            break;
        }
        case PgExpr::Opcode::PG_EXPR_BETWEEN: {
            if (ops[1]->getType() != SqlOpExpr::ExprType::VALUE || ops[2]->getType() != SqlOpExpr::ExprType::VALUE) {
                const char* msg = "2nd and 3rd expressions in BETWEEN leaf condition must be values";
                K2LOG_E(log::k2Adapter, "{}, got={}, and {}", msg, ops[1]->getType(), ops[2]->getType());
                return STATUS(InvalidCommand, msg);
            }
            // branching case. we can set both bounds but no further prefix is possible
            didBranch = true;

            if (saveForLater) {
                startValues.push_back(std::make_pair(colId, ops[1]));
                endValues.push_back(std::make_pair(colId, ops[2]));
                break;
            }

            K2Adapter::SerializeValueToSKVRecord(*(ops[1]->getValue()), start);
            K2Adapter::SerializeValueToSKVRecord(*(ops[2]->getValue()), end);
            break;
        }
        default: {
            const char* msg = "Expression Condition must be one of [BETWEEN, EQ, GE, LE]";
            K2LOG_E(log::k2Adapter, "{}", msg);
            return STATUS(InvalidCommand, msg);
        }
    }
    return Status();
}

// sorts OpExpr values by column id and serializes into SKVRecord until no more prefix can be formed
void sortAndSerializeOpValues(std::vector<std::pair<int, std::shared_ptr<SqlOpExpr>>>& values,
                                                                    k2::dto::SKVRecord& record) {
    std::sort(values.begin(), values.end(), [] (const std::pair<int, std::shared_ptr<SqlOpExpr>>& a,
                                                const std::pair<int, std::shared_ptr<SqlOpExpr>>& b) {
        return a.first < b.first; }
    );

    for (const std::pair<int, std::shared_ptr<SqlOpExpr>>& value : values) {
        int skvId = value.first + K2Adapter::SKV_FIELD_OFFSET;
        K2LOG_D(log::k2Adapter, "late serialize, got={}, cursor={}", skvId, record.getFieldCursor());
        K2LOG_V(log::k2Adapter, "late serialize, schema={}", *(record.schema));
        if (record.getFieldCursor() != skvId) {
            K2LOG_W(log::k2Adapter, "Ignoring op in condition expression because it is not a prefix");
            return;
        }

        K2Adapter::SerializeValueToSKVRecord(*(value.second->getValue()), record);
    }
}

// this method processes the given top-level condition and sets the start/end boundaries based on the condition
Status parseCondExprAsRange_(std::shared_ptr<SqlOpCondition> condition_expr,
                           k2::dto::SKVRecord& start, k2::dto::SKVRecord& end) {
    if (!condition_expr) {
        return Status::OK();
    }

    // the top level condition must be AND
    if (condition_expr->getOp() != PgExpr::Opcode::PG_EXPR_AND) {
        const char* msg = "Only AND top-level condition is supported in condition expression";
        K2LOG_E(log::k2Adapter, "{}", msg);
        return STATUS(InvalidCommand, msg);
    }

    // If ops come in out of schema order, store here for later sorting and serialization to SKV records
    std::vector<std::pair<int, std::shared_ptr<SqlOpExpr>>> startValues;
    std::vector<std::pair<int, std::shared_ptr<SqlOpExpr>>> endValues;

    int lastColId = -1; // make sure we're setting the key fields in increasing order
    bool didBranch = false;
    for (auto& expr: condition_expr->getOperands()) {
        if (didBranch) {
            // there was a branch in the processing of previous condition and we cannot continue.
            // Ideally, this shouldn't happen if the query parser did its job well.
            // This is not an error, and so we can still process the request. PG would down-filter the result set after
            K2LOG_W(log::k2Adapter, "Condition branched at previous key field. Cannot process further expressions");
            continue; // keep going so that we log all skipped expressions;
        }
        if (expr->getType() != SqlOpExpr::ExprType::CONDITION) {
            const char* msg = "First-level nested expression must be of type Condition";
            K2LOG_E(log::k2Adapter, "{}", msg);
            return STATUS(InvalidCommand, msg);
        }
        auto cond = expr->getCondition();
        auto status = handleLeafCondition(expr->getCondition(), start, end, didBranch, lastColId, startValues, endValues);
        if (!status.ok()) {
            return status;
        }
    }

    sortAndSerializeOpValues(startValues, start);
    sortAndSerializeOpValues(endValues, end);

    return Status::OK();
}

// Helper function for handleReadOp when a vector of ybctids are set in the request
void K2Adapter::handleReadByRowIds(std::shared_ptr<K23SITxn> k23SITxn,
                                    std::shared_ptr<PgReadOpTemplate> op,
                                    std::shared_ptr<std::promise<Status>> prom) {
    std::shared_ptr<SqlOpReadRequest> request = op->request();
    SqlOpResponse& response = op->response();

    k2::Status status;
    std::vector<CBFuture<k2::ReadResult<k2::SKVRecord>>> result_futures;
    for (auto& ybctid_column_value : request->ybctid_column_values) {
        k2::GetSchemaResult schema_result = k23si_->getSchema(request->collection_name,
                                                    request->table_id, k2::K23SIClient::ANY_VERSION).get();

        if (!schema_result.status.is2xxOK()) {
            throw std::runtime_error(fmt::format("Failed to get schema for {} in {} due to {}",
                        request->table_id, request->collection_name, schema_result.status));
        }
        k2::dto::SKVRecord key_record = YBCTIDToRecord(request->collection_name,
                                                             schema_result.schema,
                                                             ybctid_column_value);
        result_futures.push_back(k23SITxn->read(std::move(key_record)));
    }

    int idx = 0;
    for (auto& result_future : result_futures) {
        k2::ReadResult<k2::SKVRecord> read = result_future.get();
        if (read.status.is2xxOK()) {
            op->mutable_rows_data()->emplace_back(std::move(read.value));
            // use the last read response as the batch response
            status = std::move(read.status);
            idx++;
        } else {
            // If any read failed, abort and fail the batch
            K2LOG_E(log::k2Adapter, "Failed to read for {}, due to {}", YBCTIDToString(request->ybctid_column_values[idx]), read.status.message);
            status = std::move(read.status);
            break;
        }
    }

    response.paging_state = nullptr;
    K2LOG_D(log::k2Adapter, "handleReadByRowIds set response paging state to null for read op tid={}", op->request()->table_id);
    response.status = K2StatusToPGStatus(status);
    prom->set_value(K2StatusToYBStatus(status));
}

CBFuture<Status> K2Adapter::handleReadOp(std::shared_ptr<K23SITxn> k23SITxn,
                                            std::shared_ptr<PgReadOpTemplate> op) {
    auto prom = std::make_shared<std::promise<Status>>();
    auto result = CBFuture<Status>(prom->get_future(), []{});

    threadPool_.enqueue([this, k23SITxn, op, prom] () {
        try {

        std::shared_ptr<SqlOpReadRequest> request = op->request();
        SqlOpResponse& response = op->response();
        response.skipped = false;

        if (request->ybctid_column_values.size() > 0) {
            // TODO SKV doesn't support ybctid_column_value on query yet so no projection or filtering
            return handleReadByRowIds(k23SITxn, op, prom);
        }

        std::shared_ptr<k2::Query> scan = nullptr;

        if (request->paging_state && request->paging_state->query) {
            scan = request->paging_state->query;
        } else {
            auto scan_create_result = k23si_->createScanRead(request->collection_name, request->table_id).get();
            if (!scan_create_result.status.is2xxOK()) {
                K2LOG_E(log::k2Adapter, "Unable to create scan read request");
                response.rows_affected_count = 0;
                response.status = K2StatusToPGStatus(scan_create_result.status);
                prom->set_value(K2StatusToYBStatus(scan_create_result.status));
                return;
            }

            scan = scan_create_result.query;
            scan->setReverseDirection(!request->is_forward_scan);

            std::shared_ptr<k2::dto::Schema> schema = scan->startScanRecord.schema;
            // Projections must include key fields so that ybctid/rowid can be created from the resulting
            // record
            if (request->targets.size()) {
                for (uint32_t keyIdx : schema->partitionKeyFields) {
                    scan->addProjection(schema->fields[keyIdx].name);
                }
            }
            for (const std::shared_ptr<SqlOpExpr>& target : request->targets) {
                if (target->getType() != SqlOpExpr::ExprType::COLUMN_ID) {
                    prom->set_exception(std::make_exception_ptr(std::logic_error("Non-projection type in read targets")));
                    return;
                }

                // Skip the virtual column which is not stored in K2
                if (target->getId() == VIRTUAL_COLUMN) {
                    continue;
                }

                k2::String name = target->getName();
                // Skip key fields which were already projected above
                bool skip = false;
                for (uint32_t keyIdx : schema->partitionKeyFields) {
                    if (name == schema->fields[keyIdx].name) {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    continue;
                }

                scan->addProjection(name);
                K2LOG_V(log::k2Adapter, "Projection added for name={}", name);
            }

            // create the start/end records based on the data found in the request and the hard-coded tableid/idxid
            auto [startRecord, startStatus] = MakeSKVRecordWithKeysSerialized(*request, false);
            auto [endRecord, endStatus] = MakeSKVRecordWithKeysSerialized(*request, false);

            if (!startStatus.ok() || !endStatus.ok()) {
                // An error here means the schema could not be retrieved, which shouldn't happen
                // because the schema would have been used to make the original query
                K2LOG_E(log::k2Adapter, "Scan request cannot create SKVRecords due to: {} ;;; {}", startStatus, endStatus);
                response.status = SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
                response.rows_affected_count = 0;
                prom->set_value(std::move(startStatus.ok() ? endStatus : startStatus));
                return;
            }

            // update the records based on the condition expression found in the request
            auto parseStatus = parseCondExprAsRange_(request->condition_expr, startRecord, endRecord);
            if (!parseStatus.ok()) {
                response.status = SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
                response.rows_affected_count = 0;
                prom->set_value(std::move(parseStatus));
                return;
            }

            // update the records based on the range condition found in the request
            std::vector<PgExpr *> leftover_exprs;
            auto rngStatus = HandleRangeConditions(request->range_conds, leftover_exprs, startRecord, endRecord);
            if (!rngStatus.ok()) {
                response.status = SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
                response.rows_affected_count = 0;
                prom->set_value(std::move(rngStatus));
                return;
            }

            scan->startScanRecord = std::move(startRecord);
            scan->endScanRecord = std::move(endRecord);

            if (request->where_conds != NULL || !leftover_exprs.empty()) {
                PgOperator * pg_opr;
                if (request->where_conds == NULL) {
                    // create a temporary and operator
                    PgOperator and_op("and", NULL);
                    pg_opr = &and_op;
                } else {
                    pg_opr = static_cast<PgOperator *>(request->where_conds);
                }
                if (!leftover_exprs.empty()) {
                    // add the left over conditions to where conditions
                    // the top level expression is an AND, thus, we can add the left_over as its arguments
                    for (auto leftover_expr : leftover_exprs) {
                        pg_opr->AppendArg(leftover_expr);
                    }
                }

                if (!pg_opr->getArgs().empty()) {
                    scan->setFilterExpression(std::move(ToK2Expression(request->where_conds)));
                }
            }

            // this is a total limit.
            if (request->limit > 0) {
                scan->setLimit(request->limit);
            }
        }

        k2::QueryResult scan_result = k23SITxn->scanRead(scan).get();
        if (scan->isDone()) {
            response.paging_state = nullptr;
            K2LOG_D(log::k2Adapter, "Scan is done, set response paging state to null for request {}", request->table_id);
        } else if (request->paging_state) {
            response.paging_state = request->paging_state;
            response.paging_state->total_num_rows_read += scan_result.records.size();
            K2LOG_D(log::k2Adapter, "Request paging state is null? {}, for request {}, total num of read {}", (request->paging_state == nullptr), request->table_id, response.paging_state->total_num_rows_read);
        } else {
            response.paging_state = std::make_shared<SqlOpPagingState>();
            response.paging_state->query = scan;
            response.paging_state->total_num_rows_read += scan_result.records.size();
            K2LOG_D(log::k2Adapter, "Created paging state for request {}, total rows read={}", request->table_id,  response.paging_state->total_num_rows_read);
        }

        *(op->mutable_rows_data()) = std::move(scan_result.records);

        response.status = K2StatusToPGStatus(scan_result.status);
        prom->set_value(K2StatusToYBStatus(scan_result.status));

        } catch (const std::exception& e) {
            K2LOG_W(log::k2Adapter, "Throw in handleReadOp {}", e.what());
            prom->set_exception(std::current_exception());
        }
    });

    return result;
}

CBFuture<Status> K2Adapter::handleWriteOp(std::shared_ptr<K23SITxn> k23SITxn,
                                             std::shared_ptr<PgWriteOpTemplate> op) {
    auto prom = std::make_shared<std::promise<Status>>();
    auto result = CBFuture<Status>(prom->get_future(), [] {});

    threadPool_.enqueue([this, k23SITxn, op, prom] () {
        try {

        std::shared_ptr<SqlOpWriteRequest> writeRequest = op->request();
        SqlOpResponse& response = op->response();
        response.skipped = false;

        if (writeRequest->targets.size() || writeRequest->where_expr || writeRequest->condition_expr) {
            throw std::logic_error("Targets, where, and condition expressions are not supported for write");
        }

        bool ignoreYBCTID = writeRequest->stmt_type == SqlOpWriteRequest::StmtType::PGSQL_INSERT;
        auto [record, status] = MakeSKVRecordWithKeysSerialized(*writeRequest, writeRequest->ybctid_column_value != nullptr, ignoreYBCTID);
        if (!status.ok()) {
            response.status = SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
            response.rows_affected_count = 0;
            prom->set_value(std::move(status));
            return;
        }
        bool useYBCTID = !ignoreYBCTID && writeRequest->ybctid_column_value;

        K2LOG_V(log::k2Adapter, "Record made for write with ignore={}, ybctid={}, record={}", ignoreYBCTID, writeRequest->ybctid_column_value, record);

        // These two are INSERT, UPSERT, and DELETE only
        bool erase = writeRequest->stmt_type == SqlOpWriteRequest::StmtType::PGSQL_DELETE;
        bool rejectIfExists = writeRequest->stmt_type == SqlOpWriteRequest::StmtType::PGSQL_INSERT;

        // UDPATE and DELETE only, get the cached key record
        k2::dto::SKVRecord keyRecord{};
        if (useYBCTID) {
            keyRecord = YBCTIDToRecord(record.collectionName, record.schema, writeRequest->ybctid_column_value);
        }

        // populate the data, fieldsForUpdate is only relevant for UPDATE
        std::vector<ColumnValue>& values = writeRequest->stmt_type != SqlOpWriteRequest::StmtType::PGSQL_UPDATE
                ? writeRequest->column_values : writeRequest->column_new_values;
        std::vector<uint32_t> fieldsForUpdate = SerializeSKVValueFields(record, values);

        k2::Status writeStatus;

        // For DELETE we need to use the key record we got from ybctid if it exists,
        // not the record generated from column values
        if (writeRequest->stmt_type == SqlOpWriteRequest::StmtType::PGSQL_DELETE &&
            useYBCTID) {
            record = std::move(keyRecord);
        }

        // block-write
        if (writeRequest->stmt_type != SqlOpWriteRequest::StmtType::PGSQL_UPDATE) {
            k2::WriteResult writeResult = k23SITxn->write(std::move(record), erase, rejectIfExists).get();
            writeStatus = std::move(writeResult.status);
        } else {
            k2::PartialUpdateResult updateResult = k23SITxn->partialUpdate(std::move(record),
                            std::move(fieldsForUpdate), keyRecord.getPartitionKey()).get();
            writeStatus = std::move(updateResult.status);
        }

        if (writeStatus.is2xxOK()) {
            response.rows_affected_count = 1;
        } else {
            response.rows_affected_count = 0;
            response.error_message = writeStatus.message;
            // TODO pg_error_code or txn_error_code in response?
            K2LOG_E(log::k2Adapter, "K2 write failed due to {}", response.error_message);
        }
        K2LOG_D(log::k2Adapter, "K2 write status: {}", writeStatus);
        response.status = K2StatusToPGStatus(writeStatus);
        prom->set_value(K2StatusToYBStatus(writeStatus));

        } catch (const std::exception& e) {
            K2LOG_W(log::k2Adapter, "Throw in handlewrite: {}", e.what());
            prom->set_exception(std::current_exception());
        }
    });

    return result;
}

CBFuture<k2::Status> K2Adapter::CreateCollection(const std::string& collection_name, const std::string& nsName)
{
    auto start = k2::Clock::now();
    K2LOG_I(log::k2Adapter, "Create collection: name={}, ns={}", collection_name, nsName);

    // Working around json conversion to/from k2::String which uses b64
    std::vector<std::string> stdRangeEnds = conf_()["create_collections"][nsName]["range_ends"];
    std::vector<k2::String> rangeEnds;
    for (const std::string& end : stdRangeEnds) {
        rangeEnds.emplace_back(end);
    }

    std::vector<std::string> stdEndpoints = conf_()["create_collections"][nsName]["endpoints"];
    std::vector<k2::String> endpoints;
    for (const std::string& ep : stdEndpoints) {
        endpoints.emplace_back(ep);
    }

    k2::dto::HashScheme scheme = rangeEnds.size() ? k2::dto::HashScheme::Range : k2::dto::HashScheme::HashCRC32C;

    auto createCollectionReq = k2::dto::CollectionCreateRequest{
        .metadata{
            .name = collection_name,
            .hashScheme = scheme,
            .storageDriver = k2::dto::StorageDriver::K23SI,
            .capacity{
                // TODO: get capacity from config or pass in from param
                //.dataCapacityMegaBytes = 1000,
                //.readIOPs = 100000,
                //.writeIOPs = 100000
            },
            .retentionPeriod = k2::Duration(1h) * 90 * 24  //TODO: get this from config or from param in
        },
        .clusterEndpoints = std::move(endpoints),
        .rangeEnds = std::move(rangeEnds)
    };

    auto result = k23si_->createCollection(std::move(createCollectionReq));
    K2LOG_D(log::k2Adapter, "CreateCollection took {}", k2::Clock::now() - start);
    return result;
}

CBFuture<CreateScanReadResult> K2Adapter::CreateScanRead(const std::string& collectionName,
                                                     const std::string& schemaName) {
    auto start = k2::Clock::now();
    auto result = k23si_->createScanRead(collectionName, schemaName);
    K2LOG_V(log::k2Adapter, "CreateScanRead took {}", k2::Clock::now() - start);
    return result;
}

CBFuture<Status> K2Adapter::Exec(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgOpTemplate> op) {
    auto start = k2::Clock::now();
    // 1) check the request in op and construct the SKV request based on the op type, i.e., READ or WRITE
    // 2) call read or write on k23SITxn
    // 3) create create a runner in a thread pool to check the response of the SKV call
    // 4) return a promise and return the future as the response for this method
    // 5) once the response from SKV returns
    //   a) populate the response object in op
    //   b) populate the data field in op as result set
    //   c) set the value for future
    op->allocateResponse();
    switch (op->type()) {
        case PgOpTemplate::WRITE: {
            K2LOG_D(log::k2Adapter, "Executing writing operation for table {}", std::static_pointer_cast<PgWriteOpTemplate>(op)->request()->table_id);
            auto result = handleWriteOp(k23SITxn, std::static_pointer_cast<PgWriteOpTemplate>(op));
            K2LOG_V(log::k2Adapter, "Exec took {}", k2::Clock::now() - start);
            return result;
        } break;
        case PgOpTemplate::READ: {
            K2LOG_D(log::k2Adapter, "Executing reading operation for table {}", std::static_pointer_cast<PgReadOpTemplate>(op)->request()->table_id);
            auto result = handleReadOp(k23SITxn, std::static_pointer_cast<PgReadOpTemplate>(op));
            K2LOG_V(log::k2Adapter, "Exec took {}", k2::Clock::now() - start);
            return result;
        } break;
        default:
          throw new std::logic_error("Unsupported op template type");
    }
}

CBFuture<Status> K2Adapter::BatchExec(std::shared_ptr<K23SITxn> k23SITxn, const std::vector<std::shared_ptr<PgOpTemplate>>& ops) {
    auto start = k2::Clock::now();
    // same as the above except that send multiple requests and need to handle multiple futures from SKV
    // but only return a single future to this method caller. Return Status will be OK all Execs are
    // successful, otherwise Status will be one of the failed Execs

    // This only works if the Exec calls are executed in order by the threadpool, otherwise we could
    // deadlock if the waiting thread below is executed first
    auto op_futures = std::make_shared<std::vector<CBFuture<Status>>>();
    for (const std::shared_ptr<PgOpTemplate>& op : ops) {
        op_futures->emplace_back(Exec(k23SITxn, op));
    }

    auto prom = std::make_shared<std::promise<Status>>();
    auto result = CBFuture<Status>(prom->get_future(), [] {});
    threadPool_.enqueue([op_futures, prom] () {
        Status status = Status::OK();
        std::exception_ptr e = nullptr;

        for (CBFuture<Status>& op : *op_futures) {
            try {
                Status exec_status = op.get();
                if (!exec_status.ok()) {
                    status = std::move(exec_status);
                }
            } catch (...) {
                e = std::current_exception();
            }
        }

        if (!e) {
            prom->set_value(std::move(status));
        } else {
            prom->set_exception(e);
        }
    });
    K2LOG_V(log::k2Adapter, "BatchExec took {}", k2::Clock::now() - start);
    return result;
}

std::string K2Adapter::SerializeSKVRecordToString(k2::dto::SKVRecord& record) {
    const k2::dto::SKVRecord::Storage& storage = record.getStorage();
    k2::Payload payload(k2::Payload::DefaultAllocator);
    // Since Storage itself contains a nested Payload, we cannot do anything fancy to avoid the extra
    // copy to a new payload here, because the implementation of write() with a payload argument is to
    // share the underlying buffers but here we need one continguous piece of memory
    payload.write(storage);
    payload.seek(0);
    std::string serialized(payload.getSize(), '\0');
    payload.read(serialized.data(), payload.getSize());

    return serialized;
}

k2::dto::SKVRecord K2Adapter::YBCTIDToRecord(const std::string& collection,
                                      std::shared_ptr<k2::dto::Schema> schema,
                                      std::shared_ptr<SqlOpExpr> ybctid_column_value) {
    if (!ybctid_column_value) {
        return k2::dto::SKVRecord();
    }

    if (!ybctid_column_value->isValueType()) {
        K2LOG_W(log::k2Adapter, "ybctid_column_value value is not a Slice");
        throw std::invalid_argument("Non value type in ybctid_column_value");
    }

    std::shared_ptr<SqlValue> value = ybctid_column_value->getValue();
    if (value->type_ != SqlValue::ValueType::SLICE) {
        K2LOG_W(log::k2Adapter, "ybctid_column_value value is not a Slice");
        throw std::invalid_argument("ybctid_column_value value is not a Slice");
    }

    std::string& ybctid = value->data_.slice_val_;

    k2::dto::SKVRecord::Storage storage{};
    // Wrap the string we will return in a non-owning Binary, so we can read into it without
    // an extra copy. We are using the payload's serialization mechanism without giving the
    // payload ownership of the data.
    k2::Binary binary(ybctid.data(), ybctid.size(), seastar::deleter());
    k2::Payload payload{};
    payload.appendBinary(std::move(binary));
    payload.seek(0);
    payload.read(storage);

    // We need to copy the storage here because if we don't the fieldData inner Payload will be a
    // shared reference to the binary above, but that binary will not be valid after this function
    // returns.
    k2::dto::SKVRecord record(collection, schema, storage.copy(), true);
    record.seekField(0);
    return record;
}

std::string K2Adapter::GetRowId(std::shared_ptr<SqlOpWriteRequest> request) {
    auto start = k2::Clock::now();
    // either use the virtual row id defined in ybctid_column_value field
    // if it has been set or calculate the row id based on primary key values
    // in key_column_values in the request

    if (request->ybctid_column_value) {
        return YBCTIDToString(request->ybctid_column_value);
    }

    auto [record, status] = MakeSKVRecordWithKeysSerialized(*request, false);
    if (!status.ok()) {
        throw std::runtime_error("MakeSKVRecordWithKeysSerialized failed for GetRowId");
    }
    // Skip non-key fields in record
    size_t num_values = record.schema->fields.size() - record.schema->partitionKeyFields.size()
                        - record.schema->rangeKeyFields.size();
    for (size_t i = 0; i < num_values; ++i) {
        record.serializeNull();
    }

    K2LOG_V(log::k2Adapter, "GetRowId by request took {}", k2::Clock::now() - start);
    return SerializeSKVRecordToString(record);
}

std::string K2Adapter::GetRowId(const std::string& collection_name, const std::string& table_id, uint32_t schema_version, std::vector<std::shared_ptr<SqlValue>> key_values) {
    auto start = k2::Clock::now();
    CBFuture<k2::GetSchemaResult> schema_f = k23si_->getSchema(collection_name, table_id, schema_version);
    k2::GetSchemaResult schema_result = schema_f.get();
    if (!schema_result.status.is2xxOK()) {
        throw std::runtime_error(fmt::format("Failed to get schema for {} in {} due to {}",
                                    table_id, collection_name, schema_result.status));
    }
    k2::dto::SKVRecord record(collection_name, schema_result.schema);

    // Get table oid from table_id. 
    k2pg::sql::PgOid tableOid = k2pg::sql::PgObjectId::GetTableOidByTableUuid(table_id);

    // Serialize key data into SKVRecord
    record.serializeNext<int64_t>(tableOid);
    record.serializeNext<int64_t>(0);       //  index 0 as this is for base table
    for (std::shared_ptr<SqlValue> value : key_values) {
        K2Adapter::SerializeValueToSKVRecord(*(value.get()), record);
    }
    // Skip non-key fields in record
    size_t num_values = record.schema->fields.size() - record.schema->partitionKeyFields.size()
                        - record.schema->rangeKeyFields.size();
    for (size_t i = 0; i < num_values; ++i) {
        record.serializeNull();
    }

    k2::dto::Key key = record.getKey();
    K2LOG_D(log::k2Adapter, "Returning row id for table {} from SKV partition key: {}", table_id, key.partitionKey);
    K2LOG_V(log::k2Adapter, "GetRowId took {}", k2::Clock::now() - start);
    return SerializeSKVRecordToString(record);
}

CBFuture<K23SITxn> K2Adapter::BeginTransaction() {
    k2::K2TxnOptions options{};
    // use default values for now
    // TODO: read from configuration/env files
    // Actual partition request deadline is min of this and command line option
    options.deadline = k2::Duration(60000s);
    //options.priority = k2::dto::TxnPriority::Medium;
    auto result = k23si_->beginTxn(options);
    return result;
}

void K2Adapter::SerializeValueToSKVRecord(const SqlValue& value, k2::dto::SKVRecord& record) {
    if (value.IsNull()) {
        K2LOG_V(log::k2Adapter, "null value for field: {}", record.schema->fields[record.getFieldCursor()])
        record.serializeNull();
        return;
    }

    switch (value.type_) {
        case SqlValue::ValueType::BOOL:
            K2LOG_V(log::k2Adapter, "bool value for field: {}", record.schema->fields[record.getFieldCursor()])
            record.serializeNext<bool>(value.data_.bool_val_);
            break;
        case SqlValue::ValueType::INT:
            K2LOG_V(log::k2Adapter, "int value for field: {}", record.schema->fields[record.getFieldCursor()])
            record.serializeNext<int64_t>(value.data_.int_val_);
            break;
        case SqlValue::ValueType::FLOAT:
            K2LOG_V(log::k2Adapter, "float value for field: {}", record.schema->fields[record.getFieldCursor()])
            record.serializeNext<float>(value.data_.float_val_);
            break;
        case SqlValue::ValueType::DOUBLE:
            K2LOG_V(log::k2Adapter, "double value for field: {}", record.schema->fields[record.getFieldCursor()])
            record.serializeNext<double>(value.data_.double_val_);
            break;
        case SqlValue::ValueType::SLICE:
            K2LOG_V(log::k2Adapter, "slice value for field: {}", record.schema->fields[record.getFieldCursor()])
            record.serializeNext<k2::String>(k2::String(value.data_.slice_val_));
            break;
        default:
            throw std::logic_error("Unknown SqlValue type");
    }
}

template <class T> // Works with SqlOpWriteRequest and SqlOpReadRequest types
std::pair<k2::dto::SKVRecord, Status> K2Adapter::MakeSKVRecordWithKeysSerialized(T& request, bool existYbctids, bool ignoreYBCTID) {
    CBFuture<k2::GetSchemaResult> schema_f = k23si_->getSchema(request.collection_name, request.table_id,
                                                                  request.schema_version);
    // TODO Schemas are cached by SKVClient but we can add a cache to K2 adapter to reduce
    // cross-thread traffic
    k2::GetSchemaResult schema_result = schema_f.get();
    if (!schema_result.status.is2xxOK()) {
        return std::make_pair(k2::dto::SKVRecord(), K2StatusToYBStatus(schema_result.status));
    }

    std::shared_ptr<k2::dto::Schema>& schema = schema_result.schema;
    k2::dto::SKVRecord record(request.collection_name, schema);

    if (existYbctids && !ignoreYBCTID) {
        K2LOG_V(log::k2Adapter, "Serializing for ybctid");
        // Using a pre-stored and pre-serialized key, just need to skip key fields
        // Note, not using range keys for SQL
        for (size_t i=0; i < schema->partitionKeyFields.size(); ++i) {
            record.serializeNull();
        }
    } else {
        // Serialize key data into SKVRecord
        K2LOG_V(log::k2Adapter, "Serializing with data");
        record.serializeNext<int64_t>(request.base_table_oid);
        record.serializeNext<int64_t>(request.index_oid); 
        for (const std::shared_ptr<SqlOpExpr>& expr : request.key_column_values) {
            if (expr->getType() == SqlOpExpr::ExprType::UNBOUND) {
                K2LOG_D(log::k2Adapter, "Stopping key serialization due to unbound value");
                break;
            }
            if (!expr->isValueType()) {
                throw std::logic_error("Non value type in key_column_values");
            }

            K2Adapter::SerializeValueToSKVRecord(*(expr->getValue()), record);
        }
    }

    return std::make_pair(std::move(record), Status());
}
// Sorts values by field index, serializes values into SKVRecord, and returns skv indexes of written fields
std::vector<uint32_t> K2Adapter::SerializeSKVValueFields(k2::dto::SKVRecord& record,
                                                         std::vector<ColumnValue>& values) {
    std::vector<uint32_t> fieldsForUpdate;

    std::sort(values.begin(), values.end(), [] (const ColumnValue& a, const ColumnValue& b) {
        return a.column_id < b.column_id; }
    );

    for (const ColumnValue& column : values) {
        if (!column.expr->isValueType()) {
            throw std::logic_error("Non value type in column_values");
        }

        // Assumes field ids need to be offset for the implicit tableID and indexID SKV fields
        uint32_t skvIndex = column.column_id + SKV_FIELD_OFFSET;
        if (skvIndex < record.schema->partitionKeyFields.size() &&
                        record.getFieldCursor() >= record.schema->partitionKeyFields.size()) {
            // PG gave us both rowid and the columns for key fields, so skip the column
            // TODO why does this happen? Is something else wrong?
            continue;
        }
        while (record.getFieldCursor() != skvIndex) {
            record.serializeNull();
        }

        // TODO support update on key fields
        fieldsForUpdate.push_back(skvIndex);
        K2Adapter::SerializeValueToSKVRecord(*(column.expr->getValue()), record);
    }

    // For partial updates, need to explicitly skip remaining columns
    while (record.getFieldCursor() < record.schema->fields.size()) {
        record.serializeNull();
    }

    return fieldsForUpdate;
}

std::string K2Adapter::YBCTIDToString(std::shared_ptr<SqlOpExpr> ybctid_column_value) {
    if (!ybctid_column_value) {
        return "";
    }

    if (!ybctid_column_value->isValueType()) {
        K2LOG_W(log::k2Adapter, "ybctid_column_value value is not a Slice");
        throw std::invalid_argument("Non value type in ybctid_column_value");
    }

    std::shared_ptr<SqlValue> value = ybctid_column_value->getValue();
    if (value->type_ != SqlValue::ValueType::SLICE) {
        K2LOG_W(log::k2Adapter, "ybctid_column_value value is not a Slice");
        throw std::invalid_argument("ybctid_column_value value is not a Slice");
    }

    return value->data_.slice_val_;
}


Status K2Adapter::K2StatusToYBStatus(const k2::Status& status) {
    // TODO verify this translation with how the upper layers use the Status,
    // especially the Aborted status
    switch (status.code) {
        case 200: // OK Codes
        case 201:
        case 202:
            return Status();
        case 400: // Bad request
            return STATUS(InvalidCommand, status.message.c_str());
        case 403: // Forbidden, used to indicate AbortRequestTooOld in K23SI
            return STATUS(Aborted, status.message.c_str());
        case 404: // Not found
            return STATUS(NotFound, status.message.c_str());
        case 405: // Not allowed, indicates a bug in K2 usage or operation
        case 406: // Not acceptable, used to indicate BadFilterExpression
            return STATUS(InvalidArgument, status.message.c_str());
        case 408: // Timeout
            return STATUS(TimedOut, status.message.c_str());
        case 409: // Conflict, used to indicate K23SI transaction conflicts
            return STATUS(Aborted, status.message.c_str());
        case 410: // Gone, indicates a partition map error
            return STATUS(ServiceUnavailable, status.message.c_str());
        case 412: // Precondition failed, indicates a failed K2 insert operation
            return STATUS(AlreadyPresent, status.message.c_str());
        case 422: // Unprocessable entity, BadParameter in K23SI, indicates a bug in usage or operation
            return STATUS(InvalidArgument, status.message.c_str());
        case 500: // Internal error, indicates a bug in K2 code
            return STATUS(Corruption, status.message.c_str());
        case 503: // Service unavailable, indicates a partition is not assigned
            return STATUS(ServiceUnavailable, status.message.c_str());
        default:
            return STATUS(Corruption, "Unknown K2 status code");
    }
}

SqlOpResponse::RequestStatus K2Adapter::K2StatusToPGStatus(const k2::Status& status) {
    switch (status.code) {
        case 200: // OK Codes
        case 201:
        case 202:
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_OK;
        case 400: // Bad request
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_USAGE_ERROR;
        case 403: // Forbidden, used to indicate AbortRequestTooOld in K23SI
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RESTART_REQUIRED_ERROR;
        case 404: // Not found
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_OK; // TODO: correct?
        case 405: // Not allowed, indicates a bug in K2 usage or operation
        case 406: // Not acceptable, used to indicate BadFilterExpression
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_USAGE_ERROR;
        case 408: // Timeout
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        case 409: // Conflict, used to indicate K23SI transaction conflicts
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RESTART_REQUIRED_ERROR;
        case 410: // Gone, indicates a partition map error
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        case 412: // Precondition failed, indicates a failed K2 insert operation
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_DUPLICATE_KEY_ERROR;
        case 422: // Unprocessable entity, BadParameter in K23SI, indicates a bug in usage or operation
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_USAGE_ERROR;
        case 500: // Internal error, indicates a bug in K2 code
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        case 503: // Service unavailable, indicates a partition is not assigned
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        default:
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
    }
}

}  // namespace gate
}  // namespace k2pg
