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

#include <boost/algorithm/string.hpp>

#include "yb/common/type/decimal.h"
#include "yb/pggate/pg_op.h"
#include "yb/pggate/pg_env.h"
#include "yb/pggate/catalog/sql_catalog_defaults.h" // for the table/index name constants
#include <string>

namespace k2pg {
namespace gate {
using yb::util::Decimal;

template <typename T>  // static checker to see if a type is numeric (i.e. we can get it out from field by value)
constexpr bool isNumericType() { return std::is_arithmetic<T>::value || std::is_enum<T>::value; }

// template resolution for types that are not supported
template <typename T>
std::enable_if_t<!isNumericType<T>(), Status>
TranslateUserCol(int index, const YBCPgTypeEntity* type_entity, const PgTypeAttrs* type_attrs, std::optional<T> field, PgTuple* pg_tuple) {
    return STATUS(InternalError, "unsupported type for user column");
}

// translate numeric types (integers, bool, floats)
template <typename T>
std::enable_if_t<isNumericType<T>(), Status>
TranslateUserCol(int index, const YBCPgTypeEntity* type_entity, const PgTypeAttrs* type_attrs, std::optional<T> field, PgTuple* pg_tuple) {
    switch (type_entity->yb_type) {
        case YB_YQL_DATA_TYPE_INT8: {
            int8_t val = (int8_t)field.value();
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&val, sizeof(val), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_INT16: {
            int16_t val = (int16_t)field.value();
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&val, sizeof(val), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_INT32: {
            int32_t val = (int32_t)field.value();
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&val, sizeof(val), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_INT64: {
            int64_t val = (int64_t)field.value();
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&val, sizeof(val), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_UINT32: {
            uint32_t val = (uint32_t)field.value();
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&val, sizeof(val), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_UINT64: {
            uint64_t val = (uint64_t)field.value();
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&val, sizeof(val), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_BOOL: {
            bool val = (bool)field.value();
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&val, sizeof(val), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_FLOAT: {
            float val = (float)field.value();
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&val, sizeof(val), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_DOUBLE: {
            double val = (double)field.value();
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&val, sizeof(val), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_TIMESTAMP: {
            int64_t val = (int64_t)field.value();
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&val, sizeof(val), type_attrs));
            break;
        }
        default:
            LOG(DFATAL) << "Internal error: unsupported type " << type_entity->yb_type;
            return STATUS(InternalError, "unsupported type for user column");
    }
    return Status::OK();
}

// translate k2::String -based types
template <>
Status
TranslateUserCol<k2::String>(int index, const YBCPgTypeEntity* type_entity, const PgTypeAttrs* type_attrs, std::optional<k2::String> field, PgTuple* pg_tuple) {
    switch (type_entity->yb_type) {
        case YB_YQL_DATA_TYPE_BINARY: {
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(field.value().c_str(), field.value().size(), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_STRING: {
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(field.value().c_str(), field.value().size(), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_DECIMAL: {
            // TODO use SKV/c++ -native decimal64 type
            std::string serialized_decimal(field.value().c_str(), field.value().size());
            Decimal yb_decimal;
            if (!yb_decimal.DecodeFromComparable(serialized_decimal).ok()) {
                LOG(FATAL) << "Failed to deserialize DECIMAL from " << serialized_decimal;
                return STATUS(InternalError, "failed to deserialize DECIMAL");
            }
            auto plaintext = yb_decimal.ToString();

            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(plaintext.c_str(), field.value().size(), type_attrs));
            break;
        }
        case YB_YQL_DATA_TYPE_VARINT:
        case YB_YQL_DATA_TYPE_INET:
        case YB_YQL_DATA_TYPE_LIST:
        case YB_YQL_DATA_TYPE_MAP:
        case YB_YQL_DATA_TYPE_SET:
        case YB_YQL_DATA_TYPE_UUID:
        case YB_YQL_DATA_TYPE_TIMEUUID:
        case YB_YQL_DATA_TYPE_TUPLE:
        case YB_YQL_DATA_TYPE_TYPEARGS:
        case YB_YQL_DATA_TYPE_USER_DEFINED_TYPE:
        case YB_YQL_DATA_TYPE_FROZEN:
        case YB_YQL_DATA_TYPE_DATE:  // Not used for PG storage
        case YB_YQL_DATA_TYPE_TIME:  // Not used for PG storage
        case YB_YQL_DATA_TYPE_JSONB:
        case YB_YQL_DATA_TYPE_UINT8:
        case YB_YQL_DATA_TYPE_UINT16:
        default:
            LOG(DFATAL) << "Internal error: unsupported type " << type_entity->yb_type;
            return STATUS(InternalError, "unsupported type for user column");
    }
    return Status::OK();
}

template<typename T>
Status TranslateSysCol(int attr_num, std::optional<T> field, PgTuple* pg_tuple) {
    return STATUS(InternalError, "unsupported type for system column");
}

template<>
Status TranslateSysCol<int64_t>(int attr_num, std::optional<int64_t> field, PgTuple* pg_tuple) {
    switch (attr_num) {
        case static_cast<int>(PgSystemAttrNum::kSelfItemPointer):
            pg_tuple->syscols()->ctid = field.value();
            break;
        case static_cast<int>(PgSystemAttrNum::kObjectId):
            pg_tuple->syscols()->oid = field.value();
            break;
        case static_cast<int>(PgSystemAttrNum::kMinTransactionId):
            pg_tuple->syscols()->xmin = field.value();
            break;
        case static_cast<int>(PgSystemAttrNum::kMinCommandId):
            pg_tuple->syscols()->cmin = field.value();
            break;
        case static_cast<int>(PgSystemAttrNum::kMaxTransactionId):
            pg_tuple->syscols()->xmax = field.value();
            break;
        case static_cast<int>(PgSystemAttrNum::kMaxCommandId):
            pg_tuple->syscols()->cmax = field.value();
            break;
        case static_cast<int>(PgSystemAttrNum::kTableOid):
            pg_tuple->syscols()->tableoid = field.value();
            break;
        default:
            return STATUS(InternalError, "system column is not int64_t compatible");
    }
    return Status::OK();
}

template <>
Status TranslateSysCol<k2::String>(int attr_num, std::optional<k2::String> field, PgTuple* pg_tuple) {
    switch (attr_num) {
        case static_cast<int>(PgSystemAttrNum::kYBTupleId): {
            k2::String& val = field.value();
            pg_tuple->Write(&pg_tuple->syscols()->ybctid, (const uint8_t*)val.c_str(), val.size());
            break;
        }
        case static_cast<int>(PgSystemAttrNum::kYBIdxBaseTupleId): {
            k2::String& val = field.value();
            pg_tuple->Write(&pg_tuple->syscols()->ybbasectid, (const uint8_t*)val.c_str(), val.size());
            break;
        }
        default:
            return STATUS(InternalError, "system column is not string compatible");
    }
    return Status::OK();
}

template<typename T>
void FieldParser(std::optional<T> field, const k2::String& fieldName, const std::unordered_map<std::string, PgExpr*>& targets_by_name, PgTuple* pg_tuple, Status& result) {
    auto iter = targets_by_name.find(fieldName.c_str());
    if (iter == targets_by_name.end()) {
        if (k2pg::sql::catalog::CatalogConsts::TABLE_ID_COLUMN_NAME == fieldName.c_str() ||
            k2pg::sql::catalog::CatalogConsts::INDEX_ID_COLUMN_NAME == fieldName.c_str()) {
            result = Status::OK();
        }
        else {
            LOG(DFATAL) << "Encountered field " << fieldName << ", without target reference";
            result = STATUS(InternalError, "Encountered field without target reference");
        }
        return;
    }
    if (!iter->second->is_colref()) {
        result= STATUS(InternalError, "Unexpected expression, only column refs supported in SKV");
        return;
    }
    const PgColumnRef* target = (PgColumnRef*)iter->second;
    int attr_num = target->attr_num();

    if (attr_num < 0) {
        if (!field) {
            result= STATUS(InternalError, "Null system column encountered");
            return;
        }
        result = TranslateSysCol(attr_num, std::move(field), pg_tuple);
    }
    else {
        if (!field) {
            pg_tuple->WriteNull(attr_num-1);
            result = Status::OK();
        }
        else {
            result = TranslateUserCol(attr_num-1, target->type_entity(), target->type_attrs(), std::move(field), pg_tuple);
        }
    }
}

PgOpResult::PgOpResult(std::vector<k2::dto::SKVRecord>&& data) : data_(std::move(data)) {
}

PgOpResult::PgOpResult(std::vector<k2::dto::SKVRecord>&& data, std::list<int64_t>&& row_orders):
    data_(std::move(data)), row_orders_(move(row_orders)) {
}

PgOpResult::~PgOpResult() {
}

int64_t PgOpResult::NextRowOrder() {
    return row_orders_.size() > 0 ? row_orders_.front() : -1;
}

// Get the postgres tuple from this batch.
Status PgOpResult::WritePgTuple(const std::vector<PgExpr *> &targets, const std::unordered_map<std::string, PgExpr*>& targets_by_name, PgTuple *pg_tuple, int64_t *row_order) {
    Status result;
    FOR_EACH_RECORD_FIELD(data_[nextToConsume_], FieldParser, targets_by_name, pg_tuple, result);

    if (row_orders_.size()) {
        *row_order = row_orders_.front();
        row_orders_.pop_front();
    } else {
        *row_order = -1;
    }
    ++nextToConsume_;

    return result;
}

// Get system columns' values from this batch.
// Currently, we only have ybctids, but there could be more.
Status PgOpResult::ProcessSystemColumns() {
    if (syscol_processed_) {
        return Status::OK();
    }
    syscol_processed_ = true;
    for (auto& rec: data_) {
        ybctid_strings_.push_back(rec.getKey().partitionKey);
        ybctids_.emplace_back(ybctid_strings_.back().c_str(), ybctid_strings_.back().size());
    }
    return Status::OK();
}

//--------------------------------------------------------------------------------------------------

PgOp::PgOp(const std::shared_ptr<PgSession>& pg_session,
                const std::shared_ptr<PgTableDesc>& table_desc,
                const PgObjectId& relation_id)
    : pg_session_(pg_session),  table_desc_(table_desc), relation_id_(relation_id) {
    exec_params_.limit_count = default_ysql_prefetch_limit;
    exec_params_.limit_offset = 0;
    exec_params_.limit_use_default = true;
}

PgOp::~PgOp() {
    // Wait for result in case request was sent.
    // Operation can be part of transaction it is necessary to complete it before transaction commit.
    if (response_.InProgress()) {
        VLOG(1) << "Waiting for in progress response ";
        __attribute__((unused)) auto status = response_.GetStatus();
    }
}

void PgOp::ExecuteInit(const PgExecParameters *exec_params) {
    end_of_data_ = false;
    if (exec_params) {
        exec_params_ = *exec_params;
    }
}

Result<RequestSent> PgOp::Execute(bool force_non_bufferable) {
    // SKV is stateless and we have to call query execution every time, i.e., Exec & Fetch, Exec & Fetch
    exec_status_ = SendRequest(force_non_bufferable);
    RETURN_NOT_OK(exec_status_);
    return RequestSent(response_.InProgress());
}

Status PgOp::GetResult(std::list<PgOpResult> *rowsets) {
    // If the execution has error, return without reading any rows.
    RETURN_NOT_OK(exec_status_);

    if (!end_of_data_) {
        // Send request now in case prefetching was suppressed.
        if (suppress_next_result_prefetching_ && !response_.InProgress()) {
            exec_status_ = SendRequest(true /* force_non_bufferable */);
            RETURN_NOT_OK(exec_status_);
        }

        DCHECK(response_.InProgress());
        auto rows = VERIFY_RESULT(ProcessResponse(response_.GetStatus()));
        // In case ProcessResponse doesn't fail with an error
        // it should return non empty rows and/or set end_of_data_.
        DCHECK(!rows.empty() || end_of_data_);
        rowsets->splice(rowsets->end(), rows);
        // Prefetch next portion of data if needed.
        if (!(end_of_data_ || suppress_next_result_prefetching_)) {
            exec_status_ = SendRequest(true /* force_non_bufferable */);
            RETURN_NOT_OK(exec_status_);
        }
    }

    return Status::OK();
}

Result<int32_t> PgOp::GetRowsAffectedCount() const {
    RETURN_NOT_OK(exec_status_);
    DCHECK(end_of_data_);
    return rows_affected_count_;
}

Status PgOp::ClonePgsqlOps(int op_count) {
    SCHECK(op_count > 0, InternalError, "Table must have at least one partition");
    if (pgsql_ops_.size() < op_count) {
        pgsql_ops_.resize(op_count);
        for (int idx = 0; idx < op_count; idx++) {
        pgsql_ops_[idx] = CloneFromTemplate();

        // Initialize as inactive. Turn it on when setup argument for a specific partition.
        pgsql_ops_[idx]->set_active(false);
        }

        // Set parallism_level_ to maximum possible of operators to be executed at one time.
        parallelism_level_ = pgsql_ops_.size();
    }

    return Status::OK();
}

void PgOp::MoveInactiveOpsOutside() {
    // Move inactive op to the end.
    const int total_op_count = pgsql_ops_.size();
    bool has_sorting_order = !batch_row_orders_.empty();
    int left_iter = 0;
    int right_iter = total_op_count - 1;
    while (true) {
        // Advance left iterator.
        while (left_iter < total_op_count && pgsql_ops_[left_iter]->is_active()) left_iter++;

        // Advance right iterator.
        while (right_iter >= 0 && !pgsql_ops_[right_iter]->is_active()) right_iter--;

        // Move inactive operator to the end by swapping the pointers.
        if (left_iter < right_iter) {
        std::swap(pgsql_ops_[left_iter], pgsql_ops_[right_iter]);
        if (has_sorting_order) {
            std::swap(batch_row_orders_[left_iter], batch_row_orders_[right_iter]);
        }
        } else {
            break;
        }
    }

    // Set active op count.
    active_op_count_ = left_iter;
}

Status PgOp::SendRequest(bool force_non_bufferable) {
    DCHECK(exec_status_.ok());
    DCHECK(!response_.InProgress());
    exec_status_ = SendRequestImpl(force_non_bufferable);
    return exec_status_;
}

Status PgOp::SendRequestImpl(bool force_non_bufferable) {
    // Populate collected information into requests before sending to SKV.
    RETURN_NOT_OK(CreateRequests());

    // Send at most "parallelism_level_" number of requests at one time.
    int32_t send_count = std::min(parallelism_level_, active_op_count_);
    response_ = VERIFY_RESULT(pg_session_->RunAsync(pgsql_ops_.data(), send_count, relation_id_,
                                                    &read_time_, force_non_bufferable));

    return Status::OK();
}

Result<std::list<PgOpResult>> PgOp::ProcessResponse(const Status& status) {
    // Check operation status.
    DCHECK(exec_status_.ok());
    exec_status_ = status;
    if (exec_status_.ok()) {
        auto result = ProcessResponseImpl();
        if (result.ok()) {
        return result;
        }
        exec_status_ = result.status();
    }
    return exec_status_;
}

Result<std::list<PgOpResult>> PgOp::ProcessResponseResult() {
    VLOG(1) << __PRETTY_FUNCTION__ << ": Received response for request " << this;

    // Check for errors reported by storage server.
    for (int op_index = 0; op_index < active_op_count_; op_index++) {
        RETURN_NOT_OK(pg_session_->HandleResponse(*pgsql_ops_[op_index], PgObjectId()));
    }

    // Process data coming from storage server.
    std::list<PgOpResult> result;
    bool no_sorting_order = batch_row_orders_.size() == 0;

    rows_affected_count_ = 0;
    for (int op_index = 0; op_index < active_op_count_; op_index++) {
        PgOpTemplate *pgsql_op = pgsql_ops_[op_index].get();
        // Get total number of rows that are operated on.
        rows_affected_count_ += pgsql_op->response().rows_affected_count;

        // Get contents.
        if (!pgsql_op->rows_data().empty()) {
        if (no_sorting_order) {
            result.emplace_back(pgsql_op->rows_data());
        } else {
            result.emplace_back(pgsql_op->rows_data(), std::move(batch_row_orders_[op_index]));
        }
        }
    }

    return result;
}

//-------------------------------------------------------------------------------------------------

PgReadOp::PgReadOp(const std::shared_ptr<PgSession>& pg_session,
                        const std::shared_ptr<PgTableDesc>& table_desc,
                        std::unique_ptr<PgReadOpTemplate> read_op)
    : PgOp(pg_session, table_desc), template_op_(std::move(read_op)) {
}

void PgReadOp::ExecuteInit(const PgExecParameters *exec_params) {
    PgOp::ExecuteInit(exec_params);

    template_op_->set_return_paging_state(true);
    SetRequestPrefetchLimit();
    SetRowMark();
    SetReadTime();
}

void PgReadOp::SetReadTime() {
    read_time_ = exec_params_.read_time;
}

Result<std::list<PgOpResult>> PgReadOp::ProcessResponseImpl() {
    // Process result from storage server and check result status.
    auto result = VERIFY_RESULT(ProcessResponseResult());

    // Process paging state and check status.
    RETURN_NOT_OK(ProcessResponsePagingState());
    return result;
}

Status PgReadOp::CreateRequests() {
    if (request_population_completed_) {
        return Status::OK();
    }

    // No optimization.
    // TODO: create separate requests for different partitions once SKV partition information is available
    pgsql_ops_.push_back(template_op_);
    template_op_->set_active(true);
    active_op_count_ = 1;
    request_population_completed_ = true;
    return Status::OK();
}

Status PgReadOp::ProcessResponsePagingState() {
    // For each read_op, set up its request for the next batch of data or make it in-active.
    bool has_more_data = false;
    int32_t send_count = std::min(parallelism_level_, active_op_count_);

    for (int op_index = 0; op_index < send_count; op_index++) {
        PgReadOpTemplate *read_op = GetReadOp(op_index);
        auto& res = read_op->response();
        // Check for completion.
        bool has_more_arg = false;
        if (res.paging_state != nullptr) {
            has_more_arg = true;
            auto req = read_op->request();

            // Set up paging state for next request.
            // A query request can be nested, and paging state belong to the innermost query which is
            // the read operator that is operated first and feeds data to other queries.
            SqlOpReadRequest *innermost_req = req.get();
            while (innermost_req->index_request != nullptr) {
                    innermost_req = innermost_req->index_request.get();
            }
            *innermost_req->paging_state = std::move(*res.paging_state);
        }

        if (has_more_arg) {
            has_more_data = true;
        } else {
            read_op->set_active(false);
        }
    }

    if (has_more_data || send_count < active_op_count_) {
        // Move inactive ops to the end of pgsql_ops_ to make room for new set of arguments.
        MoveInactiveOpsOutside();
        end_of_data_ = false;
    } else {
        // There should be no active op left in queue.
        active_op_count_ = 0;
        end_of_data_ = request_population_completed_;
    }

    return Status::OK();
}

void PgReadOp::SetRequestPrefetchLimit() {
    // Predict the maximum prefetch-limit
    std::shared_ptr<SqlOpReadRequest> req = template_op_->request();
    int predicted_limit = default_ysql_prefetch_limit;
    if (!req->is_forward_scan) {
        // Backward scan is slower than forward scan, so predicted limit is a smaller number.
        predicted_limit = predicted_limit * default_ysql_backward_prefetch_scale_factor;
    }

    // System setting has to be at least 1 while user setting (LIMIT clause) can be anything that
    // is allowed by SQL semantics.
    if (predicted_limit < 1) {
        predicted_limit = 1;
    }

    // Use statement LIMIT(count + offset) if it is smaller than the predicted limit.
    int64_t limit_count = exec_params_.limit_count + exec_params_.limit_offset;
    suppress_next_result_prefetching_ = true;
    if (exec_params_.limit_use_default || limit_count > predicted_limit) {
        limit_count = predicted_limit;
        suppress_next_result_prefetching_ = false;
    }
    req->limit = limit_count;
}

void PgReadOp::SetRowMark() {
    std::shared_ptr<SqlOpReadRequest> req = template_op_->request();

    if (exec_params_.rowmark < 0) {
        req->row_mark_type = RowMarkType::ROW_MARK_ABSENT;
    } else {
        req->row_mark_type = static_cast<RowMarkType>(exec_params_.rowmark);
    }
}

Status PgReadOp::ResetInactivePgsqlOps() {
    // Clear the existing requests.
    for (int op_index = active_op_count_; op_index < pgsql_ops_.size(); op_index++) {
        std::shared_ptr<SqlOpReadRequest> read_req = GetReadOp(op_index)->request();
        read_req->ybctid_column_value = nullptr;
        read_req->paging_state = nullptr;
    }

    // Clear row orders.
    if (batch_row_orders_.size() > 0) {
        for (int op_index = active_op_count_; op_index < pgsql_ops_.size(); op_index++) {
            batch_row_orders_[op_index].clear();
        }
    }

    return Status::OK();
}

//--------------------------------------------------------------------------------------------------

PgWriteOp::PgWriteOp(const std::shared_ptr<PgSession>& pg_session,
                        const std::shared_ptr<PgTableDesc>& table_desc,
                        const PgObjectId& relation_id,
                        std::unique_ptr<PgWriteOpTemplate> write_op)
    : PgOp(pg_session, table_desc, relation_id),
    write_op_(std::move(write_op)) {
}

Result<std::list<PgOpResult>> PgWriteOp::ProcessResponseImpl() {
    // Process result from doc api and check result status.
    auto result = VERIFY_RESULT(ProcessResponseResult());

    // End execution and return result.
    end_of_data_ = true;
    VLOG(1) << __PRETTY_FUNCTION__ << ": Received response for request " << this;
    return result;
}

Status PgWriteOp::CreateRequests() {
    if (request_population_completed_) {
        return Status::OK();
    }

    // Setup a singular operator.
    // TODO: create multiple requests for multiple partitions if the information is available
    pgsql_ops_.push_back(write_op_);
    write_op_->set_active(true);
    active_op_count_ = 1;
    request_population_completed_ = true;

    // Log non buffered request.
    VLOG_IF(1, response_.InProgress()) << __PRETTY_FUNCTION__ << ": Sending request for " << this;
    return Status::OK();
}

void PgWriteOp::SetWriteTime(const uint64_t write_time) {
    write_time_ = write_time;
}

}  // namespace gate
}  // namespace k2pg
