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

#include "yb/pggate/pg_op.h"
#include "yb/pggate/pg_env.h"

namespace k2pg {
namespace gate {

    PgOpResult::PgOpResult(string&& data) : data_(move(data)) {
        LoadCache(data_, &row_count_, &row_iterator_);
    }

    PgOpResult::PgOpResult(string&& data, std::list<int64_t>&& row_orders)
        : data_(move(data)), row_orders_(move(row_orders)) {
        LoadCache(data_, &row_count_, &row_iterator_);
    }

    PgOpResult::~PgOpResult() {
    }

    void PgOpResult::LoadCache(const string& cache, int64_t *total_row_count, Slice *cursor) {
        // Setup the buffer to read the next set of tuples.
        CHECK(cursor->empty()) << "Existing cache is not yet fully read";
        *cursor = cache;

        // Read the number row_count in this set.
        int64_t this_count;
        size_t read_size = ReadNumber(cursor, &this_count);
        *total_row_count = this_count;
        cursor->remove_prefix(read_size);
    }

    //--------------------------------------------------------------------------------------------------
    // Read numbers.

    // This is not called ReadBool but ReadNumber because it is invoked from the TranslateNumber
    // template function similarly to the rest of numeric types.
    size_t PgOpResult::ReadNumber(Slice *cursor, bool *value) {
        *value = !!*reinterpret_cast<const bool*>(cursor->data());
        return sizeof(bool);
    }

    size_t PgOpResult::ReadNumber(Slice *cursor, int8_t *value) {
        *value = *reinterpret_cast<const int8_t*>(cursor->data());
        return sizeof(int8_t);
    }

    size_t PgOpResult::ReadNumber(Slice *cursor, uint8_t *value) {
        *value = *reinterpret_cast<const uint8*>(cursor->data());
        return sizeof(uint8_t);
    }

    size_t PgOpResult::ReadNumber(Slice *cursor, uint16 *value) {
        return ReadNumericValue(NetworkByteOrder::Load16, cursor, value);
    }

    size_t PgOpResult::ReadNumber(Slice *cursor, int16 *value) {
        return ReadNumericValue(NetworkByteOrder::Load16, cursor, reinterpret_cast<uint16*>(value));
    }

    size_t PgOpResult::ReadNumber(Slice *cursor, uint32 *value) {
        return ReadNumericValue(NetworkByteOrder::Load32, cursor, value);
    }

    size_t PgOpResult::ReadNumber(Slice *cursor, int32 *value) {
        return ReadNumericValue(NetworkByteOrder::Load32, cursor, reinterpret_cast<uint32*>(value));
    }

    size_t PgOpResult::ReadNumber(Slice *cursor, uint64 *value) {
        return ReadNumericValue(NetworkByteOrder::Load64, cursor, value);
    }

    size_t PgOpResult::ReadNumber(Slice *cursor, int64 *value) {
        return ReadNumericValue(NetworkByteOrder::Load64, cursor, reinterpret_cast<uint64*>(value));
    }

    size_t PgOpResult::ReadNumber(Slice *cursor, float *value) {
        uint32 int_value;
        size_t read_size = ReadNumericValue(NetworkByteOrder::Load32, cursor, &int_value);
        *value = *reinterpret_cast<float*>(&int_value);
        return read_size;
    }

    size_t PgOpResult::ReadNumber(Slice *cursor, double *value) {
        uint64 int_value;
        size_t read_size = ReadNumericValue(NetworkByteOrder::Load64, cursor, &int_value);
        *value = *reinterpret_cast<double*>(&int_value);
        return read_size;
    }

    // Read Text Data
    size_t PgOpResult::ReadBytes(Slice *cursor, char *value, int64_t bytes) {
        memcpy(value, cursor->data(), bytes);
        return bytes;
    }

    int64_t PgOpResult::NextRowOrder() {
        return row_orders_.size() > 0 ? row_orders_.front() : -1;
    }

    Status PgOpResult::WritePgTuple(const std::vector<PgExpr*>& targets, PgTuple *pg_tuple,
                                    int64_t *row_order) {
        int attr_num = 0;
        for (const PgExpr *target : targets) {
            if (!target->is_colref() && !target->is_aggregate()) {
                return STATUS(InternalError,
                            "Unexpected expression, only column refs or aggregates supported here");
            }
            if (target->opcode() == PgColumnRef::Opcode::PG_EXPR_COLREF) {
                attr_num = static_cast<const PgColumnRef *>(target)->attr_num();
            } else {
                attr_num++;
            }

            TranslateData(target, &row_iterator_, attr_num - 1, pg_tuple);
        }

        if (row_orders_.size()) {
            *row_order = row_orders_.front();
            row_orders_.pop_front();
        } else {
            *row_order = -1;
        }
        return Status::OK();
    }

    Status PgOpResult::ProcessSystemColumns() {
        if (syscol_processed_) {
            return Status::OK();
        }
        syscol_processed_ = true;

        for (int i = 0; i < row_count_; i++) {
            int64_t data_size;
            size_t read_size = ReadNumber(&row_iterator_, &data_size);
            row_iterator_.remove_prefix(read_size);

            ybctids_.emplace_back(row_iterator_.data(), data_size);
            row_iterator_.remove_prefix(data_size);
        }
        return Status::OK();
    }

    //--------------------------------------------------------------------------------------------------

    PgOp::PgOp(const PgSession::ScopedRefPtr& pg_session,
                    const PgTableDesc::ScopedRefPtr& table_desc,
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
        // TODO:: This logic is cloned from ybc and modify this based on K2 Doc storage
        // As of 09/25/2018, DocDB doesn't cache or keep any execution state for a statement, so we
        // have to call query execution every time.
        // - Normal SQL convention: Exec, Fetch, Fetch, ...
        // - Our SQL convention: Exec & Fetch, Exec & Fetch, ...
        // This refers to the sequence of operations between this layer and the underlying storage layer
        // server / DocDB layer, not to the sequence of operations between the PostgreSQL layer and this
        // layer.
        exec_status_ = SendRequest(force_non_bufferable);
        RETURN_NOT_OK(exec_status_);
        return RequestSent(response_.InProgress());
    }

    Status PgOp::GetResult(list<PgOpResult> *rowsets) {
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
        // Allocate batch operator, one per partition.
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
        // Populate collected information into protobuf requests before sending to DocDB.
        RETURN_NOT_OK(CreateRequests());

        // Currently, send and receive individual request of a batch is not yet supported
        // - Among statements, only queries by BASE-YBCTIDs need to be sent and received in batches
        //   to honor the order of how the BASE-YBCTIDs are kept in the database.
        // - For other type of statements, it could be more efficient to send them individually.
        SCHECK(wait_for_batch_completion_, InternalError,
                "Only send and receive the whole batch is supported");

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

    PgReadOp::PgReadOp(const PgSession::ScopedRefPtr& pg_session,
                            const PgTableDesc::ScopedRefPtr& table_desc,
                            std::unique_ptr<PgReadOpTemplate> read_op)
        : PgOp(pg_session, table_desc), template_op_(std::move(read_op)) {
    }

    void PgReadOp::ExecuteInit(const PgExecParameters *exec_params) {
        PgOp::ExecuteInit(exec_params);

        template_op_->set_return_paging_state(true);
        SetRequestPrefetchLimit();
        SetRowMark();
        SetReadTime();
        SetPartitionKey();
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

        // All information from the SQL request has been collected and setup. This code populate
        // Protobuf requests before sending them to DocDB. For performance reasons, requests are
        // constructed differently for different statement.
        if (template_op_->request().is_aggregate) {
            // Optimization for COUNT() operator.
            // - SELECT count(*) FROM sql_table;
            // - Multiple requests are created to run sequential COUNT() in parallel.
            return PopulateParallelSelectCountOps();

        } else if (template_op_->request().partition_column_values.size()> 0) {
            // Optimization for multiple hash keys.
            // - SELECT * FROM sql_table WHERE hash_c1 IN (1, 2, 3) AND hash_c2 IN (4, 5, 6);
            // - Multiple requests for differrent hash permutations / keys.
            return PopulateNextHashPermutationOps();

        } else {
            // No optimization.
            pgsql_ops_.push_back(template_op_);
            template_op_->set_active(true);
            active_op_count_ = 1;
            request_population_completed_ = true;
            return Status::OK();
        }
    }

    Status PgReadOp::PopulateDmlByYbctidOps(const vector<Slice> *ybctids) {
        // This function is called only when ybctids were returned from INDEX.
        //
        // NOTE on a typical process.
        // 1- Statement:
        //    SELECT xxx FROM <table> WHERE ybctid IN (SELECT ybctid FROM INDEX);
        //
        // 2- Select 1024 ybctids (prefetch limit) from INDEX.
        //
        // 3- ONLY ONE TIME: Create a batch of operators, one per partition.
        //    * Each operator has a clone requests from template_op_.
        //    * We will reuse the created operators & requests for the future batches of 1024 ybctids.
        //    * Certain fields in the protobuf requests MUST BE RESET for each batches.
        //
        // 4- Assign the selected 1024 ybctids to the batch of operators.
        //
        // 5- Send requests to storage servers to read data from <tab> associated with ybctid values.
        //
        // 6- Repeat step 2 thru 5 for the next batch of 1024 ybctids till done.
        RETURN_NOT_OK(InitializeYbctidOperators());

        // Begin a batch of ybctids.
        end_of_data_ = false;

        // Assign ybctid values.

        // TODO: need to see how to find K2 doc partitions and assign them by partitions
        // TODO: update batch operations for different partitions
 
        // Done creating request, but not all partition or operator has arguments (inactive).
        MoveInactiveOpsOutside();
        request_population_completed_ = true;

        return Status::OK();
    }

    Status PgReadOp::InitializeYbctidOperators() {
        int op_count = table_desc_->GetPartitionCount();

        if (batch_row_orders_.size() == 0) {
            // First batch:
            // - Create operators.
            // - Allocate row orders for each storage server.
            // - Protobuf fields in requests are not yet set so not needed to be cleared.
            RETURN_NOT_OK(ClonePgsqlOps(op_count));
            batch_row_orders_.resize(op_count);

            // To honor the indexing order of ybctid values, for each batch of ybctid-binds, select all rows
            // in the batch and then order them before returning result to Postgres layer.
            wait_for_batch_completion_ = true;

        } else {
            // Second and later batches: Reuse all state variables.
            // - Clear row orders for this batch to be set later.
            // - Clear protobuf fields ybctids and others before reusing them in this batch.
            RETURN_NOT_OK(ResetInactivePgsqlOps());
        }
        return Status::OK();
    }

    Status PgReadOp::PopulateNextHashPermutationOps() {
        // TODO: do we need logic to build operations based on partition hash?

        return Status::OK();
    }

    Status PgReadOp::PopulateParallelSelectCountOps() {
        // TODO:: check how do we create operation requests based on k2 partitions

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
                auto& req = read_op->request();

                // Set up paging state for next request.
                // A query request can be nested, and paging state belong to the innermost query which is
                // the read operator that is operated first and feeds data to other queries.
                SqlOpReadRequest *innermost_req = &req;
                // TODO:: enable the logic for index once we add secondary index support
                while (innermost_req->index_request != nullptr) {
                     innermost_req = innermost_req->index_request;
                }
                *innermost_req->paging_state = std::move(*res.paging_state);
            }

            if (has_more_arg) {
                has_more_data = true;
            } else {
                read_op->set_active(false);
            }
        }

        // If partition key of storage server to scan is specified, then we should be done.  This is because,
        // curently, only `BACKFILL INDEX ... PARTITION ...` statements set `partition_key`, and they scan
        // a single storage server.
        if (partition_key_) {
            has_more_data = false;
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
        // Predict the maximum prefetch-limit using the associated gflags.
        SqlOpReadRequest& req = template_op_->request();
        int predicted_limit = default_ysql_prefetch_limit;
        if (!req.is_forward_scan) {
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
        req.limit = limit_count;
    }

    void PgReadOp::SetRowMark() {
        SqlOpReadRequest& req = template_op_->request();

        if (exec_params_.rowmark < 0) {
            req.row_mark_type = RowMarkType::ROW_MARK_ABSENT;
        } else {
            req.row_mark_type = static_cast<RowMarkType>(exec_params_.rowmark);
        }
    }

    void PgReadOp::SetPartitionKey() {
        if (exec_params_.partition_key != NULL) {
            partition_key_ = std::string(exec_params_.partition_key);
            // TODO: add logic for k2 partitions
        }
    }

    Status PgReadOp::ResetInactivePgsqlOps() {
        // Clear the existing ybctids.
        for (int op_index = active_op_count_; op_index < pgsql_ops_.size(); op_index++) {
            SqlOpReadRequest& read_req = GetReadOp(op_index)->request();
            read_req.ybctid_column_value = nullptr;
            read_req.paging_state = nullptr;
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

    PgWriteOp::PgWriteOp(const PgSession::ScopedRefPtr& pg_session,
                            const PgTableDesc::ScopedRefPtr& table_desc,
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
