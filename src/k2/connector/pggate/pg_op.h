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
#include <list>
#include <vector>

#include "common/type/slice.h"
#include "common/status.h"
#include "entities/entity_ids.h"
#include "entities/schema.h"
#include "entities/type.h"
#include "entities/value.h"
#include "entities/expr.h"
#include "entities/table.h"

#include "pggate/pg_gate_defaults.h"
#include "pggate/pg_tuple.h"
#include "pggate/pg_session.h"
#include "pggate/pg_tabledesc.h"
#include "pggate/pg_op_api.h"

#include "k2_includes.h"
#include "k2_log.h"

namespace k2pg {
namespace gate {
using k2pg::Status;
using k2pg::Slice;
using k2pg::sql::PgObjectId;

//--------------------------------------------------------------------------------------------------
// PgOpResult represents a batch of rows in ONE reply from storage layer.
class PgOpResult {
public:
    explicit PgOpResult(std::vector<k2::dto::SKVRecord>&& data);
    PgOpResult(std::vector<k2::dto::SKVRecord> &&data, std::list<int64_t> &&row_orders);
    ~PgOpResult();

    // Get the order of the next row in this batch.
    int64_t NextRowOrder();

    // End of this batch.
    bool is_eof() const {
        return nextToConsume_ >= data_.size();
    }

    // Get the postgres tuple from this batch.
    CHECKED_STATUS
    WritePgTuple(const std::vector<PgExpr *>& targets,
                 const std::unordered_map<std::string, PgExpr*>& targets_by_name,
                 PgTuple *pg_tuple,
                 int64_t *row_order);

    // Get system columns' values from this batch.
    // Currently, we only have k2pgctids, but there could be more.
    CHECKED_STATUS ProcessSystemColumns();

    // Access function to k2pgctids value in this batch.
    // Sys columns must be processed before this function is called.
    const std::vector<Slice>& k2pgctids() const {
        DCHECK(syscol_processed_) << "System columns are not yet setup";
        return k2pgctids_;
    }

    // For secondary index read result, where the caller need to get a batch of base row's Id/k2pgctid
    void GetBaseRowIdBatch(std::vector<std::string>& baseRowIdBatch);

private:

    // Data selected from k2 storage.
    // TODO: refactor this based on SKV payload
    std::vector<k2::dto::SKVRecord> data_;

    // The indexing order of the row in this batch.
    // These order values help to identify the row order across all batches
    // the size is based on the returning data and thus a list instead of an array is used
    std::list<int64_t> row_orders_;

    // Cursor to the next record to consume
    int64_t nextToConsume_ = 0;

    // flag used to tell if the system columns have been processed.
    bool syscol_processed_ = false;

    // store computed k2pgctids;
    std::vector<k2::String> k2pgctid_strings_;
    //... and also as slices so that we can return them to the PG Gate API
    std::vector<Slice> k2pgctids_;

private :

    PgOpResult(const PgOpResult &) = delete;
    PgOpResult& operator=(const PgOpResult&) = delete;

    friend class PgSelectIndex;
};

class PgOp : public std::enable_shared_from_this<PgOp> {
public:
    typedef std::shared_ptr<PgOp> SharedPtr;

    // Constructors & Destructors.
    explicit PgOp(const std::shared_ptr<PgSession>& pg_session,
                    const std::shared_ptr<PgTableDesc>& table_desc,
                    const PgObjectId& relation_id = PgObjectId());
    virtual ~PgOp();

    // Initialize sql operator.
    virtual void ExecuteInit(const PgExecParameters *exec_params);

    // Execute the op. Return true if the request has been sent and is awaiting the result.
    virtual Result<bool> Execute();

    // Instruct this sql_op_ to abandon execution and querying data by setting end_of_data_ to 'true'.
    // - This op will not send request to storage layer.
    // - This op will return empty result-set when being requested for data.
    void AbandonExecution() {
        end_of_data_ = true;
    }

    // Get the result of the op. No rows will be added to rowsets in case end of data reached.
    CHECKED_STATUS GetResult(std::list<PgOpResult> *rowsets);
    Result<int32_t> GetRowsAffectedCount() const;

    virtual CHECKED_STATUS PopulateDmlByRowIdOps(const std::vector<std::string>& k2pgctids) = 0;

protected:
    // Create requests using the collected informtion for this PG operator.
    virtual CHECKED_STATUS CreateRequests() = 0;

    // Create operators.
    // - Each operator is used for one request.
    // - When parallelism by partition is applied, each operator is associated with one partition,
    //   and each operator has a batch of arguments that belong to that partition.
    CHECKED_STATUS ClonePgsqlOps(int op_count);

    // Only active operators are kept in the active range [0, active_op_count_)
    // - Not execute operators that are outside of range [0, active_op_count_).
    // - Sort the operators in "pgsql_ops_" to move "inactive" operators to the end of the list.
    void MoveInactiveOpsOutside();

    // Clone READ or WRITE "template_op_" into new operators.
    virtual std::unique_ptr<PgOpTemplate> CloneFromTemplate() = 0;

    // Process the result set in server response.
    Result<std::list<PgOpResult>> ProcessResponseResult();

private:
    CHECKED_STATUS SendRequest();

    virtual CHECKED_STATUS SendRequestImpl();

    Result<std::list<PgOpResult>> ProcessResponse(const Status& exec_status);

    virtual Result<std::list<PgOpResult>> ProcessResponseImpl() = 0;

//----------------------------------- Data Members -----------------------------------------------
protected:
    // Add expressions that are belong to this PgOp.
    void AddExpr(std::unique_ptr<PgExpr> expr);

    // Session control.
    std::shared_ptr<PgSession> pg_session_;

    // Operation time. This time is set at the start and must stay the same for the lifetime of the
    // operation to ensure that it is operating on one snapshot.
    uint64_t read_time_ = 0;

    // Target table.
    std::shared_ptr<PgTableDesc> table_desc_;
    PgObjectId relation_id_;

    // Exec control parameters.
    PgExecParameters exec_params_;

    // Suppress sending new request after processing response.
    // Next request will be sent in case upper level will ask for additional data.
    bool suppress_next_result_prefetching_ = false;

    // Populated operation request.
    std::vector<std::shared_ptr<PgOpTemplate>> pgsql_ops_;

    // Number of active operators in the pgsql_ops_ list.
    int32_t active_op_count_ = 0;

    // Indicator for completing all request populations.
    bool request_population_completed_ = false;

    // Future object to fetch a response from storage after sending a request.
    // Only one RunAsync() can be called to sent storage at a time.
    CBFuture<Status> requestAsyncRunResult_;

    // Executed row count.
    int32_t rows_affected_count_ = 0;

    // Whether all requested data by the statement has been received or there's a run-time error.
    bool end_of_data_ = false;

    // The order number of each request when batching arguments.
    std::vector<std::list<int64_t>> batch_row_orders_;

    // Parallelism level.
    // - This is the maximum number of read/write requests being sent to servers at one time.
    // - When it is 1, there's no optimization. Available requests is executed one at a time.
    int32_t parallelism_level_ = 100;

    // Expression list to be destroyed as soon as the PgOp is garbage collected
    std::list<std::unique_ptr<PgExpr>> exprs_;

private:
    // Result set either from selected or returned targets is cached in a list of strings.
    // Querying state variables.
    Status exec_status_ = Status::OK();
};

//--------------------------------------------------------------------------------------------------

class PgReadOp : public PgOp {
public:
    // Public types.
    typedef std::shared_ptr<PgReadOp> SharedPtr;

    // Constructors & Destructors.
    PgReadOp(const std::shared_ptr<PgSession>& pg_session,
                const std::shared_ptr<PgTableDesc>& table_desc,
                std::unique_ptr<PgReadOpTemplate> read_op);

    void ExecuteInit(const PgExecParameters *exec_params) override;

private:
    // Create requests using template_op_.
    CHECKED_STATUS CreateRequests() override;

    // Process response from SKV
    Result<std::list<PgOpResult>> ProcessResponseImpl() override;

    // Process response paging state from SKV
    CHECKED_STATUS ProcessResponsePagingState();

    // Reset pgsql operators before reusing them with new arguments / inputs from Postgres.
    CHECKED_STATUS ResetInactivePgsqlOps();

    // initialize op by partitions
    CHECKED_STATUS InitializeRowIdOperators();

    CHECKED_STATUS PopulateDmlByRowIdOps(const std::vector<std::string>& k2pgctids) override;

    // Analyze options and pick the appropriate prefetch limit.
    void SetRequestPrefetchLimit();

    // set the global limit on the query
    void SetRequestTotalLimit();

    // Set the row_mark_type field of our read request based on our exec control parameter.
    void SetRowMark();

    // Set the read_time for our read request based on our exec control parameter.
    void SetReadTime();

    // Clone the template into actual requests to be sent to server.
    std::unique_ptr<PgOpTemplate> CloneFromTemplate() override {
        return template_op_->DeepCopy();
    }

    // Get the read_op for a specific operation index from pgsql_ops_.
    PgReadOpTemplate *GetReadOp(int op_index) {
        return static_cast<PgReadOpTemplate *>(pgsql_ops_[op_index].get());
    }

    //----------------------------------- Data Members -----------------------------------------------

    // Template operation, used to fill in pgsql_ops_ by either assigning or cloning.
    std::shared_ptr<PgReadOpTemplate> template_op_;
};

//--------------------------------------------------------------------------------------------------

class PgWriteOp : public PgOp {
public:
    // Public types.
    typedef std::shared_ptr<PgWriteOp> SharedPtr;

    // Constructors & Destructors.
    PgWriteOp(const std::shared_ptr<PgSession>& pg_session,
                const std::shared_ptr<PgTableDesc>& table_desc,
                const PgObjectId& table_object_id,
                std::unique_ptr<PgWriteOpTemplate> write_op);

    // Set write time.
    void SetWriteTime(const uint64_t write_time);

private:
    // Process response implementation.
    Result<std::list<PgOpResult>> ProcessResponseImpl() override;

    // Create requests using template_op (write_op).
    CHECKED_STATUS CreateRequests() override;

    CHECKED_STATUS PopulateDmlByRowIdOps(const std::vector<std::string>& k2pgctids) override;

    // Get WRITE operator for a specific operator index in pgsql_ops_.
    PgWriteOpTemplate *GetWriteOp(int op_index) {
        return static_cast<PgWriteOpTemplate *>(pgsql_ops_[op_index].get());
    }

    // Clone user data from template to actual requests.
    std::unique_ptr<PgOpTemplate> CloneFromTemplate() override {
        return write_op_->DeepCopy();
    }

    //----------------------------------- Data Members -----------------------------------------------
    // Template operation all write ops.
    std::shared_ptr<PgWriteOpTemplate> write_op_;
    uint64_t write_time_ = 0;
};

}  // namespace gate
}  // namespace k2pg
