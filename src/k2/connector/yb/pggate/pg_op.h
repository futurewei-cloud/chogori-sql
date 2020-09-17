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

#ifndef CHOGORI_GATE_PG_OP_H
#define CHOGORI_GATE_PG_OP_H

#include <string>
#include <list>
#include <vector>

#include "yb/common/type/slice.h"
#include "yb/common/status.h"
#include "yb/entities/entity_ids.h"
#include "yb/entities/schema.h"
#include "yb/entities/type.h"
#include "yb/entities/value.h"
#include "yb/entities/expr.h"
#include "yb/entities/table.h"

#include "yb/pggate/pg_tuple.h"
#include "yb/pggate/pg_session.h"
#include "yb/pggate/pg_tabledesc.h"
#include "yb/pggate/pg_op_api.h"

namespace k2 {
namespace gate {
    using std::shared_ptr;
    using std::unique_ptr;
    using std::list;
    using std::vector;

    using namespace yb;
    using namespace k2::sql;

    const uint64_t default_ysql_prefetch_limit = 4;
    const uint64_t default_ysql_request_limit = 1;
    const uint64_t default_ysql_select_parallelism = 1;
    const double default_ysql_backward_prefetch_scale_factor = 0.25;

    YB_STRONGLY_TYPED_BOOL(RequestSent);
   
    //--------------------------------------------------------------------------------------------------
    // PgOpResult represents a batch of rows in ONE reply from storage layer.
    class PgOpResult {
        public:
        explicit PgOpResult(string&& data);
        PgOpResult(string&& data, std::list<int64_t>&& row_orders);
        ~PgOpResult();

        PgOpResult(const PgOpResult&) = delete;
        PgOpResult& operator=(const PgOpResult&) = delete;

        // Get the order of the next row in this batch.
        int64_t NextRowOrder();

        // End of this batch.
        bool is_eof() const {
            return row_count_ == 0 || row_iterator_.empty();
        }

        // Get the postgres tuple from this batch.
        CHECKED_STATUS WritePgTuple(const std::vector<PgExpr*>& targets, PgTuple *pg_tuple,
                                    int64_t *row_order);

        // Get system columns' values from this batch.
        // Currently, we only have ybctids, but there could be more.
        CHECKED_STATUS ProcessSystemColumns();

        // Access function to ybctids value in this batch.
        // Sys columns must be processed before this function is called.
        const vector<Slice>& ybctids() const {
            DCHECK(syscol_processed_) << "System columns are not yet setup";
            return ybctids_;
        }

        // Row count in this batch.
        int64_t row_count() const {
            return row_count_;
        }

        static void LoadCache(const string& data, int64_t *total_row_count, Slice *cursor);

        //------------------------------------------------------------------------------------------------
        // Read Numeric Data
        template<typename num_type>
        static size_t ReadNumericValue(num_type (*reader)(const void*), Slice *cursor, num_type *value) {
            *value = reader(cursor->data());
            return sizeof(num_type);
        }

        static size_t ReadNumber(Slice *cursor, bool *value);
        static size_t ReadNumber(Slice *cursor, uint8 *value);
        static size_t ReadNumber(Slice *cursor, int8 *value);
        static size_t ReadNumber(Slice *cursor, uint16 *value);
        static size_t ReadNumber(Slice *cursor, int16 *value);
        static size_t ReadNumber(Slice *cursor, uint32 *value);
        static size_t ReadNumber(Slice *cursor, int32 *value);
        static size_t ReadNumber(Slice *cursor, uint64 *value);
        static size_t ReadNumber(Slice *cursor, int64 *value);
        static size_t ReadNumber(Slice *cursor, float *value);
        static size_t ReadNumber(Slice *cursor, double *value);

        // Read Text Data
        static size_t ReadBytes(Slice *cursor, char *value, int64_t bytes);

        // Function translate_data_() reads the received data from K2 Doc API and writes it to Postgres buffer
        // using to_datum().
        // - K2 Doc storage supports a number of datatypes, and we would need to provide one translate function for
        //   each datatype to read them correctly. The translate_data() function pointer must be setup
        //   correctly during the compilation of a statement.
        // - For each postgres data type, to_datum() function pointer must be setup properly during
        //   the compilation of a statement.
        void TranslateData(const PgExpr *target, Slice *yb_cursor, int index, PgTuple *pg_tuple) {
            // TODO: implementation
        }      

        private:
        // Data selected from DocDB.
        string data_;

        // Iterator on "data_" from row to row.
        Slice row_iterator_;

        // The row number of only this batch.
        int64_t row_count_ = 0;

        // The indexing order of the row in this batch.
        // These order values help to identify the row order across all batches.
        std::list<int64_t> row_orders_;

        // System columns.
        // - ybctids_ contains pointers to the buffers "data_".
        // - System columns must be processed before these fields have any meaning.
        vector<Slice> ybctids_;
        bool syscol_processed_ = false;
    };

    class PgOp : public std::enable_shared_from_this<PgOp> {
        public:
        typedef std::shared_ptr<PgOp> SharedPtr;

        // Constructors & Destructors.
        explicit PgOp(const PgSession::ScopedRefPtr& pg_session,
                        const PgTableDesc::ScopedRefPtr& table_desc,
                        const PgObjectId& relation_id = PgObjectId());
        virtual ~PgOp();

        // Initialize doc operator.
        virtual void ExecuteInit(const PgExecParameters *exec_params);

        // Execute the op. Return true if the request has been sent and is awaiting the result.
        virtual Result<RequestSent> Execute(bool force_non_bufferable = false);

        // Instruct this doc_op to abandon execution and querying data by setting end_of_data_ to 'true'.
        // - This op will not send request to storage layer.
        // - This op will return empty result-set when being requested for data.
        void AbandonExecution() {
            end_of_data_ = true;
        }

        // Get the result of the op. No rows will be added to rowsets in case end of data reached.
        CHECKED_STATUS GetResult(std::list<PgOpResult> *rowsets);
        Result<int32_t> GetRowsAffectedCount() const;

        // This operation is requested internally within PgGate, and that request does not go through
        // all the steps as other operation from Postgres thru PgOp. This is used to create requests
        // for the following select.
        //   SELECT ... FROM <table> WHERE ybctid IN (SELECT base_ybctids from INDEX)
        // After ybctids are queried from INDEX, PgGate will call "PopulateDmlByYbctidOps" to create
        // operators to fetch rows whose rowids equal queried ybctids.
        virtual CHECKED_STATUS PopulateDmlByYbctidOps(const vector<Slice> *ybctids) = 0;

        protected:
        // Populate Protobuf requests using the collected informtion for this DocDB operator.
        virtual CHECKED_STATUS CreateRequests() = 0;

        // Create operators.
        // - Each operator is used for one request.
        // - When parallelism by partition is applied, each operator is associated with one partition,
        //   and each operator has a batch of arguments that belong to that partition.
        //   * The higher the number of partition_count, the higher the parallelism level.
        //   * If (partition_count == 1), only one operator is needed for the entire partition range.
        //   * If (partition_count > 1), each operator is used for a specific partition range.
        //   * This optimization is used by
        //       PopulateDmlByYbctidOps()
        //       PopulateParallelSelectCountOps()
        // - When parallelism by arguments is applied, each operator has only one argument.
        //   When storage layer will run the requests in parallel as it assigned one thread per request.
        //       PopulateNextHashPermutationOps()
        CHECKED_STATUS ClonePgsqlOps(int op_count);

        // Only active operators are kept in the active range [0, active_op_count_)
        // - Not execute operators that are outside of range [0, active_op_count_).
        // - Sort the operators in "pgsql_ops_" to move "inactive" operators to the end of the list.
        void MoveInactiveOpsOutside();

        // Clone READ or WRITE "template_op_" into new operators.
        virtual std::unique_ptr<SqlOpCall> CloneFromTemplate() = 0;

        // Process the result set in server response.
        Result<std::list<PgOpResult>> ProcessResponseResult();

        private:
        CHECKED_STATUS SendRequest(bool force_non_bufferable);

        virtual CHECKED_STATUS SendRequestImpl(bool force_non_bufferable);

        Result<std::list<PgOpResult>> ProcessResponse(const Status& exec_status);

        virtual Result<std::list<PgOpResult>> ProcessResponseImpl() = 0;

        //----------------------------------- Data Members -----------------------------------------------
        protected:
        // Session control.
        PgSession::ScopedRefPtr pg_session_;

        // Operation time. This time is set at the start and must stay the same for the lifetime of the
        // operation to ensure that it is operating on one snapshot.
        uint64_t read_time_ = 0;

        // Target table.
        PgTableDesc::ScopedRefPtr table_desc_;
        PgObjectId relation_id_;

        // Exec control parameters.
        PgExecParameters exec_params_;

        // Suppress sending new request after processing response.
        // Next request will be sent in case upper level will ask for additional data.
        bool suppress_next_result_prefetching_ = false;

        // Populated protobuf request.
        std::vector<std::shared_ptr<SqlOpCall>> pgsql_ops_;

        // Number of active operators in the pgsql_ops_ list.
        int32_t active_op_count_ = 0;

        // Indicator for completing all request populations.
        bool request_population_completed_ = false;

        // If true, all data for each batch must be collected before PgGate gets the reply.
        // NOTE:
        // - Currently, PgSession's default behavior is to get all responses in a batch together.
        // - We set this flag only to prevent future optimization where requests & their responses to
        //   and from different storage servers are sent and received independently. That optimization
        //   should only be done when "wait_for_batch_completion_ == false"
        bool wait_for_batch_completion_ = true;

        // Future object to fetch a response from DocDB after sending a request.
        // Object's valid() method returns false in case no request is sent
        // or sent request was buffered by the session.
        // Only one RunAsync() can be called to sent to DocDB at a time.
        PgSessionAsyncRunResult response_;

        // Executed row count.
        int32_t rows_affected_count_ = 0;

        // Whether all requested data by the statement has been received or there's a run-time error.
        bool end_of_data_ = false;

        // The order number of each request when batching arguments.
        // Currently, this is used for query by YBCTID.
        // - Each pgsql_op has a batch of ybctids selected from INDEX.
        // - The order of resulting rows should match with the order of queried ybctids.
        // - Example:
        //   Suppose we got from INDEX table
        //     { ybctid_1, ybctid_2, ybctid_3, ybctid_4, ybctid_5, ybctid_6, ybctid_7 }
        //
        //   Now pgsql_op are constructed as the following, one op per partition.
        //     pgsql_op <partition 1> (ybctid_1, ybctid_3, ybctid_4)
        //     pgsql_op <partition 2> (ybctid_2, ybctid_6)
        //     pgsql_op <partition 2> (ybctid_5, ybctid_7)
        //
        //   After getting the rows of data from pgsql, the rows must be then ordered from 1 thru 7.
        //   To do so, for each pgsql_op we kept an array of orders, batch_row_orders_.
        //   For the above pgsql_ops_, the orders would be cached as the following.
        //     vector orders { partition 1: list ( 1, 3, 4 ),
        //                     partition 2: list ( 2, 6 ),
        //                     partition 3: list ( 5, 7 ) }
        //
        //   When the "pgsql_ops_" elements are sorted and swapped order, the "batch_row_orders_"
        //   must be swaped also.
        //     std::swap ( pgsql_ops_[1], pgsql_ops_[3])
        //     std::swap ( batch_row_orders_[1], batch_row_orders_[3] )
        std::vector<std::list<int64_t>> batch_row_orders_;

        // This counter is used to maintain the row order when the operator sends requests in parallel
        // by partition. Currently only query by YBCTID uses this variable.
        int64_t batch_row_ordering_counter_ = 0;

        // Parallelism level.
        // - This is the maximum number of read/write requests being sent to servers at one time.
        // - When it is 1, there's no optimization. Available requests is executed one at a time.
        int32_t parallelism_level_ = 1;

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
        PgReadOp(const PgSession::ScopedRefPtr& pg_session,
                    const PgTableDesc::ScopedRefPtr& table_desc,
                    std::unique_ptr<SqlOpReadCall> read_op);

        void ExecuteInit(const PgExecParameters *exec_params) override;

        private:
        // Create protobuf requests using template_op_.
        CHECKED_STATUS CreateRequests() override;

        // Create operators by partition.
        // - Optimization for statement
        //     SELECT xxx FROM <table> WHERE ybctid IN (SELECT ybctid FROM INDEX)
        // - After being queried from inner select, ybctids are used for populate request for outer query.
        CHECKED_STATUS PopulateDmlByYbctidOps(const vector<Slice> *ybctids) override;
        CHECKED_STATUS InitializeYbctidOperators();

        // Create operators by partition arguments.
        // - Optimization for statement:
        //     SELECT ... WHERE <hash-columns> IN <value-lists>
        // - If partition column binds are defined, partition_column_values field of each operation
        //   is set to be the next permutation.
        // - When an operator is assigned a hash permutation, it is marked as active to be executed.
        // - When an operator completes the execution, it is marked as inactive and available for the
        //   exection of the next hash permutation.
        CHECKED_STATUS PopulateNextHashPermutationOps();
        // CHECKED_STATUS InitializeHashPermutationStates();

        // Create operators by partitions.
        // - Optimization for statement:
        //     Create parallel request for SELECT COUNT().
        CHECKED_STATUS PopulateParallelSelectCountOps();

        // Process response from DocDB.
        Result<std::list<PgOpResult>> ProcessResponseImpl() override;

        // Process response paging state from DocDB.
        CHECKED_STATUS ProcessResponsePagingState();

        // Reset pgsql operators before reusing them with new arguments / inputs from Postgres.
        CHECKED_STATUS ResetInactivePgsqlOps();

        // Analyze options and pick the appropriate prefetch limit.
        void SetRequestPrefetchLimit();

        // Set the row_mark_type field of our read request based on our exec control parameter.
        void SetRowMark();

        // Set the read_time for our read request based on our exec control parameter.
        void SetReadTime();

        // Set the partition key for our read request based on our exec control paramater.
        void SetPartitionKey();

        // Clone the template into actual requests to be sent to server.
        std::unique_ptr<SqlOpCall> CloneFromTemplate() override {
            return template_op_->DeepCopy();
        }

        // Get the read_op for a specific operation index from pgsql_ops_.
        SqlOpReadCall *GetReadOp(int op_index) {
            return static_cast<SqlOpReadCall *>(pgsql_ops_[op_index].get());
        }

        //----------------------------------- Data Members -----------------------------------------------

        // Template operation, used to fill in pgsql_ops_ by either assigning or cloning.
        std::shared_ptr<SqlOpReadCall> template_op_;

        // Used internally for PopulateNextHashPermutationOps to keep track of which permutation should
        // be used to construct the next read_op.
        // Is valid as long as request_population_completed_ is false.
        //
        // Example:
        // For a query clause "h1 = 1 AND h2 IN (2,3) AND h3 IN (4,5,6) AND h4 = 7",
        // there are 1*2*3*1 = 6 possible permutation.
        // As such, this field will take on values 0 through 5.
        int total_permutation_count_ = 0;
        int next_permutation_idx_ = 0;

        // Used internally for PopulateNextHashPermutationOps to holds all partition expressions.
        // Elements correspond to a hash columns, in the same order as they were defined
        // in CREATE TABLE statement.
        // This is somewhat similar to what hash_values_options_ in CQL is used for.
        //
        // Example:
        // For a query clause "h1 = 1 AND h2 IN (2,3) AND h3 IN (4,5,6) AND h4 = 7",
        // this will be initialized to [[1], [2, 3], [4, 5, 6], [7]]
        std::vector<std::vector<PgExpr *>> partition_exprs_;

        // The partition key identifying the sole storage server to read from.
        boost::optional<std::string> partition_key_;
    };

    //--------------------------------------------------------------------------------------------------

    class PgWriteOp : public PgOp {
        public:
        // Public types.
        typedef std::shared_ptr<PgWriteOp> SharedPtr;

        // Constructors & Destructors.
        PgWriteOp(const PgSession::ScopedRefPtr& pg_session,
                    const PgTableDesc::ScopedRefPtr& table_desc,
                    const PgObjectId& relation_id,
                    std::unique_ptr<SqlOpWriteCall> write_op);

        // Set write time.
        void SetWriteTime(const uint64_t write_time);

        private:
        // Process response implementation.
        Result<std::list<PgOpResult>> ProcessResponseImpl() override;

        // Create protobuf requests using template_op (write_op).
        CHECKED_STATUS CreateRequests() override;

        // For write ops, we are not yet batching ybctid from index query.
        // TODO(neil) This function will be implemented when we push down sub-query inside WRITE ops to
        // the proxy layer. There's many scenarios where this optimization can be done.
        CHECKED_STATUS PopulateDmlByYbctidOps(const vector<Slice> *ybctids) override {
            LOG(FATAL) << "Not yet implemented";
            return Status::OK();
        }

        // Get WRITE operator for a specific operator index in pgsql_ops_.
        SqlOpWriteCall *GetWriteOp(int op_index) {
            return static_cast<SqlOpWriteCall *>(pgsql_ops_[op_index].get());
        }

        // Clone user data from template to actual protobuf requests.
        std::unique_ptr<SqlOpCall> CloneFromTemplate() override {
            return write_op_->DeepCopy();
        }

        //----------------------------------- Data Members -----------------------------------------------
        // Template operation all write ops.
        std::shared_ptr<SqlOpWriteCall> write_op_;
        uint64_t write_time_ = 0;
    };

}  // namespace gate
}  // namespace k2

#endif //CHOGORI_GATE_PG_OP_H    