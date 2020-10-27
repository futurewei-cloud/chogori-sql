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

#include "yb/pggate/pg_gate_defaults.h"
#include "yb/pggate/pg_tuple.h"
#include "yb/pggate/pg_session.h"
#include "yb/pggate/pg_tabledesc.h"
#include "yb/pggate/pg_op_api.h"

namespace k2pg {
namespace gate {
    using std::shared_ptr;
    using std::unique_ptr;
    using std::list;
    using std::vector;

    using namespace yb;
    using namespace k2pg::sql;

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
        void TranslateData(const PgExpr *target, Slice *yb_cursor, int index, PgTuple *pg_tuple);

        void TranslateColumnRef(const PgColumnRef *target, Slice *yb_cursor, int index, PgTuple *pg_tuple); 

        private:
        // Translate system column.
        template<typename data_type>
        static void TranslateSysCol(Slice *yb_cursor, data_type *value);

        static void TranslateSysCol(Slice *yb_cursor, PgTuple *pg_tuple, uint8_t **pgbuf);

        // translate regular column
        static void TranslateRegularCol(Slice *yb_cursor, int index,
                              const YBCPgTypeEntity *type_entity, const PgTypeAttrs *type_attrs,
                              PgTuple *pg_tuple);

        template<typename data_type>
        static void TranslateNumber(Slice *yb_cursor, int index,
                                    const YBCPgTypeEntity *type_entity, const PgTypeAttrs *type_attrs,
                                    PgTuple *pg_tuple) {
            DCHECK(type_entity) << "Type entity not provided";
            DCHECK(type_entity->yb_to_datum) << "Type entity converter not provided";

            data_type result = 0;
            size_t read_size = ReadNumber(yb_cursor, &result);
            yb_cursor->remove_prefix(read_size);
            pg_tuple->WriteDatum(index, type_entity->yb_to_datum(&result, read_size, type_attrs));
        };

        // Translates char-based datatypes.
        static void TranslateText(Slice *yb_cursor, int index,
                            const YBCPgTypeEntity *type_entity, const PgTypeAttrs *type_attrs,
                            PgTuple *pg_tuple);

        // Translates binary-based datatypes.
        static void TranslateBinary(Slice *yb_cursor, int index,
                              const YBCPgTypeEntity *type_entity, const PgTypeAttrs *type_attrs,
                              PgTuple *pg_tuple);

        // Translate decimal datatype.
        static void TranslateDecimal(Slice *yb_cursor, int index,
                               const YBCPgTypeEntity *type_entity, const PgTypeAttrs *type_attrs,
                               PgTuple *pg_tuple);

        // Data selected from k2 storage.
        // TODO: refactor this based on SKV payload
        string data_;

        // Iterator on "data_" from row to row.
        Slice row_iterator_;

        // The row number of only this batch.
        int64_t row_count_ = 0;

        // The indexing order of the row in this batch.
        // These order values help to identify the row order across all batches
        // the size is based on the returning data and thus a list instead of an array is used
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

        // Initialize sql operator.
        virtual void ExecuteInit(const PgExecParameters *exec_params);

        // Execute the op. Return true if the request has been sent and is awaiting the result.
        virtual Result<RequestSent> Execute(bool force_non_bufferable = false);

        // Instruct this sql_op_ to abandon execution and querying data by setting end_of_data_ to 'true'.
        // - This op will not send request to storage layer.
        // - This op will return empty result-set when being requested for data.
        void AbandonExecution() {
            end_of_data_ = true;
        }

        // Get the result of the op. No rows will be added to rowsets in case end of data reached.
        CHECKED_STATUS GetResult(std::list<PgOpResult> *rowsets);
        Result<int32_t> GetRowsAffectedCount() const;

        CHECKED_STATUS PopulateDmlByRowIdOps(const vector<Slice>& ybctids) {
            // TODO: implement the logic to create new operations by providing a given list of row ids, i.e., ybctids
            // This is tracked by the following issue:
            //      https://github.com/futurewei-cloud/chogori-sql/issues/31
            return Status::OK();
        }

        protected:
        // Populate Protobuf requests using the collected informtion for this DocDB operator.
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

        // Populated operation request.
        std::vector<std::shared_ptr<PgOpTemplate>> pgsql_ops_;

        // Number of active operators in the pgsql_ops_ list.
        int32_t active_op_count_ = 0;

        // Indicator for completing all request populations.
        bool request_population_completed_ = false;

        // Future object to fetch a response from storage after sending a request.
        // Object's valid() method returns false in case no request is sent
        // or sent request was buffered by the session.
        // Only one RunAsync() can be called to sent storage at a time.
        PgSessionAsyncRunResult response_;

        // Executed row count.
        int32_t rows_affected_count_ = 0;

        // Whether all requested data by the statement has been received or there's a run-time error.
        bool end_of_data_ = false;

        // The order number of each request when batching arguments.
        std::vector<std::list<int64_t>> batch_row_orders_;

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

        // Analyze options and pick the appropriate prefetch limit.
        void SetRequestPrefetchLimit();

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
        PgWriteOp(const PgSession::ScopedRefPtr& pg_session,
                    const PgTableDesc::ScopedRefPtr& table_desc,
                    const PgObjectId& relation_id,
                    std::unique_ptr<PgWriteOpTemplate> write_op);

        // Set write time.
        void SetWriteTime(const uint64_t write_time);

        private:
        // Process response implementation.
        Result<std::list<PgOpResult>> ProcessResponseImpl() override;

        // Create requests using template_op (write_op).
        CHECKED_STATUS CreateRequests() override;

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

#endif //CHOGORI_GATE_PG_OP_H    