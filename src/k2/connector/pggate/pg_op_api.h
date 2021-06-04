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

#ifndef CHOGORI_GATE_OP_CALL_H
#define CHOGORI_GATE_OP_CALL_H

#include <string>
#include <list>
#include <vector>
#include <optional>

#include "entities/entity_ids.h"
#include "entities/schema.h"
#include "entities/type.h"
#include "entities/value.h"
#include "entities/table.h"
#include <k2/dto/Expression.h>
#include "pggate/pg_tuple.h"

#include "k2_includes.h"
#include "k2_log.h"

namespace k2pg {
namespace gate {
    using std::shared_ptr;
    using std::unique_ptr;

    using k2pg::Status;

    using k2pg::sql::PgOid;
    using k2pg::sql::PgExpr;
    using k2pg::sql::PgColumnRef;
    using k2pg::sql::PgConstant;
    using k2pg::sql::PgOperator;
    using k2pg::sql::RowMarkType;
    using k2pg::sql::SqlValue;
    using k2pg::sql::TableName;
    using k2pg::sql::TableId;
    using k2::dto::expression::Expression;
    using k2::dto::expression::Operation;
    using k2::dto::expression::Value;

    // a data structure used to store the binding information, i.e., a column reference and its binding expression
    // here we use column index to reference a column and the binding expression is represented by PgExpr
    struct BindVariable {
        BindVariable() {
        };

        BindVariable(int idx_in) {
            idx = idx_in;
            expr = NULL;
        };

        BindVariable(int idx_in, PgExpr *expr_in) {
            idx = idx_in;
            expr = expr_in;
        };

        friend std::ostream& operator<<(std::ostream& os, const BindVariable& variable) {
            os << "(BindVariable: id: " << variable.idx << ", expr: ";
            if (variable.expr == NULL) {
                os << "NULL)";
            } else {
                os << (*variable.expr) << ")";
            }
            return os;
        }

        // column index
        int idx;
        // binding expression
        PgExpr *expr;
    };

    struct SqlOpPagingState {
        std::shared_ptr<k2::Query> query;
        uint64_t total_num_rows_read;
    };

    struct SqlOpReadRequest {
        SqlOpReadRequest() = default;
        ~SqlOpReadRequest() {
        };

        string client_id;
        int64_t stmt_id;
        std::string collection_name;
        std::string table_id;
        PgOid base_table_oid;   // if is_index_, this is oid of the base table, otherwise, it is oid of this table.
        PgOid index_oid;        // if is_index_, this is oid of the index, otherwiese 0
        // K2 SKV schema version
        uint64_t schema_version;
        // One of either key_column_values or ybctid_column_values
        std::vector<std::shared_ptr<BindVariable>> key_column_values;
        std::vector<std::shared_ptr<BindVariable>> ybctid_column_values;

        // Projection, aggregate, etc.
        std::vector<PgExpr *> targets;
        PgExpr* range_conds;
        PgExpr* where_conds;

        bool is_forward_scan = true;
        bool distinct = false;
        // indicates if targets field above has aggregation
        bool is_aggregate = false;
        uint64_t limit = 0;
        std::shared_ptr<SqlOpPagingState> paging_state;
        bool return_paging_state = false;
        // Full, global SQL version
        uint64_t catalog_version;
        // Ignored by K2 SKV
        RowMarkType row_mark_type;

        std::unique_ptr<SqlOpReadRequest> clone();
    };

    struct SqlOpWriteRequest {
       enum StmtType {
            PGSQL_INSERT = 1,
            PGSQL_UPDATE = 2,
            PGSQL_DELETE = 3,
            PGSQL_UPSERT = 4,
            // TODO: not used in K2, remove it
            PGSQL_TRUNCATE_COLOCATED = 5
        };

        string client_id;
        int64_t stmt_id;
        StmtType stmt_type;
        std::string collection_name;
        std::string table_id;
        PgOid base_table_oid;   // if is_index_, this is oid of the base table, otherwise, it is oid of this table.
        PgOid index_oid;        // if is_index_, this is oid of the index, otherwiese 0
        uint64_t schema_version;
        std::vector<std::shared_ptr<BindVariable>> key_column_values;
        std::shared_ptr<BindVariable> ybctid_column_value;
        // Not used with UPDATEs. Use column_new_values to UPDATE a value.
        std::vector<std::shared_ptr<BindVariable>> column_values;
        // Column New Values.
        // - Columns to be overwritten (UPDATE SET clause). This field can contain primary-key columns.
        std::vector<std::shared_ptr<BindVariable>> column_new_values;
        // K2 SKV does not support the following three cluases for writes:
        std::vector<PgExpr *> targets;

        uint64_t catalog_version;
        // True only if this changes a system catalog table (or index).
        bool is_ysql_catalog_change;

        std::unique_ptr<SqlOpWriteRequest> clone();
    };

    // Response from K2 storage for both read and write.
    struct SqlOpResponse {
          enum class RequestStatus {
            PGSQL_STATUS_OK = 0,
            PGSQL_STATUS_SCHEMA_VERSION_MISMATCH = 1,
            PGSQL_STATUS_RUNTIME_ERROR = 2,
            PGSQL_STATUS_USAGE_ERROR = 3,
            PGSQL_STATUS_RESTART_REQUIRED_ERROR = 4,
            PGSQL_STATUS_DUPLICATE_KEY_ERROR = 5
        };
        RequestStatus status = RequestStatus::PGSQL_STATUS_OK;
        bool skipped;
        string error_message;
        // If paging_state is nullptr, PG knows the request is done
        std::shared_ptr<SqlOpPagingState> paging_state;
        int32_t rows_affected_count;

        //
        // External statuses.
        //
        // If you add more of those, make sure they are correctly picked up, e.g.
        // by PgSession::HandleResponse
        //

        // PostgreSQL error code encoded as in errcodes.h or k2pg_errcodes.h.
        // See https://www.postgresql.org/docs/11/errcodes-appendix.html
        uint64_t pg_error_code;

        // Transaction error code, obtained by static_cast of TransactionErrorTag::Decode
        // of Status::ErrorData(TransactionErrorTag::kCategory)
        int32_t txn_error_code;
    };

    // template operation that could be cloned and updated for actual PG operations
    class PgOpTemplate {
        public:
        enum Type {
            WRITE,
            READ,
        };

        explicit PgOpTemplate();

        ~PgOpTemplate();

        virtual std::string ToString() const = 0;
        virtual Type type() const = 0;
        virtual bool read_only() const = 0;

        SqlOpResponse& response() { return *response_; }
        void allocateResponse() { response_ = std::make_unique<SqlOpResponse>(); }

        std::vector<k2::dto::SKVRecord>&& rows_data() { return std::move(rows_data_); }

        // api to get row_data reference and set the value
        std::vector<k2::dto::SKVRecord>* mutable_rows_data() { return &rows_data_; }

        bool IsTransactional() const {
            // use transaction for all K2 operations
            return true;
        }

        bool succeeded() const {
            return response_->status == SqlOpResponse::RequestStatus::PGSQL_STATUS_OK;
        }

        bool applied() {
            return succeeded() && !response_->skipped;
        }

        bool is_active() const {
            return is_active_;
        }

        void set_active(bool val) {
            is_active_ = val;
        }

        protected:
        std::unique_ptr<SqlOpResponse> response_;
        std::vector<k2::dto::SKVRecord> rows_data_;
        bool is_active_ = true;
    };

    class PgWriteOpTemplate : public PgOpTemplate {
        public:
        explicit PgWriteOpTemplate();

        ~PgWriteOpTemplate();

        virtual Type type() const {
            return WRITE;
        }

        std::shared_ptr<SqlOpWriteRequest> request() const { return write_request_; }

        std::string ToString() const;

        bool read_only() const override { return false; };

        bool IsTransactional() const;

        void set_is_single_row_txn(bool is_single_row_txn) {
            is_single_row_txn_ = is_single_row_txn;
        }

        // Create a deep copy of this call, copying all fields
        // Does NOT, however, copy response and rows data.
        std::unique_ptr<PgWriteOpTemplate> DeepCopy();

        private:
        std::shared_ptr<SqlOpWriteRequest> write_request_;
        // Whether this operation should be run as a single row txn.
        // Else could be distributed transaction (or non-transactional) depending on target table type.
        bool is_single_row_txn_ = false;
    };

    class PgReadOpTemplate : public PgOpTemplate {
        public:
        explicit PgReadOpTemplate();

        ~PgReadOpTemplate() {};

        std::shared_ptr<SqlOpReadRequest> request() const { return read_request_; }

        std::string ToString() const;

        bool read_only() const override { return true; };

        virtual Type type() const { return READ; }

        // Create a deep copy of this call, copying all fields
        // Does NOT, however, copy response and rows data.
        std::unique_ptr<PgReadOpTemplate> DeepCopy();

        void set_return_paging_state(bool return_paging_state) {
            read_request_->return_paging_state = return_paging_state;
        }

        private:
        std::shared_ptr<SqlOpReadRequest> read_request_;
    };
}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_OP_CALL_H
