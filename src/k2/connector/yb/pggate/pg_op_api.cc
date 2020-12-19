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

    PgOpTemplate::PgOpTemplate(const std::shared_ptr<TableInfo>& table)  : table_(table) {
    }

    PgOpTemplate::~PgOpTemplate() {}

    PgWriteOpTemplate::PgWriteOpTemplate(const shared_ptr<TableInfo>& table)
            : PgOpTemplate(table), write_request_(new SqlOpWriteRequest()) {
    }

    PgWriteOpTemplate::~PgWriteOpTemplate() {}

    bool PgWriteOpTemplate::IsTransactional() const {
        return !is_single_row_txn_ && table_->schema().table_properties().is_transactional();
    }

    std::string PgWriteOpTemplate::ToString() const {
        return "PGSQL WRITE: " + write_request_->stmt_id;
    }

    std::unique_ptr<PgWriteOpTemplate> PgWriteOpTemplate::DeepCopy() {
        std::unique_ptr<PgWriteOpTemplate> result = std::make_unique<PgWriteOpTemplate>(table_);
        result->set_active(is_active());
        result->write_request_ = write_request_->clone();
        result->is_single_row_txn_ = is_single_row_txn_;
        return result;
    }

    PgReadOpTemplate::PgReadOpTemplate(const shared_ptr<TableInfo>& table)
        : PgOpTemplate(table), read_request_(new SqlOpReadRequest()) {
    }

    std::string PgReadOpTemplate::ToString() const {
        return "PGSQL READ: " + read_request_->stmt_id;
    }

    std::unique_ptr<PgReadOpTemplate> PgReadOpTemplate::DeepCopy() {
        std::unique_ptr<PgReadOpTemplate> result = std::make_unique<PgReadOpTemplate>(table_);
        result->set_active(is_active());
        result->read_request_ = read_request_->clone();
        return result;
    }
}  // namespace gate
}  // namespace k2pg
