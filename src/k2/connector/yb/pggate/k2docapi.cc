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

#include "yb/pggate/k2docapi.h"
#include "yb/common/endian.h"

namespace k2 {
namespace gate {  

    std::unique_ptr<DocReadRequest> DocReadRequest::clone() {
       std::unique_ptr<DocReadRequest> newRequest = std::make_unique<DocReadRequest>();
       newRequest->client_id = client_id;
       newRequest->stmt_id = stmt_id;
       newRequest->namespace_id = namespace_id;
       newRequest->table_id = table_id;
       newRequest->schema_version = schema_version;
       newRequest->hash_code = hash_code;
       // TODO: double check if we need to deep clone vector fields
       newRequest->partition_column_values = partition_column_values;
       newRequest->range_column_values = range_column_values;
       newRequest->ybctid_column_value = ybctid_column_value;
       newRequest->rsrow_desc.rscol_descs = rsrow_desc.rscol_descs;
       newRequest->targets = targets;
       newRequest->where_expr = where_expr;
       newRequest->condition_expr = condition_expr;
       newRequest->column_refs = column_refs;
       newRequest->is_forward_scan = is_forward_scan;
       newRequest->distinct = distinct;
       newRequest->is_aggregate = is_aggregate;
       newRequest->limit = limit;
       newRequest->paging_state = paging_state;
       newRequest->return_paging_state = return_paging_state;
       newRequest->max_hash_code = max_hash_code;
       newRequest->catalog_version = catalog_version;
       newRequest->row_mark_type = row_mark_type;
       newRequest->max_partition_key = max_partition_key;  
       return newRequest;   
    }

    std::unique_ptr<DocWriteRequest> DocWriteRequest::clone() {
       std::unique_ptr<DocWriteRequest> newRequest = std::make_unique<DocWriteRequest>();
       newRequest->client_id = client_id;
       newRequest->stmt_id = stmt_id;
       newRequest->stmt_type = stmt_type;
       newRequest->namespace_id = namespace_id;
       newRequest->table_id = table_id;
       newRequest->schema_version = schema_version;
       newRequest->hash_code = hash_code;
       // TODO: double check if we need to deep clone vector fields
       newRequest->partition_column_values = partition_column_values;
       newRequest->range_column_values = range_column_values;
       newRequest->ybctid_column_value = ybctid_column_value;
       newRequest->column_values = column_values;
       newRequest->column_new_values = column_new_values;
       newRequest->rsrow_desc.rscol_descs = rsrow_desc.rscol_descs;
       newRequest->targets = targets;
       newRequest->where_expr = where_expr;
       newRequest->condition_expr = condition_expr;
       newRequest->column_refs = column_refs;
       newRequest->catalog_version = catalog_version; 
       return newRequest;        
    }

    DocCall::DocCall(const std::shared_ptr<TableInfo>& table)  : table_(table) {
    }

    DocCall::~DocCall() {}
 
    DocWriteCall::DocWriteCall(const shared_ptr<TableInfo>& table)
            : DocCall(table), write_request_(new DocWriteRequest()) {
    }

    DocWriteCall::~DocWriteCall() {}

    bool DocWriteCall::IsTransactional() const {
        return !is_single_row_txn_ && table_->schema().table_properties().is_transactional();
    }

    std::string DocWriteCall::ToString() const {
        return "PGSQL WRITE: " + write_request_->stmt_id;
    }

    std::unique_ptr<DocWriteCall> DocWriteCall::DeepCopy() {
        std::unique_ptr<DocWriteCall> result = std::make_unique<DocWriteCall>(table_);
        result->set_active(is_active());
        result->write_request_ = write_request_->clone();
        result->is_single_row_txn_ = is_single_row_txn_;
        return result;
    }

    DocReadCall::DocReadCall(const shared_ptr<TableInfo>& table)
        : DocCall(table), read_request_(new DocReadRequest()) {
    }
    
    std::string DocReadCall::ToString() const {
        return "PGSQL READ: " + read_request_->stmt_id;
    }

    std::unique_ptr<DocReadCall> DocReadCall::DeepCopy() {
        std::unique_ptr<DocReadCall> result = std::make_unique<DocReadCall>(table_);
        result->set_active(is_active());
        result->read_request_ = read_request_->clone();
        return result;
    }

    std::string DocApi::getDocKey(DocReadRequest& request) {
        return "Not implemented";
    }
        
    std::string DocApi::getDocKey(DocWriteRequest& request) {
        return "Not implemented";
    }

/*
    SaveOrUpdateSchemaResponse DocApi::saveOrUpdateSchema(DocKey& key, Schema& schema) {
        return SaveOrUpdateSchemaResponse();
    }

    SaveOrUpdateRecordResponse DocApi::saveOrUpdateRecord(DocKey& key, RowRecord& record) {
        return SaveOrUpdateRecordResponse();
    }

    RowRecord DocApi::getRecord(DocKey& key) {
        return RowRecord();
    }

    RowRecords DocApi::batchGetRecords(DocKey& key, std::vector<PgExpr>& filters, std::string& token) {
        return RowRecords();
    }

    DeleteRecordResponse DocApi::deleteRecord(DocKey& key) {
        return DeleteRecordResponse();
    }

    DeleteRecordsResponse DocApi::batchDeleteRecords(DocKey& key, std::vector<PgExpr>& filters) {
        return DeleteRecordsResponse();
    }

    DeleteRecordsResponse DocApi::deleteAllRecordsAndSchema(DocKey& key) {
        return DeleteRecordsResponse();
    }
*/
}  // namespace gate
}  // namespace k2
