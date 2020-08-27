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

#ifndef CHOGORI_GATE_DOC_H
#define CHOGORI_GATE_DOC_H

#include <string>
#include <vector>
#include <optional>

#include "yb/entities/entity_ids.h"
#include "yb/entities/schema.h"
#include "yb/entities/type.h"
#include "yb/entities/value.h"
#include "yb/entities/expr.h"

namespace k2 {
namespace gate {

    using namespace k2::sql;
 
    // pass the information so that the under hood SKV client could generate the actual doc key from it
    struct DocKey { 
        NamespaceId namespace_id;
        TableId table_id;
        std::vector<SqlValue> key_cols;
        DocKey(NamespaceId& nid, TableId& tid, std::vector<SqlValue>& keys) : 
            namespace_id(nid), table_id(tid), key_cols(std::move(keys)) {
        }
    };

    struct ColumnValue {
        int column_id;
        PgExpr* expr;
    };

    struct RSColDesc {
        string name;
        SQLType type;
    };

    struct RSRowDesc {
        std::vector<RSColDesc> rscol_descs;
    };

    struct DocPagingState {
        TableId table_id;
        string next_token;
        uint64_t total_num_rows_read;
    };

    struct DocReadRequest {
        string client_id;
        int64_t stmt_id;
        NamespaceId namespace_id;
        TableId table_id;
        uint64_t schema_version;
        uint32_t hash_code;
        std::vector<PgExpr*> partition_column_values;
        std::vector<PgExpr*> range_column_values;
        PgExpr* ybctid_column_value;
        RSRowDesc rsrow_desc;
        std::vector<PgExpr*> targets;
        PgExpr* where_expr;
        PgExpr* condition_expr;
        std::vector<ColumnId> column_refs;
        bool is_forward_scan = true;
        bool distinct = false;
        bool is_aggregate = false;
        uint64_t limit;
        DocPagingState* paging_state;
        bool return_paging_state = false;
        uint32_t max_hash_code;
        uint64_t catalog_version;
        RowMarkType row_mark_type;
        string max_partition_key;
    };

    struct DocWriteRequest {
        enum StmtType {
            PGSQL_INSERT = 1,
            PGSQL_UPDATE = 2,
            PGSQL_DELETE = 3,
            PGSQL_UPSERT = 4,
            PGSQL_TRUNCATE_COLOCATED = 5
        };
        
        string client_id;
        int64_t stmt_id;
        StmtType stmt_type;
        NamespaceId namespace_id;
        TableId table_id;
        uint64_t schema_version;
        uint32_t hash_code;
        std::vector<PgExpr*> partition_column_values;
        std::vector<PgExpr*> range_column_values;
        PgExpr* ybctid_column_value;
        // Not used with UPDATEs. Use column_new_values to UPDATE a value.
        std::vector<ColumnValue> column_values;
        // Column New Values.
        // - Columns to be overwritten (UPDATE SET clause). This field can contain primary-key columns.
        std::vector<ColumnValue> column_new_values;
        RSRowDesc rsrow_desc;
        std::vector<PgExpr*> targets;
        PgExpr* where_expr;
        PgExpr* condition_expr;
        std::vector<ColumnId> column_refs;
        uint64_t catalog_version;
        // True only if this changes a system catalog table (or index).
        bool is_ysql_catalog_change;
    };

    // Response from K2 storage for both read and write.
    struct DocResponse {
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
        int32_t rows_data_sidecar;
        DocPagingState* paging_state;
        int32_t rows_affected_count;
   
        //
        // External statuses.
        //
        // If you add more of those, make sure they are correctly picked up, e.g.
        // by PgDocReadOp::ReceiveResponse and PgDocOp::HandleResponseStatus
        //

        // PostgreSQL error code encoded as in errcodes.h or yb_pg_errcodes.h.
        // See https://www.postgresql.org/docs/11/errcodes-appendix.html
        uint64_t pg_error_code;

        // Transaction error code, obtained by static_cast of TransactionErrorTag::Decode
        // of Status::ErrorData(TransactionErrorTag::kCategory)
        int32_t txn_error_code;      
    };

    struct SaveOrUpdateSchemaResponse {
        std::optional<std::string> error_code;
    };

    struct RowRecord {
        std::vector<SqlValue> cols;
    };

    struct SaveOrUpdateRecordResponse {
        std::optional<std::string> error_code;
    };

    struct RowRecords {
        std::vector<RowRecord> rows;
        std::string next_token;
    };

    struct DeleteRecordResponse {
        std::optional<std::string> error_code;
    };

    struct DeleteRecordsResponse {
        std::optional<std::string> error_code;
    };

    // this is the bridge between the SQL layer and the under hood K2 SKV APIs
    class DocApi {
        public:
        DocApi() = default;

        ~DocApi() {
        }

        DocResponse read(DocReadRequest request);

        DocResponse write(DocWriteRequest request);

        SaveOrUpdateSchemaResponse saveOrUpdateSchema(DocKey& key, Schema& schema);

        SaveOrUpdateRecordResponse saveOrUpdateRecord(DocKey& key, RowRecord& record);

        RowRecord getRecord(DocKey& key);

        RowRecords batchGetRecords(DocKey& key, std::vector<PgExpr>& filters, std::string& token);

        DeleteRecordResponse deleteRecord(DocKey& key);

        DeleteRecordsResponse batchDeleteRecords(DocKey& key, std::vector<PgExpr>& filters);

        DeleteRecordsResponse deleteAllRecordsAndSchema(DocKey& key);
    };

}  // namespace gate
}  // namespace k2

#endif //CHOGORI_GATE_DOC_H