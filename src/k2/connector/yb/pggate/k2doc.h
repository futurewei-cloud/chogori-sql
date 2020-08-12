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
#include "yb/entities/value.h"
#include "yb/entities/expr.h"

namespace k2 {
namespace gate {
    using k2::sql::NamespaceId;
    using k2::sql::NamespaceName;
    using k2::sql::TableId;
    using k2::sql::TableName;
    using k2::sql::Schema;
    using k2::sql::SqlValue;
    using k2::sql::SqlExpr;
 
    // pass the information so that the under hood SKV client could generate the actual doc key from it
    struct DocKey { 
        NamespaceId namespace_id;
        TableId table_id;
        std::vector<SqlValue> key_cols;
        DocKey(NamespaceId& nid, TableId& tid, std::vector<SqlValue>& keys) : 
            namespace_id(nid), table_id(tid), key_cols(std::move(keys)) {
        }
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
        std::vector<SqlValue> cols;
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

        SaveOrUpdateSchemaResponse saveOrUpdateSchema(DocKey& key, Schema& schema);

        SaveOrUpdateRecordResponse saveOrUpdateRecord(DocKey& key, RowRecord& record);

        RowRecord getRecord(DocKey& key);

        RowRecords batchGetRecords(DocKey& key, std::vector<SqlExpr>& filters, std::string& token);

        DeleteRecordResponse deleteRecord(DocKey& key);

        DeleteRecordsResponse batchDeleteRecords(DocKey& key, std::vector<SqlExpr>& filters);

        DeleteRecordsResponse deleteAllRecordsAndSchema(DocKey& key);
    };

}  // namespace gate
}  // namespace k2

#endif //CHOGORI_GATE_DOC_H