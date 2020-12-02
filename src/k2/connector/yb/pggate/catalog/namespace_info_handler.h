/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
#ifndef CHOGORI_SQL_NAMESPACE_INFO_HANDLER_H
#define CHOGORI_SQL_NAMESPACE_INFO_HANDLER_H

#include <string>

#include "yb/pggate/catalog/sql_catalog_defaults.h"
#include "yb/pggate/catalog/sql_catalog_entity.h"
#include "yb/pggate/k2_adapter.h"

namespace k2pg {
namespace sql {
namespace catalog {

using yb::Status;
using k2pg::gate::K2Adapter;
using k2pg::gate::K23SITxn;
using k2pg::gate::CreateScanReadResult;

struct CreateNamespaceTableResult {
    RStatus status;    
};

struct AddOrUpdateNamespaceResult {
    RStatus status;   
};

struct GetNamespaceResult {
    RStatus status;
    std::shared_ptr<NamespaceInfo> namespaceInfo;  
};

struct ListNamespacesResult {
    RStatus status;
    std::vector<std::shared_ptr<NamespaceInfo>> namespaceInfos;  
};

class NamespaceInfoHandler : public std::enable_shared_from_this<NamespaceInfoHandler> {
    public:
    typedef std::shared_ptr<NamespaceInfoHandler> SharedPtr;
    
    static inline k2::dto::Schema schema {
        .name = namespace_info_schema_name,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "NamespaceId", false, false},
                {k2::dto::FieldType::STRING, "NamespaceName", false, false},
                {k2::dto::FieldType::INT64T, "NamespaceOid", false, false},
                {k2::dto::FieldType::INT64T, "NextPgOid", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    NamespaceInfoHandler(std::shared_ptr<K2Adapter> k2_adapter);

    ~NamespaceInfoHandler();

    CreateNamespaceTableResult CreateNamespaceTableIfNecessary();

    AddOrUpdateNamespaceResult AddOrUpdateNamespace(std::shared_ptr<SessionTransactionContext> context, std::shared_ptr<NamespaceInfo> namespace_info);

    GetNamespaceResult GetNamespace(std::shared_ptr<SessionTransactionContext> context, const std::string& namespace_id);

    ListNamespacesResult ListNamespaces(std::shared_ptr<SessionTransactionContext> context);

    // TODO: add partial update for next_pg_oid once SKV supports partial update

    private:  
    std::string collection_name_;
    std::string schema_name_;
    std::shared_ptr<K2Adapter> k2_adapter_;  
    std::shared_ptr<k2::dto::Schema> schema_ptr;  
};

} // namespace catalog
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_NAMESPACE_INFO_HANDLER_H