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

#include "pggate/catalog/sql_catalog_defaults.h"
#include "pggate/catalog/sql_catalog_entity.h"
#include "pggate/k2_adapter.h"
#include "catalog_log.h"

namespace k2pg {
namespace sql {
namespace catalog {

using k2pg::gate::CreateScanReadResult;
using k2pg::gate::K2Adapter;
using yb::Status;

struct InitNamespaceTableResult {
    Status status;
};

struct AddOrUpdateNamespaceResult {
    Status status;
};

struct GetNamespaceResult {
    Status status;
    std::shared_ptr<NamespaceInfo> namespaceInfo;
};

struct ListNamespacesResult {
    Status status;
    std::vector<std::shared_ptr<NamespaceInfo>> namespaceInfos;
};

struct DeleteNamespaceResult {
    Status status;
};

class NamespaceInfoHandler {
    public:
    typedef std::shared_ptr<NamespaceInfoHandler> SharedPtr;

    k2::dto::Schema schema_ {
        .name = CatalogConsts::skv_schema_name_namespace_info,
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

    InitNamespaceTableResult InitNamespaceTable();

    AddOrUpdateNamespaceResult AddOrUpdateNamespace(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<NamespaceInfo> namespace_info);

    GetNamespaceResult GetNamespace(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& namespace_id);

    ListNamespacesResult ListNamespaces(std::shared_ptr<PgTxnHandler> txnHandler);

    DeleteNamespaceResult DeleteNamespace(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<NamespaceInfo> namespace_info);

    // TODO: add partial update for next_pg_oid once SKV supports partial update


    private:
    std::string collection_name_;
    std::string schema_name_;
    std::shared_ptr<k2::dto::Schema> schema_ptr_;

    std::shared_ptr<K2Adapter> k2_adapter_;
};

} // namespace catalog
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_NAMESPACE_INFO_HANDLER_H
