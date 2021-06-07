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
#pragma once

#include <string>

#include "pggate/catalog/sql_catalog_defaults.h"
#include "pggate/catalog/sql_catalog_entity.h"
#include "pggate/k2_adapter.h"
#include "catalog_log.h"

namespace k2pg {
namespace sql {
namespace catalog {

using k2pg::gate::K2Adapter;
using k2pg::Status;

struct InitClusterInfoResult {
    Status status;
};

struct UpdateClusterInfoResult {
    Status status;
};

struct GetClusterInfoResult {
    Status status;
    std::shared_ptr<ClusterInfo> clusterInfo;
};

class ClusterInfoHandler {
    public:
    typedef std::shared_ptr<ClusterInfoHandler> SharedPtr;

    k2::dto::Schema schema_ {
        .name = CatalogConsts::skv_schema_name_cluster_meta,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "ClusterId", false, false},
                {k2::dto::FieldType::INT64T, "CatalogVersion", false, false},
                {k2::dto::FieldType::BOOL, "InitDbDone", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    ClusterInfoHandler(std::shared_ptr<K2Adapter> k2_adapter);
    ~ClusterInfoHandler();

    InitClusterInfoResult InitClusterInfo(std::shared_ptr<PgTxnHandler> txnHandler, ClusterInfo& cluster_info);

    UpdateClusterInfoResult UpdateClusterInfo(std::shared_ptr<PgTxnHandler> txnHandler, ClusterInfo& cluster_info);

    GetClusterInfoResult GetClusterInfo(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& cluster_id);

    private:
    std::string collection_name_;
    std::string schema_name_;
    std::shared_ptr<k2::dto::Schema> schema_ptr_;
    std::shared_ptr<K2Adapter> k2_adapter_;
};

} // namespace catalog
} // namespace sql
} // namespace k2pg