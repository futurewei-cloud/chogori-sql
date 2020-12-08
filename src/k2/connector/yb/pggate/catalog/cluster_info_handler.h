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

#ifndef CHOGORI_SQL_CLUSTER_INFO_HANDLER_H
#define CHOGORI_SQL_CLUSTER_INFO_HANDLER_H

#include <string>

#include "yb/pggate/catalog/base_handler.h"

namespace k2pg {
namespace sql {
namespace catalog {

struct CreateClusterInfoResult {
    RStatus status;
};

struct UpdateClusterInfoResult {
    RStatus status;
};

struct GetClusterInfoResult {
    RStatus status;
    std::shared_ptr<ClusterInfo> clusterInfo;
};

class ClusterInfoHandler : public BaseHandler {
    public:
    typedef std::shared_ptr<ClusterInfoHandler> SharedPtr;

    static inline k2::dto::Schema schema {
        .name = CatalogConsts::skv_schema_name_cluster_info,
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
    
    CreateClusterInfoResult CreateClusterInfo(std::shared_ptr<SessionTransactionContext> context, ClusterInfo& cluster_info);

    UpdateClusterInfoResult UpdateClusterInfo(std::shared_ptr<SessionTransactionContext> context, ClusterInfo& cluster_info);

    GetClusterInfoResult ReadClusterInfo(std::shared_ptr<SessionTransactionContext> context, const std::string& cluster_id);

    private:  
    std::string collection_name_;
    std::string schema_name_;
    std::shared_ptr<k2::dto::Schema> schema_ptr;  
};

} // namespace sql
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_CLUSTER_INFO_HANDLER_H