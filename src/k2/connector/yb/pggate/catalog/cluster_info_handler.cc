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

#include "yb/pggate/catalog/cluster_info_handler.h"

#include <glog/logging.h>

namespace k2pg {
namespace sql {
namespace catalog {

ClusterInfoHandler::ClusterInfoHandler(std::shared_ptr<K2Adapter> k2_adapter) 
    : BaseHandler(k2_adapter), 
      collection_name_(CatalogConsts::skv_collection_name_sql_primary), 
      schema_name_(CatalogConsts::skv_schema_name_cluster_info) {
    schema_ptr_ = std::make_shared<k2::dto::Schema>(schema_);
}

ClusterInfoHandler::~ClusterInfoHandler() {
}

// Called only once in sql_catalog_manager::InitPrimaryCluster()
InitClusterInfoResult ClusterInfoHandler::InitClusterInfo(std::shared_ptr<SessionTransactionContext> context, ClusterInfo& cluster_info) {
    InitClusterInfoResult response;
    RStatus schema_result = CreateSKVSchema(collection_name_, schema_ptr_);
    if (!schema_result.IsSucceeded()) {
        response.status = std::move(schema_result);
        return response;
    }
    UpdateClusterInfoResult result = UpdateClusterInfo(context, cluster_info);
    response.status = std::move(result.status);
    return response;
}

UpdateClusterInfoResult ClusterInfoHandler::UpdateClusterInfo(std::shared_ptr<SessionTransactionContext> context, ClusterInfo& cluster_info) {
    UpdateClusterInfoResult response;
    k2::dto::SKVRecord record(collection_name_, schema_ptr_);
    record.serializeNext<k2::String>(cluster_info.GetClusterId());  
    // use signed integers for unsigned integers since SKV does not support them
    record.serializeNext<int64_t>(cluster_info.GetCatalogVersion());     
    record.serializeNext<bool>(cluster_info.IsInitdbDone());
    response.status = PersistSKVRecord(context, record);
    return response;
}

GetClusterInfoResult ClusterInfoHandler::ReadClusterInfo(std::shared_ptr<SessionTransactionContext> context, const std::string& cluster_id) {
    GetClusterInfoResult response;
    k2::dto::SKVRecord record(collection_name_, schema_ptr_);
    record.serializeNext<k2::String>(cluster_id);
    std::future<k2::ReadResult<k2::dto::SKVRecord>> read_result_future = context->GetTxn()->read(std::move(record));
    k2::ReadResult<k2::dto::SKVRecord> read_result = read_result_future.get();
    if (read_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        LOG(INFO) << "Cluster info record does not exist"; 
        response.clusterInfo = nullptr;
        response.status.Succeed();
        return response;
    }

    if (!read_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to read SKV record due to error code " << read_result.status.code
            << " and message: " << read_result.status.message;
        response.status.code = StatusCode::INTERNAL_ERROR;
        response.status.errorMessage = read_result.status.message; 
        return response;     
    }
    std::shared_ptr<ClusterInfo> cluster_info = std::make_shared<ClusterInfo>();
    cluster_info->SetClusterId(read_result.value.deserializeNext<k2::String>().value());
    // use signed integers for unsigned integers since SKV does not support them
    cluster_info->SetCatalogVersion(read_result.value.deserializeNext<int64_t>().value());
    cluster_info->SetInitdbDone(read_result.value.deserializeNext<bool>().value());
    response.clusterInfo = cluster_info;
    response.status.Succeed();
    return response;
}

} // namespace sql
} // namespace sql
} // namespace k2pg
