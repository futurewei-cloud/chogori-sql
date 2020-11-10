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

ClusterInfoHandler::ClusterInfoHandler(std::shared_ptr<K2Adapter> k2_adapter) 
    : collection_name_(sql_primary_collection_name), 
      partition_name_(cluster_info_partition_name), 
      k2_adapter_(k2_adapter) {
    schema_ptr = std::make_shared<k2::dto::Schema>();
    *(schema_ptr.get()) = schema;
}

ClusterInfoHandler::~ClusterInfoHandler() {
}

CreateClusterInfoResponse ClusterInfoHandler::CreateClusterInfo(ClusterInfo& cluster_info) {
    std::future<k2::CreateSchemaResult> schema_result_future = k2_adapter_->CreateSchema(collection_name_, schema);
    k2::CreateSchemaResult schema_result = schema_result_future.get();
    CreateClusterInfoResponse response;
    if (!schema_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to create schema due to error code " << schema_result.status.code
            << " and message: " << schema_result.status.message;
        response.succeeded = false;
        response.errorCode = schema_result.status.code;
        response.errorMessage = std::move(schema_result.status.message);
        return response;
    }

    std::future<K23SITxn> txn_future = k2_adapter_->beginTransaction();
    K23SITxn txn = txn_future.get();

    k2::dto::SKVRecord record(collection_name_, schema_ptr);
    record.serializeNext<k2::String>(cluster_info.GetClusterId());  
    // use signed integers for unsigned integers since SKV does not support them
    record.serializeNext<int64_t>(cluster_info.GetCatalogVersion());  
    record.serializeNext<bool>(cluster_info.IsInitdbDone());
    std::future<k2::WriteResult> write_result_future = txn.write(std::move(record), false);
    k2::WriteResult write_result = write_result_future.get();
    if (!write_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to create SKV record due to error code " << write_result.status.code
            << " and message: " << write_result.status.message;
        response.succeeded = false;
        response.errorCode = write_result.status.code;
        response.errorMessage = std::move(write_result.status.message);
        return response;  
    }

    std::future<k2::EndResult> txn_result_future = txn.endTxn(true);
    k2::EndResult txn_result = txn_result_future.get();
    if (!txn_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to commit transaction due to error code " << txn_result.status.code
            << " and message: " << txn_result.status.message;
        response.succeeded = false;
        response.errorCode = txn_result.status.code;
        response.errorMessage = std::move(txn_result.status.message);
        return response;             
    }
    response.succeeded = true;
    return response;
}

UpdateClusterInfoResponse ClusterInfoHandler::UpdateClusterInfo(ClusterInfo& cluster_info) {
    UpdateClusterInfoResponse response;
    std::future<K23SITxn> txn_future = k2_adapter_->beginTransaction();
    K23SITxn txn = txn_future.get();

    k2::dto::SKVRecord record(collection_name_, schema_ptr);
    record.serializeNext<k2::String>(cluster_info.GetClusterId());  
    // use signed integers for unsigned integers since SKV does not support them
    record.serializeNext<int64_t>(cluster_info.GetCatalogVersion());     
    record.serializeNext<bool>(cluster_info.IsInitdbDone());
    std::future<k2::WriteResult> write_result_future = txn.write(std::move(record), true);
    k2::WriteResult write_result = write_result_future.get();
    if (!write_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to create SKV record due to error code " << write_result.status.code
            << " and message: " << write_result.status.message;
        response.succeeded = false;
        response.errorCode = write_result.status.code;
        response.errorMessage = std::move(write_result.status.message);
        return response;    
    }

    std::future<k2::EndResult> txn_result_future = txn.endTxn(true);
    k2::EndResult txn_result = txn_result_future.get();
    if (!txn_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to commit transaction due to error code " << txn_result.status.code
            << " and message: " << txn_result.status.message;
        response.succeeded = false;
        response.errorCode = txn_result.status.code;
        response.errorMessage = std::move(txn_result.status.message);
        return response;                        
    }
    response.succeeded = true;
    return response;
}

ReadClusterInfoResponse ClusterInfoHandler::ReadClusterInfo() {
    std::future<K23SITxn> txn_future = k2_adapter_->beginTransaction();
    K23SITxn txn = txn_future.get();
    ReadClusterInfoResponse response;

    k2::dto::SKVRecord record(collection_name_, schema_ptr);
    record.serializeNext<k2::String>(partition_name_);
    std::future<k2::ReadResult<k2::dto::SKVRecord>> read_result_future = txn.read(std::move(record));
    k2::ReadResult<k2::dto::SKVRecord> read_result = read_result_future.get();
    if (read_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        LOG(INFO) << "Cluster info record does not exist"; 
        response.exist = false;
        response.succeeded = true;
        return response;
    }

    if (!read_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to read SKV record due to error code " << read_result.status.code
            << " and message: " << read_result.status.message;
        response.succeeded = false;
        response.errorCode = read_result.status.code;
        response.errorMessage = read_result.status.message; 
        return response;     
    }
    response.clusterInfo.SetClusterId(read_result.value.deserializeNext<k2::String>().value());
    // use signed integers for unsigned integers since SKV does not support them
    response.clusterInfo.SetCatalogVersion(read_result.value.deserializeNext<int64_t>().value());
    response.clusterInfo.SetInitdbDone(read_result.value.deserializeNext<bool>().value());
    response.exist = true;

    // TODO: double check if we need to commit the transaction for read only call
    std::future<k2::EndResult> txn_result_future = txn.endTxn(true);
    k2::EndResult txn_result = txn_result_future.get();
    if (!txn_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to commit transaction due to error code " << txn_result.status.code
            << " and message: " << txn_result.status.message;
        response.succeeded = false;
        response.errorCode = txn_result.status.code;
        response.errorMessage = std::move(txn_result.status.message);
        return response;                                    
    }
    response.succeeded = true;
    return response;
}

} // namespace sql
} // namespace k2pg
