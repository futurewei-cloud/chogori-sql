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

#include "yb/pggate/cluster_info_handler.h"

namespace k2pg {
namespace sql {

ClusterInfoHandler::ClusterInfoHandler(scoped_refptr<K2Adapter> k2_adapter) : k2_adapter_(k2_adapter) {
    cluster_info_schema_ptr = std::make_shared<k2::dto::Schema>();
    *(cluster_info_schema_ptr.get()) = cluster_info_schema;
}

ClusterInfoHandler::~ClusterInfoHandler() {
}

Status ClusterInfoHandler::CreateClusterInfo(ClusterInfo& cluster_info) {
    std::future<k2::CreateSchemaResult> schema_result_future = k2_adapter_->CreateSchema(cluster_info_collection_name, cluster_info_schema);
    k2::CreateSchemaResult schema_result = schema_result_future.get();
    if (!schema_result.status.is2xxOK()) {
        return STATUS_FORMAT(IOError, "Failed to create schema due to error code $0 and message $1", 
            schema_result.status.code, schema_result.status.message);
    }

    std::future<K23SITxn> txn_future = k2_adapter_->beginTransaction();
    K23SITxn txn = txn_future.get();

    k2::dto::SKVRecord record(cluster_info_collection_name, cluster_info_schema_ptr);
    record.serializeNext<k2::String>(cluster_info.GetClusterId());  
    record.serializeNext<int16_t>(cluster_info.IsInitdbDone() ? 1 : 0);
    std::future<k2::WriteResult> write_result_future = txn.write(std::move(record), false);
    k2::WriteResult write_result = write_result_future.get();
    if (!write_result.status.is2xxOK()) {
        return STATUS_FORMAT(IOError, "Failed to create SKV record due to error code $0 and message $1", 
            write_result.status.code, write_result.status.message);       
    }

    std::future<k2::EndResult> txn_result_future = txn.endTxn(true);
    k2::EndResult txn_result = txn_result_future.get();
    if (!txn_result.status.is2xxOK()) {
        return STATUS_FORMAT(IOError, "Failed to create commit transaction due to error code $0 and message $1", 
            txn_result.status.code, txn_result.status.message);              
    }
    return Status::OK();
}

Status ClusterInfoHandler::UpdateClusterInfo(ClusterInfo& cluster_info) {
    std::future<K23SITxn> txn_future = k2_adapter_->beginTransaction();
    K23SITxn txn = txn_future.get();

    k2::dto::SKVRecord record(cluster_info_collection_name, cluster_info_schema_ptr);
    record.serializeNext<k2::String>(cluster_info.GetClusterId());  
    record.serializeNext<int16_t>(cluster_info.IsInitdbDone() ? 1 : 0);
    std::future<k2::WriteResult> write_result_future = txn.write(std::move(record), true);
    k2::WriteResult write_result = write_result_future.get();
    if (!write_result.status.is2xxOK()) {
        return STATUS_FORMAT(IOError, "Failed to create SKV record due to error code $0 and message $1", 
            write_result.status.code, write_result.status.message);       
    }

    std::future<k2::EndResult> txn_result_future = txn.endTxn(true);
    k2::EndResult txn_result = txn_result_future.get();
    if (!txn_result.status.is2xxOK()) {
        return STATUS_FORMAT(IOError, "Failed to create commit transaction due to error code $0 and message $1", 
            txn_result.status.code, txn_result.status.message);              
    }
    return Status::OK();
}

Status ClusterInfoHandler::ReadClusterInfo(ClusterInfo& cluster_info) {
    std::future<K23SITxn> txn_future = k2_adapter_->beginTransaction();
    K23SITxn txn = txn_future.get();

    k2::dto::SKVRecord record(cluster_info_collection_name, cluster_info_schema_ptr);
    record.serializeNext<k2::String>(cluster_info_partition_name);
    std::future<k2::ReadResult<k2::dto::SKVRecord>> read_result_future = txn.read(std::move(record));
    k2::ReadResult<k2::dto::SKVRecord> read_result = read_result_future.get();
    if (!read_result.status.is2xxOK()) {
        return STATUS_FORMAT(IOError, "Failed to create SKV record due to error code $0 and message $1", 
            read_result.status.code, read_result.status.message);       
    }
    cluster_info.SetClusterId(read_result.value.deserializeNext<k2::String>().value());
    cluster_info.SetInitdbDone(read_result.value.deserializeNext<int16_t>().value() == 1 ? true : false);

    std::future<k2::EndResult> txn_result_future = txn.endTxn(true);
    k2::EndResult txn_result = txn_result_future.get();
    if (!txn_result.status.is2xxOK()) {
        return STATUS_FORMAT(IOError, "Failed to create commit transaction due to error code $0 and message $1", 
            txn_result.status.code, txn_result.status.message);              
    }
    return Status::OK();
}

} // namespace sql
} // namespace k2pg
