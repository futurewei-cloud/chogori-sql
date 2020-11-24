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

#include "yb/pggate/catalog/namespace_info_handler.h"

#include <glog/logging.h>

namespace k2pg {
namespace sql {
namespace catalog {

NamespaceInfoHandler::NamespaceInfoHandler(std::shared_ptr<K2Adapter> k2_adapter) 
    : collection_name_(sql_primary_collection_name), 
      schema_name_(namespace_info_schema_name), 
      k2_adapter_(k2_adapter) {
    schema_ptr = std::make_shared<k2::dto::Schema>();
    *(schema_ptr.get()) = schema;
}

NamespaceInfoHandler::~NamespaceInfoHandler() {
}

CreateNamespaceTableResult NamespaceInfoHandler::CreateNamespaceTableIfNecessary() {
    // check if the schema already exists or not, which is an indication of whether if we have created the table or not
    std::future<k2::GetSchemaResult> schema_result_future = k2_adapter_->GetSchema(collection_name_, schema_name_, 1);
    k2::GetSchemaResult schema_result = schema_result_future.get();
    CreateNamespaceTableResult response;
    // TODO: double check if this check is valid for schema
    if (schema_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        LOG(INFO) << "Namespace info table does not exist"; 
        // create the table schema since it does not exist
        std::future<k2::CreateSchemaResult> result_future = k2_adapter_->CreateSchema(collection_name_, schema_ptr);
        k2::CreateSchemaResult result = result_future.get();
        if (!result.status.is2xxOK()) {
            LOG(FATAL) << "Failed to create SKV schema for namespaces due to error code " << result.status.code
                << " and message: " << result.status.message;
            response.status.code = StatusCode::INTERNAL_ERROR;
            response.status.errorMessage = std::move(result.status.message);
            return response;            
       }
    }
    response.status.Succeed();
    return response;
}

AddOrUpdateNamespaceResult NamespaceInfoHandler::AddOrUpdateNamespace(std::shared_ptr<Context> context, std::shared_ptr<NamespaceInfo> namespace_info) {
    AddOrUpdateNamespaceResult response;     
    k2::dto::SKVRecord record(collection_name_, schema_ptr);
    record.serializeNext<k2::String>(namespace_info->GetNamespaceId());  
    record.serializeNext<k2::String>(namespace_info->GetNamespaceName());  
    // use signed integers for unsigned integers since SKV does not support them
    record.serializeNext<int32_t>(namespace_info->GetNamespaceOid());  
    record.serializeNext<int32_t>(namespace_info->GetNextPgOid());
    std::future<k2::WriteResult> write_result_future = context->GetTxn()->write(std::move(record), false);
    k2::WriteResult write_result = write_result_future.get();
    if (!write_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to add or update SKV record due to error code " << write_result.status.code
            << " and message: " << write_result.status.message;
        response.status.code = StatusCode::INTERNAL_ERROR;
        response.status.errorMessage = std::move(write_result.status.message);
        return response;  
    }
    response.status.Succeed();
    return response;
}

GetNamespaceResult NamespaceInfoHandler::GetNamespace(std::shared_ptr<Context> context, const std::string& namespace_id) {
    GetNamespaceResult response;
    k2::dto::SKVRecord record(collection_name_, schema_ptr);
    record.serializeNext<k2::String>(namespace_id);
    std::future<k2::ReadResult<k2::dto::SKVRecord>> read_result_future = context->GetTxn()->read(std::move(record));
    k2::ReadResult<k2::dto::SKVRecord> read_result = read_result_future.get();
    if (read_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        LOG(INFO) << "SKV record does not exist for namespace " << namespace_id; 
        response.namespaceInfo = nullptr;
        response.status.Succeed();
        return response;
    }

    if (!read_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to read SKV record due to error code " << read_result.status.code
            << " and message: " << read_result.status.message;
        response.status.code = StatusCode::INTERNAL_ERROR;
        response.status.errorMessage = std::move(read_result.status.message); 
        return response;     
    }
    std::shared_ptr<NamespaceInfo> namespace_ptr = std::make_shared<NamespaceInfo>();
    namespace_ptr->SetNamespaceId(read_result.value.deserializeNext<k2::String>().value());
    namespace_ptr->SetNamespaceName(read_result.value.deserializeNext<k2::String>().value());
    // use signed integers for unsigned integers since SKV does not support them
    namespace_ptr->SetNamespaceOid(read_result.value.deserializeNext<int32_t>().value());
    namespace_ptr->SetNextPgOid(read_result.value.deserializeNext<int32_t>().value());
    response.namespaceInfo = namespace_ptr;
    response.status.Succeed();
    return response;
}

ListNamespacesResult NamespaceInfoHandler::ListNamespaces(std::shared_ptr<Context> context) {
    ListNamespacesResult response;
    std::future<CreateScanReadResult> create_result_future = k2_adapter_->CreateScanRead(collection_name_, schema_name_);
    CreateScanReadResult create_result = create_result_future.get();
    if (!create_result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to create scan read due to error code " << create_result.status.code
            << " and message: " << create_result.status.message;
        response.status.code = StatusCode::INTERNAL_ERROR;
        response.status.errorMessage = std::move(create_result.status.message);
        return response;                                           
    }

    std::shared_ptr<k2::Query> query = create_result.query;
    do {
        std::future<k2::QueryResult> query_result_future = context->GetTxn()->scanRead(query);
        k2::QueryResult query_result = query_result_future.get();
        if (!query_result.status.is2xxOK()) {
            LOG(FATAL) << "Failed to run scan read due to error code " << query_result.status.code
                << " and message: " << query_result.status.message;
            response.status.code = StatusCode::INTERNAL_ERROR;
            response.status.errorMessage = std::move(query_result.status.message);
            return response;                                                  
        }

        if (!query_result.records.empty()) {
            for (k2::dto::SKVRecord& record : query_result.records) {
                std::shared_ptr<NamespaceInfo> namespace_ptr = std::make_shared<NamespaceInfo>();
                namespace_ptr->SetNamespaceId(record.deserializeNext<k2::String>().value());
                namespace_ptr->SetNamespaceName(record.deserializeNext<k2::String>().value());
                // use signed integers for unsigned integers since SKV does not support them
                namespace_ptr->SetNamespaceOid(record.deserializeNext<int32_t>().value());
                namespace_ptr->SetNextPgOid(record.deserializeNext<int32_t>().value()); 
                response.namespaceInfos.push_back(namespace_ptr);
            } 
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
    } while (!query->isDone());
    response.status.Succeed();
    return response;
}

} // namespace catalog
} // namespace sql
} // namespace k2pg
