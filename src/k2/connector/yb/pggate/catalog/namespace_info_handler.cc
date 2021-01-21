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
    : BaseHandler(k2_adapter),
      collection_name_(CatalogConsts::skv_collection_name_sql_primary),
      schema_name_(CatalogConsts::skv_schema_name_namespace_info) {
    schema_ptr_ = std::make_shared<k2::dto::Schema>(schema_);
}

NamespaceInfoHandler::~NamespaceInfoHandler() {
}

// Verify the Namespace(database)_info corresponding SKVSchema in the PG primary SKVCollection doesn't exist and create it
// Called only once in sql_catalog_manager::InitPrimaryCluster()
InitNamespaceTableResult NamespaceInfoHandler::InitNamespaceTable() {
    // check to make sure the schema doesn't exists
    std::future<k2::GetSchemaResult> schema_result_future = k2_adapter_->GetSchema(collection_name_, schema_name_, 1);
    k2::GetSchemaResult schema_result = schema_result_future.get();
    InitNamespaceTableResult response;
    // TODO: double check if this check is valid for schema
    CHECK(schema_result.status == k2::dto::K23SIStatus::KeyNotFound);
    if (schema_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        K2LOG_D(log::catalog, "Namespace info table does not exist");
        // create the table schema since it does not exist
        RStatus schema_result = CreateSKVSchema(collection_name_, schema_ptr_);
        response.status = std::move(schema_result);
    } else {
        K2LOG_E(log::catalog, "Unexpected Namespace SKV schema already exists during init.");
        response.status.code = StatusCode::INTERNAL_ERROR;
        response.status.errorMessage = std::move("Unexpected Namespace SKV schema already exists during init.");
        return response;
    }
    return response;
}

AddOrUpdateNamespaceResult NamespaceInfoHandler::AddOrUpdateNamespace(std::shared_ptr<SessionTransactionContext> context, std::shared_ptr<NamespaceInfo> namespace_info) {
    AddOrUpdateNamespaceResult response;
    k2::dto::SKVRecord record(collection_name_, schema_ptr_);
    record.serializeNext<k2::String>(namespace_info->GetNamespaceId());
    record.serializeNext<k2::String>(namespace_info->GetNamespaceName());
    // use int64_t to represent uint32_t since since SKV does not support them
    record.serializeNext<int64_t>(namespace_info->GetNamespaceOid());
    record.serializeNext<int64_t>(namespace_info->GetNextPgOid());
    response.status = PersistSKVRecord(context, record);
    return response;
}

GetNamespaceResult NamespaceInfoHandler::GetNamespace(std::shared_ptr<SessionTransactionContext> context, const std::string& namespace_id) {
    GetNamespaceResult response;
    k2::dto::SKVRecord record(collection_name_, schema_ptr_);
    record.serializeNext<k2::String>(namespace_id);
    std::future<k2::ReadResult<k2::dto::SKVRecord>> read_result_future = context->GetTxn()->read(std::move(record));
    k2::ReadResult<k2::dto::SKVRecord> read_result = read_result_future.get();
    if (read_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        K2LOG_D(log::catalog, "SKV record does not exist for namespace {}", namespace_id);
        response.namespaceInfo = nullptr;
        response.status.Succeed();
        return response;
    }

    if (!read_result.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to read SKV record due to {}", read_result.status);
        response.status.code = StatusCode::INTERNAL_ERROR;
        response.status.errorMessage = std::move(read_result.status.message);
        return response;
    }
    std::shared_ptr<NamespaceInfo> namespace_ptr = std::make_shared<NamespaceInfo>();
    namespace_ptr->SetNamespaceId(read_result.value.deserializeNext<k2::String>().value());
    namespace_ptr->SetNamespaceName(read_result.value.deserializeNext<k2::String>().value());
     // use int64_t to represent uint32_t since since SKV does not support them
    namespace_ptr->SetNamespaceOid(read_result.value.deserializeNext<int64_t>().value());
    namespace_ptr->SetNextPgOid(read_result.value.deserializeNext<int64_t>().value());
    response.namespaceInfo = namespace_ptr;
    response.status.Succeed();
    return response;
}

ListNamespacesResult NamespaceInfoHandler::ListNamespaces(std::shared_ptr<SessionTransactionContext> context) {
    ListNamespacesResult response;
    std::future<CreateScanReadResult> create_result_future = k2_adapter_->CreateScanRead(collection_name_, schema_name_);
    CreateScanReadResult create_result = create_result_future.get();
    if (!create_result.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create scan read due to {}", create_result.status);
        response.status.code = StatusCode::INTERNAL_ERROR;
        response.status.errorMessage = std::move(create_result.status.message);
        return response;
    }

    std::shared_ptr<k2::Query> query = create_result.query;
    do {
        // For a forward full schema scan in SKV, we need to explictly set the start record
        query->startScanRecord.serializeNext<k2::String>("");

        std::future<k2::QueryResult> query_result_future = context->GetTxn()->scanRead(query);
        k2::QueryResult query_result = query_result_future.get();
        if (!query_result.status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to run scan read due to {}", query_result.status);
            response.status.code = StatusCode::INTERNAL_ERROR;
            response.status.errorMessage = std::move(query_result.status.message);
            return response;
        }

        if (!query_result.records.empty()) {
            for (k2::dto::SKVRecord& record : query_result.records) {
                std::shared_ptr<NamespaceInfo> namespace_ptr = std::make_shared<NamespaceInfo>();
                namespace_ptr->SetNamespaceId(record.deserializeNext<k2::String>().value());
                namespace_ptr->SetNamespaceName(record.deserializeNext<k2::String>().value());
                // use int64_t to represent uint32_t since since SKV does not support them
                namespace_ptr->SetNamespaceOid(record.deserializeNext<int64_t>().value());
                namespace_ptr->SetNextPgOid(record.deserializeNext<int64_t>().value());
                response.namespaceInfos.push_back(namespace_ptr);
            }
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
    } while (!query->isDone());
    response.status.Succeed();
    return response;
}

DeleteNamespaceResult NamespaceInfoHandler::DeleteNamespace(std::shared_ptr<SessionTransactionContext> context, std::shared_ptr<NamespaceInfo> namespace_info) {
    DeleteNamespaceResult response;
    k2::dto::SKVRecord record(collection_name_, schema_ptr_);
    record.serializeNext<k2::String>(namespace_info->GetNamespaceId());
    record.serializeNext<k2::String>(namespace_info->GetNamespaceName());
    // use int64_t to represent uint32_t since since SKV does not support them
    record.serializeNext<int64_t>(namespace_info->GetNamespaceOid());
    record.serializeNext<int64_t>(namespace_info->GetNextPgOid());
    response.status = DeleteSKVRecord(context, record);
    return response;
}

RStatus NamespaceInfoHandler::CreateSKVCollection(const std::string& collection_name, const std::string& nsName)
{
    RStatus response;

    auto result = k2_adapter_->CreateCollection(collection_name, nsName).get();
    if (!result.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create SKV Collection {}, due to {}", collection_name, result);
        response.code = StatusCode::INTERNAL_ERROR;
        response.errorMessage = std::move(result.message);
    } else {
        response.Succeed();
    }
    return response;
}

} // namespace catalog
} // namespace sql
} // namespace k2pg
