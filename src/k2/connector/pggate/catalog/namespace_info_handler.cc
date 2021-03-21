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

#include "pggate/catalog/namespace_info_handler.h"

#include <glog/logging.h>

namespace k2pg {
namespace sql {
namespace catalog {

NamespaceInfoHandler::NamespaceInfoHandler(std::shared_ptr<K2Adapter> k2_adapter)
    : collection_name_(CatalogConsts::skv_collection_name_sql_primary),
      schema_name_(CatalogConsts::skv_schema_name_namespace_info) {
    schema_ptr_ = std::make_shared<k2::dto::Schema>(schema_);
    k2_adapter_ = k2_adapter;
}

NamespaceInfoHandler::~NamespaceInfoHandler() {
}

// Verify the Namespace(database)_info corresponding SKVSchema in the PG primary SKVCollection doesn't exist and create it
// Called only once in sql_catalog_manager::InitPrimaryCluster()
InitNamespaceTableResult NamespaceInfoHandler::InitNamespaceTable() {
    InitNamespaceTableResult response;
    
    // check to make sure the schema doesn't exists
    std::shared_ptr<k2::dto::Schema> outSchema = nullptr;
    Status schema_status = k2_adapter_->SyncGetSchema(collection_name_, schema_name_, 1, outSchema);
    if (!schema_status.IsNotFound()) {  // expect NotFound
        if (schema_status.ok()) {
            K2LOG_E(log::catalog, "Unexpected NamespaceInfo SKV schema already exists during init.");
            response.status = STATUS(InternalError, "Unexpected NamespaceInfo SKV schema already exists during init.");            
        } else {  // other read error 
            K2LOG_E(log::catalog, "Unexpected NamespaceInfo SKV schema read error during init.{}", schema_status.code());
            response.status = std::move(schema_status);
        }
        return response;
    }

    K2LOG_D(log::catalog, "Namespace info table does not exist");
    // create the table schema since it does not exist
    response.status = k2_adapter_->SyncCreateSchema(collection_name_, schema_ptr_);
    return response;
}

AddOrUpdateNamespaceResult NamespaceInfoHandler::AddOrUpdateNamespace(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<NamespaceInfo> namespace_info) {
    AddOrUpdateNamespaceResult response;
    k2::dto::SKVRecord record(collection_name_, schema_ptr_);
    record.serializeNext<k2::String>(namespace_info->GetNamespaceId());
    record.serializeNext<k2::String>(namespace_info->GetNamespaceName());
    // use int64_t to represent uint32_t since since SKV does not support them
    record.serializeNext<int64_t>(namespace_info->GetNamespaceOid());
    record.serializeNext<int64_t>(namespace_info->GetNextPgOid());
    response.status = k2_adapter_->SyncUpsertRecord(txnHandler->GetTxnHandle(), record);
    return response;
}

GetNamespaceResult NamespaceInfoHandler::GetNamespace(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& namespace_id) {
    GetNamespaceResult response;
    k2::dto::SKVRecord recordKey(collection_name_, schema_ptr_);
    recordKey.serializeNext<k2::String>(namespace_id);
    k2::dto::SKVRecord resultRecord;
    response.status = k2_adapter_->SyncReadRecord(txnHandler->GetTxnHandle(), recordKey, resultRecord);
    if (!response.status.ok()) {
        K2LOG_E(log::catalog, "Failed to read SKV record due to {}", response.status.code());
        return response;
    }
    std::shared_ptr<NamespaceInfo> namespace_ptr = std::make_shared<NamespaceInfo>();
    namespace_ptr->SetNamespaceId(resultRecord.deserializeNext<k2::String>().value());
    namespace_ptr->SetNamespaceName(resultRecord.deserializeNext<k2::String>().value());
     // use int64_t to represent uint32_t since since SKV does not support them
    namespace_ptr->SetNamespaceOid(resultRecord.deserializeNext<int64_t>().value());
    namespace_ptr->SetNextPgOid(resultRecord.deserializeNext<int64_t>().value());
    response.namespaceInfo = namespace_ptr;
    return response;
}

ListNamespacesResult NamespaceInfoHandler::ListNamespaces(std::shared_ptr<PgTxnHandler> txnHandler) {
    ListNamespacesResult response;
    auto create_result = k2_adapter_->CreateScanRead(collection_name_, schema_name_).get();
    if (!create_result.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create scan read due to {}", create_result.status);
        response.status = K2Adapter::K2StatusToYBStatus(create_result.status);
        return response;
    }

    std::shared_ptr<k2::Query> query = create_result.query;
    do {
        // For a forward full schema scan in SKV, we need to explictly set the start record
        query->startScanRecord.serializeNext<k2::String>("");

        std::vector<k2::dto::SKVRecord> outRecords;
        response.status = k2_adapter_->SyncScanRead(txnHandler->GetTxnHandle(), query, outRecords);
        if (!response.status.ok()) {
            K2LOG_E(log::catalog, "Failed to run scan read due to {}", response.status.code());
            return response;
        }

        for (auto& record : outRecords) {
            std::shared_ptr<NamespaceInfo> namespace_ptr = std::make_shared<NamespaceInfo>();
            namespace_ptr->SetNamespaceId(record.deserializeNext<k2::String>().value());
            namespace_ptr->SetNamespaceName(record.deserializeNext<k2::String>().value());
            // use int64_t to represent uint32_t since since SKV does not support them
            namespace_ptr->SetNamespaceOid(record.deserializeNext<int64_t>().value());
            namespace_ptr->SetNextPgOid(record.deserializeNext<int64_t>().value());
            response.namespaceInfos.push_back(namespace_ptr);
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
    } while (!query->isDone());
    response.status = Status::OK(); 
    return response;
}

DeleteNamespaceResult NamespaceInfoHandler::DeleteNamespace(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<NamespaceInfo> namespace_info) {
    DeleteNamespaceResult response;
    k2::dto::SKVRecord record(collection_name_, schema_ptr_);
    record.serializeNext<k2::String>(namespace_info->GetNamespaceId());
    record.serializeNext<k2::String>(namespace_info->GetNamespaceName());
    // use int64_t to represent uint32_t since since SKV does not support them
    record.serializeNext<int64_t>(namespace_info->GetNamespaceOid());
    record.serializeNext<int64_t>(namespace_info->GetNextPgOid());

    response.status = k2_adapter_->SyncDeleteRecord(txnHandler->GetTxnHandle(), record);
    if (!response.status.ok()) {
        K2LOG_E(log::catalog, "Failed to delete namespace ID {} in Collection {}, due to {}", namespace_info->GetNamespaceId(), collection_name_, response.status.code());
    }

    return response;
}

} // namespace catalog
} // namespace sql
} // namespace k2pg
