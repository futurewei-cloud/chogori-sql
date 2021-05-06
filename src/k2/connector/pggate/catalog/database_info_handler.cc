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

#include "pggate/catalog/database_info_handler.h"

#include <glog/logging.h>

namespace k2pg {
namespace sql {
namespace catalog {

DatabaseInfoHandler::DatabaseInfoHandler(std::shared_ptr<K2Adapter> k2_adapter)
    : collection_name_(CatalogConsts::skv_collection_name_primary_cluster),
      schema_name_(CatalogConsts::skv_schema_name_database_meta) {
    schema_ptr_ = std::make_shared<k2::dto::Schema>(schema_);
    k2_adapter_ = k2_adapter;
}

DatabaseInfoHandler::~DatabaseInfoHandler() {
}

// Verify the database_info corresponding SKVSchema in the PG primary SKVCollection doesn't exist and create it
// Called only once in sql_catalog_manager::InitPrimaryCluster()
InitDatabaseTableResult DatabaseInfoHandler::InitDatabasTable() {
    InitDatabaseTableResult response;
    
    // check to make sure the schema doesn't exists
    auto result = k2_adapter_->GetSchema(collection_name_, schema_name_, 1).get();
    if (result.status.code != 404) {  // expect NotFound
        if (result.status.is2xxOK()) {
            K2LOG_E(log::catalog, "Unexpected DatabaseInfo SKV schema already exists during init.");
            response.status = STATUS(InternalError, "Unexpected DatabaseInfo SKV schema already exists during init.");            
        } else {  // other read error 
            K2LOG_E(log::catalog, "Unexpected DatabaseInfo SKV schema read error during init.{}", result.status);
            response.status = K2Adapter::K2StatusToYBStatus(result.status);
        }
        return response;
    }

    // create the table schema since it does not exist
    auto createResult = k2_adapter_->CreateSchema(collection_name_, schema_ptr_).get();
    if (!createResult.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create schema for {} in {}, due to {}", schema_ptr_->name, collection_name_, result.status);
        response.status = K2Adapter::K2StatusToYBStatus(createResult.status);
        return response;
    }

    K2LOG_I(log::catalog, "InitDatabasTable succeeded schema as {} in {}", schema_ptr_->name, collection_name_);
    response.status = Status();  // OK
    return response;
}

AddOrUpdateDatabaseResult DatabaseInfoHandler::UpsertDatabase(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<DatabaseInfo> database_info) {
    AddOrUpdateDatabaseResult response;
    k2::dto::SKVRecord record(collection_name_, schema_ptr_);
    record.serializeNext<k2::String>(database_info->GetDatabaseId());
    record.serializeNext<k2::String>(database_info->GetDatabaseName());
    // use int64_t to represent uint32_t since since SKV does not support them
    record.serializeNext<int64_t>(database_info->GetDatabaseOid());
    record.serializeNext<int64_t>(database_info->GetNextPgOid());

    auto upsertRes = k2_adapter_->UpsertRecord(txnHandler->GetTxn(), record).get();
    if (!upsertRes.status.is2xxOK())
    {
        K2LOG_E(log::catalog, "Failed to upsert databaseinfo record {} due to {}", database_info->GetDatabaseId(), upsertRes.status);
        response.status = K2Adapter::K2StatusToYBStatus(upsertRes.status);
        return response;
    }

    response.status = Status();  // OK    
    return response;
}

GetDatabaseResult DatabaseInfoHandler::GetDatabase(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& database_id) {
    GetDatabaseResult response;
    k2::dto::SKVRecord recordKey(collection_name_, schema_ptr_);
    recordKey.serializeNext<k2::String>(database_id);

    auto result = k2_adapter_->ReadRecord(txnHandler->GetTxn(), recordKey).get();
    if (!result.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to read SKV record due to {}", result.status);
        response.status = K2Adapter::K2StatusToYBStatus(result.status);
        return response;
    }

    std::shared_ptr<DatabaseInfo> database_ptr = std::make_shared<DatabaseInfo>();
    database_ptr->SetDatabaseId(result.value.deserializeNext<k2::String>().value());
    database_ptr->SetDatabaseName(result.value.deserializeNext<k2::String>().value());
     // use int64_t to represent uint32_t since since SKV does not support them
    database_ptr->SetDatabaseOid(result.value.deserializeNext<int64_t>().value());
    database_ptr->SetNextPgOid(result.value.deserializeNext<int64_t>().value());
    response.databaseInfo = database_ptr;
    return response;
}

ListDatabaseResult DatabaseInfoHandler::ListDatabases(std::shared_ptr<PgTxnHandler> txnHandler) {
    ListDatabaseResult response;
    auto create_result = k2_adapter_->CreateScanRead(collection_name_, schema_name_).get();
    if (!create_result.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create scan read for ListDatabases due to {} in collection {}.", create_result.status, collection_name_);
        response.status = K2Adapter::K2StatusToYBStatus(create_result.status);
        return response;
    }

    std::shared_ptr<k2::Query> query = create_result.query;
    do {
        // For a forward full schema scan in SKV, we need to explictly set the start record
        query->startScanRecord.serializeNext<k2::String>("");

        auto query_result = k2_adapter_->ScanRead(txnHandler->GetTxn(), query).get();
        if (!query_result.status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to run scan read due to {}", query_result.status);
            response.status = K2Adapter::K2StatusToYBStatus(query_result.status);
            return response;
        }

        for (auto& record : query_result.records) {
            std::shared_ptr<DatabaseInfo> database_ptr = std::make_shared<DatabaseInfo>();
            database_ptr->SetDatabaseId(record.deserializeNext<k2::String>().value());
            database_ptr->SetDatabaseName(record.deserializeNext<k2::String>().value());
            // use int64_t to represent uint32_t since since SKV does not support them
            database_ptr->SetDatabaseOid(record.deserializeNext<int64_t>().value());
            database_ptr->SetNextPgOid(record.deserializeNext<int64_t>().value());
            response.databaseInfos.push_back(database_ptr);
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
    } while (!query->isDone());
    response.status = Status::OK(); 
    return response;
}

DeleteDataseResult DatabaseInfoHandler::DeleteDatabase(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<DatabaseInfo> database_info) {
    DeleteDataseResult response;
    k2::dto::SKVRecord record(collection_name_, schema_ptr_);
    record.serializeNext<k2::String>(database_info->GetDatabaseId());
    record.serializeNext<k2::String>(database_info->GetDatabaseName());
    // use int64_t to represent uint32_t since since SKV does not support them
    record.serializeNext<int64_t>(database_info->GetDatabaseOid());
    record.serializeNext<int64_t>(database_info->GetNextPgOid());

    auto delResponse = k2_adapter_->DeleteRecord(txnHandler->GetTxn(), record).get();
    if (!delResponse.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to delete database ID {} in Collection {}, due to {}", 
            database_info->GetDatabaseId(), collection_name_, delResponse.status);
        response.status = K2Adapter::K2StatusToYBStatus(delResponse.status);
        return response;
    }

    response.status = Status(); // OK
    return response;
}

} // namespace catalog
} // namespace sql
} // namespace k2pg
