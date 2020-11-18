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

#include "yb/pggate/catalog/table_info_handler.h"

#include <glog/logging.h>

namespace k2pg {
namespace sql {

TableInfoHandler::TableInfoHandler(std::shared_ptr<K2Adapter> k2_adapter) 
    : k2_adapter_(k2_adapter),  
    tablelist_schema_name_(sys_catalog_tablelist_schema_name),
    table_schema_name_(sys_catalog_table_schema_schema_name), 
    index_schema_name_(sys_catalog_index_schema_schema_name) {
    tablelist_schema_ptr = std::make_shared<k2::dto::Schema>();
    *(tablelist_schema_ptr.get()) = sys_catalog_tablelist_schema;
    table_schema_ptr = std::make_shared<k2::dto::Schema>();
    *(table_schema_ptr.get()) = sys_catalog_table_schema;
    index_schema_ptr = std::make_shared<k2::dto::Schema>();
    *(index_schema_ptr.get()) = sys_catalog_index_schema;
}

TableInfoHandler::~TableInfoHandler() {
}

CreateSysTablesResult TableInfoHandler::CreateSysTablesIfNecessary(const Context& context, std::string collection_name) {
    CreateSysTablesResult response;
    // TODO: use sequential calls for now, could be optimized later for concurrent SKV api calls
    CheckSysTableResult result = CheckAndCreateSysTable(context, collection_name, tablelist_schema_name_, tablelist_schema_ptr);
    if (!result.status.succeeded) {
        response.status = std::move(result.status);
        return response;
    }

    result = CheckAndCreateSysTable(context, collection_name, table_schema_name_, table_schema_ptr);
     if (!result.status.succeeded) {
        response.status = std::move(result.status);
        return response;
    }
   
    result = CheckAndCreateSysTable(context, collection_name, index_schema_name_, index_schema_ptr);
     if (!result.status.succeeded) {
        response.status = std::move(result.status);
        return response;
    }

    response.status.succeeded = true;
    return response;
}

CheckSysTableResult TableInfoHandler::CheckAndCreateSysTable(const Context& context, std::string collection_name, 
        std::string schema_name, std::shared_ptr<k2::dto::Schema> schema) {
     // check if the schema already exists or not, which is an indication of whether if we have created the table or not
    std::future<k2::GetSchemaResult> schema_result_future = k2_adapter_->GetSchema(collection_name, schema_name, 1);
    k2::GetSchemaResult schema_result = schema_result_future.get();   
    CheckSysTableResult response;
    // TODO: double check if this check is valid for schema
    if (schema_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        response.status.succeeded = true;
        LOG(INFO) << schema_name << " table does not exist in " << collection_name; 
        // create the table schema since it does not exist
        std::future<k2::CreateSchemaResult> result_future = k2_adapter_->CreateSchema(collection_name, schema);
        k2::CreateSchemaResult result = result_future.get();
        if (!result.status.is2xxOK()) {
            LOG(FATAL) << "Failed to create SKV schema for " << schema_name << "in" << collection_name
                << " due to error code " << result.status.code
                << " and message: " << result.status.message;
            response.status.succeeded = false;
            response.status.errorCode = result.status.code;
            response.status.errorMessage = std::move(result.status.message);
            return response;            
        }        
    }
    response.status.succeeded = true;
    return response;
}

CreateUpdateTableResult TableInfoHandler::CreateOrUpdateTable(const Context& context, std::string collection_name, std::string table_id, 
        uint32_t table_oid, std::shared_ptr<TableInfo> table) {
    CreateUpdateTableResult response;

    return response;
}

GetTableResult TableInfoHandler::GetTable(const Context& context, std::string collection_name, std::string table_id) {
    GetTableResult response;

    return response;
}
    
ListTablesResult TableInfoHandler::ListTables(const Context& context, std::string collection_name, bool isSysTableIncluded) {
    ListTablesResult response;

    return response;
}

CheckSchemaResult TableInfoHandler::CheckSchema(const Context& context, std::string collection_name, std::string schema_name) {
    CheckSchemaResult response;
    std::future<k2::GetSchemaResult> schema_result_future = k2_adapter_->GetSchema(collection_name, schema_name, 1);
    k2::GetSchemaResult schema_result = schema_result_future.get();
    if (schema_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        response.exist = false;
        response.status.succeeded = true;
    } else if (schema_result.status.is2xxOK()) {
        if(schema_result.schema != nullptr) {
            response.exist = true;
            response.status.succeeded = true;
        } else {
            response.exist = false;
            response.status.succeeded = true;          
        }
    } else {
        response.status.succeeded = false;          
        response.status.errorCode = schema_result.status.code;
        response.status.errorMessage = std::move(schema_result.status.message);    
    }
    
    return response;
}

} // namespace sql
} // namespace k2pg