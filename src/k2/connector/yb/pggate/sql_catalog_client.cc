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

#include "yb/pggate/sql_catalog_client.h"

namespace k2pg {
namespace sql {

Status SqlCatalogClient::IsInitDbDone(bool* isDone) {                                  
  return catalog_manager_->IsInitDbDone(isDone);
}

Status SqlCatalogClient::CreateNamespace(const std::string& namespace_name,
                                 const std::string& creator_role_name,
                                 const std::string& namespace_id,
                                 const std::string& source_namespace_id,
                                 const std::optional<uint32_t>& next_pg_oid) {
  std::shared_ptr<CreateNamespaceRequest> request = std::make_shared<CreateNamespaceRequest>();
  request->namespaceName = std::move(namespace_name);
  if(!namespace_id.empty()) {
    request->namespaceId = std::move(namespace_id);
  }
  if (!creator_role_name.empty()) {
    request->creatorRoleName = std::move(creator_role_name); 
  }  
  if (!source_namespace_id.empty()) {
    request->sourceNamespaceId = std::move(source_namespace_id);
  }
  if (next_pg_oid) {
    request->nextPgOid = next_pg_oid;  
  }
  std::shared_ptr<CreateNamespaceResponse> response;
  catalog_manager_->CreateNamespace(request, &response);
  if (!response->errorMessage.empty()) {
     return STATUS_SUBSTITUTE(RuntimeError,
                               "Failed to create namespace $0 due to error: $1", namespace_name, response->errorMessage);
  }
  return Status::OK();
}

Status SqlCatalogClient::DeleteNamespace(const std::string& namespace_name,
                                 const std::string& namespace_id) {
  std::shared_ptr<DeleteNamespaceRequest> request = std::make_shared<DeleteNamespaceRequest>();
  request->namespaceName = std::move(namespace_name);
  if (!namespace_id.empty()) {
    request->namespaceId = std::move(namespace_id);
  }     
  std::shared_ptr<DeleteNamespaceResponse> response = std::make_shared<DeleteNamespaceResponse>();
  catalog_manager_->DeleteNamespace(request, &response);
  if (!response->errorMessage.empty()) {
     return STATUS_SUBSTITUTE(RuntimeError,
                               "Failed to delete namespace $0 due to error: $1", namespace_name, response->errorMessage);
  }                             
  return Status::OK();
}

Status SqlCatalogClient::CreateTable(
    const std::string& namespace_name, 
    const std::string& table_name, 
    const PgObjectId& table_id, 
    PgSchema& schema, 
    bool is_pg_catalog_table, 
    bool is_shared_table, 
    bool if_not_exist) {
  std::shared_ptr<CreateTableRequest> request = std::make_shared<CreateTableRequest>();
  request->namespaceName = std::move(namespace_name);
  request->namespaceId = table_id.database_oid;
  request->tableName = std::move(table_name);
  request->tableId = table_id.object_oid;
  request->schema = std::move(schema);
  request->isSysCatalogTable = is_pg_catalog_table;
  request->isSharedTable = is_shared_table;
  std::shared_ptr<CreateTableResponse> response = std::make_shared<CreateTableResponse>();   
  catalog_manager_->CreateTable(request, &response);                                 
  if (!response->errorMessage.empty()) {
     return STATUS_SUBSTITUTE(RuntimeError,
                               "Failed to create table $0 in database $1 due to error: $2", table_name, namespace_name, response->errorMessage);
  }     
  return Status::OK();
}

Status SqlCatalogClient::CreateIndexTable(
    const std::string& namespace_name, 
    const std::string& table_name, 
    const PgObjectId& table_id, 
    const PgObjectId& base_table_id, 
    PgSchema& schema, 
    bool is_unique_index, 
    bool skip_index_backfill,
    bool is_pg_catalog_table, 
    bool is_shared_table, 
    bool if_not_exist) {

  // TODO: add implementation                                   
  return Status::OK();
}

Status SqlCatalogClient::DeleteTable(const PgOid database_oid, const PgOid table_id, bool wait) {
  std::shared_ptr<DeleteTableRequest> request = std::make_shared<DeleteTableRequest>();
  request->namespaceId = database_oid;
  request->tableId = table_id;
  request->isIndexTable = false;
  std::shared_ptr<DeleteTableResponse> response = std::make_shared<DeleteTableResponse>();
  catalog_manager_->DeleteTable(request, &response);
  if (!response->errorMessage.empty()) {
     return STATUS_SUBSTITUTE(RuntimeError,
                               "Failed to delete table $0 due to error: $1", table_id, response->errorMessage);
  }   
  return Status::OK();
}

Status SqlCatalogClient::DeleteIndexTable(const PgOid database_oid, const PgOid table_id, PgOid *base_table_id, bool wait) {
  std::shared_ptr<DeleteTableRequest> request = std::make_shared<DeleteTableRequest>();
  request->namespaceId = database_oid;
  request->tableId = table_id;
  request->isIndexTable = true;
  std::shared_ptr<DeleteTableResponse> response = std::make_shared<DeleteTableResponse>();
  catalog_manager_->DeleteTable(request, &response);
  if (!response->errorMessage.empty()) {
     return STATUS_SUBSTITUTE(RuntimeError,
                               "Failed to delete index table $0 due to error: $1", table_id, response->errorMessage);
  }   
  *base_table_id = response->indexedTableId;
  return Status::OK();
} 
   
Status SqlCatalogClient::OpenTable(const PgOid database_oid, const PgOid table_id, std::shared_ptr<TableInfo>* table) {
  std::shared_ptr<GetTableSchemaRequest> request = std::make_shared<GetTableSchemaRequest>();
  request->namespaceId = database_oid;
  request->tableId = table_id;
  std::shared_ptr<GetTableSchemaResponse> response = std::make_shared<GetTableSchemaResponse>();
  catalog_manager_->GetTableSchema(request, &response);
  if (!response->errorMessage.empty()) {
     return STATUS_SUBSTITUTE(RuntimeError,
                               "Failed to get schema for table $0 due to error: $1", table_id, response->errorMessage);
  } 
  std::shared_ptr<TableInfo> result = std::make_shared<TableInfo>(response->namespaceName, response->tableName, response->schema);   
  // TODO: double check wether we treat indexInfo as secondary Index or the index table itself                             
  if (response->indexInfo != std::nullopt) {
    PgObjectId pgObjectId(database_oid, table_id);
    result->add_secondary_index(pgObjectId.GetYBTableId(), response->indexInfo.value());
  }
  table->swap(result);
  return Status::OK();
}

Status SqlCatalogClient::ReservePgOids(const PgOid database_oid,
                                  const uint32_t next_oid, 
                                  const uint32_t count,
                                  uint32_t* begin_oid, 
                                  uint32_t* end_oid) {
  std::shared_ptr<ReservePgOidsRequest> request =  std::make_shared<ReservePgOidsRequest>();   
  request->namespaceId = database_oid;
  request->nextOid = next_oid;
  request->count = count;
  std::shared_ptr<ReservePgOidsResponse> response = std::make_shared<ReservePgOidsResponse>();
  catalog_manager_->ReservePgOid(request, &response);
  if (!response->errorMessage.empty()) {
     return STATUS_SUBSTITUTE(RuntimeError,
                               "Failed to reserve PG Oids for database $0 due to error: $1", database_oid, response->errorMessage);
  }        
  *begin_oid = response->beginOid;
  *end_oid = response->endOid;                        
  return Status::OK();
}

Status SqlCatalogClient::GetCatalogVersion(uint64_t *catalog_version) {
  *catalog_version = catalog_manager_->GetCatalogVersion();
  return Status::OK();
}    
 
}  // namespace sql
}  // namespace k2pg
