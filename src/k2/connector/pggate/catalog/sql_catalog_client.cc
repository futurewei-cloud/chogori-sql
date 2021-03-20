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

#include "pggate/catalog/sql_catalog_client.h"

namespace k2pg {
namespace sql {
namespace catalog {

Status SqlCatalogClient::IsInitDbDone(bool* isDone) {
  GetInitDbRequest request;
  auto start = k2::Clock::now();
  GetInitDbResponse response = catalog_manager_->IsInitDbDone(request);
  K2LOG_I(log::catalog, "IsInitDbDone took {}", k2::Clock::now() - start);
  if(!response.status.ok()) {
    return response.status;
  }
  *isDone = response.isInitDbDone;
  return Status::OK();
}

Status SqlCatalogClient::InitPrimaryCluster()
{
  return catalog_manager_->InitPrimaryCluster();
}

Status SqlCatalogClient::FinishInitDB()
{
  return catalog_manager_->FinishInitDB();
}

Status SqlCatalogClient::CreateNamespace(const std::string& namespace_name,
                                 const std::string& namespace_id,
                                 uint32_t namespace_oid,
                                 const std::string& source_namespace_id,
                                 const std::string& creator_role_name,
                                 const std::optional<uint32_t>& next_pg_oid) {
  CreateNamespaceRequest request {
    .namespaceName = namespace_name,
    .namespaceId = namespace_id,
    .namespaceOid = namespace_oid,
    .sourceNamespaceId = source_namespace_id,
    .creatorRoleName = creator_role_name,
    .nextPgOid = next_pg_oid
  };
  auto start = k2::Clock::now();
  CreateNamespaceResponse response = catalog_manager_->CreateNamespace(request);
  K2LOG_I(log::catalog, "CreateNamespace {} took {}", namespace_name, k2::Clock::now() - start);
  return response.status;
}

Status SqlCatalogClient::DeleteNamespace(const std::string& namespace_name,
                                 const std::string& namespace_id) {
  DeleteNamespaceRequest request {.namespaceName = namespace_name, .namespaceId = namespace_id};
  DeleteNamespaceResponse response = catalog_manager_->DeleteNamespace(request);
  return response.status;
}

Status SqlCatalogClient::UseDatabase(const std::string& database_name) {
  UseDatabaseRequest request {.databaseName = database_name};
  auto start = k2::Clock::now();
  UseDatabaseResponse response = catalog_manager_->UseDatabase(request);
  K2LOG_I(log::catalog, "UseDatabase {} took {}", database_name, k2::Clock::now() - start);
  return response.status;
}

Status SqlCatalogClient::CreateTable(
    const std::string& namespace_name,
    const std::string& table_name,
    const PgObjectId& table_object_id,
    PgSchema& schema,
    bool is_pg_catalog_table,
    bool is_shared_table,
    bool if_not_exist) {
  CreateTableRequest request {
    .namespaceName = namespace_name,
    .namespaceOid = table_object_id.GetDatabaseOid(),
    .tableName = table_name,
    .tableOid = table_object_id.GetObjectOid(),
    .schema = schema,
    .isSysCatalogTable = is_pg_catalog_table,
    .isSharedTable = is_shared_table,
    .isNotExist = if_not_exist
  };
  auto start = k2::Clock::now();
  CreateTableResponse response = catalog_manager_->CreateTable(request);
  K2LOG_I(log::catalog, "CreateTable {} in {} took {}", table_name, namespace_name, k2::Clock::now() - start);
  return response.status;
}

Status SqlCatalogClient::CreateIndexTable(
    const std::string& namespace_name,
    const std::string& table_name,
    const PgObjectId& table_object_id,
    const PgObjectId& base_table_object_id,
    PgSchema& schema,
    bool is_unique_index,
    bool skip_index_backfill,
    bool is_pg_catalog_table,
    bool is_shared_table,
    bool if_not_exist) {
  CreateIndexTableRequest request {
    .namespaceName = namespace_name,
    .namespaceOid = table_object_id.GetDatabaseOid(),
    .tableName = table_name,
    .tableOid = table_object_id.GetObjectOid(),
    // index and the base table should be in the same namespace, i.e., database
    .baseTableOid = base_table_object_id.GetObjectOid(),
    .schema = schema,
    .isUnique = is_unique_index,
    .skipIndexBackfill = skip_index_backfill,
    .isSysCatalogTable = is_pg_catalog_table,
    .isSharedTable = is_shared_table,
    .isNotExist = if_not_exist
  };
  auto start = k2::Clock::now();
  CreateIndexTableResponse response = catalog_manager_->CreateIndexTable(request);
  K2LOG_I(log::catalog, "CreateIndexTable {} in {} took {}", table_name, namespace_name, k2::Clock::now() - start);
  return response.status;
}

Status SqlCatalogClient::DeleteTable(const PgOid database_oid, const PgOid table_oid, bool wait) {
  DeleteTableRequest request {
    .namespaceOid = database_oid,
    .tableOid = table_oid
  };
  DeleteTableResponse response = catalog_manager_->DeleteTable(request);
  return response.status;
}

Status SqlCatalogClient::DeleteIndexTable(const PgOid database_oid, const PgOid table_oid, PgOid *base_table_oid, bool wait) {
  DeleteIndexRequest request {
    .namespaceOid = database_oid,
    .tableOid = table_oid
  };
  DeleteIndexResponse response = catalog_manager_->DeleteIndex(request);
  if (!response.status.ok()) {
     return response.status;
  }
  *base_table_oid = response.baseIndexTableOid;
  // TODO: add wait logic once we refactor the catalog manager APIs to be asynchronous for state/response
  return response.status;
}

Status SqlCatalogClient::OpenTable(const PgOid database_oid, const PgOid table_oid, std::shared_ptr<TableInfo>* table) {
  GetTableSchemaRequest request {
    .namespaceOid = database_oid,
    .tableOid = table_oid
  };
  auto start = k2::Clock::now();
  GetTableSchemaResponse response = catalog_manager_->GetTableSchema(request);
  K2LOG_I(log::catalog, "GetTableSchema ({} : {}) took {}", database_oid, table_oid, k2::Clock::now() - start);
  if (!response.status.ok()) {
     return response.status;
  }

  table->swap(response.tableInfo);
  return response.status;
}

Status SqlCatalogClient::ReservePgOids(const PgOid database_oid,
                                  const uint32_t next_oid,
                                  const uint32_t count,
                                  uint32_t* begin_oid,
                                  uint32_t* end_oid) {
  ReservePgOidsRequest request {
    .namespaceId = PgObjectId::GetNamespaceUuid(database_oid),
    .nextOid = next_oid,
    .count = count
  };
  auto start = k2::Clock::now();
  ReservePgOidsResponse response = catalog_manager_->ReservePgOid(request);
  K2LOG_I(log::catalog, "ReservePgOid took {}", k2::Clock::now() - start);
  if (!response.status.ok()) {
     return response.status;
  }
  *begin_oid = response.beginOid;
  *end_oid = response.endOid;
  return response.status;
}

Status SqlCatalogClient::GetCatalogVersion(uint64_t *pg_catalog_version) {
  GetCatalogVersionRequest request;
  auto start = k2::Clock::now();
  GetCatalogVersionResponse response = catalog_manager_->GetCatalogVersion(request);
  K2LOG_I(log::catalog, "GetCatalogVersion took {}", k2::Clock::now() - start);
  if (!response.status.ok()) {
     return response.status;
  }
  *pg_catalog_version = response.catalogVersion;
  return response.status;
}

Status SqlCatalogClient::IncrementCatalogVersion() {
  IncrementCatalogVersionRequest request;
  auto start = k2::Clock::now();
  IncrementCatalogVersionResponse response = catalog_manager_->IncrementCatalogVersion(request);
  K2LOG_I(log::catalog, "IncrementCatalogVersion took {}", k2::Clock::now() - start);
  return response.status;
}
} // namespace catalog
}  // namespace sql
}  // namespace k2pg
