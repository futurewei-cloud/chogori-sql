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

#ifndef CHOGORI_SQL_CATALOG_MANAGER_H
#define CHOGORI_SQL_CATALOG_MANAGER_H

#include <boost/functional/hash.hpp>

#include "yb/common/env.h"
#include "yb/common/status.h"
#include "yb/common/concurrent/locks.h"
#include "yb/entities/schema.h"
#include "yb/entities/index.h"
#include "yb/entities/table.h"
#include "yb/pggate/k2_adapter.h"
#include "yb/pggate/catalog/sql_catalog_defaults.h"
#include "yb/pggate/catalog/cluster_info_handler.h"
#include "yb/pggate/catalog/namespace_info_handler.h"
#include "yb/pggate/catalog/table_info_handler.h"

namespace k2pg {
namespace sql {
namespace catalog {    
    using yb::Env;
    using yb::Status;
    using yb::simple_spinlock;
    using k2pg::gate::K2Adapter;
    using k2pg::gate::PgObjectId;

    struct GetInitDbRequest {
    };

    struct GetInitDbResponse {
        RStatus status;
        bool isInitDbDone;
    };

    struct GetCatalogVersionRequest {  
    };

    struct GetCatalogVersionResponse {
        RStatus status;
        uint64_t catalogVersion;     
    };

    struct CreateNamespaceRequest {
        string namespaceName;
        string namespaceId;
        string sourceNamespaceId;
        string creatorRoleName;
        // next oid to assign. Ignored when sourceNamespaceId is given and the nextPgOid from source namespace will be used
        std::optional<uint32_t> nextPgOid;
    };

    struct CreateNamespaceResponse {
        RStatus status;
        std::shared_ptr<NamespaceInfo> namespaceInfo;
    };

    struct ListNamespacesRequest {
    };

    struct ListNamespacesResponse {
        RStatus status;  
        std::vector<std::shared_ptr<NamespaceInfo>> namespace_infos;
    };

    struct GetNamespaceRequest {
        string namespaceName;
        string namespaceId;
    };

    struct GetNamespaceResponse {
        RStatus status;  
        std::shared_ptr<NamespaceInfo> namespace_info;
    };

    struct DeleteNamespaceRequest {
        string namespaceName;
        string namespaceId;      
    };

    struct DeleteNamespaceResponse {
        RStatus status;       
    };

    struct CreateTableRequest {
        string namespaceName;
        uint32_t namespaceOid;
        string tableName;
        uint32_t tableOid;
        Schema schema;
        bool isSysCatalogTable;
        // should put shared table in primary collection
        bool isSharedTable;
        bool isNotExist;
    };

    struct CreateTableResponse {        
        RStatus status;
        std::shared_ptr<TableInfo> tableInfo;
    };

    struct CreateIndexTableRequest {
        string namespaceName;
        uint32_t namespaceOid;
        string tableName;
        uint32_t tableOid;
        uint32_t baseTableOid;
        Schema schema;
        bool isUnique;
        bool skipIndexBackfill;
        bool isSysCatalogTable;
        bool isSharedTable;
        bool isNotExist;
    };

    struct CreateIndexTableResponse {
        RStatus status;
        std::shared_ptr<IndexInfo> indexInfo;
    };

    struct GetTableSchemaRequest {
        uint32_t namespaceOid;
        uint32_t tableOid;    
    };

    struct GetTableSchemaResponse {
        RStatus status;
        std::shared_ptr<TableInfo> tableInfo;
    };

    struct ListTablesRequest {
        string namespaceName;
        // use string match for table name
        string nameFilter;
        bool excludeSystemTables = false;
    };

    struct ListTablesResponse {
        RStatus status;
        string namespaceName;
        std::vector<string> tableNames;
    };

    struct DeleteTableRequest {
        uint32_t namespaceId;
        uint32_t tableId;
        bool isIndexTable;
    };

    struct DeleteTableResponse {
        RStatus status;
        uint32_t namespaceId;
        uint32_t tableId;
        uint32_t indexedTableId;  
    };

    struct ReservePgOidsRequest {
        std::string namespaceId;
        uint32_t nextOid;
        uint32_t count;
    };

    struct ReservePgOidsResponse {
        RStatus status;
        std::string namespaceId;
        // the beginning of the oid reserver, which could be higher than requested
        uint32_t beginOid;
        // the end (exclusive) oid reserved
        uint32_t endOid;
    };

    class SqlCatalogManager : public std::enable_shared_from_this<SqlCatalogManager> {

    public:
        typedef std::shared_ptr<SqlCatalogManager> SharedPtr;

        SqlCatalogManager(std::shared_ptr<K2Adapter> k2_adapter);
        ~SqlCatalogManager();

        CHECKED_STATUS Start();

        virtual void Shutdown();

        virtual Env* GetEnv();

        // use synchronous APIs for the first attempt here
        // TODO: change them to asynchronous with status check later by introducing threadpool task processing

        GetInitDbResponse IsInitDbDone(const GetInitDbRequest& request);

        GetCatalogVersionResponse GetCatalogVersion(const GetCatalogVersionRequest& request);

        CreateNamespaceResponse CreateNamespace(const CreateNamespaceRequest& request);  

        ListNamespacesResponse ListNamespaces(const ListNamespacesRequest& request);

        GetNamespaceResponse GetNamespace(const GetNamespaceRequest& request);

        DeleteNamespaceResponse DeleteNamespace(const DeleteNamespaceRequest& request);

        CreateTableResponse CreateTable(const CreateTableRequest& request);

        CreateIndexTableResponse CreateIndexTable(const CreateIndexTableRequest& request);
        
        GetTableSchemaResponse GetTableSchema(const GetTableSchemaRequest& request);

        ListTablesResponse ListTables(const ListTablesRequest& request);

        DeleteTableResponse DeleteTable(const DeleteTableRequest& request);

        ReservePgOidsResponse ReservePgOid(const ReservePgOidsRequest& request);

    protected:
        std::atomic<bool> initted_{false};

        mutable simple_spinlock lock_;

        RStatus UpdateCatalogVersion(uint64_t new_version);

        void UpdateNamespaceCache(std::vector<std::shared_ptr<NamespaceInfo>> namespace_infos);

        void UpdateTableCache(std::shared_ptr<TableInfo> table_info);

        std::shared_ptr<NamespaceInfo> GetCachedNamespaceById(std::string namespace_id);

        std::shared_ptr<NamespaceInfo> GetCachedNamespaceByName(std::string namespace_name);

        std::shared_ptr<TableInfo> GetCachedTableInfoById(std::string table_id);
        
        std::shared_ptr<TableInfo> GetCachedTableInfoByName(std::string namespace_id, std::string table_name);

        std::shared_ptr<Context> NewTransactionContext();

        void EndTransactionContext(std::shared_ptr<Context> context, bool should_commit);

        void IncreaseCatalogVersion();

        IndexInfo BuildIndexInfo(std::shared_ptr<TableInfo> base_table_info, std::string index_id, std::string index_name, uint32_t pg_oid,
                const Schema& index_schema, bool is_unique, IndexPermissions index_permissions);

    private:

        std::string cluster_id_;

        std::shared_ptr<K2Adapter> k2_adapter_;

        std::atomic<bool> init_db_done_{false};

        // catalog version 0 stands for uninitialized 
        std::atomic<uint64_t> catalog_version_{0};

        std::shared_ptr<ClusterInfoHandler> cluster_info_handler_;

        std::shared_ptr<NamespaceInfoHandler> namespace_info_handler_;

        std::shared_ptr<TableInfoHandler> table_info_handler_;

        std::unordered_map<std::string, std::shared_ptr<NamespaceInfo>> namespace_id_map_;

        std::unordered_map<std::string, std::shared_ptr<NamespaceInfo>> namespace_name_map_;

        // a table is uniquely referenced by its id, which is generated based on its 
        // database (namespace) PgOid and table PgOid, as a result, no namespace name is required here
        std::unordered_map<std::string, std::shared_ptr<TableInfo>> table_id_map_;

        // to reference a table by its name, we have to use both namespaceId and table name
        std::unordered_map<TableNameKey, std::shared_ptr<TableInfo>, boost::hash<TableNameKey>> table_name_map_;

    };

} // namespace catalog
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_CATALOG_MANAGER_H
