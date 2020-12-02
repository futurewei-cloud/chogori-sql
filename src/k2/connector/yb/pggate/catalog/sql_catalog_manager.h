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

------------- READ ME --------------

Catalog Manager is the service that manages database metadata. It is local for now, but will become a central one 
to avoid metadata update conflicts and reduce the need to access SKV frequently.

The database data on SKV consists of the following:

1) NamespaceInfo and TableInfo (IndexInfo): where NamespaceInfo represents database information and TableInfo for table metadata.
    Both information are needed by PG Gate as database/table schema metadata to avoid deriving them by using the complicated logic in 
    PG from PG system catalog tables.

2) PG system catalog tables/indexes: each table has its own SKV schema and its data are managed by DMLs. Each system catalog
   table has a TableInfo representation in 1). PG still has its own internal logic to update the system catalog tables.

3) User tables/indexes: each table has its own SKV schema and its data are managed by DMLs. The table metadata are stored 
   in PG system catalog and is also represented as a TableInfo in 1).

For a namespace, i.e., database here, its identifier consists
1) name (std::string): it could be changed by DDLs such as rename database
2) PG Oid (uint32_t): object ID assigned by PG
3) id (std::string): generated UUID string based on PG Oid. It is actually used to uniquely identifier a namespace

Similary for a table:
1) name (std::string): table name and it could be renamed by DDLs
2) PG oid (uint32_t): object ID assigned by PG
3) id (std::string): generated UUID string based on database oid and table oid. It is used to identifer a table

NamespaceInfo, TableInfo, and IndexInfo are cached in catalog manager.

The catalog system has a primary SKV collection to store
1) cluster level information: the init_db_done flag and catalog version. The current pg uses a global catalog version
    across different databases to be used as an variable to decide whether PG needs to refresh its internal cache.
    We could refactor this to avoid a global catalog version and reduce the chance for cache refresh in PG later.
    One SKV record in the primary collection is used for a cluster and it is accessed by the ClusterInfoHandler

2) namespace information: database information is shared across different databases and thus, we need to store them
    in the primary SKV record. It is accessed by the NamespaceInfoHandler

3) Table information: PG gate needs to know the table schema and it is represented by the TableInfo object. 
    The TableInfoHandler is used to acccess the TableInfo (including IndexInfo) objects on SKV

All tables in a namespace (database) are mapped to a SKV collection by namespace id, not namespace name to make the 
rename database working. Similary, a table's SKV schema name is identified by the table id, not the table name to 
avoid moving data around after renaming a table. 

To store a TableInfo with possible multiple IndexInfo on SKV (an IndexInfo represents a secondary index for a table),
we have the following three catalog tables with fixed schema defined in table_info_handler.h for each namespace (database)
1) table head: store table and index informaton. One entry for a table or an index
2) table column: store column information for a table. One entry for a table column
3) index column: store index column for an index. One entry for an index column

The TableInfoHandler is used to persist or read a TableInfo/IndexInfo object on SKV. When a TableInfo is stored to the 
above catalog tables, the actual SKV schema is obtained dynamically and is created by TableInfoHandler as well by 
table id so that DMLs could insert, update, select, and delete from the SKV table. 

Please be aware that blocking APIs are used for the catalog manager for now. Will refactor them to include task submission
and task state checking APIs later by using thread pools.

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

    struct DeleteTableRequest {
        uint32_t namespaceOid;
        uint32_t tableOid;
    };

    struct DeleteTableResponse {
        RStatus status;
        string namespaceId;
        string tableId;
    };

    struct DeleteIndexRequest {
        uint32_t namespaceOid;
        uint32_t tableOid;
    };

    struct DeleteIndexResponse {
        RStatus status;
        string namespaceId;
        uint32_t baseIndexTableOid; 
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

        DeleteTableResponse DeleteTable(const DeleteTableRequest& request);

        DeleteIndexResponse DeleteIndex(const DeleteIndexRequest& request);

        ReservePgOidsResponse ReservePgOid(const ReservePgOidsRequest& request);

    protected:
        std::atomic<bool> initted_{false};

        mutable simple_spinlock lock_;

        RStatus UpdateCatalogVersion(std::shared_ptr<SessionTransactionContext> context, uint64_t new_version);

        void UpdateNamespaceCache(std::vector<std::shared_ptr<NamespaceInfo>> namespace_infos);

        void UpdateTableCache(std::shared_ptr<TableInfo> table_info);

        void ClearTableCache(std::shared_ptr<TableInfo> table_info);

        void ClearIndexCacheForTable(std::string table_id);

        void UpdateIndexCacheForTable(std::shared_ptr<TableInfo> table_info);

        void AddIndexCache(std::shared_ptr<IndexInfo> index_info);

        std::shared_ptr<NamespaceInfo> GetCachedNamespaceById(std::string namespace_id);

        std::shared_ptr<NamespaceInfo> GetCachedNamespaceByName(std::string namespace_name);

        std::shared_ptr<TableInfo> GetCachedTableInfoById(std::string table_id);
        
        std::shared_ptr<TableInfo> GetCachedTableInfoByName(std::string namespace_id, std::string table_name);

        std::shared_ptr<IndexInfo> GetCachedIndexInfoById(std::string index_id);

        std::shared_ptr<SessionTransactionContext> NewTransactionContext();

        void EndTransactionContext(std::shared_ptr<SessionTransactionContext> context, bool should_commit);

        void IncreaseCatalogVersion();

        IndexInfo BuildIndexInfo(std::shared_ptr<TableInfo> base_table_info, std::string index_id, std::string index_name, uint32_t pg_oid,
                const Schema& index_schema, bool is_unique, IndexPermissions index_permissions);

    private:
        // cluster identifier
        std::string cluster_id_;

        std::shared_ptr<K2Adapter> k2_adapter_;

        // flag to indicate whether init_db is done or not
        std::atomic<bool> init_db_done_{false};

        // catalog version, 0 stands for uninitialized 
        std::atomic<uint64_t> catalog_version_{0};

        // handler to access ClusterInfo record including init_db_done flag and catalog version
        std::shared_ptr<ClusterInfoHandler> cluster_info_handler_;

        // handler to access the namespace info record, which consists of database information 
        // and it is a shared table across all databases
        std::shared_ptr<NamespaceInfoHandler> namespace_info_handler_;

        // handler to access table and index information
        std::shared_ptr<TableInfoHandler> table_info_handler_;

        // namespace information cache based on namespace id
        std::unordered_map<std::string, std::shared_ptr<NamespaceInfo>> namespace_id_map_;

        // namespace information cache based on namespace name
        std::unordered_map<std::string, std::shared_ptr<NamespaceInfo>> namespace_name_map_;

        // a table is uniquely referenced by its id, which is generated based on its 
        // database (namespace) PgOid and table PgOid, as a result, no namespace name is required here
        std::unordered_map<std::string, std::shared_ptr<TableInfo>> table_id_map_;

        // to reference a table by its name, we have to use both namespaceId and table name
        std::unordered_map<TableNameKey, std::shared_ptr<TableInfo>, boost::hash<TableNameKey>> table_name_map_;

        // index id to quickly search for the index information and base table id
        std::unordered_map<std::string, std::shared_ptr<IndexInfo>> index_id_map_;
    };

} // namespace catalog
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_CATALOG_MANAGER_H
