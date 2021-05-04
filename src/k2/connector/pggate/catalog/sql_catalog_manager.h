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

1) DatabaseInfo and TableInfo (IndexInfo): where DatabaseInfo represents database information and TableInfo for table metadata.
    Both information are needed by PG Gate as database/table schema metadata to avoid deriving them by using the complicated logic in
    PG from PG system catalog tables.

2) PG system catalog tables/indexes: each table has its own SKV schema and its data are managed by DMLs. Each system catalog
   table has a TableInfo representation in 1). PG still has its own internal logic to update the system catalog tables.

3) User tables/indexes: each table has its own SKV schema and its data are managed by DMLs. The table metadata are stored
   in PG system catalog and is also represented as a TableInfo in 1).

For a database, its identifier consists
1) name (std::string): it could be changed by DDLs such as rename database
2) PG Oid (uint32_t): object ID assigned by PG
3) id (std::string): generated UUID string based on PG Oid. It is actually used to uniquely identifier a database

Similary for a table:
1) name (std::string): table name and it could be renamed by DDLs
2) PG oid (uint32_t): object ID assigned by PG
3) id (std::string): generated UUID string based on database oid and table oid. It is used to identifer a table

DatabaseInfo, TableInfo, and IndexInfo are cached in catalog manager.

The catalog system has a primary SKV collection to store
1) cluster level information: the init_db_done flag and catalog version. The current pg uses a global catalog version
    across different databases to be used as an variable to decide whether PG needs to refresh its internal cache.
    We could refactor this to avoid a global catalog version and reduce the chance for cache refresh in PG later.
    One SKV record in the primary collection is used for a cluster and it is accessed by the ClusterInfoHandler

2) database information: database information is shared across different databases and thus, we need to store them
    in the primary SKV record. It is accessed by the DatabaseInfoHandler

3) Table information: PG gate needs to know the table schema and it is represented by the TableInfo object.
    The TableInfoHandler is used to acccess the TableInfo (including IndexInfo) objects on SKV

All tables in a database are mapped to a SKV collection by database id, not database name to make the
rename database working. Similary, a table's SKV schema name is identified by the table id, not the table name to
avoid moving data around after renaming a table.

To store a TableInfo with possible multiple IndexInfo on SKV (an IndexInfo represents a secondary index for a table),
we have the following three catalog tables with fixed schema defined in table_info_handler.h for each database
1) table meta: store table and index informaton. One entry for a table or an index
2) table column meta: store column information for a table. One entry for a table column
3) index column meta: store index column for an index. One entry for an index column

The TableInfoHandler is used to persist or read a TableInfo/IndexInfo object on SKV. When a TableInfo is stored to the
above catalog tables, the actual SKV schema is obtained dynamically and is created by TableInfoHandler as well by
table id so that DMLs could insert, update, select, and delete from the SKV table.

Please be aware that blocking APIs are used for the catalog manager for now. Will refactor them to include task submission
and task state checking APIs later by using thread pools.

*/

#ifndef CHOGORI_SQL_CATALOG_MANAGER_H
#define CHOGORI_SQL_CATALOG_MANAGER_H

#include <boost/functional/hash.hpp>

#include "common/env.h"
#include "common/status.h"
#include "common/concurrent/locks.h"
#include "entities/entity_ids.h"
#include "entities/schema.h"
#include "entities/index.h"
#include "entities/table.h"
#include "pggate/k2_adapter.h"
#include "pggate/k2_thread_pool.h"
#include "pggate/catalog/sql_catalog_defaults.h"
#include "pggate/catalog/cluster_info_handler.h"
#include "pggate/catalog/database_info_handler.h"
#include "pggate/catalog/table_info_handler.h"
#include "pggate/catalog/background_task.h"

#include "catalog_log.h"

namespace k2pg {
namespace sql {
namespace catalog {
    using yb::Status;
    using k2pg::ThreadPool;
    using k2pg::gate::K2Adapter;
    using k2pg::sql::PgObjectId;

    struct GetInitDbRequest {
    };

    struct GetInitDbResponse {
        Status status;
        bool isInitDbDone;
    };

    struct GetCatalogVersionRequest {
    };

    struct GetCatalogVersionResponse {
        Status status;
        uint64_t catalogVersion;
    };

    struct CreateDatabaseRequest {
        std::string databaseName;
        std::string databaseId;
        uint32_t databaseOid;
        std::string sourceDatabaseId;
        std::string creatorRoleName;
        // next oid to assign. Ignored when sourceDatabaseId is given and the nextPgOid from source database will be used
        std::optional<uint32_t> nextPgOid;
    };

    struct CreateDatabaseResponse {
        Status status;
        std::shared_ptr<DatabaseInfo> databaseInfo;
    };

    struct ListDatabasesRequest {
    };

    struct ListDatabasesResponse {
        Status status;
        std::vector<std::shared_ptr<DatabaseInfo>> database_infos;
    };

    struct GetDatabaseRequest {
        std::string databaseName;
        std::string databaseId;
    };

    struct GetDatabaseResponse {
        Status status;
        std::shared_ptr<DatabaseInfo> database_info;
    };

    struct DeleteDatabaseRequest {
        std::string databaseName;
        std::string databaseId;
    };

    struct DeleteDatabaseResponse {
        Status status;
    };

    struct UseDatabaseRequest {
        std::string databaseName;
    };

    struct UseDatabaseResponse {
        Status status;
    };

    struct CreateTableRequest {
        std::string databaseName;
        uint32_t databaseOid;
        std::string tableName;
        uint32_t tableOid;
        Schema schema;
        bool isSysCatalogTable;
        // should put shared table in primary collection
        bool isSharedTable;
        bool isNotExist;
    };

    struct CreateTableResponse {
        Status status;
        std::shared_ptr<TableInfo> tableInfo;
    };

    struct CreateIndexTableRequest {
        std::string databaseName;
        uint32_t databaseOid;
        std::string tableName;
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
        Status status;
        std::shared_ptr<IndexInfo> indexInfo;
    };

    struct GetTableSchemaRequest {
        uint32_t databaseOid;
        uint32_t tableOid;
    };

    struct GetTableSchemaResponse {
        Status status;
        std::shared_ptr<TableInfo> tableInfo;
    };

    struct CacheTablesFromStorageRequest {
        std::string databaseName;
        bool isSysTableIncluded = false;
    };

    struct CacheTablesFromStorageResponse {
        Status status;
        std::string databaseId;
        std::vector<std::shared_ptr<TableInfo>> tableInfos;
    };

    struct DeleteTableRequest {
        uint32_t databaseOid;
        uint32_t tableOid;
    };

    struct DeleteTableResponse {
        Status status;
        std::string databaseId;
        std::string tableId;
    };

    struct DeleteIndexRequest {
        uint32_t databaseOid;
        uint32_t tableOid;
    };

    struct DeleteIndexResponse {
        Status status;
        std::string databaseId;
        uint32_t baseIndexTableOid;
    };

    struct ReservePgOidsRequest {
        std::string databaseId;
        uint32_t nextOid;
        uint32_t count;
    };

    struct ReservePgOidsResponse {
        Status status;
        std::string databaseId;
        // the beginning of the oid reserver, which could be higher than requested
        uint32_t beginOid;
        // the end (exclusive) oid reserved
        uint32_t endOid;
    };

    struct IncrementCatalogVersionRequest {
    };

    struct IncrementCatalogVersionResponse {
        Status status;
        uint64_t version;
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

        Status InitPrimaryCluster();

        Status FinishInitDB();

        GetInitDbResponse IsInitDbDone(const GetInitDbRequest& request);

        GetCatalogVersionResponse GetCatalogVersion(const GetCatalogVersionRequest& request);

        IncrementCatalogVersionResponse IncrementCatalogVersion(const IncrementCatalogVersionRequest& request);

        CreateDatabaseResponse CreateDatabase(const CreateDatabaseRequest& request);

        ListDatabasesResponse ListDatabases(const ListDatabasesRequest& request);

        GetDatabaseResponse GetDatabase(const GetDatabaseRequest& request);

        DeleteDatabaseResponse DeleteDatabase(const DeleteDatabaseRequest& request);

        UseDatabaseResponse UseDatabase(const UseDatabaseRequest& request);

        CreateTableResponse CreateTable(const CreateTableRequest& request);

        CreateIndexTableResponse CreateIndexTable(const CreateIndexTableRequest& request);

        // Get (base) table schema - if passed-in id is that of a index, return base table schema, if is that of a table, return its table schema
        GetTableSchemaResponse GetTableSchema(const GetTableSchemaRequest& request);

        DeleteTableResponse DeleteTable(const DeleteTableRequest& request);

        DeleteIndexResponse DeleteIndex(const DeleteIndexRequest& request);

        ReservePgOidsResponse ReservePgOid(const ReservePgOidsRequest& request);

    protected:
        std::atomic<bool> initted_{false};

        mutable std::mutex lock_;

        void UpdateDatabaseCache(std::vector<std::shared_ptr<DatabaseInfo>> database_infos);

        void UpdateTableCache(std::shared_ptr<TableInfo> table_info);

        void ClearTableCache(std::shared_ptr<TableInfo> table_info);

        void ClearIndexCacheForTable(const std::string& base_table_id);

        void UpdateIndexCacheForTable(std::shared_ptr<TableInfo> table_info);

        void AddIndexCache(std::shared_ptr<IndexInfo> index_info);

        std::shared_ptr<DatabaseInfo> GetCachedDatabaseById(const std::string& database_id);

        std::shared_ptr<DatabaseInfo> GetCachedDatabaseByName(const std::string& database_name);

        std::shared_ptr<TableInfo> GetCachedTableInfoById(const std::string& table_uuid);

        std::shared_ptr<TableInfo> GetCachedTableInfoByName(const std::string& database_id, const std::string& table_name);

        std::shared_ptr<IndexInfo> GetCachedIndexInfoById(const std::string& index_uuid);

        std::shared_ptr<TableInfo> GetCachedBaseTableInfoByIndexId(uint32_t databaseOid, const std::string& index_uuid);

        CacheTablesFromStorageResponse CacheTablesFromStorage(const CacheTablesFromStorageRequest& request);

        // start a new PG transaction and return the handler of it. NOTE: the underhood transaction has begun.
        std::shared_ptr<PgTxnHandler> NewTransaction();

        IndexInfo BuildIndexInfo(std::shared_ptr<TableInfo> base_table_info, std::string index_name, uint32_t table_oid, std::string index_uuid,
                const Schema& index_schema, bool is_unique, bool is_shared, IndexPermissions index_permissions);

        std::shared_ptr<DatabaseInfo> CheckAndLoadDatabaseByName(const std::string& database_name);

        std::shared_ptr<DatabaseInfo> CheckAndLoadDatabaseById(const std::string& database_id);

        void CheckCatalogVersion();

    private:
        // cluster identifier
        std::string cluster_id_;

        std::shared_ptr<K2Adapter> k2_adapter_;

        // thread pool to run background tasks
        ThreadPool thread_pool_;

        // flag to indicate whether init_db is done or not
        std::atomic<bool> init_db_done_{false};

        // catalog version, 0 stands for uninitialized
        std::atomic<uint64_t> catalog_version_{0};

        // handler to access ClusterInfo record including init_db_done flag and catalog version
        std::shared_ptr<ClusterInfoHandler> cluster_info_handler_;

        // handler to access the database info record, which consists of database information
        // and it is a shared table across all databases
        std::shared_ptr<DatabaseInfoHandler> database_info_handler_;

        // handler to access table and index information
        std::shared_ptr<TableInfoHandler> table_info_handler_;

        // database information cache based on database id
        std::unordered_map<std::string, std::shared_ptr<DatabaseInfo>> database_id_map_;

        // database information cache based on database name
        std::unordered_map<std::string, std::shared_ptr<DatabaseInfo>> database_name_map_;

        // a table is uniquely referenced by its id, which is generated based on its
        // database (database) PgOid and table PgOid, as a result, no database name is required here
        std::unordered_map<std::string, std::shared_ptr<TableInfo>> table_uuid_map_;

        // to reference a table by its name, we have to use both databaseId and table name
        std::unordered_map<TableNameKey, std::shared_ptr<TableInfo>, boost::hash<TableNameKey>> table_name_map_;

        // index id to quickly search for the index information and base table id
        std::unordered_map<std::string, std::shared_ptr<IndexInfo>> index_uuid_map_;

        // background task
        std::unique_ptr<SingleThreadedPeriodicTask> catalog_version_task_ = nullptr;
    };

} // namespace catalog
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_CATALOG_MANAGER_H
