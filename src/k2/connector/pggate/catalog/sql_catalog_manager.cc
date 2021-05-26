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

#include "pggate/catalog/sql_catalog_manager.h"

#include <algorithm>
#include <list>
#include <thread>
#include <vector>

#include <glog/logging.h>

namespace k2pg {
namespace sql {
namespace catalog {
    using yb::Result;
    using k2pg::gate::K2Adapter;

    // TODO: clean up the exception throwing and handling logic in this class

    SqlCatalogManager::SqlCatalogManager(std::shared_ptr<K2Adapter> k2_adapter) :
        cluster_id_(CatalogConsts::primary_cluster_id), k2_adapter_(k2_adapter),
        thread_pool_(CatalogConsts::catalog_manager_background_task_thread_pool_size) {
        cluster_info_handler_ = std::make_shared<ClusterInfoHandler>(k2_adapter);
        database_info_handler_ = std::make_shared<DatabaseInfoHandler>(k2_adapter);
        table_info_handler_ = std::make_shared<TableInfoHandler>(k2_adapter);
    }

    SqlCatalogManager::~SqlCatalogManager() {
    }

    Status SqlCatalogManager::Start() {
        K2LOG_I(log::catalog, "Starting Catalog Manager...");
        CHECK(!initted_.load(std::memory_order_acquire));

        std::shared_ptr<PgTxnHandler> ci_txnHandler = NewTransaction();
        // load cluster info
        GetClusterInfoResult ciresp = cluster_info_handler_->GetClusterInfo(ci_txnHandler, cluster_id_);
        if (!ciresp.status.ok()) {
            ci_txnHandler->AbortTransaction();
            if (ciresp.status.IsNotFound()) {
                K2LOG_W(log::catalog, "Empty cluster info record, likely primary cluster is not initialized. Only operation allowed is primary cluster initialization");
                // it is ok, but only InitPrimaryCluster can be executed on the SqlCatalogrMager, keep initted_ to be false;
                return Status::OK();
            } else {
                K2LOG_E(log::catalog, "Failed to read cluster info record due to {}", ciresp.status.code());
                return ciresp.status;
            }
        }

        init_db_done_.store(ciresp.clusterInfo->IsInitdbDone(), std::memory_order_relaxed);
        catalog_version_.store(ciresp.clusterInfo->GetCatalogVersion(), std::memory_order_relaxed);
        K2LOG_I(log::catalog, "Loaded cluster info record succeeded, init_db_done: {}, catalog_version: {}", init_db_done_, catalog_version_);
        // end the current transaction so that we use a different one for later operations
        ci_txnHandler->CommitTransaction();

        // load databases
        std::shared_ptr<PgTxnHandler> ns_txnHandler = NewTransaction();
        ListDatabaseResult nsresp = database_info_handler_->ListDatabases(ns_txnHandler);
        ns_txnHandler->CommitTransaction();

        if (nsresp.status.ok()) {
            if (!nsresp.databaseInfos.empty()) {
                for (auto ns_ptr : nsresp.databaseInfos) {
                    // cache databases by database id and database name
                    database_id_map_[ns_ptr->GetDatabaseId()] = ns_ptr;
                    database_name_map_[ns_ptr->GetDatabaseName()] = ns_ptr;
                    K2LOG_I(log::catalog, "Loaded database id: {}, name: {}", ns_ptr->GetDatabaseId(), ns_ptr->GetDatabaseName());
                }
            } else {
                K2LOG_D(log::catalog, "databases are empty");
            }
        } else {
            K2LOG_E(log::catalog, "Failed to load databases due to {}", nsresp.status);
            return STATUS_FORMAT(IOError, "Failed to load databases due to error code $0",
                nsresp.status.code());
        }

        // only start background tasks in normal mode, i.e., not in InitDB mode
        if (init_db_done_) {
            std::function<void()> catalog_version_task([this]{
                CheckCatalogVersion();
            });
            catalog_version_task_ = std::make_unique<SingleThreadedPeriodicTask>(catalog_version_task, "catalog-version-task",
                CatalogConsts::catalog_manager_background_task_initial_wait,
                CatalogConsts::catalog_manager_background_task_sleep_interval);
            catalog_version_task_->Start();
        }

        initted_.store(true, std::memory_order_release);
        K2LOG_I(log::catalog, "Catalog Manager started up successfully");
        return Status::OK();
    }

    void SqlCatalogManager::Shutdown() {
        K2LOG_I(log::catalog, "SQL CatalogManager shutting down...");

        bool expected = true;
        if (initted_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            // shut down steps
            if (catalog_version_task_ != nullptr) {
                catalog_version_task_.reset(nullptr);
            }
        }

        K2LOG_I(log::catalog, "SQL CatalogManager shut down complete. Bye!");
    }

    // Called only once during PG initDB
    // TODO: handle partial failure(maybe simply fully cleanup) to allow retry later
    Status SqlCatalogManager::InitPrimaryCluster()
    {
        K2LOG_D(log::catalog, "SQL CatalogManager initialize primary Cluster!");

        CHECK(!initted_.load(std::memory_order_relaxed));

        // step 1/4 create the SKV collection for the primary
        auto ccResult = k2_adapter_->CreateCollection(CatalogConsts::skv_collection_name_primary_cluster, CatalogConsts::primary_cluster_id).get();
        if (!ccResult.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to create SKV collection during initialization primary PG cluster due to {}", ccResult);
            return K2Adapter::K2StatusToYBStatus(ccResult);
        }

        std::shared_ptr<PgTxnHandler> init_txnHandler = NewTransaction();

        // step 2/4 Init Cluster info, including create the SKVSchema in the primary cluster's SKVCollection for cluster_info and insert current cluster info into
        //      Note: Initialize cluster info's init_db column with TRUE
        ClusterInfo cluster_info(cluster_id_, catalog_version_, false /*init_db_done*/);

        InitClusterInfoResult initCIRes = cluster_info_handler_->InitClusterInfo(init_txnHandler, cluster_info);
        if (!initCIRes.status.ok()) {
            init_txnHandler->AbortTransaction();
            K2LOG_E(log::catalog, "Failed to initialize cluster info due to {}", initCIRes.status.code());
            return initCIRes.status;
        }

        // step 3/4 Init database_info - create the SKVSchema in the primary cluster's SKVcollection for database_info
        InitDatabaseTableResult initRes = database_info_handler_->InitDatabasTable();
        if (!initRes.status.ok()) {
            K2LOG_E(log::catalog, "Failed to initialize creating database table due to {}", initRes.status.code());
            return initCIRes.status;
        }

        init_txnHandler->CommitTransaction();

        // step 4/4 re-start this catalog manager so it can execute other APIs
        Status status = Start();
        if (status.ok())
        {
            // check things are ready
            CHECK(initted_.load(std::memory_order_relaxed));
            K2LOG_D(log::catalog, "SQL CatalogManager successfully initialized primary Cluster!");
        }
        else
        {
            K2LOG_E(log::catalog, "Failed to create SKV collection during initialization primary PG cluster due to {}", status.code());
        }

        return status;
    }

    Status SqlCatalogManager::FinishInitDB()
    {
        K2LOG_D(log::catalog, "Setting initDbDone to be true...");
        if (!init_db_done_) {
            std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
            // check the latest cluster info on SKV
            GetClusterInfoResult result = cluster_info_handler_->GetClusterInfo(txnHandler, cluster_id_);
            if (!result.status.ok()) {
                txnHandler->AbortTransaction();
                K2LOG_E(log::catalog, "Cannot read cluster info record on SKV due to {}", result.status.code());
                return result.status;
            }

            if (result.clusterInfo->IsInitdbDone()) {
                txnHandler->CommitTransaction();
                init_db_done_.store(true, std::memory_order_relaxed);
                K2LOG_D(log::catalog, "InitDbDone is already true on SKV");
                return Status::OK();
            }

            K2LOG_D(log::catalog, "Updating cluster info with initDbDone to be true");
            std::shared_ptr<ClusterInfo> new_cluster_info = result.clusterInfo;
            new_cluster_info->SetInitdbDone(true);
            UpdateClusterInfoResult update_result = cluster_info_handler_->UpdateClusterInfo(txnHandler, *new_cluster_info.get());
            if (!update_result.status.ok()) {
                txnHandler->AbortTransaction();
                K2LOG_E(log::catalog, "Failed to update cluster info due to {}", update_result.status.code());
                return update_result.status;
            }
            txnHandler->CommitTransaction();
            init_db_done_.store(true, std::memory_order_relaxed);
            K2LOG_D(log::catalog, "Set initDbDone to be true successfully");
        } else {
            K2LOG_D(log::catalog, "InitDb is true already");
        }
        return Status::OK();
    }

    GetInitDbResponse SqlCatalogManager::IsInitDbDone(const GetInitDbRequest& request) {
        GetInitDbResponse response;
        if (!init_db_done_) {
            std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
            GetClusterInfoResult result = cluster_info_handler_->GetClusterInfo(txnHandler, cluster_id_);
            txnHandler->CommitTransaction();
            if (!result.status.ok()) {
                K2LOG_E(log::catalog, "Failed to check IsInitDbDone from SKV due to {}", result.status);
                response.status = std::move(result.status);
                return response;
            }

            K2LOG_D(log::catalog, "Checked IsInitDbDone from SKV {}", result.clusterInfo->IsInitdbDone());
            if (result.clusterInfo->IsInitdbDone()) {
                init_db_done_.store(result.clusterInfo->IsInitdbDone(), std::memory_order_relaxed);
            }
            if (result.clusterInfo->GetCatalogVersion() > catalog_version_) {
                catalog_version_.store(result.clusterInfo->GetCatalogVersion(), std::memory_order_relaxed);
            }

        }
        K2LOG_D(log::catalog, "Get InitDBDone successfully {}", init_db_done_);
        response.isInitDbDone = init_db_done_;
        response.status = Status(); // OK
        return response;
    }

    void SqlCatalogManager::CheckCatalogVersion() {
        std::lock_guard<std::mutex> l(lock_);
        K2LOG_D(log::catalog, "Checking catalog version...");
        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        GetClusterInfoResult result = cluster_info_handler_->GetClusterInfo(txnHandler, cluster_id_);
        if (!result.status.ok()) {
            K2LOG_E(log::catalog, "Failed to check cluster info due to {}", result.status);
            txnHandler->AbortTransaction();
            return;
        }
        txnHandler->CommitTransaction();
        if (result.clusterInfo->GetCatalogVersion() > catalog_version_) {
            catalog_version_ = result.clusterInfo->GetCatalogVersion();
            K2LOG_D(log::catalog, "Updated catalog version to {}", catalog_version_);
        }
    }

    GetCatalogVersionResponse SqlCatalogManager::GetCatalogVersion(const GetCatalogVersionRequest& request) {
        GetCatalogVersionResponse response;
        response.catalogVersion = catalog_version_;
        response.status = Status(); // OK
        K2LOG_D(log::catalog, "Returned catalog version {}", response.catalogVersion);
        return response;
    }

    IncrementCatalogVersionResponse SqlCatalogManager::IncrementCatalogVersion(const IncrementCatalogVersionRequest& request) {
        std::lock_guard<std::mutex> l(lock_);
        IncrementCatalogVersionResponse response;
        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        // TODO: use a background thread to fetch the ClusterInfo record periodically instead of fetching it for each call
        GetClusterInfoResult read_result = cluster_info_handler_->GetClusterInfo(txnHandler, cluster_id_);
        if (!read_result.status.ok()) {
            K2LOG_E(log::catalog, "Failed to check cluster info due to {}", read_result.status);
            txnHandler->AbortTransaction();
            response.status = std::move(read_result.status);
            return response;
        }

        K2LOG_D(log::catalog, "Found SKV catalog version: {}", read_result.clusterInfo->GetCatalogVersion());
        catalog_version_ = read_result.clusterInfo->GetCatalogVersion() + 1;
        // need to update the catalog version on SKV
        // the update frequency could be reduced once we have a single or a quorum of catalog managers
        ClusterInfo cluster_info(cluster_id_, catalog_version_, init_db_done_);
        UpdateClusterInfoResult update_result = cluster_info_handler_->UpdateClusterInfo(txnHandler, cluster_info);
        if (!update_result.status.ok()) {
            txnHandler->AbortTransaction();
            response.status = std::move(update_result.status);
            catalog_version_ = read_result.clusterInfo->GetCatalogVersion();
            K2LOG_D(log::catalog, "Failed to update catalog version due to {}, revert catalog version to {}", update_result.status, catalog_version_);
            return response;
        }
        txnHandler->CommitTransaction();
        response.version = catalog_version_;
        response.status = Status(); // OK;
        K2LOG_D(log::catalog, "Increase catalog version to {}", catalog_version_);
        return response;
    }

    CreateDatabaseResponse SqlCatalogManager::CreateDatabase(const CreateDatabaseRequest& request) {
        CreateDatabaseResponse response;
        K2LOG_D(log::catalog,
            "Creating database with name: {}, id: {}, oid: {}, source_id: {}, nextPgOid: {}",
            request.databaseName, request.databaseId, request.databaseOid, request.sourceDatabaseId, request.nextPgOid.value_or(-1));
        // step 1/3:  check input conditions
        //      check if the target database has already been created, if yes, return already present
        //      check the source database is already there, if it present in the create requet
        std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(request.databaseName);
        if (database_info != nullptr) {
            K2LOG_E(log::catalog, "Database {} has already existed", request.databaseName);
            response.status = STATUS_FORMAT(AlreadyPresent, "Database $0 has already existed", request.databaseName);
            return response;
        }

        std::shared_ptr<DatabaseInfo> source_database_info = nullptr;
        uint32_t t_nextPgOid;
        // validate source database id and check source database to set nextPgOid properly
        if (!request.sourceDatabaseId.empty())
        {
            source_database_info = CheckAndLoadDatabaseById(request.sourceDatabaseId);
            if (source_database_info == nullptr) {
                K2LOG_E(log::catalog, "Failed to find source databases {}", request.sourceDatabaseId);
                response.status = STATUS_FORMAT(NotFound, "Sounrcedatabase $0 not found", request.sourceDatabaseId);
                return response;
            }
            t_nextPgOid = source_database_info->GetNextPgOid();
        } else {
            t_nextPgOid = request.nextPgOid.value();
        }

        // step 2/3: create new database(database), total 3 sub-steps

        // step 2.1 create new SKVCollection
        //   Note: using unique immutable databaseId as SKV collection name
        //   TODO: pass in other collection configurations/parameters later.
        K2LOG_D(log::catalog, "Creating SKV collection for database {}", request.databaseId);
        auto ccResult = k2_adapter_->CreateCollection(request.databaseId, request.databaseName).get();
        if (!ccResult.is2xxOK())
        {
            K2LOG_E(log::catalog, "Failed to create SKV collection {} due to {}", request.databaseId, ccResult);
            response.status = K2Adapter::K2StatusToYBStatus(ccResult);
            return response;
        }

        // step 2.2 Add new database(database) entry into default cluster database table and update in-memory cache
        std::shared_ptr<DatabaseInfo> new_ns = std::make_shared<DatabaseInfo>();
        new_ns->SetDatabaseId(request.databaseId);
        new_ns->SetDatabaseName(request.databaseName);
        new_ns->SetDatabaseOid(request.databaseOid);
        new_ns->SetNextPgOid(t_nextPgOid);
        // persist the new database record
        K2LOG_D(log::catalog, "Adding database {} on SKV", request.databaseId);
        std::shared_ptr<PgTxnHandler> ns_txnHandler = NewTransaction();
        AddOrUpdateDatabaseResult add_result = database_info_handler_->UpsertDatabase(ns_txnHandler, new_ns);
        if (!add_result.status.ok()) {
            K2LOG_E(log::catalog, "Failed to add database {}, due to {}", request.databaseId,add_result.status);
            ns_txnHandler->AbortTransaction();
            response.status = std::move(add_result.status);
            return response;
        }
        // cache databases by database id and database name
        database_id_map_[new_ns->GetDatabaseId()] = new_ns;
        database_name_map_[new_ns->GetDatabaseName()] = new_ns;
        response.databaseInfo = new_ns;

        // step 2.3 Add new system tables for the new database(database)
        std::shared_ptr<PgTxnHandler> target_txnHandler = NewTransaction();
        K2LOG_D(log::catalog, "Creating system tables for target database {}", new_ns->GetDatabaseId());
        CreateMetaTablesResult table_result = table_info_handler_->CreateMetaTables(target_txnHandler, new_ns->GetDatabaseId());
        if (!table_result.status.ok()) {
            K2LOG_E(log::catalog, "Failed to create meta tables for target database {} due to {}",
                new_ns->GetDatabaseId(), table_result.status.code());
            target_txnHandler->AbortTransaction();
            ns_txnHandler->AbortTransaction();
            response.status = std::move(table_result.status);
            return response;
        }

        // step 3/3: If source database(database) is present in the request, copy all the rest of tables from source database(database)
        if (!request.sourceDatabaseId.empty())
        {
            K2LOG_D(log::catalog, "Creating database from source database {}", request.sourceDatabaseId);
            std::shared_ptr<PgTxnHandler> source_txnHandler = NewTransaction();
            // get the source table ids
            K2LOG_D(log::catalog, "Listing table ids from source database {}", request.sourceDatabaseId);
            ListTableIdsResult list_table_result = table_info_handler_->ListTableIds(source_txnHandler, source_database_info->GetDatabaseId(), true);
            if (!list_table_result.status.ok()) {
                K2LOG_E(log::catalog, "Failed to list table ids for database {} due to {}", source_database_info->GetDatabaseId(), list_table_result.status.code());
                source_txnHandler->AbortTransaction();
                target_txnHandler->AbortTransaction();
                ns_txnHandler->AbortTransaction();
                response.status = std::move(list_table_result.status);
                return response;
            }
            K2LOG_D(log::catalog, "Found {} table ids from source database {}", list_table_result.tableIds.size(), request.sourceDatabaseId);
            int num_index = 0;
            for (auto& source_table_id : list_table_result.tableIds) {
                // copy the source table metadata to the target table
                K2LOG_D(log::catalog, "Copying from source table {}", source_table_id);
                CopyTableResult copy_result = table_info_handler_->CopyTable(
                    target_txnHandler,
                    new_ns->GetDatabaseId(),
                    new_ns->GetDatabaseName(),
                    new_ns->GetDatabaseOid(),
                    source_txnHandler,
                    source_database_info->GetDatabaseId(),
                    source_database_info->GetDatabaseName(),
                    source_table_id);
                if (!copy_result.status.ok()) {
                    K2LOG_E(log::catalog, "Failed to copy from source table {} due to {}", source_table_id, copy_result.status.code());
                    source_txnHandler->AbortTransaction();
                    target_txnHandler->AbortTransaction();
                    ns_txnHandler->AbortTransaction();
                    response.status = std::move(copy_result.status);
                    return response;
                }
                num_index += copy_result.num_index;
            }
            source_txnHandler->CommitTransaction();
            K2LOG_D(log::catalog, "Finished copying {} tables and {} indexes from source database {} to {}",
                list_table_result.tableIds.size(), num_index, source_database_info->GetDatabaseId(), new_ns->GetDatabaseId());
        }

        target_txnHandler->CommitTransaction();
        ns_txnHandler->CommitTransaction();
        K2LOG_D(log::catalog, "Created database {}", new_ns->GetDatabaseId());
        response.status = Status(); // OK;
        return response;
    }

    ListDatabasesResponse SqlCatalogManager::ListDatabases(const ListDatabasesRequest& request) {
        ListDatabasesResponse response;
        K2LOG_D(log::catalog, "Listing databases...");
        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        ListDatabaseResult result = database_info_handler_->ListDatabases(txnHandler);
        if (!result.status.ok()) {
            txnHandler->AbortTransaction();
            K2LOG_E(log::catalog, "Failed to list databases due to {}", result.status.code());
            response.status = std::move(result.status);
            return response;
        }
        txnHandler->CommitTransaction();
        if (result.databaseInfos.empty()) {
            K2LOG_W(log::catalog, "No databases are found");
        } else {
            UpdateDatabaseCache(result.databaseInfos);
            for (auto ns_ptr : result.databaseInfos) {
                response.database_infos.push_back(ns_ptr);
            }
        }
        K2LOG_D(log::catalog, "Found {} databases", result.databaseInfos.size());
        response.status = Status(); // OK
        return response;
    }

    GetDatabaseResponse SqlCatalogManager::GetDatabase(const GetDatabaseRequest& request) {
        GetDatabaseResponse response;
        K2LOG_D(log::catalog, "Getting database with name: {}, id: {}", request.databaseName, request.databaseId);
        std::shared_ptr<DatabaseInfo> database_info = GetCachedDatabaseById(request.databaseId);
        if (database_info != nullptr) {
            response.database_info = database_info;
            response.status = Status(); // OK
            return response;
        }
        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        // TODO: use a background task to refresh the database caches to avoid fetching from SKV on each call
        GetDatabaseResult result = database_info_handler_->GetDatabase(txnHandler, request.databaseId);
        if (!result.status.ok()) {
            txnHandler->AbortTransaction();
            K2LOG_E(log::catalog, "Failed to get database {}, due to {}", request.databaseId, result.status);
            response.status = std::move(result.status);
            return response;
        }

        txnHandler->CommitTransaction();
        response.database_info = result.databaseInfo;

        // update database caches
        database_id_map_[response.database_info->GetDatabaseId()] = response.database_info ;
        database_name_map_[response.database_info->GetDatabaseName()] = response.database_info;
        K2LOG_D(log::catalog, "Found database {}", request.databaseId);
        response.status = Status(); // OK;
        return response;
    }

    DeleteDatabaseResponse SqlCatalogManager::DeleteDatabase(const DeleteDatabaseRequest& request) {
        DeleteDatabaseResponse response{};
        K2LOG_D(log::catalog, "Deleting database with name: {}, id: {}", request.databaseName, request.databaseId);
        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        // TODO: use a background task to refresh the database caches to avoid fetching from SKV on each call
        GetDatabaseResult result = database_info_handler_->GetDatabase(txnHandler, request.databaseId);
        txnHandler->CommitTransaction();
        if (!result.status.ok()) {
            K2LOG_E(log::catalog, "Failed to get deletion target database {}.", request.databaseId);
            response.status = std::move(result.status);
            return response;
        }
        std::shared_ptr<DatabaseInfo> database_info = result.databaseInfo;

        // No need to delete table data or metadata, it will be dropped with the SKV collection

        std::shared_ptr<PgTxnHandler> ns_txnHandler = NewTransaction();
        DeleteDataseResult del_result = database_info_handler_->DeleteDatabase(ns_txnHandler, database_info);
        if (!del_result.status.ok()) {
            response.status = std::move(del_result.status);
            ns_txnHandler->AbortTransaction();
            return response;
        }
        ns_txnHandler->CommitTransaction();

        // remove database from local cache
        database_id_map_.erase(database_info->GetDatabaseId());
        database_name_map_.erase(database_info->GetDatabaseName());

        // DropCollection will remove the K2 collection, all of its schemas, and all of its data.
        // It is non-transactional with no rollback ability, but that matches PG's drop database semantics.
        auto drop_result = k2_adapter_->DropCollection(database_info->GetDatabaseId()).get();

        response.status = k2pg::gate::K2Adapter::K2StatusToYBStatus(drop_result);
        return response;
    }

    UseDatabaseResponse SqlCatalogManager::UseDatabase(const UseDatabaseRequest& request) {
        UseDatabaseResponse response;
        // check if the database exists
        std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(request.databaseName);
        if (database_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find database {}", request.databaseName);
            response.status = STATUS_FORMAT(NotFound, "Cannot find database $0", request.databaseName);
            return response;
        }

        // preload tables for a database
        thread_pool_.enqueue([this, request] () {
            K2LOG_I(log::catalog, "Preloading database {}", request.databaseName);
            Status result = CacheTablesFromStorage(request.databaseName, true /*isSysTableIncluded*/);
            if (!result.ok()) {
              K2LOG_W(log::catalog, "Failed to preloading database {} due to {}", request.databaseName, result.code());
            }
        });

        response.status = Status(); // OK;
        return response;
    }

    CreateTableResponse SqlCatalogManager::CreateTable(const CreateTableRequest& request) {
        CreateTableResponse response;
        K2LOG_D(log::catalog,
        "Creating table ns name: {}, ns oid: {}, table name: {}, table oid: {}, systable: {}, shared: {}",
            request.databaseName, request.databaseOid, request.tableName, request.tableOid, request.isSysCatalogTable, request.isSharedTable);
        std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(request.databaseName);
        if (database_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find databaseName {}", request.databaseName);
            response.status = STATUS_FORMAT(NotFound, "Cannot find database $0", request.databaseName);
            return response;
        }

        // check if the Table has already existed or not
        std::shared_ptr<TableInfo> table_info = GetCachedTableInfoByName(database_info->GetDatabaseId(), request.tableName);
        if (table_info != nullptr) {
            // only create table when it does not exist
            if (request.isNotExist) {
                response.status = Status(); // OK;
                response.tableInfo = table_info;
                // return if the table already exists
                return response;
            }

            // return table already present error if table already exists
           response.status = STATUS_FORMAT(AlreadyPresent,
                "Table $0 has already existed in $1", request.tableName, request.databaseName);
            K2LOG_E(log::catalog, "Table {} has already existed in {}", request.tableName, request.databaseName);
            return response;
        }

        // new table
        uint32_t schema_version = request.schema.version();
        CHECK(schema_version == 0) << "Schema version was not initialized to be zero";
        schema_version++;
        // generate a string format table id based database object oid and table oid
        std::string uuid = PgObjectId::GetTableUuid(request.databaseOid, request.tableOid);
        Schema table_schema = request.schema;
        table_schema.set_version(schema_version);
        std::shared_ptr<TableInfo> new_table_info = std::make_shared<TableInfo>(database_info->GetDatabaseId(), request.databaseName,
                request.tableOid, request.tableName, uuid, table_schema);
        new_table_info->set_is_sys_table(request.isSysCatalogTable);
        new_table_info->set_is_shared_table(request.isSharedTable);
        new_table_info->set_next_column_id(table_schema.max_col_id() + 1);

        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        K2LOG_D(log::catalog, "Create or update table id: {}, name: {} in {}, shared: {}", new_table_info->table_id(), request.tableName,
            database_info->GetDatabaseId(), request.isSharedTable);
        try {
            CreateUpdateTableResult result = table_info_handler_->CreateOrUpdateTable(txnHandler, database_info->GetDatabaseId(), new_table_info);
            if (!result.status.ok()) {
                // abort the transaction
                txnHandler->AbortTransaction();
                K2LOG_E(log::catalog, "Failed to create table id: {}, name: {} in {}, due to {}", new_table_info->table_id(), new_table_info->table_name(),
                    database_info->GetDatabaseId(), result.status);
                response.status = std::move(result.status);
                return response;
            }

            // commit transactions
            txnHandler->CommitTransaction();
            K2LOG_D(log::catalog, "Created table id: {}, name: {} in {}, with schema version {}", new_table_info->table_id(), new_table_info->table_name(),
                database_info->GetDatabaseId(), schema_version);
            // update table caches
            UpdateTableCache(new_table_info);

            // return response
            response.status = Status(); // OK;
            response.tableInfo = new_table_info;
        }  catch (const std::exception& e) {
            txnHandler->AbortTransaction();
            response.status = STATUS_FORMAT(RuntimeError, "Failed to create table $0  in $1 due to $2",
                request.tableName, database_info->GetDatabaseId(), e.what());
            K2LOG_E(log::catalog, "Failed to create table {} in {}", request.tableName, database_info->GetDatabaseId());
        }
        return response;
    }

    CreateIndexTableResponse SqlCatalogManager::CreateIndexTable(const CreateIndexTableRequest& request) {
        CreateIndexTableResponse response;

        K2LOG_D(log::catalog, "Creating index ns name: {}, ns oid: {}, index name: {}, index oid: {}, base table oid: {}",
            request.databaseName, request.databaseOid, request.tableName, request.tableOid, request.baseTableOid);
        std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(request.databaseName);
        if (database_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find databaseName {}", request.databaseName);
		    response.status = STATUS_FORMAT(NotFound, "Cannot find database $0", request.databaseName);
            return response;
        }
        // generate table uuid from database oid and table oid
        std::string base_table_uuid = PgObjectId::GetTableUuid(request.databaseOid, request.baseTableOid);
        std::string base_table_id = PgObjectId::GetTableId(request.baseTableOid);

        // check if the base table exists or not
        std::shared_ptr<TableInfo> base_table_info = GetCachedTableInfoById(base_table_uuid);
        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        // try to fetch the table from SKV if not found
        if (base_table_info == nullptr) {
            GetTableResult table_result = table_info_handler_->GetTable(txnHandler, database_info->GetDatabaseId(), database_info->GetDatabaseName(),
                base_table_id);
            if (table_result.status.ok() && table_result.tableInfo != nullptr) {
                // update table cache
                UpdateTableCache(table_result.tableInfo);
                base_table_info = table_result.tableInfo;
            }
        }

        if (base_table_info == nullptr) {
            txnHandler->AbortTransaction();
            // cannot find the base table
            K2LOG_E(log::catalog, "Cannot find base table {} for index {} in {}", base_table_id, request.tableName, database_info->GetDatabaseId());
  		        response.status = STATUS_FORMAT(NotFound,  "Cannot find base table $0 for index $1 in $2 ", base_table_id, request.tableName, database_info->GetDatabaseId());
            return response;
        }
        
        CreateIndexTableParams index_params;
        index_params.index_name = request.tableName;
        index_params.table_oid = request.tableOid;
        index_params.index_schema = request.schema;
        index_params.is_unique = request.isUnique;
        index_params.is_shared = request.isSharedTable;
        index_params.is_not_exist = request.isNotExist;
        index_params.skip_index_backfill = request.skipIndexBackfill;
        index_params.index_permissions = IndexPermissions::INDEX_PERM_READ_WRITE_AND_DELETE;

        // create the table index
        CreateIndexTableResult index_table_result = table_info_handler_->CreateIndexTable(txnHandler, database_info, base_table_info, index_params);
        if (index_table_result.status.ok()) {
            K2ASSERT(log::catalog, index_table_result.indexInfo != nullptr, "Table index can't be null");
            K2LOG_D(log::catalog, "Updating cache for table id: {}, name: {} in {}", index_table_result.indexInfo->table_id(), index_table_result.indexInfo->table_name(), database_info->GetDatabaseId());
            // update table cache
            UpdateTableCache(base_table_info);

            // update index cache
            std::shared_ptr<IndexInfo> new_index_info_ptr = std::move(index_table_result.indexInfo);
            AddIndexCache(new_index_info_ptr);

            // commit and return the new index table
            response.indexInfo = new_index_info_ptr;
            txnHandler->CommitTransaction();

            K2LOG_D(log::catalog, "Created index ns name: {}, ns oid: {}, index name: {}, index oid: {}, base table oid: {}",
                request.databaseName, request.databaseOid, request.tableName, request.tableOid, request.baseTableOid);
        } else {
            txnHandler->AbortTransaction();
            K2LOG_E(log::catalog, "Failed to create index ns name: {}, ns oid: {}, index name: {}, index oid: {}, base table oid: {}",
                request.databaseName, request.databaseOid, request.tableName, request.tableOid, request.baseTableOid);
        }
        response.status = std::move(index_table_result.status);
        return response;
    }

    // Get (base) table schema - if passed-in id is that of a index, return base table schema, if is that of a table, return its table schema
    GetTableSchemaResponse SqlCatalogManager::GetTableSchema(const GetTableSchemaRequest& request) {
        GetTableSchemaResponse response;
        // generate table id from database oid and table oid
        std::string table_uuid = PgObjectId::GetTableUuid(request.databaseOid, request.tableOid);
        std::string table_id = PgObjectId::GetTableId(request.tableOid);
        K2LOG_D(log::catalog, "Get table schema ns oid: {}, table oid: {}, table id: {}",
            request.databaseOid, request.tableOid, table_id);
        // check the table schema from cache
        std::shared_ptr<TableInfo> table_info = GetCachedTableInfoById(table_uuid);
        if (table_info != nullptr) {
            K2LOG_D(log::catalog, "Returned cached table schema name: {}, id: {}", table_info->table_name(), table_info->table_id());
            response.tableInfo = table_info;
            response.status = Status(); // OK
            return response;
        }

        // check if passed in id is that of an index and if so return base table info by index uuid
        table_info = GetCachedBaseTableInfoByIndexId(request.databaseOid, table_uuid);
        if (table_info != nullptr) {
            K2LOG_D(log::catalog, "Returned cached table schema name: {}, id: {} for index {}",
                table_info->table_name(), table_info->table_id(), table_uuid);
            response.tableInfo = table_info;
            response.status = Status(); // OK;
            return response;
        }

        // TODO: refactor following SKV lookup code(till cache update) into tableHandler class
        // Can't find the id from cache above, now look into storage.
        std::string database_id = PgObjectId::GetDatabaseUuid(request.databaseOid);
        std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseById(database_id);
        if (database_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find database {}", database_id);
            response.status = STATUS_FORMAT(NotFound, "Cannot find database $0", database_id);
            return response;
        }
        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        std::shared_ptr<IndexInfo> index_info = GetCachedIndexInfoById(table_uuid);
        GetTableSchemaResult table_schema_result = table_info_handler_->GetTableSchema(txnHandler, database_info,
                table_id,
                index_info,
                [this] (const string &db_id) { return CheckAndLoadDatabaseById(db_id); },
                [this] () { return NewTransaction(); }
                );
        response.status = std::move(table_schema_result.status);
        if (table_schema_result.status.ok() && table_schema_result.tableInfo != nullptr)
            response.tableInfo = table_schema_result.tableInfo;

        // update table cache
        UpdateTableCache(response.tableInfo);

        return response;
    }

    Status SqlCatalogManager::CacheTablesFromStorage(const std::string& databaseName, bool isSysTableIncluded) {
        K2LOG_D(log::catalog, "cache tables for database {}", databaseName);

        std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(databaseName);
        if (database_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find database {}", databaseName);
            return STATUS_FORMAT(NotFound, "Cannot find databaseName $0", databaseName);
        }

        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        ListTablesResult tables_result = table_info_handler_->ListTables(txnHandler, database_info->GetDatabaseId(),
                database_info->GetDatabaseName(), isSysTableIncluded);
        if (!tables_result.status.ok()) {
            txnHandler->AbortTransaction();
            return tables_result.status;
        }

        txnHandler->CommitTransaction();
        
        K2LOG_D(log::catalog, "Found {} tables in database {}", tables_result.tableInfos.size(), databaseName);
        for (auto& tableInfo : tables_result.tableInfos) {
            K2LOG_D(log::catalog, "Caching table name: {}, id: {} in {}", tableInfo->table_name(), tableInfo->table_id(), database_info->GetDatabaseId());
            UpdateTableCache(tableInfo);
        }

        return Status(); // OK;
    }

    DeleteTableResponse SqlCatalogManager::DeleteTable(const DeleteTableRequest& request) {
        K2LOG_D(log::catalog, "Deleting table {} in database {}", request.tableOid, request.databaseOid);
        DeleteTableResponse response;
        std::string database_id = PgObjectId::GetDatabaseUuid(request.databaseOid);
        std::string table_uuid = PgObjectId::GetTableUuid(request.databaseOid, request.tableOid);
        std::string table_id = PgObjectId::GetTableId(request.tableOid);
        response.databaseId = database_id;
        response.tableId = table_id;

        std::shared_ptr<TableInfo> table_info = GetCachedTableInfoById(table_uuid);
        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        if (table_info == nullptr) {
            // try to find table from SKV by looking at database first
            std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseById(database_id);
            if (database_info == nullptr) {
                K2LOG_E(log::catalog, "Cannot find database {}", database_id);
                response.status = STATUS_FORMAT(NotFound, "Cannot find database $0", database_id);
                return response;
            }

            // fetch the table from SKV
            GetTableResult table_result = table_info_handler_->GetTable(txnHandler, database_info->GetDatabaseId(), database_info->GetDatabaseName(),
                table_id);
            if (!table_result.status.ok()) {
                txnHandler->AbortTransaction();
                response.status = std::move(table_result.status);
                return response;
            }

            if (table_result.tableInfo == nullptr) {
                txnHandler->AbortTransaction();
                response.status = STATUS_FORMAT(NotFound, "Cannot find table $0", table_id);
                return response;
            }

            table_info = table_result.tableInfo;
        }

        // delete indexes and the table itself
        // delete table data
         DeleteTableResult delete_data_result = table_info_handler_->DeleteTableData(txnHandler, database_id, table_info);
        if (!delete_data_result.status.ok()) {
            txnHandler->AbortTransaction();
            response.status = std::move(delete_data_result.status);
            return response;
        }

        // delete table schema metadata
        DeleteTableResult delete_metadata_result = table_info_handler_->DeleteTableMetadata(txnHandler, database_id, table_info);
        if (!delete_metadata_result.status.ok()) {
            txnHandler->AbortTransaction();
            response.status = std::move(delete_metadata_result.status);
            return response;
        }

        txnHandler->CommitTransaction();
        // clear table cache after table deletion
        ClearTableCache(table_info);
        response.status = Status(); // OK;
        return response;
    }

    DeleteIndexResponse SqlCatalogManager::DeleteIndex(const DeleteIndexRequest& request) {
        K2LOG_D(log::catalog, "Deleting index {} in ns {}", request.tableOid, request.databaseOid);
        DeleteIndexResponse response;
        std::string database_id = PgObjectId::GetDatabaseUuid(request.databaseOid);
        std::string table_uuid = PgObjectId::GetTableUuid(request.databaseOid, request.tableOid);
        std::string table_id = PgObjectId::GetTableId(request.tableOid);
        response.databaseId = database_id;
        std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseById(database_id);
        if (database_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find database {}", database_id);
            response.status = STATUS_FORMAT(NotFound, "Cannot find database $0", database_id);
            return response;
        }

        std::shared_ptr<PgTxnHandler> txnHandler = NewTransaction();
        std::shared_ptr<IndexInfo> index_info = GetCachedIndexInfoById(table_uuid);
        std::string base_table_id;
        if (index_info == nullptr) {
            GetBaseTableIdResult index_result = table_info_handler_->GetBaseTableId(txnHandler, database_id, table_id);
            if (!index_result.status.ok()) {
                response.status = std::move(index_result.status);
                txnHandler->AbortTransaction();
                return response;
            }
            base_table_id = index_result.baseTableId;
        } else {
            base_table_id = index_info->base_table_id();
        }

        std::shared_ptr<TableInfo> base_table_info = GetCachedTableInfoById(base_table_id);
        // try to fetch the table from SKV if not found
        if (base_table_info == nullptr) {
            GetTableResult table_result = table_info_handler_->GetTable(txnHandler, database_id, database_info->GetDatabaseName(),
                    base_table_id);
            if (!table_result.status.ok()) {
                txnHandler->AbortTransaction();
                response.status = std::move(table_result.status);
                return response;
            }

            if (table_result.tableInfo == nullptr) {
                txnHandler->AbortTransaction();
                response.status = STATUS_FORMAT(NotFound, "Cannot find Base table $0", base_table_id);
                return response;
            }

            base_table_info = table_result.tableInfo;
        }

        // delete index data
        DeleteIndexResult delete_data_result = table_info_handler_->DeleteIndexData(txnHandler, database_id, table_id);
        if (!delete_data_result.status.ok()) {
            txnHandler->AbortTransaction();
            response.status = std::move(delete_data_result.status);
            return response;
        }

        // delete index metadata
        DeleteIndexResult delete_metadata_result = table_info_handler_->DeleteIndexMetadata(txnHandler, database_id, table_id);
        if (!delete_metadata_result.status.ok()) {
            txnHandler->AbortTransaction();
            response.status = std::move(delete_metadata_result.status);
            return response;
        }

        txnHandler->CommitTransaction();
        // remove index from the table_info object
        base_table_info->drop_index(table_id);
        // update table cache with the index removed, index cache is updated accordingly
        UpdateTableCache(base_table_info);
        response.baseIndexTableOid = base_table_info->table_oid();
        response.status = Status(); // OK;
        return response;
    }

    ReservePgOidsResponse SqlCatalogManager::ReservePgOid(const ReservePgOidsRequest& request) {
        ReservePgOidsResponse response;
        K2LOG_D(log::catalog, "Reserving PgOid with nextOid: {}, count: {}, for ns: {}",
            request.nextOid, request.count, request.databaseId);
        std::shared_ptr<PgTxnHandler> ns_txnHandler = NewTransaction();
        GetDatabaseResult result = database_info_handler_->GetDatabase(ns_txnHandler, request.databaseId);
        if (!result.status.ok()) {
            ns_txnHandler->AbortTransaction();
            K2LOG_E(log::catalog, "Failed to get database {}", request.databaseId);
            response.status = std::move(result.status);
            return response;
        }

        uint32_t begin_oid = result.databaseInfo->GetNextPgOid();
        if (begin_oid < request.nextOid) {
            begin_oid = request.nextOid;
        }
        if (begin_oid == std::numeric_limits<uint32_t>::max()) {
            ns_txnHandler->AbortTransaction();
            K2LOG_W(log::catalog, "No more object identifier is available for Postgres database {}", request.databaseId);
            response.status = STATUS_FORMAT(InvalidArgument, "FNo more object identifier is available for $0", request.databaseId);
            return response;
        }

        uint32_t end_oid = begin_oid + request.count;
        if (end_oid < begin_oid) {
            end_oid = std::numeric_limits<uint32_t>::max(); // Handle wraparound.
        }
        response.databaseId = request.databaseId;
        response.beginOid = begin_oid;
        response.endOid = end_oid;

        // update the database record on SKV
        // We use read and write in the same transaction so that K23SI guarantees that concurrent SKV records on SKV
        // won't override each other and won't lose the correctness of PgNextOid
        std::shared_ptr<DatabaseInfo> updated_ns = std::move(result.databaseInfo);
        updated_ns->SetNextPgOid(end_oid);
        K2LOG_D(log::catalog, "Updating nextPgOid on SKV to {} for database {}", end_oid, request.databaseId);
        AddOrUpdateDatabaseResult update_result = database_info_handler_->UpsertDatabase(ns_txnHandler, updated_ns);
        if (!update_result.status.ok()) {
            ns_txnHandler->AbortTransaction();
            K2LOG_E(log::catalog, "Failed to update nextPgOid on SKV due to {}", update_result.status);
            response.status = std::move(update_result.status);
            return response;
        }

        ns_txnHandler->CommitTransaction();
        K2LOG_D(log::catalog, "Reserved PgOid succeeded for database {}", request.databaseId);
        // update database caches after persisting to SKV successfully
        database_id_map_[updated_ns->GetDatabaseId()] = updated_ns;
        database_name_map_[updated_ns->GetDatabaseName()] = updated_ns;
        response.status = Status(); // OK;
        return response;
    }

    // update database caches
    void SqlCatalogManager::UpdateDatabaseCache(std::vector<std::shared_ptr<DatabaseInfo>> database_infos) {
        std::lock_guard<std::mutex> l(lock_);
        database_id_map_.clear();
        database_name_map_.clear();
        for (auto ns_ptr : database_infos) {
            database_id_map_[ns_ptr->GetDatabaseId()] = ns_ptr;
            database_name_map_[ns_ptr->GetDatabaseName()] = ns_ptr;
        }
    }

    // update table caches
    void SqlCatalogManager::UpdateTableCache(std::shared_ptr<TableInfo> table_info) {
        std::lock_guard<std::mutex> l(lock_);
        table_uuid_map_[table_info->table_uuid()] = table_info;
        // TODO: add logic to remove table with old name if rename table is called
        TableNameKey key = std::make_pair(table_info->database_id(), table_info->table_name());
        table_name_map_[key] = table_info;
        // update the corresponding index cache
        UpdateIndexCacheForTable(table_info);
    }

    // remove table info from table cache and its related indexes from index cache
    void SqlCatalogManager::ClearTableCache(std::shared_ptr<TableInfo> table_info) {
        std::lock_guard<std::mutex> l(lock_);
        ClearIndexCacheForTable(table_info->table_id());
        table_uuid_map_.erase(table_info->table_uuid());
        TableNameKey key = std::make_pair(table_info->database_id(), table_info->table_name());
        table_name_map_.erase(key);
    }

    // clear index infos for a table in the index cache
    void SqlCatalogManager::ClearIndexCacheForTable(const std::string& base_table_id) {
        std::vector<std::string> index_uuids;
        for (std::pair<std::string, std::shared_ptr<IndexInfo>> pair : index_uuid_map_) {
            // first find all indexes that belong to the table
            if (base_table_id == pair.second->base_table_id()) {
                index_uuids.push_back(pair.second->table_uuid());
            }
        }
        // delete the indexes in cache
        for (std::string index_uuid : index_uuids) {
            index_uuid_map_.erase(index_uuid);
        }
    }

    void SqlCatalogManager::UpdateIndexCacheForTable(std::shared_ptr<TableInfo> table_info) {
        // clear existing index informaton first
        ClearIndexCacheForTable(table_info->table_id());
        // add the new indexes to the index cache
        if (table_info->has_secondary_indexes()) {
            for (std::pair<std::string, IndexInfo> pair : table_info->secondary_indexes()) {
                AddIndexCache(std::make_shared<IndexInfo>(pair.second));
            }
        }
    }

    void SqlCatalogManager::AddIndexCache(std::shared_ptr<IndexInfo> index_info) {
        index_uuid_map_[index_info->table_uuid()] = index_info;
    }

    std::shared_ptr<DatabaseInfo> SqlCatalogManager::GetCachedDatabaseById(const std::string& database_id) {
        if (!database_id_map_.empty()) {
            const auto itr = database_id_map_.find(database_id);
            if (itr != database_id_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<DatabaseInfo> SqlCatalogManager::GetCachedDatabaseByName(const std::string& database_name) {
        if (!database_name_map_.empty()) {
            const auto itr = database_name_map_.find(database_name);
            if (itr != database_name_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<TableInfo> SqlCatalogManager::GetCachedTableInfoById(const std::string& table_uuid) {
        if (!table_uuid_map_.empty()) {
            const auto itr = table_uuid_map_.find(table_uuid);
            if (itr != table_uuid_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<TableInfo> SqlCatalogManager::GetCachedTableInfoByName(const std::string& database_id, const std::string& table_name) {
        if (!table_name_map_.empty()) {
           TableNameKey key = std::make_pair(database_id, table_name);
           const auto itr = table_name_map_.find(key);
            if (itr != table_name_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<IndexInfo> SqlCatalogManager::GetCachedIndexInfoById(const std::string& index_uuid) {
        if (!index_uuid_map_.empty()) {
            const auto itr = index_uuid_map_.find(index_uuid);
            if (itr != index_uuid_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<TableInfo> SqlCatalogManager::GetCachedBaseTableInfoByIndexId(uint32_t databaseOid, const std::string& index_uuid) {
        std::shared_ptr<IndexInfo> index_info = nullptr;
        if (!index_uuid_map_.empty()) {
            const auto itr = index_uuid_map_.find(index_uuid);
            if (itr != index_uuid_map_.end()) {
                index_info = itr->second;
            }
        }
        if (index_info == nullptr) {
            return nullptr;
        }

        // get base table uuid from database oid and base table id
        uint32_t base_table_oid = PgObjectId::GetTableOidByTableUuid(index_info->base_table_id());
        if (base_table_oid == kPgInvalidOid) {
            K2LOG_W(log::catalog, "Invalid base table id {}", index_info->base_table_id());
            return nullptr;
        }
        std::string base_table_uuid = PgObjectId::GetTableUuid(databaseOid, base_table_oid);
        return GetCachedTableInfoById(base_table_uuid);
    }

    // TODO: return Status instead of throw exception.
    std::shared_ptr<PgTxnHandler> SqlCatalogManager::NewTransaction() {
        std::shared_ptr<PgTxnHandler> handler = std::make_shared<PgTxnHandler>(k2_adapter_);
        auto result = handler->BeginTransaction();
        if (!result.ok())
        {
            throw std::runtime_error("Cannot start new transaction.");
        }
        return handler;
    }

    std::shared_ptr<DatabaseInfo> SqlCatalogManager::CheckAndLoadDatabaseByName(const std::string& database_name) {
        std::shared_ptr<DatabaseInfo> database_info = GetCachedDatabaseByName(database_name);
        if (database_info == nullptr) {
            // try to refresh databases from SKV in case that the requested database is created by another catalog manager instance
            // this could be avoided by use a single or a quorum of catalog managers
            std::shared_ptr<PgTxnHandler> ns_txnHandler = NewTransaction();
            ListDatabaseResult result = database_info_handler_->ListDatabases(ns_txnHandler);
            ns_txnHandler->CommitTransaction();
            if (result.status.ok() && !result.databaseInfos.empty()) {
                // update database caches
                UpdateDatabaseCache(result.databaseInfos);
                // recheck database
                database_info = GetCachedDatabaseByName(database_name);
            }
        }
        return database_info;
    }

    std::shared_ptr<DatabaseInfo> SqlCatalogManager::CheckAndLoadDatabaseById(const std::string& database_id) {
        std::shared_ptr<DatabaseInfo> database_info = GetCachedDatabaseById(database_id);
        if (database_info == nullptr) {
            // try to refresh databases from SKV in case that the requested database is created by another catalog manager instance
            // this could be avoided by use a single or a quorum of catalog managers
            std::shared_ptr<PgTxnHandler> ns_txnHandler = NewTransaction();
            ListDatabaseResult result = database_info_handler_->ListDatabases(ns_txnHandler);
            ns_txnHandler->CommitTransaction();
            if (result.status.ok() && !result.databaseInfos.empty()) {
                // update database caches
                UpdateDatabaseCache(result.databaseInfos);
                // recheck database
                database_info = GetCachedDatabaseById(database_id);
            }
        }
        return database_info;
    }


} // namespace catalog
}  // namespace sql
}  // namespace k2pg
