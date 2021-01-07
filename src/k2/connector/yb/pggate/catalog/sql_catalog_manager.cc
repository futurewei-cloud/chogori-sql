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

#include "yb/pggate/catalog/sql_catalog_manager.h"

#include <algorithm>
#include <list>
#include <thread>
#include <vector>

#include <glog/logging.h>

namespace k2pg {
namespace sql {
namespace catalog {

    using yb::Status;
    using k2pg::gate::K2Adapter;

    SqlCatalogManager::SqlCatalogManager(std::shared_ptr<K2Adapter> k2_adapter) :
        cluster_id_(CatalogConsts::default_cluster_id), k2_adapter_(k2_adapter) {
        cluster_info_handler_ = std::make_shared<ClusterInfoHandler>(k2_adapter);
        namespace_info_handler_ = std::make_shared<NamespaceInfoHandler>(k2_adapter);
        table_info_handler_ = std::make_shared<TableInfoHandler>(k2_adapter_);
    }

    SqlCatalogManager::~SqlCatalogManager() {
    }

    Status SqlCatalogManager::Start() {
        K2DEBUG("Starting Catalog Manager...");
        CHECK(!initted_.load(std::memory_order_acquire));

        std::shared_ptr<SessionTransactionContext> ci_context = NewTransactionContext();
        // load cluster info
        GetClusterInfoResult ciresp = cluster_info_handler_->ReadClusterInfo(ci_context, cluster_id_);
        if (ciresp.status.IsSucceeded()) {
            if (ciresp.clusterInfo != nullptr) {
                init_db_done_.store(ciresp.clusterInfo->IsInitdbDone(), std::memory_order_relaxed);
                catalog_version_.store(ciresp.clusterInfo->GetCatalogVersion(), std::memory_order_relaxed);
                K2DEBUG("Loaded cluster info record succeeded");
            } else {
                ci_context->Abort();  // no difference either abort or commit
                K2WARN("Empty cluster info record, likely primary cluster is not initialized. Only operation allowed is primary cluster initialization. ");
                // it is ok, but only InitPrimaryCluster can be executed on the SqlCatalogrMager
                // keep initted_ to be false;
                return Status::OK();
            }
        } else {
            K2ERROR("Failed to read cluster info record due to " << ciresp.status.errorMessage);
            ci_context->Abort();
            return STATUS_FORMAT(IOError, "Failed to read cluster info record to error code $0 and message $1",
                    ciresp.status.code, ciresp.status.errorMessage);
        }

        // end the current transaction so that we use a different one for later operations
        ci_context->Commit();

        // load namespaces
        std::shared_ptr<SessionTransactionContext> ns_context = NewTransactionContext();
        ListNamespacesResult nsresp = namespace_info_handler_->ListNamespaces(ns_context);
        ns_context->Commit();

        if (nsresp.status.IsSucceeded()) {
            if (!nsresp.namespaceInfos.empty()) {
                for (auto ns_ptr : nsresp.namespaceInfos) {
                    // cache namespaces by namespace id and namespace name
                    namespace_id_map_[ns_ptr->GetNamespaceId()] = ns_ptr;
                    namespace_name_map_[ns_ptr->GetNamespaceName()] = ns_ptr;
                }
            } else {
                K2DEBUG("namespaces are empty");
            }
        } else {
            K2ERROR("Failed to load namespaces due to " <<  nsresp.status.errorMessage);
            return STATUS_FORMAT(IOError, "Failed to load namespaces due to error code $0 and message $1",
                nsresp.status.code, nsresp.status.errorMessage);
        }

        initted_.store(true, std::memory_order_release);
        K2DEBUG("Catalog Manager started up successfully");
        return Status::OK();
    }

    void SqlCatalogManager::Shutdown() {
        K2DEBUG("SQL CatalogManager shutting down...");

        bool expected = true;
        if (initted_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            // TODO: add shut down steps

        }

        K2DEBUG("SQL CatalogManager shut down complete. Bye!");
    }

    // Called only once during PG initDB
    // TODO: handle partial failure(maybe simply fully cleanup) to allow retry later
    Status SqlCatalogManager::InitPrimaryCluster()
    {
        K2DEBUG("SQL CatalogManager initialize primary Cluster!");

        CHECK(!initted_.load(std::memory_order_relaxed));

        // step 1/4 create the SKV collection for the primary
        RStatus rs = namespace_info_handler_->CreateSKVCollection(CatalogConsts::skv_collection_name_sql_primary, CatalogConsts::default_cluster_id);
        if (!rs.IsSucceeded())
        {
            K2ERROR("Failed to create SKV collection during initialization primary PG cluster due to " <<  rs.errorMessage);
            return STATUS_FORMAT(IOError, "Failed to create SKV collection during initialization primary PG cluster due to error code $0 and message $1",
                    rs.code, rs.errorMessage);
        }

        std::shared_ptr<SessionTransactionContext> init_context = NewTransactionContext();

        // step 2/4 Init Cluster info, including create the SKVSchema in the primary cluster's SKVCollection for cluster_info and insert current cluster info into
        //      Note: Initialize cluster info's init_db column with TRUE
        ClusterInfo cluster_info(cluster_id_, catalog_version_, false /*init_db_done*/);

        InitClusterInfoResult initCIRes = cluster_info_handler_->InitClusterInfo(init_context, cluster_info);
        if (initCIRes.status.IsSucceeded()) {
            K2DEBUG("Initialization of cluster info succeeded");
        } else {
            init_context->Abort();
                K2ERROR("Failed to initialize cluster info due to " << initCIRes.status.errorMessage);
                return STATUS_FORMAT(IOError, "Failed to create cluster info record to error code $0 and message $1",
                    initCIRes.status.code, initCIRes.status.errorMessage);
        }

        // step 3/4 Init namespace_info - create the SKVSchema in the primary cluster's SKVcollection for namespace_info
        InitNamespaceTableResult initRes = namespace_info_handler_->InitNamespaceTable();
        if (!initRes.status.IsSucceeded()) {
            K2ERROR("Failed to initialize creating namespace table due to " <<  initRes.status.errorMessage);
            return STATUS_FORMAT(IOError, "Failed to initialize creating namespace table due to error code $0 and message $1",
                initRes.status.code, initRes.status.errorMessage);
        }

        init_context->Commit();

        // step 4/4 re-start this catalog manager so it can execute other APIs
        Status status = Start();
        if (status.ok())
        {
            // check things are ready
            CHECK(initted_.load(std::memory_order_relaxed));
            K2DEBUG("SQL CatalogManager successfully initialized primary Cluster!");
        }
        else
        {
            K2ERROR("Failed to create SKV collection during initialization primary PG cluster due to " <<  status.ToUserMessage());
        }

        return status;
    }

    Status SqlCatalogManager::FinishInitDB()
    {
        K2DEBUG("Setting initDbDone to be true...");
        if (!init_db_done_) {
            std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
            // check the latest cluster info on SKV
            GetClusterInfoResult result = cluster_info_handler_->ReadClusterInfo(context, cluster_id_);
            if (!result.status.IsSucceeded()) {
                context->Abort();
                K2ERROR("Cannot read cluster info record on SKV due to " << result.status.errorMessage);
                return STATUS_FORMAT(IOError, "Cannot read cluster info record on SKV due to error code $0 and message $1",
                    result.status.code, result.status.errorMessage);
            }
            if (result.clusterInfo == nullptr) {
                context->Abort();
                K2ERROR("Cluster info record is empty on SKV");
                return STATUS(IOError, "Cluster info record is empty on SKV");
            }

            if (result.clusterInfo->IsInitdbDone()) {
                context->Commit();
                init_db_done_.store(true, std::memory_order_relaxed);
                K2DEBUG("InitDbDone is already true on SKV");
                return Status::OK();
            }

            K2DEBUG("Updating cluster info with initDbDone to be true");
            std::shared_ptr<ClusterInfo> new_cluster_info = result.clusterInfo;
            new_cluster_info->SetInitdbDone(true);
            UpdateClusterInfoResult update_result = cluster_info_handler_->UpdateClusterInfo(context, *new_cluster_info.get());
            if (!update_result.status.IsSucceeded()) {
                context->Abort();
                K2ERROR("Failed to update cluster info due to " << update_result.status.errorMessage);
                return STATUS_FORMAT(IOError, "Failed to update cluster info due to error code $0 and message $1",
                    update_result.status.code, update_result.status.errorMessage);
            }
            context->Commit();
            init_db_done_.store(true, std::memory_order_relaxed);
            K2DEBUG("Set initDbDone to be true successfully");
        } else {
            K2DEBUG("InitDb is true already");
        }
        return Status::OK();
    }

    GetInitDbResponse SqlCatalogManager::IsInitDbDone(const GetInitDbRequest& request) {
        GetInitDbResponse response;
        if (!init_db_done_) {
            std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
            GetClusterInfoResult result = cluster_info_handler_->ReadClusterInfo(context, cluster_id_);
            context->Commit();
            if (result.status.IsSucceeded() && result.clusterInfo != nullptr) {
                K2DEBUG("Checked IsInitDbDone from SKV " << result.clusterInfo->IsInitdbDone());
                if (result.clusterInfo->IsInitdbDone()) {
                    init_db_done_.store(result.clusterInfo->IsInitdbDone(), std::memory_order_relaxed);
                }
                if (result.clusterInfo->GetCatalogVersion() > catalog_version_) {
                    catalog_version_.store(result.clusterInfo->GetCatalogVersion(), std::memory_order_relaxed);
                }
            } else {
                K2ERROR("Failed to check IsInitDbDone from SKV due to " << result.status.errorMessage);
                response.status = std::move(result.status);
                return response;
            }
        }
        K2DEBUG("Get InitDBDone successfully " << init_db_done_);
        response.isInitDbDone = init_db_done_;
        response.status.Succeed();
        return response;
    }

    GetCatalogVersionResponse SqlCatalogManager::GetCatalogVersion(const GetCatalogVersionRequest& request) {
        GetCatalogVersionResponse response;
        K2DEBUG("Checking catalog version...");
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        // TODO: use a background thread to fetch the ClusterInfo record periodically instead of fetching it for each call
        GetClusterInfoResult result = cluster_info_handler_->ReadClusterInfo(context, cluster_id_);
        if (!result.status.IsSucceeded()) {
            K2ERROR("Failed to check cluster info due to " << result.status.errorMessage);
            context->Abort();
            response.status = std::move(result.status);
            return response;
        }
        if (result.clusterInfo == nullptr) {
            context->Abort();
            response.status.errorMessage = "Cluster Info record is empty";
            response.status.code = StatusCode::NOT_FOUND;
            return response;
        }
        RStatus status = UpdateCatalogVersion(context, result.clusterInfo->GetCatalogVersion());
        if (!status.IsSucceeded()) {
            context->Abort();
            K2ERROR("Failed to update catalog version due to " << status.errorMessage);
            response.status = std::move(status);
            return response;
        }
        context->Commit();
        K2DEBUG("Returned catalog version " << catalog_version_);
        response.catalogVersion = catalog_version_;
        response.status.Succeed();
        return response;
    }

    CreateNamespaceResponse SqlCatalogManager::CreateNamespace(const CreateNamespaceRequest& request) {
        CreateNamespaceResponse response;
        K2DEBUG("Creating namespace with name: " << request.namespaceName << ", id: " << request.namespaceId << ", oid: " << request.namespaceOid
            << ", source id: " << request.sourceNamespaceId << ", nextPgOid: " << request.nextPgOid.value_or(-1));
        // step 1/3:  check input conditions
        //      check if the target namespace has already been created, if yes, return already present
        //      check the source namespace is already there, if it present in the create requet
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceByName(request.namespaceName);
        if (namespace_info != nullptr) {
            K2ERROR("Namespace " << request.namespaceName << " has already existed");
            response.status.code = StatusCode::ALREADY_PRESENT;
            response.status.errorMessage = "Namespace " + request.namespaceName + " has already existed";
            return response;
        }

        std::shared_ptr<NamespaceInfo> source_namespace_info = nullptr;
        uint32_t t_nextPgOid;
        // validate source namespace id and check source namespace to set nextPgOid properly
        if (!request.sourceNamespaceId.empty())
        {
            source_namespace_info = CheckAndLoadNamespaceById(request.sourceNamespaceId);
            if (source_namespace_info == nullptr) {
                K2ERROR("Failed to find source namespaces " << request.sourceNamespaceId);
                response.status.code = StatusCode::ALREADY_PRESENT;
                response.status.errorMessage = "Namespace " + request.namespaceName + " does not exist";
                return response;
            }
            t_nextPgOid = source_namespace_info->GetNextPgOid();
        } else {
            t_nextPgOid = request.nextPgOid.value();
        }

        // step 2/3: create new namespace(database), total 3 sub-steps

        // step 2.1 create new SKVCollection
        //   Note: using unique immutable namespaceId as SKV collection name
        //   TODO: pass in other collection configurations/parameters later.
        K2DEBUG("Creating SKV collection for namespace " << request.namespaceId);
        response.status = namespace_info_handler_->CreateSKVCollection(request.namespaceId, request.namespaceName);
        if (!response.status.IsSucceeded())
        {
            K2ERROR("Failed to create SKV collection " << request.namespaceId);
            return response;
        }

        // step 2.2 Add new namespace(database) entry into default cluster Namespace table and update in-memory cache
        std::shared_ptr<NamespaceInfo> new_ns = std::make_shared<NamespaceInfo>();
        new_ns->SetNamespaceId(request.namespaceId);
        new_ns->SetNamespaceName(request.namespaceName);
        new_ns->SetNamespaceOid(request.namespaceOid);
        new_ns->SetNextPgOid(t_nextPgOid);
        // persist the new namespace record
        K2DEBUG("Adding namespace " << request.namespaceId << " on SKV");
        std::shared_ptr<SessionTransactionContext> ns_context = NewTransactionContext();
        AddOrUpdateNamespaceResult add_result = namespace_info_handler_->AddOrUpdateNamespace(ns_context, new_ns);
        if (!add_result.status.IsSucceeded()) {
            K2ERROR("Failed to add namespace " << request.namespaceId << " due to " << add_result.status.errorMessage);
            ns_context->Abort();
            response.status = std::move(add_result.status);
            return response;
        }
        // cache namespaces by namespace id and namespace name
        namespace_id_map_[new_ns->GetNamespaceId()] = new_ns;
        namespace_name_map_[new_ns->GetNamespaceName()] = new_ns;
        response.namespaceInfo = new_ns;

        // step 2.3 Add new system tables for the new namespace(database)
        std::shared_ptr<SessionTransactionContext> target_context = NewTransactionContext();
        K2DEBUG("Creating system tables for target namespace " << new_ns->GetNamespaceId());
        CreateSysTablesResult table_result = table_info_handler_->CheckAndCreateSystemTables(target_context, new_ns->GetNamespaceId());
        if (!table_result.status.IsSucceeded()) {
            K2ERROR("Failed to create system tables for target namespace " << new_ns->GetNamespaceId() << " due to " << table_result.status.errorMessage);
            target_context->Abort();
            ns_context->Abort();
            response.status = std::move(table_result.status);
            return response;
        }

        // step 3/3: If source namespace(database) is present in the request, copy all the rest of tables from source namespace(database)
        if (!request.sourceNamespaceId.empty())
        {
            K2DEBUG("Creating namespace from source namespace " << request.sourceNamespaceId);
            std::shared_ptr<SessionTransactionContext> source_context = NewTransactionContext();
            // get the source table ids
            K2DEBUG("Listing table ids from source namespace " << request.sourceNamespaceId);
            ListTableIdsResult list_table_result = table_info_handler_->ListTableIds(source_context, source_namespace_info->GetNamespaceId(), true);
            if (!list_table_result.status.IsSucceeded()) {
                K2ERROR("Failed to list table ids for namespace " << source_namespace_info->GetNamespaceId());
                source_context->Abort();
                target_context->Abort();
                ns_context->Abort();
                response.status = std::move(list_table_result.status);
                return response;
            }
            for (auto& source_table_id : list_table_result.tableIds) {
                // copy the source table metadata to the target table
                K2DEBUG("Copying from source table " << source_table_id);
                CopyTableResult copy_result = table_info_handler_->CopyTable(
                    target_context,
                    new_ns->GetNamespaceId(),
                    new_ns->GetNamespaceName(),
                    new_ns->GetNamespaceOid(),
                    source_context,
                    source_namespace_info->GetNamespaceId(),
                    source_namespace_info->GetNamespaceName(),
                    source_table_id);
                if (!copy_result.status.IsSucceeded()) {
                    K2ERROR("Failed to copy from source table " << source_table_id);
                    source_context->Abort();
                    target_context->Abort();
                    ns_context->Abort();
                    response.status = std::move(copy_result.status);
                    return response;
                }
            }
            source_context->Commit();
            K2DEBUG("Finished copying tables from source namespace " << source_namespace_info->GetNamespaceId()
                << " to " << new_ns->GetNamespaceId());
        }

        target_context->Commit();
        ns_context->Commit();
        K2DEBUG("Created namespace " << new_ns->GetNamespaceId() << " successfully");
        response.status.Succeed();
        return response;
    }

    ListNamespacesResponse SqlCatalogManager::ListNamespaces(const ListNamespacesRequest& request) {
        ListNamespacesResponse response;
        K2DEBUG("Listing namespaces...");
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        ListNamespacesResult result = namespace_info_handler_->ListNamespaces(context);
        if (!result.status.IsSucceeded()) {
            context->Abort();
            K2ERROR("Failed to list namespaces due to " << result.status.errorMessage);
            response.status = std::move(result.status);
            return response;
        }
        context->Commit();
        if (result.namespaceInfos.empty()) {
            K2WARN("No namespaces are found");
        } else {
            UpdateNamespaceCache(result.namespaceInfos);
            for (auto ns_ptr : result.namespaceInfos) {
                response.namespace_infos.push_back(ns_ptr);
            }
        }
        K2DEBUG("Found namespaces " << result.namespaceInfos.size());
        response.status.Succeed();
        return response;
    }

    GetNamespaceResponse SqlCatalogManager::GetNamespace(const GetNamespaceRequest& request) {
        GetNamespaceResponse response;
        K2DEBUG("Getting namespace with name: " << request.namespaceName << ", id: " << request.namespaceId);
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        // TODO: use a background task to refresh the namespace caches to avoid fetching from SKV on each call
        GetNamespaceResult result = namespace_info_handler_->GetNamespace(context, request.namespaceId);
        if (!result.status.IsSucceeded()) {
            context->Abort();
            K2ERROR("Failed to get namespace " << request.namespaceId << " due to " << result.status.errorMessage);
            response.status = std::move(result.status);
            return response;
        }
        if (result.namespaceInfo == nullptr) {
            context->Abort();
            K2WARN("Cannot find namespace " << request.namespaceId);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + request.namespaceId;
            return response;
        }
        context->Commit();
        response.namespace_info = result.namespaceInfo;

        // update namespace caches
        namespace_id_map_[response.namespace_info->GetNamespaceId()] = response.namespace_info ;
        namespace_name_map_[response.namespace_info->GetNamespaceName()] = response.namespace_info;
        K2DEBUG("Found namespace " << request.namespaceId << " successfully");
        response.status.Succeed();
        return response;
    }

    DeleteNamespaceResponse SqlCatalogManager::DeleteNamespace(const DeleteNamespaceRequest& request) {
        DeleteNamespaceResponse response;
        K2DEBUG("Deleting namespace with name: " << request.namespaceName << ", id: " << request.namespaceId);
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        // TODO: use a background task to refresh the namespace caches to avoid fetching from SKV on each call
        GetNamespaceResult result = namespace_info_handler_->GetNamespace(context, request.namespaceId);
        context->Commit();
        if (!result.status.IsSucceeded() || result.namespaceInfo == nullptr) {
            K2WARN("Cannot find namespace " << request.namespaceId);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + request.namespaceId;
            return response;
        }
        std::shared_ptr<NamespaceInfo> namespace_info = result.namespaceInfo;

        // delete all namespace tables and indexes
        std::shared_ptr<SessionTransactionContext> tb_context = NewTransactionContext();
        ListTableIdsResult list_table_result = table_info_handler_->ListTableIds(tb_context, request.namespaceId, true);
        if (!list_table_result.status.IsSucceeded()) {
            response.status = std::move(list_table_result.status);
            tb_context->Abort();
            return response;
        }
        for (auto& table_id : list_table_result.tableIds) {
            GetTableResult table_result = table_info_handler_->GetTable(tb_context, request.namespaceId,
                    request.namespaceName, table_id);
            if (!table_result.status.IsSucceeded() || table_result.tableInfo == nullptr) {
                response.status = std::move(table_result.status);
                tb_context->Abort();
                return response;
            }
            // delete table data
            DeleteTableResult tb_data_result = table_info_handler_->DeleteTableData(tb_context, request.namespaceId, table_result.tableInfo);
            if (!tb_data_result.status.IsSucceeded()) {
                response.status = std::move(tb_data_result.status);
                tb_context->Abort();
                return response;
            }
            // delete table schema metadata
            DeleteTableResult tb_metadata_result = table_info_handler_->DeleteTableMetadata(tb_context, request.namespaceId, table_result.tableInfo);
            if (!tb_metadata_result.status.IsSucceeded()) {
                response.status = std::move(tb_metadata_result.status);
                tb_context->Abort();
                return response;
            }
        }

        std::shared_ptr<SessionTransactionContext> ns_context = NewTransactionContext();
        DeleteNamespaceResult del_result = namespace_info_handler_->DeleteNamespace(ns_context, namespace_info);
        if (!del_result.status.IsSucceeded()) {
            response.status = std::move(del_result.status);
            tb_context->Abort();
            ns_context->Abort();
            return response;
        }
        tb_context->Commit();
        ns_context->Commit();

        // remove namespace from local cache
        namespace_id_map_.erase(namespace_info->GetNamespaceId());
        namespace_name_map_.erase(namespace_info->GetNamespaceName());
        response.status.Succeed();
        return response;
    }

    CreateTableResponse SqlCatalogManager::CreateTable(const CreateTableRequest& request) {
        CreateTableResponse response;
        K2DEBUG("Creating table ns name: " << request.namespaceName << ", ns oid: " << request.namespaceOid
            << ", table name: " << request.tableName << ", table oid: " << request.tableOid
            << ", systable: " << request.isSysCatalogTable << ", shared: " << request.isSharedTable);
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceByName(request.namespaceName);
        if (namespace_info == nullptr) {
            K2ERROR("Cannot find namespace " << request.namespaceName);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + request.namespaceName;
            return response;
        }
        // check if the Table has already existed or not
        std::shared_ptr<TableInfo> table_info = GetCachedTableInfoByName(namespace_info->GetNamespaceId(), request.tableName);
        uint32_t schema_version = 0;
        std::string table_id;
        if (table_info != nullptr) {
            // only create table when it does not exist
            if (request.isNotExist) {
                response.status.Succeed();
                response.tableInfo = table_info;
                // return if the table already exists
                return response;
            }

            // return table already present error if table already exists
            response.status.code = StatusCode::ALREADY_PRESENT;
            response.status.errorMessage = "Table " + table_id + " has already existed in " + request.namespaceName;
            return response;
        } else {
            // new table
            schema_version = request.schema.version();
            CHECK(schema_version == 0) << "Schema version was not initialized to be zero";
            schema_version++;
            // generate a string format table id based database object oid and table oid
            table_id = GetPgsqlTableId(request.namespaceOid, request.tableOid);
        }
        Schema table_schema = std::move(request.schema);
        table_schema.set_version(schema_version);
        std::shared_ptr<TableInfo> new_table_info = std::make_shared<TableInfo>(namespace_info->GetNamespaceId(), request.namespaceName,
                table_id, request.tableName, table_schema);
        new_table_info->set_pg_oid(request.tableOid);
        new_table_info->set_is_sys_table(request.isSysCatalogTable);
        new_table_info->set_next_column_id(table_schema.max_col_id() + 1);

        // TODO: add logic for shared table
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        K2DEBUG("Create or update table id: " << table_id << ", name: " << request.tableName);
        CreateUpdateTableResult result = table_info_handler_->CreateOrUpdateTable(context, new_table_info->namespace_id(), new_table_info);
        if (result.status.IsSucceeded()) {
            // commit transaction
            context->Commit();
            K2DEBUG("Created table " << new_table_info->table_id() << " with schema version " << schema_version << " successfully");
            // update table caches
            UpdateTableCache(new_table_info);
            // increase catalog version
            IncreaseCatalogVersion();
            // return response
            response.status.Succeed();
            response.tableInfo = new_table_info;
        } else {
            // abort the transaction
            context->Abort();
            K2ERROR("Failed to create table " << new_table_info->table_id() << " due to " << result.status.errorMessage);
            response.status = std::move(result.status);
        }

        return response;
    }

    CreateIndexTableResponse SqlCatalogManager::CreateIndexTable(const CreateIndexTableRequest& request) {
        CreateIndexTableResponse response;
        K2DEBUG("Creating index ns name: " << request.namespaceName << ", ns oid: " << request.namespaceOid
            << ", index name: " << request.tableName << ", index oid: " << request.tableOid
            << ", base table oid: " << request.baseTableOid);
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceByName(request.namespaceName);
        if (namespace_info == nullptr) {
            K2ERROR("Cannot find namespace " << request.namespaceName);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + request.namespaceName;
            return response;
        }
        // generate table id from namespace oid and table oid
        std::string base_table_id = GetPgsqlTableId(request.namespaceOid, request.baseTableOid);
        std::string index_table_id = GetPgsqlTableId(request.namespaceOid, request.tableOid);

        // check if the base table exists or not
        std::shared_ptr<TableInfo> base_table_info = GetCachedTableInfoById(base_table_id);
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        // try to fetch the table from SKV if not found
        if (base_table_info == nullptr) {
            GetTableResult table_result = table_info_handler_->GetTable(context, namespace_info->GetNamespaceId(), namespace_info->GetNamespaceName(),
                base_table_id);
            if (table_result.status.IsSucceeded() && table_result.tableInfo != nullptr) {
                // update table cache
                UpdateTableCache(table_result.tableInfo);
                base_table_info = table_result.tableInfo;
            }
        }

        if (base_table_info == nullptr) {
            // cannot find the base table
            K2ERROR("Cannot find base table " << base_table_id << " for index " << request.tableName);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find base table " + base_table_id + " for index " + request.tableName;
            return response;
        }

        bool need_create_index = false;
        if (base_table_info->has_secondary_indexes()) {
            const IndexMap& index_map = base_table_info->secondary_indexes();
            const auto itr = index_map.find(index_table_id);
            // the index has already been defined
            if (itr != index_map.end()) {
                // return if 'create .. if not exist' clause is specified
                if (request.isNotExist) {
                    const IndexInfo& index_info = itr->second;
                    response.indexInfo = std::make_shared<IndexInfo>(index_info);
                    response.status.Succeed();
                    return response;
                } else {
                    // BUGBUG: change to alter index instead of recreating one here
                    need_create_index = true;
                }
            } else {
                need_create_index = true;
            }
        } else {
            need_create_index = true;
        }

        if (need_create_index) {
            try {
                // use default index permission, could be customized by user/api
                IndexInfo new_index_info = BuildIndexInfo(base_table_info, index_table_id, request.tableName, request.tableOid,
                    request.schema, request.isUnique, IndexPermissions::INDEX_PERM_READ_WRITE_AND_DELETE);

                K2DEBUG("Persisting index table id: " << new_index_info.table_id() << ", name: " << new_index_info.table_name());
                // persist the index table metadata to the system catalog SKV tables
                table_info_handler_->PersistIndexTable(context, namespace_info->GetNamespaceId(), base_table_info, new_index_info);

                K2DEBUG("Persisting index SKV schema id: " << new_index_info.table_id() << ", name: " << new_index_info.table_name());
                // create a SKV schema to insert the actual index data
                table_info_handler_->CreateOrUpdateIndexSKVSchema(context, namespace_info->GetNamespaceId(), base_table_info, new_index_info);

                // update the base table with the new index
                base_table_info->add_secondary_index(index_table_id, new_index_info);

                K2DEBUG("Updating cache for table id: " << new_index_info.table_id() << ", name: " << new_index_info.table_name());
                // update table cache
                UpdateTableCache(base_table_info);

                // update index cache
                std::shared_ptr<IndexInfo> new_index_info_ptr = std::make_shared<IndexInfo>(new_index_info);
                AddIndexCache(new_index_info_ptr);

                // increase catalog version
                IncreaseCatalogVersion();

                if (!request.skipIndexBackfill) {
                    // TODO: add logic to backfill the index
                }
                response.indexInfo = new_index_info_ptr;
                response.status.Succeed();
                context->Commit();
                K2DEBUG("Created index id: " << new_index_info.table_id() << ", name: " << request.tableName << " successfully");
            } catch (const std::exception& e) {
                context->Abort();
                response.status.code = StatusCode::RUNTIME_ERROR;
                response.status.errorMessage = e.what();
                K2ERROR("Failed to create index " << request.tableName << " due to " << response.status.errorMessage);
           }
        } else {
            context->Commit();
        }
        return response;
    }

    GetTableSchemaResponse SqlCatalogManager::GetTableSchema(const GetTableSchemaRequest& request) {
        K2DEBUG("Get table schema ns oid: " << request.namespaceOid << ", table oid: " << request.tableOid);
        GetTableSchemaResponse response;
        // generate table id from namespace oid and table oid
        std::string table_id = GetPgsqlTableId(request.namespaceOid, request.tableOid);
        // check the table schema from cache
        std::shared_ptr<TableInfo> table_info = GetCachedTableInfoById(table_id);
        if (table_info != nullptr) {
            response.tableInfo = table_info;
            response.status.Succeed();
            return response;
        }

        std::string namespace_id = GetPgsqlNamespaceId(request.namespaceOid);
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceById(namespace_id);
        if (namespace_info == nullptr) {
            K2ERROR("Cannot find namespace " << namespace_id);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + namespace_id;
            return response;
        }

        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        // fetch the table from SKV
        GetTableResult table_result = table_info_handler_->GetTable(context, namespace_info->GetNamespaceId(), namespace_info->GetNamespaceName(),
                table_id);
        if (!table_result.status.IsSucceeded()) {
            context->Abort();
            response.status = std::move(table_result.status);
            response.tableInfo = nullptr;
            return response;
        }

        if (table_result.tableInfo == nullptr) {
            context->Abort();
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find table " + table_id;
            response.tableInfo = nullptr;
            return response;
        }

        context->Commit();
        response.status.Succeed();
        response.tableInfo = table_result.tableInfo;
        // update table cache
        UpdateTableCache(table_result.tableInfo);
        K2DEBUG("Returned schema for table name: " << response.tableInfo->table_name() << ", id: " << response.tableInfo->table_id() << " successfully");
        return response;
    }

    ListTablesResponse SqlCatalogManager::ListTables(const ListTablesRequest& request) {
        K2DEBUG("Listing tables for namespace " << request.namespaceName);
        ListTablesResponse response;
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceByName(request.namespaceName);
        if (namespace_info == nullptr) {
            K2ERROR("Cannot find namespace " << request.namespaceName);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + request.namespaceName;
            return response;
        }
        response.namespaceId = namespace_info->GetNamespaceId();

        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        ListTablesResult tables_result = table_info_handler_->ListTables(context, namespace_info->GetNamespaceId(),
                namespace_info->GetNamespaceName(), request.isSysTableIncluded);
        if (!tables_result.status.IsSucceeded()) {
            context->Abort();
            response.status = std::move(tables_result.status);
            return response;
        }

        context->Commit();
        for (auto& tableInfo : tables_result.tableInfos) {
            response.tableInfos.push_back(std::move(tableInfo));
        }
        response.status.Succeed();
        K2DEBUG("Found " << response.tableInfos.size() << " tables in namespace " << request.namespaceName);
        return response;
    }

    DeleteTableResponse SqlCatalogManager::DeleteTable(const DeleteTableRequest& request) {
        K2DEBUG("Deleting table " << request.tableOid << " in namespace " << request.namespaceOid);
        DeleteTableResponse response;
        std::string namespace_id = GetPgsqlNamespaceId(request.namespaceOid);
        std::string table_id = GetPgsqlTableId(request.namespaceOid, request.tableOid);
        response.namespaceId = namespace_id;
        response.tableId = table_id;

        std::shared_ptr<TableInfo> table_info = GetCachedTableInfoById(table_id);
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        if (table_info == nullptr) {
            // try to find table from SKV by looking at namespace first
            std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceById(namespace_id);
            if (namespace_info == nullptr) {
                K2ERROR("Cannot find namespace " << namespace_id);
                response.status.code = StatusCode::NOT_FOUND;
                response.status.errorMessage = "Cannot find namespace " + namespace_id;
                return response;
            }

            // fetch the table from SKV
            GetTableResult table_result = table_info_handler_->GetTable(context, namespace_info->GetNamespaceId(), namespace_info->GetNamespaceName(),
                table_id);
            if (!table_result.status.IsSucceeded()) {
                context->Abort();
                response.status = std::move(table_result.status);
                return response;
            }

            if (table_result.tableInfo == nullptr) {
                context->Abort();
                response.status.code = StatusCode::NOT_FOUND;
                response.status.errorMessage = "Cannot find table " + table_id;
                return response;
            }

            table_info = table_result.tableInfo;
        }

        // delete indexes and the table itself
        // delete table data
         DeleteTableResult delete_data_result = table_info_handler_->DeleteTableData(context, namespace_id, table_info);
        if (!delete_data_result.status.IsSucceeded()) {
            context->Abort();
            response.status = std::move(delete_data_result.status);
            return response;
        }

        // delete table schema metadata
        DeleteTableResult delete_metadata_result = table_info_handler_->DeleteTableMetadata(context, namespace_id, table_info);
        if (!delete_metadata_result.status.IsSucceeded()) {
            context->Abort();
            response.status = std::move(delete_metadata_result.status);
            return response;
        }

        context->Commit();
        // clear table cache after table deletion
        ClearTableCache(table_info);
        response.status.Succeed();
        return response;
    }

    DeleteIndexResponse SqlCatalogManager::DeleteIndex(const DeleteIndexRequest& request) {
        K2DEBUG("Deleting index " << request.tableOid << " in namespace " << request.namespaceOid);
        DeleteIndexResponse response;
        std::string namespace_id = GetPgsqlNamespaceId(request.namespaceOid);
        std::string table_id = GetPgsqlTableId(request.namespaceOid, request.tableOid);
        response.namespaceId = namespace_id;
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceById(namespace_id);
        if (namespace_info == nullptr) {
            K2ERROR("Cannot find namespace " << namespace_id);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + namespace_id;
            return response;
        }

        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        std::shared_ptr<IndexInfo> index_info = GetCachedIndexInfoById(table_id);
        std::string base_table_id;
        if (index_info == nullptr) {
            GeBaseTableIdResult index_result = table_info_handler_->GeBaseTableId(context, namespace_id, table_id);
            if (!index_result.status.IsSucceeded()) {
                response.status = std::move(index_result.status);
                context->Abort();
                return response;
            }
            base_table_id = index_result.baseTableId;
        } else {
            base_table_id = index_info->indexed_table_id();
        }

        std::shared_ptr<TableInfo> base_table_info = GetCachedTableInfoById(base_table_id);
        // try to fetch the table from SKV if not found
        if (base_table_info == nullptr) {
            GetTableResult table_result = table_info_handler_->GetTable(context, namespace_id, namespace_info->GetNamespaceName(),
                    base_table_id);
            if (!table_result.status.IsSucceeded()) {
                context->Abort();
                response.status = std::move(table_result.status);
                return response;
            }

            if (table_result.tableInfo == nullptr) {
                context->Abort();
                response.status.code = StatusCode::NOT_FOUND;
                response.status.errorMessage = "Base table " + base_table_id + " cannot be found";
                return response;
            }

            base_table_info = table_result.tableInfo;
        }

        // delete index data
        DeleteIndexResult delete_data_result = table_info_handler_->DeleteIndexData(context, namespace_id, table_id);
        if (!delete_data_result.status.IsSucceeded()) {
            context->Abort();
            response.status = std::move(delete_data_result.status);
            return response;
        }

        // delete index metadata
        DeleteIndexResult delete_metadata_result = table_info_handler_->DeleteIndexMetadata(context, namespace_id, table_id);
        if (!delete_metadata_result.status.IsSucceeded()) {
            context->Abort();
            response.status = std::move(delete_metadata_result.status);
            return response;
        }

        context->Commit();
        // remove index from the table_info object
        base_table_info->drop_index(table_id);
        // update table cache with the index removed, index cache is updated accordingly
        UpdateTableCache(base_table_info);
        response.baseIndexTableOid = base_table_info->pg_oid();
        response.status.Succeed();
        return response;
    }

    ReservePgOidsResponse SqlCatalogManager::ReservePgOid(const ReservePgOidsRequest& request) {
        ReservePgOidsResponse response;
        K2DEBUG("Reserving PgOid with nextOid: " << request.nextOid << ", count: " << request.count << " for namespace: " << request.namespaceId);
        std::shared_ptr<SessionTransactionContext> ns_context = NewTransactionContext();
        GetNamespaceResult result = namespace_info_handler_->GetNamespace(ns_context, request.namespaceId);
        if (!result.status.IsSucceeded()) {
            ns_context->Abort();
            K2ERROR("Failed to get namespace " << request.namespaceId);
            response.status = std::move(result.status);
            return response;
        }
        if (result.namespaceInfo == nullptr) {
            ns_context->Abort();
            K2ERROR("Namespace " << request.namespaceId << " is empty");
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + request.namespaceId;
            return response;
        }
        uint32_t begin_oid = result.namespaceInfo->GetNextPgOid();
        if (begin_oid < request.nextOid) {
            begin_oid = request.nextOid;
        }
        if (begin_oid == std::numeric_limits<uint32_t>::max()) {
            ns_context->Abort();
            K2WARN("No more object identifier is available for Postgres database " << request.namespaceId);
            response.status.code = StatusCode::INVALID_ARGUMENT;
            response.status.errorMessage = "No more object identifier is available for " + request.namespaceId;
            return response;
        }

        uint32_t end_oid = begin_oid + request.count;
        if (end_oid < begin_oid) {
            end_oid = std::numeric_limits<uint32_t>::max(); // Handle wraparound.
        }
        response.namespaceId = request.namespaceId;
        response.beginOid = begin_oid;
        response.endOid = end_oid;

        // update the namespace record on SKV
        // We use read and write in the same transaction so that K23SI guarantees that concurrent SKV records on SKV
        // won't override each other and won't lose the correctness of PgNextOid
        std::shared_ptr<NamespaceInfo> updated_ns = std::move(result.namespaceInfo);
        updated_ns->SetNextPgOid(end_oid);
        K2DEBUG("Updating nextPgOid on SKV to " << end_oid << " for namespace " << request.namespaceId);
        AddOrUpdateNamespaceResult update_result = namespace_info_handler_->AddOrUpdateNamespace(ns_context, updated_ns);
        if (!update_result.status.IsSucceeded()) {
            ns_context->Abort();
            K2ERROR("Failed to update nextPgOid on SKV due to " << update_result.status.errorMessage);
            response.status = std::move(update_result.status);
            return response;
        }

        ns_context->Commit();
        K2DEBUG("Reserved PgOid succeeded for namespace " << request.namespaceId);
        // update namespace caches after persisting to SKV successfully
        namespace_id_map_[updated_ns->GetNamespaceId()] = updated_ns;
        namespace_name_map_[updated_ns->GetNamespaceName()] = updated_ns;
        response.status.Succeed();
        return response;
    }

    RStatus SqlCatalogManager::UpdateCatalogVersion(std::shared_ptr<SessionTransactionContext> context, uint64_t new_version) {
        std::lock_guard<std::mutex> l(lock_);
        // compare new_version with the local version
        uint64_t local_catalog_version = catalog_version_.load(std::memory_order_acquire);
        K2DEBUG("Local catalog version: " << local_catalog_version << ". new version: " << new_version);
        if (new_version < local_catalog_version) {
            K2DEBUG("Catalog version update: version on SKV is too old."
                        << " New: " << new_version << ", Old: " << local_catalog_version);
            ClusterInfo cluster_info(cluster_id_, init_db_done_, local_catalog_version);
            K2DEBUG("Updating catalog version on SKV to " << new_version);
            UpdateClusterInfoResult result = cluster_info_handler_->UpdateClusterInfo(context, cluster_info);
            if (!result.status.IsSucceeded()) {
                K2ERROR("ClusterInfo update failed due to error code " << result.status.code << " and message "
                    << result.status.errorMessage);
                return result.status;
            }
        } else if (new_version > local_catalog_version) {
            K2DEBUG("Updating local catalog version to " << new_version);
            catalog_version_.store(new_version, std::memory_order_release);
        }
        return StatusOK;
    }

    // update namespace caches
    void SqlCatalogManager::UpdateNamespaceCache(std::vector<std::shared_ptr<NamespaceInfo>> namespace_infos) {
        std::lock_guard<std::mutex> l(lock_);
        namespace_id_map_.clear();
        namespace_name_map_.clear();
        for (auto ns_ptr : namespace_infos) {
            namespace_id_map_[ns_ptr->GetNamespaceId()] = ns_ptr;
            namespace_name_map_[ns_ptr->GetNamespaceName()] = ns_ptr;
        }
    }

    // update table caches
    void SqlCatalogManager::UpdateTableCache(std::shared_ptr<TableInfo> table_info) {
        std::lock_guard<std::mutex> l(lock_);
        table_id_map_[table_info->table_id()] = table_info;
        // TODO: add logic to remove table with old name if rename table is called
        TableNameKey key = std::make_pair(table_info->namespace_id(), table_info->table_name());
        table_name_map_[key] = table_info;
        // update the corresponding index cache
        UpdateIndexCacheForTable(table_info);
    }

    // remove table info from table cache and its related indexes from index cache
    void SqlCatalogManager::ClearTableCache(std::shared_ptr<TableInfo> table_info) {
        std::lock_guard<std::mutex> l(lock_);
        ClearIndexCacheForTable(table_info->table_id());
        table_id_map_.erase(table_info->table_id());
        TableNameKey key = std::make_pair(table_info->namespace_id(), table_info->table_name());
        table_name_map_.erase(key);
    }

    // clear index infos for a table in the index cache
    void SqlCatalogManager::ClearIndexCacheForTable(std::string table_id) {
        std::vector<std::string> index_ids;
        for (std::pair<std::string, std::shared_ptr<IndexInfo>> pair : index_id_map_) {
            // first find all indexes that belong to the table
            if (table_id == pair.second->indexed_table_id()) {
                index_ids.push_back(pair.first);
            }
        }
        // delete the indexes in cache
        for (std::string index_id : index_ids) {
            index_id_map_.erase(index_id);
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
        index_id_map_[index_info->table_id()] = index_info;
    }

    std::shared_ptr<NamespaceInfo> SqlCatalogManager::GetCachedNamespaceById(std::string namespace_id) {
        if (!namespace_id_map_.empty()) {
            const auto itr = namespace_id_map_.find(namespace_id);
            if (itr != namespace_id_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<NamespaceInfo> SqlCatalogManager::GetCachedNamespaceByName(std::string namespace_name) {
        if (!namespace_name_map_.empty()) {
            const auto itr = namespace_name_map_.find(namespace_name);
            if (itr != namespace_name_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<TableInfo> SqlCatalogManager::GetCachedTableInfoById(std::string table_id) {
        if (!table_id_map_.empty()) {
            const auto itr = table_id_map_.find(table_id);
            if (itr != table_id_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<TableInfo> SqlCatalogManager::GetCachedTableInfoByName(std::string namespace_id, std::string table_name) {
        if (!table_id_map_.empty()) {
           TableNameKey key = std::make_pair(namespace_id, table_name);
           const auto itr = table_name_map_.find(key);
            if (itr != table_name_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<IndexInfo> SqlCatalogManager::GetCachedIndexInfoById(std::string index_id) {
        if (!index_id_map_.empty()) {
            const auto itr = index_id_map_.find(index_id);
            if (itr != index_id_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<SessionTransactionContext> SqlCatalogManager::NewTransactionContext() {
        std::future<K23SITxn> txn_future = k2_adapter_->beginTransaction();
        std::shared_ptr<K23SITxn> txn = std::make_shared<K23SITxn>(txn_future.get());
        std::shared_ptr<SessionTransactionContext> context = std::make_shared<SessionTransactionContext>(txn);
        return context;
    }

    void SqlCatalogManager::IncreaseCatalogVersion() {
        catalog_version_++;
        // need to update the catalog version on SKV
        // the update frequency could be reduced once we have a single or a quorum of catalog managers
        ClusterInfo cluster_info(cluster_id_, init_db_done_, catalog_version_);
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        cluster_info_handler_->UpdateClusterInfo(context, cluster_info);
        context->Commit();
    }

    IndexInfo SqlCatalogManager::BuildIndexInfo(std::shared_ptr<TableInfo> base_table_info, std::string index_id, std::string index_name, uint32_t pg_oid,
            const Schema& index_schema, bool is_unique, IndexPermissions index_permissions) {
        std::vector<IndexColumn> columns;
        for (ColumnId col_id: index_schema.column_ids()) {
            int col_idx = index_schema.find_column_by_id(col_id);
            if (col_idx == Schema::kColumnNotFound) {
                throw std::runtime_error("Cannot find column with id " + col_id);
            }
            const ColumnSchema& col_schema = index_schema.column(col_idx);
            if (col_schema.name().compare("ybuniqueidxkeysuffix") == 0 || col_schema.name().compare("ybidxbasectid") == 0) {
                // skip checking "ybuniqueidxkeysuffix" and "ybidxbasectid" on base table, which only exist on index table
                IndexColumn col(col_id, col_schema.name(),  -1);
                columns.push_back(col);
            } else {
                std::pair<bool, ColumnId> pair = base_table_info->schema().FindColumnIdByName(col_schema.name());
                if (!pair.first) {
                    throw std::runtime_error("Cannot find column id in base table with name " + col_schema.name());
                }
                ColumnId indexed_column_id = pair.second;
                IndexColumn col(col_id, col_schema.name(),  indexed_column_id);
                columns.push_back(col);
            }
        }
        IndexInfo index_info(index_id, index_name, pg_oid, base_table_info->table_id(), index_schema.version(),
                is_unique, columns, index_permissions);
        return index_info;
    }

    std::shared_ptr<NamespaceInfo> SqlCatalogManager::CheckAndLoadNamespaceByName(const std::string& namespace_name) {
        std::shared_ptr<NamespaceInfo> namespace_info = GetCachedNamespaceByName(namespace_name);
        if (namespace_info == nullptr) {
            // try to refresh namespaces from SKV in case that the requested namespace is created by another catalog manager instance
            // this could be avoided by use a single or a quorum of catalog managers
            std::shared_ptr<SessionTransactionContext> ns_context = NewTransactionContext();
            ListNamespacesResult result = namespace_info_handler_->ListNamespaces(ns_context);
            ns_context->Commit();
            if (result.status.IsSucceeded() && !result.namespaceInfos.empty()) {
                // update namespace caches
                UpdateNamespaceCache(result.namespaceInfos);
                // recheck namespace
                namespace_info = GetCachedNamespaceByName(namespace_name);
            }
        }
        return namespace_info;
    }

    std::shared_ptr<NamespaceInfo> SqlCatalogManager::CheckAndLoadNamespaceById(const std::string& namespace_id) {
        std::shared_ptr<NamespaceInfo> namespace_info = GetCachedNamespaceById(namespace_id);
        if (namespace_info == nullptr) {
            // try to refresh namespaces from SKV in case that the requested namespace is created by another catalog manager instance
            // this could be avoided by use a single or a quorum of catalog managers
            std::shared_ptr<SessionTransactionContext> ns_context = NewTransactionContext();
            ListNamespacesResult result = namespace_info_handler_->ListNamespaces(ns_context);
            ns_context->Commit();
            if (result.status.IsSucceeded() && !result.namespaceInfos.empty()) {
                // update namespace caches
                UpdateNamespaceCache(result.namespaceInfos);
                // recheck namespace
                namespace_info = GetCachedNamespaceById(namespace_id);
            }
        }
        return namespace_info;
    }

} // namespace catalog
}  // namespace sql
}  // namespace k2pg




