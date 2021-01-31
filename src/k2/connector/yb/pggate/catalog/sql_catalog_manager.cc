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
        K2LOG_D(log::catalog, "Starting Catalog Manager...");
        CHECK(!initted_.load(std::memory_order_acquire));

        std::shared_ptr<SessionTransactionContext> ci_context = NewTransactionContext();
        // load cluster info
        GetClusterInfoResult ciresp = cluster_info_handler_->ReadClusterInfo(ci_context, cluster_id_);
        if (ciresp.status.IsSucceeded()) {
            if (ciresp.clusterInfo != nullptr) {
                init_db_done_.store(ciresp.clusterInfo->IsInitdbDone(), std::memory_order_relaxed);
                catalog_version_.store(ciresp.clusterInfo->GetCatalogVersion(), std::memory_order_relaxed);
                K2LOG_D(log::catalog, "Loaded cluster info record succeeded");
            } else {
                ci_context->Abort();  // no difference either abort or commit
                K2LOG_W(log::catalog, "Empty cluster info record, likely primary cluster is not initialized. Only operation allowed is primary cluster initialization");
                // it is ok, but only InitPrimaryCluster can be executed on the SqlCatalogrMager
                // keep initted_ to be false;
                return Status::OK();
            }
        } else {
            K2LOG_E(log::catalog, "Failed to read cluster info record due to {}", ciresp.status);
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
                K2LOG_D(log::catalog, "namespaces are empty");
            }
        } else {
            K2LOG_E(log::catalog, "Failed to load namespaces due to {}", nsresp.status);
            return STATUS_FORMAT(IOError, "Failed to load namespaces due to error code $0 and message $1",
                nsresp.status.code, nsresp.status.errorMessage);
        }

        initted_.store(true, std::memory_order_release);
        K2LOG_D(log::catalog, "Catalog Manager started up successfully");
        return Status::OK();
    }

    void SqlCatalogManager::Shutdown() {
        K2LOG_D(log::catalog, "SQL CatalogManager shutting down...");

        bool expected = true;
        if (initted_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            // TODO: add shut down steps

        }

        K2LOG_D(log::catalog, "SQL CatalogManager shut down complete. Bye!");
    }

    // Called only once during PG initDB
    // TODO: handle partial failure(maybe simply fully cleanup) to allow retry later
    Status SqlCatalogManager::InitPrimaryCluster()
    {
        K2LOG_D(log::catalog, "SQL CatalogManager initialize primary Cluster!");

        CHECK(!initted_.load(std::memory_order_relaxed));

        // step 1/4 create the SKV collection for the primary
        RStatus rs = namespace_info_handler_->CreateSKVCollection(CatalogConsts::skv_collection_name_sql_primary, CatalogConsts::default_cluster_id);
        if (!rs.IsSucceeded())
        {
            K2LOG_E(log::catalog, "Failed to create SKV collection during initialization primary PG cluster due to {}", rs);
            return STATUS_FORMAT(IOError, "Failed to create SKV collection during initialization primary PG cluster due to error code $0 and message $1",
                    rs.code, rs.errorMessage);
        }

        std::shared_ptr<SessionTransactionContext> init_context = NewTransactionContext();

        // step 2/4 Init Cluster info, including create the SKVSchema in the primary cluster's SKVCollection for cluster_info and insert current cluster info into
        //      Note: Initialize cluster info's init_db column with TRUE
        ClusterInfo cluster_info(cluster_id_, catalog_version_, false /*init_db_done*/);

        InitClusterInfoResult initCIRes = cluster_info_handler_->InitClusterInfo(init_context, cluster_info);
        if (initCIRes.status.IsSucceeded()) {
            K2LOG_D(log::catalog, "Initialization of cluster info succeeded");
        } else {
            init_context->Abort();
                K2LOG_E(log::catalog, "Failed to initialize cluster info due to {}", initCIRes.status);
                return STATUS_FORMAT(IOError, "Failed to create cluster info record to error code $0 and message $1",
                    initCIRes.status.code, initCIRes.status.errorMessage);
        }

        // step 3/4 Init namespace_info - create the SKVSchema in the primary cluster's SKVcollection for namespace_info
        InitNamespaceTableResult initRes = namespace_info_handler_->InitNamespaceTable();
        if (!initRes.status.IsSucceeded()) {
            K2LOG_E(log::catalog, "Failed to initialize creating namespace table due to {}", initRes.status);
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
            K2LOG_D(log::catalog, "SQL CatalogManager successfully initialized primary Cluster!");
        }
        else
        {
            K2LOG_E(log::catalog, "Failed to create SKV collection during initialization primary PG cluster due to {}", status.ToUserMessage());
        }

        return status;
    }

    Status SqlCatalogManager::FinishInitDB()
    {
        K2LOG_D(log::catalog, "Setting initDbDone to be true...");
        if (!init_db_done_) {
            std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
            // check the latest cluster info on SKV
            GetClusterInfoResult result = cluster_info_handler_->ReadClusterInfo(context, cluster_id_);
            if (!result.status.IsSucceeded()) {
                context->Abort();
                K2LOG_E(log::catalog, "Cannot read cluster info record on SKV due to {}", result.status.errorMessage);
                return STATUS_FORMAT(IOError, "Cannot read cluster info record on SKV due to error code $0 and message $1",
                    result.status.code, result.status.errorMessage);
            }
            if (result.clusterInfo == nullptr) {
                context->Abort();
                K2LOG_E(log::catalog, "Cluster info record is empty on SKV");
                return STATUS(IOError, "Cluster info record is empty on SKV");
            }

            if (result.clusterInfo->IsInitdbDone()) {
                context->Commit();
                init_db_done_.store(true, std::memory_order_relaxed);
                K2LOG_D(log::catalog, "InitDbDone is already true on SKV");
                return Status::OK();
            }

            K2LOG_D(log::catalog, "Updating cluster info with initDbDone to be true");
            std::shared_ptr<ClusterInfo> new_cluster_info = result.clusterInfo;
            new_cluster_info->SetInitdbDone(true);
            UpdateClusterInfoResult update_result = cluster_info_handler_->UpdateClusterInfo(context, *new_cluster_info.get());
            if (!update_result.status.IsSucceeded()) {
                context->Abort();
                K2LOG_E(log::catalog, "Failed to update cluster info due to {}", update_result.status);
                return STATUS_FORMAT(IOError, "Failed to update cluster info due to error code $0 and message $1",
                    update_result.status.code, update_result.status.errorMessage);
            }
            context->Commit();
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
            std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
            GetClusterInfoResult result = cluster_info_handler_->ReadClusterInfo(context, cluster_id_);
            context->Commit();
            if (result.status.IsSucceeded() && result.clusterInfo != nullptr) {
                K2LOG_D(log::catalog, "Checked IsInitDbDone from SKV {}", result.clusterInfo->IsInitdbDone());
                if (result.clusterInfo->IsInitdbDone()) {
                    init_db_done_.store(result.clusterInfo->IsInitdbDone(), std::memory_order_relaxed);
                }
                if (result.clusterInfo->GetCatalogVersion() > catalog_version_) {
                    catalog_version_.store(result.clusterInfo->GetCatalogVersion(), std::memory_order_relaxed);
                }
            } else {
                K2LOG_E(log::catalog, "Failed to check IsInitDbDone from SKV due to {}", result.status);
                response.status = std::move(result.status);
                return response;
            }
        }
        K2LOG_D(log::catalog, "Get InitDBDone successfully {}", init_db_done_);
        response.isInitDbDone = init_db_done_;
        response.status.Succeed();
        return response;
    }

    GetCatalogVersionResponse SqlCatalogManager::GetCatalogVersion(const GetCatalogVersionRequest& request) {
        GetCatalogVersionResponse response;
        K2LOG_D(log::catalog, "Checking catalog version...");
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        // TODO: use a background thread to fetch the ClusterInfo record periodically instead of fetching it for each call
        GetClusterInfoResult result = cluster_info_handler_->ReadClusterInfo(context, cluster_id_);
        if (!result.status.IsSucceeded()) {
            K2LOG_E(log::catalog, "Failed to check cluster info due to {}", result.status);
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
            K2LOG_E(log::catalog, "Failed to update catalog version due to {}", status);
            response.status = std::move(status);
            return response;
        }
        context->Commit();
        K2LOG_D(log::catalog, "Returned catalog version {}", catalog_version_);
        response.catalogVersion = catalog_version_;
        response.status.Succeed();
        return response;
    }

    CreateNamespaceResponse SqlCatalogManager::CreateNamespace(const CreateNamespaceRequest& request) {
        CreateNamespaceResponse response;
        K2LOG_D(log::catalog,
            "Creating namespace with name: {}, id: {}, oid: {}, source_id: {}, nextPgOid: {}",
            request.namespaceName, request.namespaceId, request.namespaceOid, request.sourceNamespaceId, request.nextPgOid.value_or(-1));
        // step 1/3:  check input conditions
        //      check if the target namespace has already been created, if yes, return already present
        //      check the source namespace is already there, if it present in the create requet
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceByName(request.namespaceName);
        if (namespace_info != nullptr) {
            K2LOG_E(log::catalog, "Namespace {} has already existed", request.namespaceName);
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
                K2LOG_E(log::catalog, "Failed to find source namespaces {}", request.sourceNamespaceId);
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
        K2LOG_D(log::catalog, "Creating SKV collection for namespace {}", request.namespaceId);
        response.status = namespace_info_handler_->CreateSKVCollection(request.namespaceId, request.namespaceName);
        if (!response.status.IsSucceeded())
        {
            K2LOG_E(log::catalog, "Failed to create SKV collection {}", request.namespaceId);
            return response;
        }

        // step 2.2 Add new namespace(database) entry into default cluster Namespace table and update in-memory cache
        std::shared_ptr<NamespaceInfo> new_ns = std::make_shared<NamespaceInfo>();
        new_ns->SetNamespaceId(request.namespaceId);
        new_ns->SetNamespaceName(request.namespaceName);
        new_ns->SetNamespaceOid(request.namespaceOid);
        new_ns->SetNextPgOid(t_nextPgOid);
        // persist the new namespace record
        K2LOG_D(log::catalog, "Adding namespace {} on SKV", request.namespaceId);
        std::shared_ptr<SessionTransactionContext> ns_context = NewTransactionContext();
        AddOrUpdateNamespaceResult add_result = namespace_info_handler_->AddOrUpdateNamespace(ns_context, new_ns);
        if (!add_result.status.IsSucceeded()) {
            K2LOG_E(log::catalog, "Failed to add namespace {}, due to {}", request.namespaceId,add_result.status);
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
        K2LOG_D(log::catalog, "Creating system tables for target namespace {}", new_ns->GetNamespaceId());
        CreateSysTablesResult table_result = table_info_handler_->CheckAndCreateSystemTables(target_context, new_ns->GetNamespaceId());
        if (!table_result.status.IsSucceeded()) {
            K2LOG_E(log::catalog, "Failed to create system tables for target namespace {} due to {}",
                new_ns->GetNamespaceId(), table_result.status.errorMessage);
            target_context->Abort();
            ns_context->Abort();
            response.status = std::move(table_result.status);
            return response;
        }

        // step 3/3: If source namespace(database) is present in the request, copy all the rest of tables from source namespace(database)
        if (!request.sourceNamespaceId.empty())
        {
            K2LOG_D(log::catalog, "Creating namespace from source namespace {}", request.sourceNamespaceId);
            std::shared_ptr<SessionTransactionContext> source_context = NewTransactionContext();
            // get the source table ids
            K2LOG_D(log::catalog, "Listing table ids from source namespace {}", request.sourceNamespaceId);
            ListTableIdsResult list_table_result = table_info_handler_->ListTableIds(source_context, source_namespace_info->GetNamespaceId(), true);
            if (!list_table_result.status.IsSucceeded()) {
                K2LOG_E(log::catalog, "Failed to list table ids for namespace {} due to {}", source_namespace_info->GetNamespaceId(), list_table_result.status.errorMessage);
                source_context->Abort();
                target_context->Abort();
                ns_context->Abort();
                response.status = std::move(list_table_result.status);
                return response;
            }
            K2LOG_D(log::catalog, "Found {} table ids from source namespace {}", list_table_result.tableIds.size(), request.sourceNamespaceId);
            int num_index = 0;
            for (auto& source_table_id : list_table_result.tableIds) {
                // copy the source table metadata to the target table
                K2LOG_D(log::catalog, "Copying from source table {}", source_table_id);
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
                    K2LOG_E(log::catalog, "Failed to copy from source table {} due to {}", source_table_id, copy_result.status.errorMessage);
                    source_context->Abort();
                    target_context->Abort();
                    ns_context->Abort();
                    response.status = std::move(copy_result.status);
                    return response;
                }
                num_index += copy_result.num_index;
            }
            source_context->Commit();
            K2LOG_D(log::catalog, "Finished copying {} tables and {} indexes from source namespace {} to {}",
                list_table_result.tableIds.size(), num_index, source_namespace_info->GetNamespaceId(), new_ns->GetNamespaceId());
        }

        target_context->Commit();
        ns_context->Commit();
        K2LOG_D(log::catalog, "Created namespace {}", new_ns->GetNamespaceId());
        response.status.Succeed();
        return response;
    }

    ListNamespacesResponse SqlCatalogManager::ListNamespaces(const ListNamespacesRequest& request) {
        ListNamespacesResponse response;
        K2LOG_D(log::catalog, "Listing namespaces...");
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        ListNamespacesResult result = namespace_info_handler_->ListNamespaces(context);
        if (!result.status.IsSucceeded()) {
            context->Abort();
            K2LOG_E(log::catalog, "Failed to list namespaces due to {}", result.status.errorMessage);
            response.status = std::move(result.status);
            return response;
        }
        context->Commit();
        if (result.namespaceInfos.empty()) {
            K2LOG_W(log::catalog, "No namespaces are found");
        } else {
            UpdateNamespaceCache(result.namespaceInfos);
            for (auto ns_ptr : result.namespaceInfos) {
                response.namespace_infos.push_back(ns_ptr);
            }
        }
        K2LOG_D(log::catalog, "Found {} namespaces", result.namespaceInfos.size());
        response.status.Succeed();
        return response;
    }

    GetNamespaceResponse SqlCatalogManager::GetNamespace(const GetNamespaceRequest& request) {
        GetNamespaceResponse response;
        K2LOG_D(log::catalog, "Getting namespace with name: {}, id: {}", request.namespaceName, request.namespaceId);
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        // TODO: use a background task to refresh the namespace caches to avoid fetching from SKV on each call
        GetNamespaceResult result = namespace_info_handler_->GetNamespace(context, request.namespaceId);
        if (!result.status.IsSucceeded()) {
            context->Abort();
            K2LOG_E(log::catalog, "Failed to get namespace {}, due to {}", request.namespaceId, result.status);
            response.status = std::move(result.status);
            return response;
        }
        if (result.namespaceInfo == nullptr) {
            context->Abort();
            K2LOG_W(log::catalog, "Cannot find namespace {}", request.namespaceId);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + request.namespaceId;
            return response;
        }
        context->Commit();
        response.namespace_info = result.namespaceInfo;

        // update namespace caches
        namespace_id_map_[response.namespace_info->GetNamespaceId()] = response.namespace_info ;
        namespace_name_map_[response.namespace_info->GetNamespaceName()] = response.namespace_info;
        K2LOG_D(log::catalog, "Found namespace {}", request.namespaceId);
        response.status.Succeed();
        return response;
    }

    DeleteNamespaceResponse SqlCatalogManager::DeleteNamespace(const DeleteNamespaceRequest& request) {
        DeleteNamespaceResponse response;
        K2LOG_D(log::catalog, "Deleting namespace with name: {}, id: {}", request.namespaceName, request.namespaceId);
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        // TODO: use a background task to refresh the namespace caches to avoid fetching from SKV on each call
        GetNamespaceResult result = namespace_info_handler_->GetNamespace(context, request.namespaceId);
        context->Commit();
        if (!result.status.IsSucceeded() || result.namespaceInfo == nullptr) {
            K2LOG_W(log::catalog, "Cannot find namespace {}", request.namespaceId);
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
        K2LOG_D(log::catalog,
        "Creating table ns name: {}, ns oid: {}, table name: {}, table oid: {}, systable: {}, shared: {}",
            request.namespaceName, request.namespaceOid, request.tableName, request.tableOid, request.isSysCatalogTable, request.isSharedTable);
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceByName(request.namespaceName);
        if (namespace_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find namespace {}", request.namespaceName);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + request.namespaceName;
            return response;
        }

        // check if the Table has already existed or not
        std::shared_ptr<TableInfo> table_info = GetCachedTableInfoByName(namespace_info->GetNamespaceId(), request.tableName);
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
            response.status.errorMessage = "Table " + request.tableName + " has already existed in " + request.namespaceName;
            K2LOG_E(log::catalog, "{}", response.status);
            return response;
        }

        // new table
        uint32_t schema_version = request.schema.version();
        CHECK(schema_version == 0) << "Schema version was not initialized to be zero";
        schema_version++;
        // generate a string format table id based database object oid and table oid
        std::string uuid = PgObjectId::GetTableUuid(request.namespaceOid, request.tableOid);
        Schema table_schema = std::move(request.schema);
        table_schema.set_version(schema_version);
        std::shared_ptr<TableInfo> new_table_info = std::make_shared<TableInfo>(namespace_info->GetNamespaceId(), request.namespaceName,
                request.tableOid, request.tableName, uuid, table_schema);
        new_table_info->set_is_sys_table(request.isSysCatalogTable);
        new_table_info->set_is_shared_table(request.isSharedTable);
        new_table_info->set_next_column_id(table_schema.max_col_id() + 1);

        // TODO: add logic for shared table
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        K2LOG_D(log::catalog, "Create or update table id: {}, name: {}, shared: {}", new_table_info->table_id(), request.tableName, request.isSharedTable);
        CreateUpdateTableResult result = table_info_handler_->CreateOrUpdateTable(context, namespace_info->GetNamespaceId(), new_table_info);
        if (result.status.IsSucceeded()) {
            // commit transaction
            context->Commit();
            K2LOG_D(log::catalog, "Created table {}, with schema {}", new_table_info->table_id(), schema_version);
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
            K2LOG_E(log::catalog, "Failed to create table {}, due to {}", new_table_info->table_id(), result.status);
            response.status = std::move(result.status);
        }

        return response;
    }

    CreateIndexTableResponse SqlCatalogManager::CreateIndexTable(const CreateIndexTableRequest& request) {
        CreateIndexTableResponse response;
        K2LOG_D(log::catalog, "Creating index ns name: {}, ns oid: {}, index name: {}, index oid: {}, base table oid: {}",
            request.namespaceName, request.namespaceOid, request.tableName, request.tableOid, request.baseTableOid);
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceByName(request.namespaceName);
        if (namespace_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find namespace {}", request.namespaceName);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + request.namespaceName;
            return response;
        }
        // generate table uuid from namespace oid and table oid
        std::string base_table_uuid = PgObjectId::GetTableUuid(request.namespaceOid, request.baseTableOid);
        std::string index_table_uuid = PgObjectId::GetTableUuid(request.namespaceOid, request.tableOid);

        std::string base_table_id = PgObjectId::GetTableId(request.baseTableOid);

        // check if the base table exists or not
        std::shared_ptr<TableInfo> base_table_info = GetCachedTableInfoById(base_table_uuid);
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
            context->Abort();
            // cannot find the base table
            K2LOG_E(log::catalog, "Cannot find base table {} for index {}", base_table_id, request.tableName);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find base table " + base_table_id + " for index " + request.tableName;
            return response;
        }

        if (base_table_info->has_secondary_indexes()) {
            const IndexMap& index_map = base_table_info->secondary_indexes();
            const auto itr = index_map.find(index_table_uuid);
            // the index has already been defined
            if (itr != index_map.end()) {
                // return if 'create .. if not exist' clause is specified
                if (request.isNotExist) {
                    const IndexInfo& index_info = itr->second;
                    response.indexInfo = std::make_shared<IndexInfo>(index_info);
                    response.status.Succeed();
                    context->Commit();
                    return response;
                } else {
                    context->Commit();
                    // return index already present error if index already exists
                    response.status.code = StatusCode::ALREADY_PRESENT;
                    response.status.errorMessage = "Index " + index_table_uuid + " has already existed in ns " + namespace_info->GetNamespaceId();
                    K2LOG_E(log::catalog, "{}", response.status);
                    return response;
                }
            }
        }

        try {
            // use default index permission, could be customized by user/api
            IndexInfo new_index_info = BuildIndexInfo(base_table_info, request.tableName, request.tableOid, index_table_uuid,
                    request.schema, request.isUnique, request.isSharedTable, IndexPermissions::INDEX_PERM_READ_WRITE_AND_DELETE);

            K2LOG_D(log::catalog, "Persisting index table id: {}, name: {}", new_index_info.table_id(), new_index_info.table_name());
            // persist the index table metadata to the system catalog SKV tables
            table_info_handler_->PersistIndexTable(context, namespace_info->GetNamespaceId(), base_table_info, new_index_info);

            K2LOG_D(log::catalog, "Persisting index SKV schema id: {}, name: {}", new_index_info.table_id(), new_index_info.table_name());
            // create a SKV schema to insert the actual index data
            table_info_handler_->CreateOrUpdateIndexSKVSchema(context, namespace_info->GetNamespaceId(), base_table_info, new_index_info);

            // update the base table with the new index
            base_table_info->add_secondary_index(new_index_info.table_id(), new_index_info);

            K2LOG_D(log::catalog, "Updating cache for table id: {}, name: {}", new_index_info.table_id(), new_index_info.table_name());
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
            context->Commit();
            response.indexInfo = new_index_info_ptr;
            response.status.Succeed();
            K2LOG_D(log::catalog, "Created index id: {}, name: {}", new_index_info.table_id(), request.tableName);
        } catch (const std::exception& e) {
            context->Abort();
            response.status.code = StatusCode::RUNTIME_ERROR;
            response.status.errorMessage = e.what();
            K2LOG_E(log::catalog, "Failed to create index {} due to {}", request.tableName, response.status);
        }
        return response;
    }

    GetTableSchemaResponse SqlCatalogManager::GetTableSchema(const GetTableSchemaRequest& request) {
        GetTableSchemaResponse response;
        // generate table id from namespace oid and table oid
        std::string table_uuid = PgObjectId::GetTableUuid(request.namespaceOid, request.tableOid);
        std::string table_id = PgObjectId::GetTableId(request.tableOid);
        K2LOG_D(log::catalog, "Get table schema ns oid: {}, table oid: {}, table id: {}",
            request.namespaceOid, request.tableOid, table_id);
        // check the table schema from cache
        std::shared_ptr<TableInfo> table_info = GetCachedTableInfoById(table_uuid);
        if (table_info != nullptr) {
            K2LOG_D(log::catalog, "Returned cached table schema name: {}, id: {}", table_info->table_name(), table_info->table_id());
            response.tableInfo = table_info;
            response.status.Succeed();
            return response;
        }

        std::string namespace_id = PgObjectId::GetNamespaceUuid(request.namespaceOid);
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceById(namespace_id);
        if (namespace_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find namespace {}", namespace_id);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + namespace_id;
            return response;
        }

        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        // fetch the table from SKV
        K2LOG_D(log::catalog, "Checking if table {} is an index or not", table_id);
        TableOrIndexResult is_index_result = table_info_handler_->IsIndexTable(context, namespace_info->GetNamespaceId(), table_id);
        if (!is_index_result.status.IsSucceeded()) {
            context->Abort();
            K2LOG_E(log::catalog, "Failed to check table {} in ns {}, due to {}",
                table_id, namespace_info->GetNamespaceId(), is_index_result.status);
            response.status = std::move(is_index_result.status);
            response.tableInfo = nullptr;
            return response;
        }
        if (!is_index_result.isIndex) {
            K2LOG_D(log::catalog, "Fetching table schema {} in ns {}", table_id, namespace_info->GetNamespaceId());
            // the table id belongs to a table
            GetTableResult table_result = table_info_handler_->GetTable(context, namespace_info->GetNamespaceId(), namespace_info->GetNamespaceName(),
                table_id);
            if (!table_result.status.IsSucceeded()) {
                context->Abort();
                K2LOG_E(log::catalog, "Failed to check table {} in ns {}, due to {}",
                    table_id, namespace_info->GetNamespaceId(), table_result.status);
                response.status = std::move(table_result.status);
                response.tableInfo = nullptr;
                return response;
            }
            if (table_result.tableInfo == nullptr) {
                context->Commit();
                K2LOG_E(log::catalog, "Failed to find table {} in ns {}", table_id, namespace_info->GetNamespaceId());
                response.status.code = StatusCode::NOT_FOUND;
                response.status.errorMessage = "Cannot find table " + table_id;
                response.tableInfo = nullptr;
                return response;
            }

            response.tableInfo = table_result.tableInfo;
            context->Commit();
            response.status.Succeed();
            // update table cache
            UpdateTableCache(response.tableInfo);
            K2LOG_D(log::catalog, "Returned schema for table name: {}, id: {}",
                response.tableInfo->table_name(), response.tableInfo->table_id());
            return response;
        }

        // Check the index table
        K2LOG_D(log::catalog, "Fetching table schema for index {} in ns {}", table_id, namespace_info->GetNamespaceId());
        std::shared_ptr<IndexInfo> index_ptr = GetCachedIndexInfoById(table_uuid);
        std::string base_table_id;
        if (index_ptr == nullptr) {
            // not founnd in cache, try to check the base table id from SKV
            GeBaseTableIdResult table_id_result = table_info_handler_->GeBaseTableId(context, namespace_info->GetNamespaceId(), table_id);
            if (!table_id_result.status.IsSucceeded()) {
                context->Abort();
                K2LOG_E(log::catalog, "Failed to check base table id for index {}, {}, due to {}",
                    table_id, namespace_info->GetNamespaceId(), table_id_result.status.errorMessage);
                response.status = std::move(table_id_result.status);
                response.tableInfo = nullptr;
                return response;
            }
            base_table_id = table_id_result.baseTableId;
        } else {
            base_table_id = index_ptr->indexed_table_id();
        }

        if (base_table_id.empty()) {
            // cannot find the id as either a table id or an index id
            context->Abort();
            K2LOG_E(log::catalog, "Failed to find base table id for index {}, {}", table_id, namespace_info->GetNamespaceId());
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find base table for index " + table_id;
            response.tableInfo = nullptr;
            return response;
        }

        K2LOG_D(log::catalog, "Fetching base table schema {} for index {}, {}",
                base_table_id, table_id, namespace_info->GetNamespaceId());
        GetTableResult base_table_result = table_info_handler_->GetTable(context, namespace_info->GetNamespaceId(), namespace_info->GetNamespaceName(),
                base_table_id);
        if (!base_table_result.status.IsSucceeded()) {
            context->Abort();
            response.status = std::move(base_table_result.status);
            response.tableInfo = nullptr;
            return response;
        }
        context->Commit();
        response.status.Succeed();
        response.tableInfo = base_table_result.tableInfo;
        // update table cache
        UpdateTableCache(response.tableInfo);
        K2LOG_D(log::catalog, "Returned base table schema id: {}, name {}, for index: {}",
            base_table_id, response.tableInfo->table_name(), table_id);
        return response;
    }

    ListTablesResponse SqlCatalogManager::ListTables(const ListTablesRequest& request) {
        K2LOG_D(log::catalog, "Listing tables for namespace {}", request.namespaceName);
        ListTablesResponse response;
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceByName(request.namespaceName);
        if (namespace_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find namespace {}", request.namespaceName);
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
        K2LOG_D(log::catalog, "Found {} tables in namespace {}", response.tableInfos.size(), request.namespaceName);
        return response;
    }

    DeleteTableResponse SqlCatalogManager::DeleteTable(const DeleteTableRequest& request) {
        K2LOG_D(log::catalog, "Deleting table {} in namespace {}", request.tableOid, request.namespaceOid);
        DeleteTableResponse response;
        std::string namespace_id = PgObjectId::GetNamespaceUuid(request.namespaceOid);
        std::string table_id = PgObjectId::GetTableUuid(request.namespaceOid, request.tableOid);
        response.namespaceId = namespace_id;
        response.tableId = table_id;

        std::shared_ptr<TableInfo> table_info = GetCachedTableInfoById(table_id);
        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        if (table_info == nullptr) {
            // try to find table from SKV by looking at namespace first
            std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceById(namespace_id);
            if (namespace_info == nullptr) {
                K2LOG_E(log::catalog, "Cannot find namespace {}", namespace_id);
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
        K2LOG_D(log::catalog, "Deleting index {} in ns {}", request.tableOid, request.namespaceOid);
        DeleteIndexResponse response;
        std::string namespace_id = PgObjectId::GetNamespaceUuid(request.namespaceOid);
        std::string table_uuid = PgObjectId::GetTableUuid(request.namespaceOid, request.tableOid);
        std::string table_id = PgObjectId::GetTableId(request.tableOid);
        response.namespaceId = namespace_id;
        std::shared_ptr<NamespaceInfo> namespace_info = CheckAndLoadNamespaceById(namespace_id);
        if (namespace_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find namespace {}", namespace_id);
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + namespace_id;
            return response;
        }

        std::shared_ptr<SessionTransactionContext> context = NewTransactionContext();
        std::shared_ptr<IndexInfo> index_info = GetCachedIndexInfoById(table_uuid);
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
        response.baseIndexTableOid = base_table_info->table_oid();
        response.status.Succeed();
        return response;
    }

    ReservePgOidsResponse SqlCatalogManager::ReservePgOid(const ReservePgOidsRequest& request) {
        ReservePgOidsResponse response;
        K2LOG_D(log::catalog, "Reserving PgOid with nextOid: {}, count: {}, for ns: {}",
            request.nextOid, request.count, request.namespaceId);
        std::shared_ptr<SessionTransactionContext> ns_context = NewTransactionContext();
        GetNamespaceResult result = namespace_info_handler_->GetNamespace(ns_context, request.namespaceId);
        if (!result.status.IsSucceeded()) {
            ns_context->Abort();
            K2LOG_E(log::catalog, "Failed to get namespace {}", request.namespaceId);
            response.status = std::move(result.status);
            return response;
        }
        if (result.namespaceInfo == nullptr) {
            ns_context->Abort();
            K2LOG_E(log::catalog, "Namespace {} is empty", request.namespaceId);
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
            K2LOG_W(log::catalog, "No more object identifier is available for Postgres database {}", request.namespaceId);
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
        K2LOG_D(log::catalog, "Updating nextPgOid on SKV to {} for namespace {}", end_oid, request.namespaceId);
        AddOrUpdateNamespaceResult update_result = namespace_info_handler_->AddOrUpdateNamespace(ns_context, updated_ns);
        if (!update_result.status.IsSucceeded()) {
            ns_context->Abort();
            K2LOG_E(log::catalog, "Failed to update nextPgOid on SKV due to {}", update_result.status);
            response.status = std::move(update_result.status);
            return response;
        }

        ns_context->Commit();
        K2LOG_D(log::catalog, "Reserved PgOid succeeded for namespace {}", request.namespaceId);
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
        K2LOG_D(log::catalog, "Local catalog version: {}. new version: {}", local_catalog_version, new_version);
        if (new_version < local_catalog_version) {
            K2LOG_D(log::catalog,
                "Catalog version update: version on SKV is too old. New: {}, Old: {}",
                 new_version, local_catalog_version);
            ClusterInfo cluster_info(cluster_id_, init_db_done_, local_catalog_version);
            K2LOG_D(log::catalog, "Updating catalog version on SKV to {}", new_version);
            UpdateClusterInfoResult result = cluster_info_handler_->UpdateClusterInfo(context, cluster_info);
            if (!result.status.IsSucceeded()) {
                K2LOG_E(log::catalog, "ClusterInfo update failed due to {}", result.status);
                return result.status;
            }
        } else if (new_version > local_catalog_version) {
            K2LOG_D(log::catalog, "Updating local catalog version to {}", new_version);
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
        table_uuid_map_[table_info->table_uuid()] = table_info;
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
        table_uuid_map_.erase(table_info->table_uuid());
        TableNameKey key = std::make_pair(table_info->namespace_id(), table_info->table_name());
        table_name_map_.erase(key);
    }

    // clear index infos for a table in the index cache
    void SqlCatalogManager::ClearIndexCacheForTable(const std::string& base_table_id) {
        std::vector<std::string> index_uuids;
        for (std::pair<std::string, std::shared_ptr<IndexInfo>> pair : index_uuid_map_) {
            // first find all indexes that belong to the table
            if (base_table_id == pair.second->indexed_table_id()) {
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

    std::shared_ptr<NamespaceInfo> SqlCatalogManager::GetCachedNamespaceById(const std::string& namespace_id) {
        if (!namespace_id_map_.empty()) {
            const auto itr = namespace_id_map_.find(namespace_id);
            if (itr != namespace_id_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<NamespaceInfo> SqlCatalogManager::GetCachedNamespaceByName(const std::string& namespace_name) {
        if (!namespace_name_map_.empty()) {
            const auto itr = namespace_name_map_.find(namespace_name);
            if (itr != namespace_name_map_.end()) {
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

    std::shared_ptr<TableInfo> SqlCatalogManager::GetCachedTableInfoByName(const std::string& namespace_id, const std::string& table_name) {
        if (!table_name_map_.empty()) {
           TableNameKey key = std::make_pair(namespace_id, table_name);
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

    IndexInfo SqlCatalogManager::BuildIndexInfo(std::shared_ptr<TableInfo> base_table_info, std::string index_name, uint32_t table_oid, std::string index_uuid,
            const Schema& index_schema, bool is_unique, bool is_shared, IndexPermissions index_permissions) {
        std::vector<IndexColumn> columns;
        for (ColumnId col_id: index_schema.column_ids()) {
            int col_idx = index_schema.find_column_by_id(col_id);
            if (col_idx == Schema::kColumnNotFound) {
                throw std::runtime_error("Cannot find column with id " + col_id);
            }
            const ColumnSchema& col_schema = index_schema.column(col_idx);
            int32_t indexed_column_id = -1;
            if (col_schema.name().compare("ybuniqueidxkeysuffix") != 0 && col_schema.name().compare("ybidxbasectid") != 0) {
                // skip checking "ybuniqueidxkeysuffix" and "ybidxbasectid" on base table, which only exist on index table
                std::pair<bool, ColumnId> pair = base_table_info->schema().FindColumnIdByName(col_schema.name());
                if (!pair.first) {
                    throw std::runtime_error("Cannot find column id in base table with name " + col_schema.name());
                }
                indexed_column_id = pair.second;
            }
            K2LOG_D(log::catalog,
                "Index column id: {}, name: {}, type: {}, is_primary: {}, is_hash: {}, order: {}",
                col_id, col_schema.name(), col_schema.type()->id(), col_schema.is_primary(), col_schema.is_hash(), col_schema.order());
            // TODO: change all Table schema and index schema to use is_hash and is_range directly instead of is_primary
            bool is_range = false;
            if (col_schema.is_primary() && !col_schema.is_hash()) {
                is_range = true;
            }
            IndexColumn col(col_id, col_schema.name(), col_schema.type()->id(), col_schema.is_nullable(),
                    col_schema.is_hash(), is_range, col_schema.order(), col_schema.sorting_type(), indexed_column_id);
            columns.push_back(col);
        }
        IndexInfo index_info(index_name, table_oid, index_uuid, base_table_info->table_id(), index_schema.version(),
                is_unique, is_shared, columns, index_permissions);
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
