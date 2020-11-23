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

#include "yb/common/status.h"
#include "yb/common/env.h"

namespace k2pg {
namespace sql {
namespace catalog {

    using yb::Status;
    using k2pg::gate::K2Adapter;

    static yb::Env* default_env;

    SqlCatalogManager::SqlCatalogManager(std::shared_ptr<K2Adapter> k2_adapter) : 
        cluster_id_(default_cluster_id), k2_adapter_(k2_adapter) {
        cluster_info_handler_ = std::make_shared<ClusterInfoHandler>(k2_adapter);
        namespace_info_handler_ = std::make_shared<NamespaceInfoHandler>(k2_adapter);
        table_info_handler_ = std::make_shared<TableInfoHandler>(k2_adapter_);
    }

    SqlCatalogManager::~SqlCatalogManager() {
    }

    Status SqlCatalogManager::Start() {
        CHECK(!initted_.load(std::memory_order_acquire));
        
        // load cluster info
        GetClusterInfoResult ciresp = cluster_info_handler_->ReadClusterInfo(cluster_id_);
        if (ciresp.status.IsSucceeded()) {
            if (ciresp.clusterInfo != nullptr) {
                init_db_done_.store(ciresp.clusterInfo->IsInitdbDone(), std::memory_order_relaxed); 
                catalog_version_.store(ciresp.clusterInfo->GetCatalogVersion(), std::memory_order_relaxed); 
                LOG(INFO) << "Loaded cluster info record succeeded";  
            } else {
                ClusterInfo cluster_info(cluster_id_, catalog_version_, init_db_done_);
                CreateClusterInfoResult clresp = cluster_info_handler_->CreateClusterInfo(cluster_info);
                if (clresp.status.IsSucceeded()) {
                    LOG(INFO) << "Created cluster info record succeeded";
                } else {
                    LOG(FATAL) << "Failed to create cluster info record due to " << clresp.status.errorMessage;
                    return STATUS_FORMAT(IOError, "Failed to create cluster info record to error code $0 and message $1",
                        clresp.status.code, clresp.status.errorMessage);               
                }
            }
        } else {
            LOG(FATAL) << "Failed to read cluster info record";
            return STATUS_FORMAT(IOError, "Failed to read cluster info record to error code $0 and message $1",
                ciresp.status.code, ciresp.status.errorMessage);             
        }

        // load namespaces
        CreateNamespaceTableResult cnresp = namespace_info_handler_->CreateNamespaceTableIfNecessary();
        if (cnresp.status.IsSucceeded()) {
            ListNamespacesResult nsresp = namespace_info_handler_->ListNamespaces();
            if (nsresp.status.IsSucceeded()) {
                if (!nsresp.namespaceInfos.empty()) {
                    for (auto ns_ptr : nsresp.namespaceInfos) {
                        // cache namespaces by namespace id and namespace name
                        namespace_id_map_[ns_ptr->GetNamespaceId()] = ns_ptr;
                        namespace_name_map_[ns_ptr->GetNamespaceName()] = ns_ptr;
                    }
                } else {
                    LOG(INFO) << "namespaces are empty";
                }
            } else {
                LOG(FATAL) << "Failed to load namespaces due to " <<  nsresp.status.errorMessage;
                return STATUS_FORMAT(IOError, "Failed to load namespaces due to error code $0 and message $1",
                    nsresp.status.code, nsresp.status.errorMessage);
            }
        } else {
            LOG(FATAL) << "Failed to create or check namespace table due to " <<  cnresp.status.errorMessage;
            return STATUS_FORMAT(IOError, "Failed to create or check namespace table due to error code $0 and message $1",
                cnresp.status.code, cnresp.status.errorMessage);  
        }

        initted_.store(true, std::memory_order_release);
        return Status::OK();
    }

    void SqlCatalogManager::Shutdown() {
        LOG(INFO) << "SQL CatalogManager shutting down...";

        bool expected = true;
        if (initted_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            // TODO: add shut down steps

        }

        LOG(INFO) << "SQL CatalogManager shut down complete. Bye!"; 
    }

    Env* SqlCatalogManager::GetEnv() {
        return default_env;
    }
    
    GetInitDbResponse SqlCatalogManager::IsInitDbDone(const GetInitDbRequest& request) {
        GetInitDbResponse response;
        if (!init_db_done_) {
            GetClusterInfoResult result = cluster_info_handler_->ReadClusterInfo(cluster_id_);
            if (result.status.IsSucceeded() && result.clusterInfo != nullptr) {
               if (result.clusterInfo->IsInitdbDone()) {
                    init_db_done_.store(result.clusterInfo->IsInitdbDone(), std::memory_order_relaxed);                             
                }
                if (result.clusterInfo->GetCatalogVersion() > catalog_version_) {
                    catalog_version_.store(result.clusterInfo->GetCatalogVersion(), std::memory_order_relaxed);
                }             
            } else {
                response.status = std::move(result.status);
                return response;
            }
        }
        response.isInitDbDone = init_db_done_;
        response.status.Succeed();
        return response;
    }

    GetCatalogVersionResponse SqlCatalogManager::GetCatalogVersion(const GetCatalogVersionRequest& request) {
        GetCatalogVersionResponse response;
        // TODO: use a background thread to fetch the ClusterInfo record periodically instead of fetching it for each call
        GetClusterInfoResult result = cluster_info_handler_->ReadClusterInfo(cluster_id_);
        if (result.status.IsSucceeded() && result.clusterInfo != nullptr) {
            RStatus status = UpdateCatalogVersion(result.clusterInfo->GetCatalogVersion());
            if (!status.IsSucceeded()) {
                response.status = std::move(status);
            } else {
                response.catalogVersion = catalog_version_;
                response.status.Succeed();
            }
        } else {
            response.status = std::move(result.status);
        }
        return response;
    }      

    CreateNamespaceResponse SqlCatalogManager::CreateNamespace(const CreateNamespaceRequest& request) {
        CreateNamespaceResponse response;
        return response;
    }
  
    ListNamespacesResponse SqlCatalogManager::ListNamespaces(const ListNamespacesRequest& request) {
        ListNamespacesResponse response;
        ListNamespacesResult result = namespace_info_handler_->ListNamespaces();
        if (result.status.IsSucceeded()) {
            response.status.Succeed();             
            if (!result.namespaceInfos.empty()) {
                UpdateNamespaceCache(result.namespaceInfos);
                for (auto ns_ptr : result.namespaceInfos) {
                    response.namespace_infos.push_back(ns_ptr);
                }
            } else {
                LOG(WARNING) << "No namespaces are found";    
            }
        } else {
            LOG(ERROR) << "Failed to list namespaces due to code " << result.status.code 
                << " and message " << result.status.errorMessage;
            response.status = std::move(result.status);
        }

        return response;
    }

    GetNamespaceResponse SqlCatalogManager::GetNamespace(const GetNamespaceRequest& request) {
        GetNamespaceResponse response;
        // TODO: use a background task to refresh the namespace caches to avoid fetching from SKV on each call
        GetNamespaceResult result = namespace_info_handler_->GetNamespace(request.namespaceId);
        if (result.status.IsSucceeded()) {
            if (result.namespaceInfo != nullptr) {
                response.namespace_info = result.namespaceInfo;

                // update namespace caches
                namespace_id_map_[response.namespace_info->GetNamespaceId()] = response.namespace_info ;
                namespace_name_map_[response.namespace_info->GetNamespaceName()] = response.namespace_info; 
                response.status.Succeed();             
            } else {
                LOG(WARNING) << "Cannot find namespace " << request.namespaceId;
                response.status.code = StatusCode::NOT_FOUND;
                response.status.errorMessage = "Cannot find namespace " + request.namespaceId;
            }
        } else {
            LOG(ERROR) << "Failed to read namespace " << request.namespaceId << " due to error code " 
                << result.status.code << " and message " << result.status.errorMessage;
            response.status = std::move(result.status);
        }

        return response;
    }

    DeleteNamespaceResponse SqlCatalogManager::DeleteNamespace(const DeleteNamespaceRequest& request) {
        DeleteNamespaceResponse response;
        return response;
    }

    CreateTableResponse SqlCatalogManager::CreateTable(const CreateTableRequest& request) {
        CreateTableResponse response;
        std::shared_ptr<NamespaceInfo> namespace_info = GetCachedNamespaceByName(request.namespaceName);
        if (namespace_info == nullptr) {
            // try to refresh namespaces from SKV in case that the requested namespace is created by another catalog manager instance
            // this could be avoided by use a single or a quorum of catalog managers 
            ListNamespacesResult result = namespace_info_handler_->ListNamespaces();
            if (result.status.IsSucceeded() && !result.namespaceInfos.empty()) {
                // update namespace caches
                UpdateNamespaceCache(result.namespaceInfos);    
                // recheck namespace
                namespace_info = GetCachedNamespaceByName(request.namespaceName);          
            }          
        }
        if (namespace_info == nullptr) {
            LOG(FATAL) << "Cannot find namespace " << request.namespaceName;
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
            // increase the schema version by one
            schema_version = request.schema.version() + 1;
            table_id = table_info->table_id();
        } else {
            // new table
            schema_version = request.schema.version();
            if (schema_version == 0) {
                schema_version++;
            } 
            // generate a string format table id based database object oid and table oid
            table_id = GetPgsqlTableId(request.namespaceOid, request.tableOid);
        }
        Schema table_schema = std::move(request.schema);
        table_schema.set_version(schema_version);
        std::shared_ptr<TableInfo> new_table_info = std::make_shared<TableInfo>(namespace_info->GetNamespaceId(), request.namespaceName, 
                table_info->table_id(), request.tableName, table_schema);
        new_table_info->set_pg_oid(request.tableOid);
        new_table_info->set_is_sys_table(request.isSysCatalogTable);
        new_table_info->set_next_column_id(table_schema.max_col_id() + 1);
        
        // TODO: add logic for shared table
        std::shared_ptr<Context> context = NewTransactionContext();
        CreateUpdateTableResult result = table_info_handler_->CreateOrUpdateTable(context, new_table_info->namespace_id(), new_table_info);
        if (result.status.IsSucceeded()) {
            // commit transaction
            EndTransactionContext(context, true);
            // update table caches
            UpdateTableCache(new_table_info);
            // increase catalog version
            IncreaseCatalogVersion();
            // return response
            response.status.Succeed();
            response.tableInfo = new_table_info;
        } else {
            // abort the transaction
            EndTransactionContext(context, false);
            response.status = std::move(result.status);
        }

        return response;
    }
   
    CreateIndexTableResponse SqlCatalogManager::CreateIndexTable(const CreateIndexTableRequest& request) {
        CreateIndexTableResponse response;
        std::shared_ptr<NamespaceInfo> namespace_info = GetCachedNamespaceByName(request.namespaceName);
        if (namespace_info == nullptr) {
            // try to refresh namespaces from SKV in case that the requested namespace is created by another catalog manager instance
            // this could be avoided by use a single or a quorum of catalog managers 
            ListNamespacesResult result = namespace_info_handler_->ListNamespaces();
            if (result.status.IsSucceeded() && !result.namespaceInfos.empty()) {
                // update namespace caches
                UpdateNamespaceCache(result.namespaceInfos);    
                // recheck namespace
                namespace_info = GetCachedNamespaceByName(request.namespaceName);          
            }          
        }
        if (namespace_info == nullptr) {
            LOG(FATAL) << "Cannot find namespace " << request.namespaceName;
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find namespace " + request.namespaceName;
            return response;
        }
        // generate table id from namespace oid and table oid
        std::string base_table_id = GetPgsqlTableId(request.namespaceOid, request.baseTableOid);
        std::string index_table_id = GetPgsqlTableId(request.namespaceOid, request.tableOid);

        // check if the base table exists or not
        std::shared_ptr<TableInfo> base_table_info = GetCachedTableInfoById(base_table_id);
        std::shared_ptr<Context> context = NewTransactionContext();
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
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find base table " + base_table_id + " for index " + request.tableName;
        } else {
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
                        // TODO: change to alter index instead of recreating one here
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
                    // persist the index table metadata to the system catalog SKV tables   
                    table_info_handler_->PersistIndexTable(context, namespace_info->GetNamespaceId(), base_table_info, new_index_info); 
                    // create a SKV schema to insert the actual index data
                    table_info_handler_->CreateOrUpdateIndexSKVSchema(context, namespace_info->GetNamespaceId(), base_table_info, new_index_info); 
                    // update the base table with the new index 
                    base_table_info->add_secondary_index(index_table_id, new_index_info);
                    // update table cache
                    UpdateTableCache(base_table_info);
                    // increase catalog version
                    IncreaseCatalogVersion();
                    if (!request.skipIndexBackfill) {
                        // TODO: add logic to backfill the index
                    }
                    response.indexInfo = std::make_shared<IndexInfo>(new_index_info);
                    response.status.Succeed();
                } catch (const std::exception& e) {
                    response.status.code = StatusCode::RUNTIME_ERROR;
                    response.status.errorMessage = e.what();
                }
            }   
        }

        return response;
    }

    GetTableSchemaResponse SqlCatalogManager::GetTableSchema(const GetTableSchemaRequest& request) {
        GetTableSchemaResponse response;
        // generate table id from namespace oid and table oid
        std::string table_id = GetPgsqlTableId(request.namespaceOid, request.tableOid);
        // check the table schema from cache
        std::shared_ptr<TableInfo> table_info = GetCachedTableInfoById(table_id);
        if (table_info != nullptr) {
            response.tableInfo = table_info;
            response.status.Succeed();
        } else {
            std::string namespace_id = GetPgsqlNamespaceId(request.namespaceOid);
            std::shared_ptr<NamespaceInfo> namespace_info = GetCachedNamespaceById(namespace_id);
            if (namespace_info == nullptr) {
                // try to refresh namespaces from SKV in case that the requested namespace is created by another catalog manager instance
                // this could be avoided by use a single or a quorum of catalog managers 
                ListNamespacesResult result = namespace_info_handler_->ListNamespaces();
                if (result.status.IsSucceeded() && !result.namespaceInfos.empty()) {
                    // update namespace caches
                    UpdateNamespaceCache(result.namespaceInfos);    
                    // recheck namespace
                    namespace_info = GetCachedNamespaceById(namespace_id);          
                }          
            }
            if (namespace_info == nullptr) {
                LOG(FATAL) << "Cannot find namespace " << namespace_id;
                response.status.code = StatusCode::NOT_FOUND;
                response.status.errorMessage = "Cannot find namespace " + namespace_id;
                return response;
            }
          
            std::shared_ptr<Context> context = NewTransactionContext();
            // fetch the table from SKV
            GetTableResult table_result = table_info_handler_->GetTable(context, namespace_info->GetNamespaceId(), namespace_info->GetNamespaceName(),
                table_id);
            if (table_result.status.IsSucceeded()) {
                if (table_result.tableInfo != nullptr) {
                    response.status.Succeed();
                    response.tableInfo = table_result.tableInfo;
                    // update table cache
                    UpdateTableCache(table_result.tableInfo);
                } else {
                    response.status.code = StatusCode::NOT_FOUND;
                    response.status.errorMessage = "Cannot find table " + table_id;
                    response.tableInfo = nullptr;
                }
                EndTransactionContext(context, true);
            } else {
                response.status = std::move(table_result.status);
                response.tableInfo = nullptr;
                EndTransactionContext(context, false);
           }
        }

        return response;
    }

    ListTablesResponse SqlCatalogManager::ListTables(const ListTablesRequest& request) {
        ListTablesResponse response;
        return response;
    }

    DeleteTableResponse SqlCatalogManager::DeleteTable(const DeleteTableRequest& request) {
        DeleteTableResponse response;
        return response;
    }

    ReservePgOidsResponse SqlCatalogManager::ReservePgOid(const ReservePgOidsRequest& request) {
        ReservePgOidsResponse response;
        GetNamespaceResult result = namespace_info_handler_->GetNamespace(request.namespaceId);
        if (result.status.IsSucceeded()) {
            if (result.namespaceInfo != nullptr) {
                uint32_t begin_oid = result.namespaceInfo->GetNextPgOid();
                if (begin_oid < request.nextOid) {
                    begin_oid = request.nextOid;
                }
                if (begin_oid == std::numeric_limits<uint32_t>::max()) {
                    LOG(WARNING) << "No more object identifier is available for Postgres database " << request.namespaceId;
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
                // TODO: would K23SI guarantees that concurrent SKV records on SKV won't override each other
                // and won't lose the correctness of PgNextOid updates?
                std::shared_ptr<NamespaceInfo> updated_ns = std::move(result.namespaceInfo);
                updated_ns->SetNextPgOid(end_oid);
                AddOrUpdateNamespaceResult update_result = namespace_info_handler_->AddOrUpdateNamespace(updated_ns);
                if (!update_result.status.IsSucceeded()) {
                    response.status = std::move(update_result.status);                 
                } else {
                    // update namespace caches after persisting to SKV successfully
                    namespace_id_map_[updated_ns->GetNamespaceId()] = updated_ns;
                    namespace_name_map_[updated_ns->GetNamespaceName()] = updated_ns;
                    response.status.Succeed();              
                }
            } else {
                response.status.code = StatusCode::NOT_FOUND;
                response.status.errorMessage = "Cannot find namespace " + request.namespaceId;
            }
        } else {
            response.status = std::move(result.status);
        }

        return response;
    }

    RStatus SqlCatalogManager::UpdateCatalogVersion(uint64_t new_version) {
        std::lock_guard<simple_spinlock> l(lock_);
        // compare new_version with the local version
        uint64_t local_catalog_version = catalog_version_.load(std::memory_order_acquire);
        if (new_version < local_catalog_version) {
            LOG(INFO) << "Catalog version update: version on SKV is too old. "
                        << "New: " << new_version << ", Old: " << local_catalog_version;
            ClusterInfo cluster_info(cluster_id_, init_db_done_, local_catalog_version);
            UpdateClusterInfoResult result = cluster_info_handler_->UpdateClusterInfo(cluster_info);
            if (!result.status.IsSucceeded()) {
                LOG(ERROR) << "ClusterInfo update failed due to error code " << result.status.code << " and message " 
                    << result.status.errorMessage;
                return result.status;
            }
        } else if (new_version > local_catalog_version) {
            catalog_version_.store(new_version, std::memory_order_release);
        }
        return StatusOK;
    }

    // update namespace caches
    void SqlCatalogManager::UpdateNamespaceCache(std::vector<std::shared_ptr<NamespaceInfo>> namespace_infos) {
        std::lock_guard<simple_spinlock> l(lock_);
        namespace_id_map_.clear();
        namespace_name_map_.clear();
        for (auto ns_ptr : namespace_infos) {
            namespace_id_map_[ns_ptr->GetNamespaceId()] = ns_ptr;
            namespace_name_map_[ns_ptr->GetNamespaceName()] = ns_ptr; 
        }
    }    

    // update table caches
    void SqlCatalogManager::UpdateTableCache(std::shared_ptr<TableInfo> table_info) {
        std::lock_guard<simple_spinlock> l(lock_);
        table_id_map_[table_info->table_id()] = table_info;
        // TODO: add logic to remove table with old name if rename table is called
        TableNameKey key = std::make_pair(table_info->namespace_id(), table_info->table_name());
        table_name_map_[key] = table_info;
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
    
    std::shared_ptr<Context> SqlCatalogManager::NewTransactionContext() {
        std::future<K23SITxn> txn_future = k2_adapter_->beginTransaction();
        std::shared_ptr<K23SITxn> txn = std::make_shared<K23SITxn>(txn_future.get());
        std::shared_ptr<Context> context = std::make_shared<Context>();
        context->SetTxn(txn);
        return context;  
    }

    void SqlCatalogManager::EndTransactionContext(std::shared_ptr<Context> context, bool should_commit) {
        std::future<k2::EndResult> txn_result_future = context->GetTxn()->endTxn(should_commit);
        k2::EndResult txn_result = txn_result_future.get();
        if (!txn_result.status.is2xxOK()) {
            LOG(FATAL) << "Failed to commit transaction due to error code " << txn_result.status.code
                    << " and message: " << txn_result.status.message;
            throw std::runtime_error("Failed to end transaction, should_commit: " + should_commit);                                 
        }   
    }

    void SqlCatalogManager::IncreaseCatalogVersion() {
        catalog_version_++; 
        // need to update the catalog version on SKV
        // the update frequency could be reduced once we have a single or a quorum of catalog managers
        ClusterInfo cluster_info(cluster_id_, init_db_done_, catalog_version_);
        cluster_info_handler_->UpdateClusterInfo(cluster_info);             
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
            std::pair<bool, ColumnId> pair = base_table_info->schema().FindColumnIdByName(col_schema.name());
            if (!pair.first) {
                throw std::runtime_error("Cannot find column id in base table with name " + col_schema.name());
            }
            ColumnId indexed_column_id = pair.second;
            IndexColumn col(col_id, col_schema.name(),  indexed_column_id);
            columns.push_back(col);   
        }
        IndexInfo index_info(index_id, index_name, pg_oid, base_table_info->table_id(), index_schema.version(), 
                is_unique, columns, index_permissions);
        return index_info;        
    }

} // namespace catalog
}  // namespace sql
}  // namespace k2pg




