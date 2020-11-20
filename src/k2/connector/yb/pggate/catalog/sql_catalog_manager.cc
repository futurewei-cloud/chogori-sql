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
        if (ciresp.status.succeeded) {
            if (ciresp.clusterInfo != nullptr) {
                init_db_done_.store(ciresp.clusterInfo->IsInitdbDone(), std::memory_order_relaxed); 
                catalog_version_.store(ciresp.clusterInfo->GetCatalogVersion(), std::memory_order_relaxed); 
                LOG(INFO) << "Loaded cluster info record succeeded";  
            } else {
                ClusterInfo cluster_info(cluster_id_, catalog_version_, init_db_done_);
                CreateClusterInfoResult clresp = cluster_info_handler_->CreateClusterInfo(cluster_info);
                if (clresp.status.succeeded) {
                    LOG(INFO) << "Created cluster info record succeeded";
                } else {
                    // TODO: what to do if the creation fails?
                    LOG(FATAL) << "Failed to create cluster info record due to " << clresp.status.errorMessage;
                }
            }
        } else {
            LOG(FATAL) << "Failed to read cluster info record";
        }

        // load namespaces
        CreateNamespaceTableResult cnresp = namespace_info_handler_->CreateNamespaceTableIfNecessary();
        if (cnresp.status.succeeded) {
            ListNamespacesResult nsresp = namespace_info_handler_->ListNamespaces();
            if (nsresp.status.succeeded) {
                if (!nsresp.namespaceInfos.empty()) {
                    for (auto ns_ptr : nsresp.namespaceInfos) {
                        // caching namespaces by namespace id and namespace name
                        namespace_id_map_[ns_ptr->GetNamespaceId()] = ns_ptr;
                        namespace_name_map_[ns_ptr->GetNamespaceName()] = ns_ptr;
                    }
                } else {
                    LOG(INFO) << "namespaces are empty";
                }
            } else {
                LOG(FATAL) << "Failed to load namespaces due to " <<  nsresp.status.errorMessage;
                return STATUS_FORMAT(IOError, "Failed to load namespaces due to error code $0 and message $1",
                    nsresp.status.errorCode, nsresp.status.errorMessage);
            }
        } else {
            LOG(FATAL) << "Failed to create or check namespace table due to " <<  cnresp.status.errorMessage;
            return STATUS_FORMAT(IOError, "Failed to create or check namespace table due to error code $0 and message $1",
                cnresp.status.errorCode, cnresp.status.errorMessage);  
        }

        initted_.store(true, std::memory_order_release);
        return Status::OK();
    }
    
    Status SqlCatalogManager::GetLatestClusterInfo(bool *initdb_done, uint64_t *catalog_version) {
        GetClusterInfoResult result = cluster_info_handler_->ReadClusterInfo(cluster_id_);
        if (result.status.succeeded) {
            if (result.clusterInfo != nullptr) {
                *initdb_done = result.clusterInfo->IsInitdbDone(); 
                *catalog_version = result.clusterInfo->GetCatalogVersion(); 
                LOG(INFO) << "Loaded cluster info record succeeded";  
             } else {
               return STATUS(NotFound, "Cluster info record does not exist");            
            }
        } else {
            return STATUS_FORMAT(IOError, "Failed to read cluster info due to error code $0 and message $1",
                result.status.errorCode, result.status.errorMessage);
        }

        return Status::OK();      
    }

    void SqlCatalogManager::Shutdown() {
        LOG(INFO) << "SQL CatalogManager shutting down...";

        bool expected = true;
        if (initted_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            // TODO: shut down steps

        }

        LOG(INFO) << "SQL CatalogManager shut down complete. Bye!"; 
    }

    Env* SqlCatalogManager::GetEnv() {
        return default_env;
    }
        
    Status SqlCatalogManager::IsInitDbDone(bool* isDone) {
        if (!init_db_done_) {
            // only need to check SKV if initdb flag is false locally
            bool initdb_done = false;
            uint64_t catalog_version;
            RETURN_NOT_OK(GetLatestClusterInfo(&initdb_done, &catalog_version));

            if (initdb_done) {
                init_db_done_.store(true, std::memory_order_relaxed);                             
            }
            if (catalog_version > catalog_version_) {
                catalog_version_.store(catalog_version, std::memory_order_relaxed);
            }
        }
        *isDone = init_db_done_;
        return Status::OK();
    }

    Status SqlCatalogManager::SetCatalogVersion(uint64_t new_version) {
        std::lock_guard<simple_spinlock> l(lock_);
        // first, compare new_version with the local version
        uint64_t local_catalog_version = catalog_version_.load(std::memory_order_acquire);
        if (new_version < local_catalog_version) {
            LOG(DFATAL) << "Ignoring catalog version update: new version too old. "
                        << "New: " << new_version << ", Old: " << local_catalog_version;
            new_version = local_catalog_version;            
        }

       // then, read the latest catalog version from SKV and do comparision
        bool initdb_done = false;
        uint64_t catalog_version;
        RETURN_NOT_OK(GetLatestClusterInfo(&initdb_done, &catalog_version));
        
        if (initdb_done && !init_db_done_) {
           // update initdb flag if it has been updated to true in SKV
           init_db_done_.store(true, std::memory_order_relaxed);                             
        }
        if (catalog_version >= new_version) {
            new_version = catalog_version;
        } else {
            ClusterInfo cluster_info(cluster_id_, init_db_done_, new_version);
            cluster_info_handler_->UpdateClusterInfo(cluster_info);
            // TODO: handle update failure
        }
        catalog_version_.store(new_version, std::memory_order_release);
        return Status::OK();
    }
    
    Status SqlCatalogManager::GetCatalogVersion(uint64_t *pg_catalog_version) {
        std::lock_guard<simple_spinlock> l(lock_);
        uint64_t local_catalog_version = catalog_version_.load(std::memory_order_acquire);

        // check the latest catalog version from SKV
        // this could be optimized once we move catalog manager as a remote service
        // such that the check would be performance in-memory
        bool initdb_done = false;
        uint64_t catalog_version;
        RETURN_NOT_OK(GetLatestClusterInfo(&initdb_done, &catalog_version));
        if (initdb_done && !init_db_done_) {
           // update initdb flag if it has been updated to true in SKV
           init_db_done_.store(true, std::memory_order_relaxed);                             
        }
       
        if (catalog_version > local_catalog_version) {
            catalog_version_.store(catalog_version, std::memory_order_release); 
        } else if (catalog_version == local_catalog_version) {
            // do nothing
        } else {
            // update the catalog version to SKV
            ClusterInfo cluster_info(cluster_id_, init_db_done_, local_catalog_version);
            cluster_info_handler_->UpdateClusterInfo(cluster_info);
            // TODO: handle update failure          
        }
        *pg_catalog_version = catalog_version_;
       return Status::OK();
    }

    Status SqlCatalogManager::CreateNamespace(const std::shared_ptr<CreateNamespaceRequest> request, std::shared_ptr<CreateNamespaceResponse> response) {
        return Status::OK();
    }
  
    Status SqlCatalogManager::ListNamespaces(const std::shared_ptr<ListNamespacesRequest> request, std::shared_ptr<ListNamespacesResponse> response) {
        ListNamespacesResult result = namespace_info_handler_->ListNamespaces();
        if (result.status.succeeded) {
            response->status.succeeded = true;             
            if (!result.namespaceInfos.empty()) {
                UpdateNamespaceCache(result.namespaceInfos);
                for (auto ns_ptr : result.namespaceInfos) {
                    response->namespace_infos.push_back(ns_ptr);
                }
            } else {
                LOG(WARNING) << "No namespaces are found";    
            }
        } else {
            response->status.succeeded = false;
            response->status.errorCode = result.status.errorCode;
            response->status.errorMessage = result.status.errorMessage;
            return STATUS_FORMAT(IOError, "Failed to list namespaces due to error code $0 and message $1",
                result.status.errorCode, result.status.errorMessage);
        }

        return Status::OK();
    }

    Status SqlCatalogManager::GetNamespace(const std::shared_ptr<GetNamespaceRequest> request, std::shared_ptr<GetNamespaceResponse> response) {
        // TODO: use a background task to refresh the namespace caches to avoid fetching from SKV on each call
        GetNamespaceResult result = namespace_info_handler_->GetNamespace(request->namespaceId);
        if (result.status.succeeded) {
            if (result.namespaceInfo != nullptr) {
                response->namespace_info = std::move(result.namespaceInfo);

                // update namespace caches
                namespace_id_map_[response->namespace_info->GetNamespaceId()] = response->namespace_info ;
                namespace_name_map_[response->namespace_info->GetNamespaceName()] = response->namespace_info; 
                response->status.succeeded = true;             
            } else {
                response->status.succeeded = false;
                // 1 stands for NOT_FOUND in ybc PG logic, details see status.h
                response->status.errorCode = 1;
                response->status.errorMessage = "Cannot find namespace " + request->namespaceId;
                return STATUS_FORMAT(NotFound, "Cannot find namespace $0", request->namespaceId);
            }
        } else {
            response->status.succeeded = false;
            response->status.errorCode = result.status.errorCode;
            response->status.errorMessage = result.status.errorMessage;
            return STATUS_FORMAT(IOError, "Failed to read namespace $0 due to error code $1 and message $2",
                request->namespaceId, result.status.errorCode, result.status.errorMessage);
        }

        return Status::OK();
    }

    Status SqlCatalogManager::DeleteNamespace(const std::shared_ptr<DeleteNamespaceRequest> request, std::shared_ptr<DeleteNamespaceResponse> response) {
        return Status::OK();
    }

    Status SqlCatalogManager::CreateTable(const std::shared_ptr<CreateTableRequest> request, std::shared_ptr<CreateTableResponse> response) {
        std::shared_ptr<NamespaceInfo> namespace_info = GetNamespaceByName(request->namespaceName);
        if (namespace_info == nullptr) {
            // try to refresh namespaces from SKV in case that the requested namespace is created by another catalog manager instance
            // this could be avoided by use a single or a quorum of catalog managers 
            ListNamespacesResult result = namespace_info_handler_->ListNamespaces();
            if (result.status.succeeded && !result.namespaceInfos.empty()) {
                // update namespace caches
                UpdateNamespaceCache(result.namespaceInfos);    
                // recheck namespace
                namespace_info = GetNamespaceByName(request->namespaceName);          
            }          
        }
        if (namespace_info == nullptr) {
            throw std::runtime_error("Cannot find namespace " + request->namespaceName);
        }
        // check if the Table has already existed or not
        std::shared_ptr<TableInfo> table_info = GetTableInfoByName(namespace_info->GetNamespaceId(), request->tableName);
        uint32_t schema_version = 0;
        std::string table_id;
        if (table_info != nullptr) {
            // increase the schema version by one
            schema_version = request->schema.version() + 1;
            table_id = table_info->table_id();
        } else {
            // new table
            schema_version = request->schema.version();
            if (schema_version == 0) {
                schema_version++;
            } 
            // generate a string format table id based database object oid and table oid
            table_id = GetPgsqlTableId(request->namespaceOid, request->tableOid);
        }
        Schema table_schema = std::move(request->schema);
        table_schema.set_version(schema_version);
        std::shared_ptr<TableInfo> new_table_info = std::make_shared<TableInfo>(namespace_info->GetNamespaceId(), request->namespaceName, 
                table_info->table_id(), request->tableName, table_schema);
        new_table_info->set_pg_oid(request->tableOid);
        new_table_info->set_is_sys_table(request->isSysCatalogTable);
        new_table_info->set_next_column_id(table_schema.max_col_id() + 1);
        
        // TODO: add logic for shared table
        std::shared_ptr<Context> context = BeginTransaction();
        CreateUpdateTableResult result = table_info_handler_->CreateOrUpdateTable(context, new_table_info->namespace_id(), new_table_info);
        if (result.status.succeeded) {
            // commit transaction
            EndTransaction(context, true);
            // update table caches
            UpdateTableCache(new_table_info);
            // increase catalog version
            IncreaseCatalogVersion();
            // return response
            response->status.succeeded = true;
            response->tableInfo = new_table_info;
        } else {
            // abort the transaction
            EndTransaction(context, false);
            response->status = std::move(result.status);
        }

        return Status::OK();
    }
    
    Status SqlCatalogManager::GetTableSchema(const std::shared_ptr<GetTableSchemaRequest> request, std::shared_ptr<GetTableSchemaResponse> response) {
        // generate table id from namespace oid and table oid
        std::string table_id = GetPgsqlTableId(request->namespaceOid, request->tableOid);
        // check the table schema from cache
        std::shared_ptr<TableInfo> table_info = GetTableInfoById(table_id);
        if (table_info != nullptr) {
            response->tableInfo = table_info;
            response->status.succeeded = true;
        } else {
            std::string namespace_id = GetPgsqlNamespaceId(request->namespaceOid);
            std::shared_ptr<NamespaceInfo> namespace_info = GetNamespaceById(namespace_id);
            std::shared_ptr<Context> context = BeginTransaction();
            if (namespace_info == nullptr) {
                // try to refresh namespaces from SKV in case that the requested namespace is created by another catalog manager instance
                // this could be avoided by use a single or a quorum of catalog managers 
                ListNamespacesResult result = namespace_info_handler_->ListNamespaces();
                if (result.status.succeeded && !result.namespaceInfos.empty()) {
                    // update namespace caches
                    UpdateNamespaceCache(result.namespaceInfos);    
                    // recheck namespace
                    namespace_info = GetNamespaceById(namespace_id);          
                }          
            }
            if (namespace_info == nullptr) {
                throw std::runtime_error("Cannot find namespace " + namespace_id);
            }
          
            // fetch the table from SKV
            GetTableResult table_result = table_info_handler_->GetTable(context, namespace_info->GetNamespaceId(), namespace_info->GetNamespaceName(),
                table_id);
            if (table_result.status.succeeded) {
                if (table_result.tableInfo != nullptr) {
                    response->status.succeeded = true;
                    response->tableInfo = table_result.tableInfo;
                    // update table cache
                    UpdateTableCache(table_result.tableInfo);
                } else {
                    response->status.succeeded = false;
                    response->status.errorCode = 0;
                    response->status.errorMessage = "Cannot find table " + table_id;
                    response->tableInfo = nullptr;
                }
                EndTransaction(context, true);
            } else {
                response->status = std::move(table_result.status);
                response->tableInfo = nullptr;
                EndTransaction(context, false);
           }
        }

        return Status::OK();
    }

    Status SqlCatalogManager::ListTables(const std::shared_ptr<ListTablesRequest> request, std::shared_ptr<ListTablesResponse> response) {
        return Status::OK();
    }

    Status SqlCatalogManager::DeleteTable(const std::shared_ptr<DeleteTableRequest> request, std::shared_ptr<DeleteTableResponse> response) {
        return Status::OK();
    }

    Status SqlCatalogManager::ReservePgOid(const std::shared_ptr<ReservePgOidsRequest> request, std::shared_ptr<ReservePgOidsResponse> response) {
        GetNamespaceResult result = namespace_info_handler_->GetNamespace(request->namespaceId);
        if (result.status.succeeded) {
            if (result.namespaceInfo != nullptr) {
                uint32_t begin_oid = result.namespaceInfo->GetNextPgOid();
                if (begin_oid < request->nextOid) {
                    begin_oid = request->nextOid;
                }
                if (begin_oid == std::numeric_limits<uint32_t>::max()) {
                    LOG(WARNING) << "No more object identifier is available for Postgres database " << request->namespaceId;
                    return STATUS_FORMAT(InvalidArgument, "No more object identifier is available for $0", request->namespaceId);
                }

                uint32_t end_oid = begin_oid + request->count;
                if (end_oid < begin_oid) {
                    end_oid = std::numeric_limits<uint32_t>::max(); // Handle wraparound.
                }
                response->namespaceId = request->namespaceId;
                response->beginOid = begin_oid;
                response->endOid = end_oid; 

                // update the namespace record on SKV
                // TODO: how can we guarantee that concurrent SKV records on SKV won't override each other
                // and lose the correctness of PgNextOid updates?
                std::shared_ptr<NamespaceInfo> updated_ns = std::move(result.namespaceInfo);
                updated_ns->SetNextPgOid(end_oid);
                AddOrUpdateNamespaceResult update_result = namespace_info_handler_->AddOrUpdateNamespace(updated_ns);
                if (!update_result.status.succeeded) {
                    return STATUS_FORMAT(IOError, "Failed to update namespace $0 due to error code $1 and message $2",
                    request->namespaceId, update_result.status.errorCode, update_result.status.errorMessage);                   
                }

                // update namespace caches
                namespace_id_map_[updated_ns->GetNamespaceId()] = updated_ns;
                namespace_name_map_[updated_ns->GetNamespaceName()] = updated_ns;              
            } else {
                return STATUS_FORMAT(NotFound, "Cannot find namespace $0", request->namespaceId);
            }
        } else {
            return STATUS_FORMAT(IOError, "Failed to read namespace $0 due to error code $1 and message $2",
                request->namespaceId, result.status.errorCode, result.status.errorMessage);
        }

        return Status::OK();
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

    std::shared_ptr<NamespaceInfo> SqlCatalogManager::GetNamespaceById(std::string namespace_id) {
        if (!namespace_id_map_.empty()) {
            const auto itr = namespace_id_map_.find(namespace_id);
            if (itr != namespace_id_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<NamespaceInfo> SqlCatalogManager::GetNamespaceByName(std::string namespace_name) {
        if (!namespace_name_map_.empty()) {
            const auto itr = namespace_name_map_.find(namespace_name);
            if (itr != namespace_name_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }

    std::shared_ptr<TableInfo> SqlCatalogManager::GetTableInfoById(std::string table_id) {
        if (!table_id_map_.empty()) {
            const auto itr = table_id_map_.find(table_id);
            if (itr != table_id_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }
        
    std::shared_ptr<TableInfo> SqlCatalogManager::GetTableInfoByName(std::string namespace_id, std::string table_name) {
        if (!table_id_map_.empty()) {
           TableNameKey key = std::make_pair(namespace_id, table_name);
           const auto itr = table_name_map_.find(key);
            if (itr != table_name_map_.end()) {
                return itr->second;
            }
        }
        return nullptr;
    }   
    
    std::shared_ptr<Context> SqlCatalogManager::BeginTransaction() {
        std::future<K23SITxn> txn_future = k2_adapter_->beginTransaction();
        std::shared_ptr<K23SITxn> txn = std::make_shared<K23SITxn>(txn_future.get());
        std::shared_ptr<Context> context = std::make_shared<Context>();
        context->SetTxn(txn);
        return context;  
    }

    void SqlCatalogManager::EndTransaction(std::shared_ptr<Context> context, bool should_commit) {
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
}  // namespace sql
}  // namespace k2pg




