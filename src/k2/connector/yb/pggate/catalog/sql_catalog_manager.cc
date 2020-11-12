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
        cluster_id_("test_cluster"), k2_adapter_(k2_adapter) {
        cluster_info_handler_ = std::make_shared<ClusterInfoHandler>(k2_adapter);
        namespace_info_handler_ = std::make_shared<NamespaceInfoHandler>(k2_adapter);
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
        return Status::OK();
    }
    
    Status SqlCatalogManager::GetTableSchema(const std::shared_ptr<GetTableSchemaRequest> request, std::shared_ptr<GetTableSchemaResponse> response) {
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
}  // namespace sql
}  // namespace k2pg




