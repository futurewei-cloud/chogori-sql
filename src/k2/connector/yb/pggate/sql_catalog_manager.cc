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

#include "yb/pggate/sql_catalog_manager.h"

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

    SqlCatalogManager::SqlCatalogManager(std::shared_ptr<K2Adapter> k2_adapter) : k2_adapter_(k2_adapter) {
    }

    SqlCatalogManager::~SqlCatalogManager() {
    }

    Status SqlCatalogManager::Start() {
        CHECK(!initted_.load(std::memory_order_acquire));
        // TODO: initialization steps

        initted_.store(true, std::memory_order_release);
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
        
    Status SqlCatalogManager::IsInitDbDone(bool* isDone) {
        *isDone = init_db_done_;
        return Status::OK();
    }

    void SqlCatalogManager::SetCatalogVersion(uint64_t new_version) {
        std::lock_guard<simple_spinlock> l(lock_);
        uint64_t ysql_catalog_version_ = catalog_version_.load(std::memory_order_acquire);
        if (new_version > ysql_catalog_version_) {
            catalog_version_.store(new_version, std::memory_order_release);
        } else if (new_version < ysql_catalog_version_) {
            LOG(DFATAL) << "Ignoring ysql catalog version update: new version too old. "
                        << "New: " << new_version << ", Old: " << ysql_catalog_version_;
        }
    }
    
    uint64_t SqlCatalogManager::GetCatalogVersion() const {
        return catalog_version_;
    }

    Status SqlCatalogManager::CreateNamespace(const std::shared_ptr<CreateNamespaceRequest> request, std::shared_ptr<CreateNamespaceResponse>* response) {
        return Status::OK();
    }
    
    Status SqlCatalogManager::ListNamespaces(const std::shared_ptr<ListNamespacesRequest> request, std::shared_ptr<ListNamespaceResponse>* response) {
        return Status::OK();
    }

    Status SqlCatalogManager::GetNamespace(const std::shared_ptr<GetNamespaceRequest> request, std::shared_ptr<GetNamespaceResponse>* response) {
        return Status::OK();
    }

    Status SqlCatalogManager::DeleteNamespace(const std::shared_ptr<DeleteNamespaceRequest> request, std::shared_ptr<DeleteNamespaceResponse> *response) {
        return Status::OK();
    }

    Status SqlCatalogManager::CreateTable(const std::shared_ptr<CreateTableRequest> request, std::shared_ptr<CreateTableResponse>* response) {
        return Status::OK();
    }
    
    Status SqlCatalogManager::GetTableSchema(const std::shared_ptr<GetTableSchemaRequest> request, std::shared_ptr<GetTableSchemaResponse>* response) {
        return Status::OK();
    }

    Status SqlCatalogManager::ListTables(const std::shared_ptr<ListTablesRequest> request, std::shared_ptr<ListTablesResponse>* response) {
        return Status::OK();
    }

    Status SqlCatalogManager::DeleteTable(const std::shared_ptr<DeleteTableRequest> request, std::shared_ptr<DeleteTableResponse> * response) {
        return Status::OK();
    }

    Status SqlCatalogManager::ReservePgOid(const std::shared_ptr<ReservePgOidsRequest> request, std::shared_ptr<ReservePgOidsResponse>* response) {
        return Status::OK();
    }
}  // namespace sql
}  // namespace k2pg




