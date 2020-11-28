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

#include "yb/common/env.h"
#include "yb/common/status.h"
#include "yb/common/concurrent/locks.h"
#include "yb/entities/schema.h"
#include "yb/entities/index.h"
#include "yb/pggate/k2_adapter.h"

namespace k2pg {
namespace sql {
    using yb::Env;
    using yb::Status;
    using yb::simple_spinlock;
    using k2pg::gate::K2Adapter;

    struct CreateNamespaceRequest {
        string namespaceName;
        string namespaceId;
        string sourceNamespaceId;
        string creatorRoleName;
        // next oid to assign. Ignored when sourceNamespaceId is given and the nextPgOid from source namespace will be used
        std::optional<uint32_t> nextPgOid;
    };

    struct CreateNamespaceResponse {
        string namespaceId;
        string errorMessage;
    };

    struct ListNamespacesRequest {
    };

    struct ListNamespaceResponse {
        std::vector<string> namespaceNames;
        string errorMessage;  
    };

    struct GetNamespaceRequest {
        string namespaceName;
        string namespaceId;
    };

    struct GetNamespaceResponse {
        string namespaceName;
        string namespaceId;     
        string errorMessage;  
    };

    struct DeleteNamespaceRequest {
        string namespaceName;
        string namespaceId;      
    };

    struct DeleteNamespaceResponse {
        string namespaceName;
        string namespaceId;     
        string errorMessage;        
    };

    struct CreateTableRequest {
        string namespaceName;
        uint32_t namespaceId;
        string tableName;
        uint32_t tableId;
        Schema schema;
        bool isSysCatalogTable;
        bool isSharedTable;

        // for index table
        std::optional<IndexInfo> indexInfo;
    };

    struct CreateTableResponse {
        uint32_t namespaceId;
        uint32_t tableId;
        string errorMessage;
    };

    struct GetTableSchemaRequest {
        uint32_t namespaceId;
        uint32_t tableId;    
    };

    struct GetTableSchemaResponse {
        uint32_t namespaceId;
        string namespaceName;
        uint32_t tableId;    
        string tableName;
        Schema schema;
        uint32_t version;
        std::optional<IndexInfo> indexInfo;
        string errorMessage;
    };

    struct ListTablesRequest {
        string namespaceName;
        // use string match for table name
        string nameFilter;
        bool excludeSystemTables = false;
    };

    struct ListTablesResponse {
        string namespaceName;
        std::vector<string> tableNames;
        string errorMessage;
    };

    struct DeleteTableRequest {
        uint32_t namespaceId;
        uint32_t tableId;
        bool isIndexTable;
    };

    struct DeleteTableResponse {
        uint32_t namespaceId;
        uint32_t tableId;
        uint32_t indexedTableId;
        string errorMessage;
    };

    struct ReservePgOidsRequest {
        uint32_t namespaceId;
        uint32_t nextOid;
        uint32_t count;
    };

    struct ReservePgOidsResponse {
        uint32_t namespaceId;
        // the beginning of the oid reserver, which could be higher than requested
        uint32_t beginOid;
        // the end (exclusive) oid reserved
        uint32_t endOid;
        string errorMessage;  
    };

    class SqlCatalogManager : public std::enable_shared_from_this<SqlCatalogManager>{

    public:
        typedef std::shared_ptr<SqlCatalogManager> SharedPtr;

        SqlCatalogManager(std::shared_ptr<K2Adapter> k2_adapter);
        ~SqlCatalogManager();

        CHECKED_STATUS Start();

        virtual void Shutdown();

        CHECKED_STATUS IsInitDbDone(bool* isDone);

        void SetCatalogVersion(uint64_t new_version);

        uint64_t GetCatalogVersion() const;

        CHECKED_STATUS CreateNamespace(const std::shared_ptr<CreateNamespaceRequest> request, std::shared_ptr<CreateNamespaceResponse>* response);  

        CHECKED_STATUS ListNamespaces(const std::shared_ptr<ListNamespacesRequest> request, std::shared_ptr<ListNamespaceResponse>* response);

        CHECKED_STATUS GetNamespace(const std::shared_ptr<GetNamespaceRequest> request, std::shared_ptr<GetNamespaceResponse>* response);

        CHECKED_STATUS DeleteNamespace(const std::shared_ptr<DeleteNamespaceRequest> request, std::shared_ptr<DeleteNamespaceResponse> *response);

        CHECKED_STATUS CreateTable(const std::shared_ptr<CreateTableRequest> request, std::shared_ptr<CreateTableResponse>* response);
        
        CHECKED_STATUS GetTableSchema(const std::shared_ptr<GetTableSchemaRequest> request, std::shared_ptr<GetTableSchemaResponse>* response);

        CHECKED_STATUS ListTables(const std::shared_ptr<ListTablesRequest> request, std::shared_ptr<ListTablesResponse>* response);

        CHECKED_STATUS DeleteTable(const std::shared_ptr<DeleteTableRequest> request, std::shared_ptr<DeleteTableResponse> * response);

        CHECKED_STATUS ReservePgOid(const std::shared_ptr<ReservePgOidsRequest> request, std::shared_ptr<ReservePgOidsResponse>* response);

    protected:
        std::atomic<bool> initted_{false};

        mutable simple_spinlock lock_;

    private:
        std::shared_ptr<K2Adapter> k2_adapter_;

        std::atomic<bool> init_db_done_{false};

        std::atomic<uint64_t> catalog_version_{0};

    };

} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_CATALOG_MANAGER_H