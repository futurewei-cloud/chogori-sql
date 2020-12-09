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

#ifndef CHOGORI_SQL_CATALOG_PERSISTENCE_H
#define CHOGORI_SQL_CATALOG_PERSISTENCE_H

#include <string>
#include <assert.h>     

#include "yb/pggate/k23si_txn.h"

namespace k2pg {
namespace sql {
namespace catalog {

using std::string;
using k2pg::gate::K23SITxn;
// use the pair <namespace_id, table_name> to reference a table
typedef std::pair<std::string, std::string> TableNameKey;

class ClusterInfo {
    public: 
    ClusterInfo();

    ClusterInfo(string cluster_id, uint64_t catalog_version, bool initdb_done);

    ~ClusterInfo();

    void SetClusterId(string cluster_id) {
        cluster_id_ = std::move(cluster_id);
    }

    const string& GetClusterId() {
        return cluster_id_;
    } 

    void SetCatalogVersion(uint64_t catalog_version) {
        catalog_version_ = catalog_version;
    }

    uint64_t GetCatalogVersion() {
        return catalog_version_;
    }

    void SetInitdbDone(bool initdb_done) {
        initdb_done_ = initdb_done;
    }

    bool IsInitdbDone() {
        return initdb_done_;
    }

    private: 
    // cluster id, could be randomly generated or from a configuration parameter
    std::string cluster_id_;

    // Right now, the YB logic in PG uses a single global catalog caching version defined in 
    //  src/include/pg_yb_utils.h
    //
    //  extern uint64_t yb_catalog_cache_version;
    //
    // to check if the catalog needs to be refreshed or not. To not break the above caching
    // logic, we need to store the catalog_version as a global variable here.
    // 
    // TODO: update both YB logic in PG, PG gate APIs, and catalog manager to be more fine-grained to
    // reduce frequency and/or duration of cache refreshes. One good example is to use a separate
    // catalog version for a database, however, we do need to consider the catalog version change
    // for shared system tables in PG if we go this path. 
    //
    // Only certain system catalogs (such as pg_database) are shared.
    uint64_t catalog_version_;

    // whether initdb, i.e., PG bootstrap procedure to create template DBs, has been done or not
    bool initdb_done_ = false;
};

class NamespaceInfo {
    public:
    NamespaceInfo() = default;
    ~NamespaceInfo() = default;

    void SetNamespaceId(string id) {
        namespace_id_ = std::move(id);
    }

    const string& GetNamespaceId() const {
        return namespace_id_;
    }

    void SetNamespaceName(string name) {
        namespace_name_ = std::move(name);
    }

    const string& GetNamespaceName() const {
        return namespace_name_;
    }

    void SetNamespaceOid(uint32_t pg_oid) {
        namespace_oid_ = pg_oid;
    }

    uint32_t GetNamespaceOid() {
        return namespace_oid_;
    }

    void SetNextPgOid(uint32_t next_pg_oid) {
        assert(next_pg_oid > next_pg_oid_);
        next_pg_oid_ = next_pg_oid;
    }

    uint32_t GetNextPgOid() {
        return next_pg_oid_;
    }

    private:
    // encoded id, for example, uuid
    string namespace_id_;

    // name
    string namespace_name_;

    // object id assigned by PG
    uint32_t namespace_oid_;

    // next PG Oid that is available for object id assignment for this namespace
    uint32_t next_pg_oid_;
};

class SessionTransactionContext {
    public:
    SessionTransactionContext(std::shared_ptr<K23SITxn> txn);
    ~SessionTransactionContext();

    std::shared_ptr<K23SITxn> GetTxn() {
        return txn_;
    }

    void Commit() {
        EndTransaction(true);
        finished_ = true;
    }

    void Abort() {
        EndTransaction(false);
        finished_ = true;
    }

    private: 
    void EndTransaction(bool should_commit);

    std::shared_ptr<K23SITxn> txn_;
    bool finished_;   
};

// mapping to the status code defined in yb's status.h (some are not applicable and thus, not included here)
typedef enum RStatusCode {
    OK = 0,
    NOT_FOUND = 1,
    CORRUPTION = 2,
    NOT_SUPPORTED = 3,
    INVALID_ARGUMENT = 4,
    IO_ERROR = 5,
    ALREADY_PRESENT = 6,
    RUNTIME_ERROR = 7,
    NETWORK_ERROR = 8,
    ILLEGAL_STATE = 9,
    NOT_AUTHORIZED = 10,
    ABORTED = 11,
    REMOTE_ERROR = 12,
    SERVICE_UNAVAILABLE = 13,
    TIMED_OUT = 14,
    UNINITIALIZED = 15,
    CONFIGURATION_ERROR = 16,
    INCOMPLETE = 17,
    END_OF_FILE = 18,
    INVALID_COMMAND = 19,
    QUERY_ERROR = 20,
    INTERNAL_ERROR = 21,
    EXPIRED = 22,    
} StatusCode;

// response status
struct RStatus {
    RStatus() = default;
    ~RStatus() = default;

    StatusCode code;
    std::string errorMessage;

    void Succeed() {
        code = StatusCode::OK;
    }

    bool IsSucceeded() {
        return code == StatusCode::OK;
    }  
};

static const inline RStatus StatusOK{.code = StatusCode::OK, .errorMessage=""};

} // namespace catalog
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_CATALOG_PERSISTENCE_H    


