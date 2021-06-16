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
#pragma once

#include <string>
#include <assert.h>

#include "pggate/pg_txn_handler.h"
#include "catalog_log.h"
#include <k2/common/FormattingUtils.h>

namespace k2pg {
namespace sql {
namespace catalog {

using k2pg::gate::K23SITxn;
using k2pg::gate::PgTxnHandler;

// use the pair <database_id, table_name> to reference a table
typedef std::pair<std::string, std::string> TableNameKey;

// TODO: for each these entity type, add conversion code between them and SKVRecord to move redundant conversion code scattered.

class ClusterInfo {
    public:
    ClusterInfo(){};

    ClusterInfo(std::string cluster_id, uint64_t catalog_version, bool initdb_done)
        : cluster_id_(cluster_id), catalog_version_(catalog_version), initdb_done_(initdb_done) {};

    ~ClusterInfo(){};

    void SetClusterId(std::string cluster_id) {
        cluster_id_ = std::move(cluster_id);
    }

    const std::string& GetClusterId() {
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

    // Right now, the K2PG logic in PG uses a single global catalog caching version defined in
    //  src/include/pg_k2pg_utils.h
    //
    //  extern uint64_t k2pg_catalog_cache_version;
    //
    // to check if the catalog needs to be refreshed or not. To not break the above caching
    // logic, we need to store the catalog_version as a global variable here.
    //
    // TODO: update both K2PG logic in PG, PG gate APIs, and catalog manager to be more fine-grained to
    // reduce frequency and/or duration of cache refreshes. One good example is to use a separate
    // catalog version for a database, however, we do need to consider the catalog version change
    // for shared system tables in PG if we go this path.
    //
    // Only certain system catalogs (such as pg_database) are shared.
    uint64_t catalog_version_;

    // whether initdb, i.e., PG bootstrap procedure to create template DBs, has been done or not
    bool initdb_done_ = false;
};

class DatabaseInfo {
    public:
    DatabaseInfo() = default;
    ~DatabaseInfo() = default;

    void SetDatabaseId(std::string id) {
        database_id_ = std::move(id);
    }

    const std::string& GetDatabaseId() const {
        return database_id_;
    }

    void SetDatabaseName(std::string name) {
        database_name_ = std::move(name);
    }

    const std::string& GetDatabaseName() const {
        return database_name_;
    }

    void SetDatabaseOid(uint32_t pg_oid) {
        database_oid_ = pg_oid;
    }

    uint32_t GetDatabaseOid() {
        return database_oid_;
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
    std::string database_id_;

    // name
    std::string database_name_;

    // object id assigned by PG
    uint32_t database_oid_;

    // next PG Oid that is available for object id assignment for this namespace
    uint32_t next_pg_oid_;
};

} // namespace catalog
} // namespace sql
} // namespace k2pg
