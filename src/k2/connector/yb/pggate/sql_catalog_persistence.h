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

namespace k2pg {
namespace sql {

using std::string;

class ClusterInfo {
    public: 
    ClusterInfo() {};
    ~ClusterInfo() {};

    void SetClusterId(string& cluster_id) {
        cluster_id_ = std::move(cluster_id);
    }

    const string& GetClusterId() {
        return cluster_id_;
    } 

    void SetInitdbDone(bool initdb_done) {
        initdb_done_ = initdb_done;
    }

    bool IsInitdbDone() {
        return initdb_done_;
    }

    private: 
    string cluster_id_;

    bool initdb_done_ = false;
};

class NamespaceInfo {
    public:
    NamespaceInfo() = default;
    ~NamespaceInfo() = default;

    void SetNamespaceName(string& name) {
        namespace_name_ = std::move(name);
    }

    const string& GetNamespaceName() const {
        return namespace_name_;
    }

    void SetNamespaceId(string& id) {
        namespace_id_ = std::move(id);
    }

    const string& GetNamespaceId() const {
        return namespace_id_;
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

    void SetCatalogVersion(uint64_t catalog_version) {
        catalog_version_ = catalog_version;
    }

    uint64_t GetCatalogVersion() {
        return catalog_version_;
    }

    private:
    // name
    string namespace_name_;

    // encoded id, for example, uuid
    string namespace_id_;

    // object id assigned by PG
    uint32_t namespace_oid_;

    // next PG Oid that is available for object id assignment for this namespace
    uint32_t next_pg_oid_;

    // catalog schema version
    uint64_t catalog_version_ = 0;
};

} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_CATALOG_PERSISTENCE_H    


