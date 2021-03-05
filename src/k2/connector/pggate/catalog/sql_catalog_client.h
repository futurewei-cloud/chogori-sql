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

#ifndef CHOGORI_SQL_CATALOG_CLIENT_H
#define CHOGORI_SQL_CATALOG_CLIENT_H

#include <string>

#include "common/status.h"
#include "entities/entity_ids.h"
#include "entities/schema.h"
#include "entities/value.h"
#include "pggate/pg_env.h"
#include "pggate/catalog/sql_catalog_manager.h"

namespace k2pg {
namespace sql {
namespace catalog {

using yb::Status;
using k2pg::sql::PgObjectId;
using k2pg::sql::PgOid;

class SqlCatalogClient {
    public:
    SqlCatalogClient(std::shared_ptr<SqlCatalogManager> catalog_manager) : catalog_manager_(catalog_manager) {
    };

    ~SqlCatalogClient() {};

    CHECKED_STATUS IsInitDbDone(bool* isDone);

    CHECKED_STATUS InitPrimaryCluster();

    CHECKED_STATUS FinishInitDB();

    // Create a new namespace with the given name.
    CHECKED_STATUS CreateNamespace(const std::string& namespace_name,
                                const std::string& namespace_id,
                                uint32_t namespace_oid,
                                const std::string& source_namespace_id,
                                const std::string& creator_role_name,
                                const std::optional<uint32_t>& next_pg_oid = std::nullopt);

    // Delete namespace with the given name.
    CHECKED_STATUS DeleteNamespace(const std::string& namespace_name,
                                const std::string& namespace_id);

    CHECKED_STATUS UseDatabase(const std::string& database_name);

    CHECKED_STATUS CreateTable(const std::string& namespace_name,
                            const std::string& table_name,
                            const PgObjectId& table_object_id,
                            PgSchema& schema,
                            bool is_pg_catalog_table,
                            bool is_shared_table,
                            bool if_not_exist);

    CHECKED_STATUS CreateIndexTable(const std::string& namespace_name,
                            const std::string& table_name,
                            const PgObjectId& table_object_id,
                            const PgObjectId& base_table_object_id,
                            PgSchema& schema,
                            bool is_unique_index,
                            bool skip_index_backfill,
                            bool is_pg_catalog_table,
                            bool is_shared_table,
                            bool if_not_exist);

    // Delete the specified table.
    // Set 'wait' to true if the call must wait for the table to be fully deleted before returning.
    CHECKED_STATUS DeleteTable(const PgOid database_oid, const PgOid table_oid, bool wait = true);

    CHECKED_STATUS DeleteIndexTable(const PgOid database_oid, const PgOid table_oid, PgOid *base_table_oid, bool wait = true);

    CHECKED_STATUS OpenTable(const PgOid database_oid, const PgOid table_oid, std::shared_ptr<TableInfo>* table);

    Result<std::shared_ptr<TableInfo>> OpenTable(const PgOid database_oid, const PgOid table_oid) {
        std::shared_ptr<TableInfo> result;
        RETURN_NOT_OK(OpenTable(database_oid, table_oid, &result));
        return result;
    }

    // For Postgres: reserve oids for a Postgres database.
    CHECKED_STATUS ReservePgOids(const PgOid database_oid,
                                uint32_t next_oid, uint32_t count,
                                uint32_t* begin_oid, uint32_t* end_oid);

    CHECKED_STATUS GetCatalogVersion(uint64_t *pg_catalog_version);

    CHECKED_STATUS IncrementCatalogVersion();

    private:
    std::shared_ptr<SqlCatalogManager> catalog_manager_;
};

} // namespace catalog
}  // namespace sql
}  // namespace k2pg

#endif //CHOGORI_SQL_CATALOG_CLIENT_H
