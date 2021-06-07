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

#include "pggate/catalog/sql_catalog_defaults.h"
#include "pggate/catalog/sql_catalog_entity.h"
#include "pggate/k2_adapter.h"
#include "catalog_log.h"

namespace k2pg {
namespace sql {
namespace catalog {

using k2pg::gate::CreateScanReadResult;
using k2pg::gate::K2Adapter;
using k2pg::Status;

struct InitDatabaseTableResult {
    Status status;
};

struct AddOrUpdateDatabaseResult {
    Status status;
};

struct GetDatabaseResult {
    Status status;
    std::shared_ptr<DatabaseInfo> databaseInfo;
};

struct ListDatabaseResult {
    Status status;
    std::vector<std::shared_ptr<DatabaseInfo>> databaseInfos;
};

struct DeleteDataseResult {
    Status status;
};

// DatabaseInfo is a cluster level system table holding info for each database,
// currently its Name/Oid, and next PgOid(for objects inside this DB).
class DatabaseInfoHandler {
    public:
    typedef std::shared_ptr<DatabaseInfoHandler> SharedPtr;

    k2::dto::Schema schema_ {
        .name = CatalogConsts::skv_schema_name_database_meta,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "DatabaseId", false, false},
                {k2::dto::FieldType::STRING, "DatabaseName", false, false},
                {k2::dto::FieldType::INT64T, "DatabaseOid", false, false},
                {k2::dto::FieldType::INT64T, "NextPgOid", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    DatabaseInfoHandler(std::shared_ptr<K2Adapter> k2_adapter);

    ~DatabaseInfoHandler();

    InitDatabaseTableResult InitDatabasTable();

    AddOrUpdateDatabaseResult UpsertDatabase(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<DatabaseInfo> database_info);

    GetDatabaseResult GetDatabase(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& database_id);

    ListDatabaseResult ListDatabases(std::shared_ptr<PgTxnHandler> txnHandler);

    DeleteDataseResult DeleteDatabase(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<DatabaseInfo> database_info);

    // TODO: add partial update for next_pg_oid once SKV supports partial update


    private:
    std::string collection_name_;
    std::string schema_name_;
    std::shared_ptr<k2::dto::Schema> schema_ptr_;

    std::shared_ptr<K2Adapter> k2_adapter_;
};

} // namespace catalog
} // namespace sql
} // namespace k2pg