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
#include "pggate/catalog/sql_catalog_defaults.h"

#include <string>

namespace k2pg {
namespace sql {
namespace catalog {

const std::string CatalogConsts::primary_cluster_id = "PG_DEFAULT_CLUSTER";
const std::string CatalogConsts::skv_collection_name_primary_cluster =  "K2RESVD_COLLECTION_SQL_PRIMARY_CLUSTER";

// two meta tables/SKVSchema in singleton SKV collection (sql primary cluster).
const std::string CatalogConsts::skv_schema_name_cluster_meta =         "K2RESVD_SCHEMA_SQL_CLUSTER_META";
const std::string CatalogConsts::skv_schema_name_database_meta =        "K2RESVD_SCHEMA_SQL_DATABASE_META";

// Names of three system meta tables holding definition of tables, table columns, index columns (as using Postgre provided sys catalog pg_class, pg_index, etc is too complex)
// All database/SKV collection, except "sql primary cluster", contains a set of them.
// TODO: consider remvoing them as these string name can be generate from their Oid respectively
const std::string CatalogConsts::skv_schema_name_table_meta =           "K2RESVD_SCHEMA_SQL_TABLE_META";
const std::string CatalogConsts::skv_schema_name_tablecolumn_meta =     "K2RESVD_SCHEMA_SQL_TABLECOLUMN_META";
const std::string CatalogConsts::skv_schema_name_indexcolumn_meta =     "K2RESVD_SCHEMA_SQL_INDEXCOLUMN_META";
// hard coded PgOid for these above three meta tables (used for SKV )
// NOTE: these system unused oids are got from script src/includ/catlog/unused_oids. Try staying in 48XX range for future such need in Chogori-SQL.
const PgOid CatalogConsts::oid_table_meta = 4800;
const PgOid CatalogConsts::oid_tablecolumn_meta = 4801;
const PgOid CatalogConsts::oid_indexcolumn_meta = 4802;


// following three columns are SKV schema columns inside skv_schema_name_table_meta(i.e. the SKV schema for "tables" table like pg_class containing all tables/index definition)
const std::string CatalogConsts::TABLE_ID_COLUMN_NAME = "TableId";              // containing PGOid(uint32) of this table (or based table in case of a secondary index)
const std::string CatalogConsts::INDEX_ID_COLUMN_NAME = "IndexId";              // containing PGOid of this index if this entry is a secondary index, or 0 if this entry is a table.
const std::string CatalogConsts::BASE_TABLE_ID_COLUMN_NAME = "BaseTableId";     // containing PGOid of the basetable if this entry is an index

// HACKHACK - collection id for template1 database
const std::string CatalogConsts::shared_table_skv_colllection_id = "00000001000030008000000000000000";

const std::string& CatalogConsts::physical_collection(const std::string& database_id, bool is_shared) {
    if (is_shared) {
        // for a shared table/index, we need to store and access it on a specific collection
        return CatalogConsts::shared_table_skv_colllection_id;
    }
    return database_id;
}

bool CatalogConsts::is_on_physical_collection(const std::string& database_id, bool is_shared) {
    if (is_shared) {
        std::string physical_collection = CatalogConsts::physical_collection(database_id, is_shared);
        // the database_id is the same as the physical collection
        return physical_collection.compare(CatalogConsts::shared_table_skv_colllection_id) == 0;
    }

    // for a non-shared table/index, database_id is always physical
    return true;
}

} // namespace catalog
}  // namespace sql
}  // namespace k2pg
