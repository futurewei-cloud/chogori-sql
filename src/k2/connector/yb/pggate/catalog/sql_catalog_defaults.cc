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
#include "yb/pggate/catalog/sql_catalog_defaults.h"

#include <string>

namespace k2pg {
namespace sql {
namespace catalog {

const std::string CatalogConsts::default_cluster_id = "PG_DEFAULT_CLUSTER";
const std::string CatalogConsts::skv_collection_name_sql_primary = "K2RESVD_COLLECTION_SQL_PRIMARY";
const std::string CatalogConsts::skv_schema_name_cluster_info = "K2RESVD_SCHEMA_SQL_CLUSTER_INFO";
const std::string CatalogConsts::skv_schema_name_namespace_info = "K2RESVD_SCHEMA_SQL_NAMESPACE_INFO";
const std::string CatalogConsts::skv_schema_name_sys_catalog_tablehead = "K2RESVD_SCHEMA_SQL_SYS_CATALOG_TABLEHEAD";
const std::string CatalogConsts::skv_schema_name_sys_catalog_tablecolumn = "K2RESVD_SCHEMA_SQL_SYS_CATALOG_TABLECOLUMN";
const std::string CatalogConsts::skv_schema_name_sys_catalog_indexcolumn = "K2RESVD_SCHEMA_SQL_SYS_CATALOG_INDEXCOLUMN";

const std::string CatalogConsts::TABLE_ID_COLUMN_NAME = "TableId";
const std::string CatalogConsts::INDEX_ID_COLUMN_NAME = "IndexId";
const std::string CatalogConsts::INDEXED_TABLE_ID_COLUMN_NAME = "IndexedTableId";

} // namespace catalog
}  // namespace sql
}  // namespace k2pg