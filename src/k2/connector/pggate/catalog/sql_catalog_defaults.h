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

#include <k2/common/Chrono.h>
#include "entities/entity_ids.h"

namespace k2pg {
namespace sql {
namespace catalog {

struct CatalogConsts {
    // const for PG primary cluster and corresponding sql primary SKV collection
    static const std::string primary_cluster_id;
    static const std::string skv_collection_name_primary_cluster;

    // two meta tables/SKVSchemas in PG primary cluster(corresponding SKV collection)
    static const std::string skv_schema_name_cluster_meta;
    static const std::string skv_schema_name_database_meta;

    // table/index and their column meta tables - exist in every
    static const std::string skv_schema_name_table_meta;
    static const std::string skv_schema_name_tablecolumn_meta;
    static const std::string skv_schema_name_indexcolumn_meta;
    static const PgOid oid_table_meta;
    static const PgOid oid_tablecolumn_meta;
    static const PgOid oid_indexcolumn_meta;

    static const std::string TABLE_ID_COLUMN_NAME;
    static const std::string INDEX_ID_COLUMN_NAME;
    static const std::string BASE_TABLE_ID_COLUMN_NAME;

    static const std::string shared_table_skv_colllection_id;

    static inline const k2::Duration catalog_manager_background_task_initial_wait = 1s;
    static inline const k2::Duration catalog_manager_background_task_sleep_interval = 30s;

    static inline int catalog_manager_background_task_thread_pool_size = 2;

    static const std::string& physical_collection(const std::string& database_id, bool is_shared);

    static bool is_on_physical_collection(const std::string& database_id, bool is_shared);
};

}  // namespace catalog
}  // namespace sql
}  // namespace k2pg
