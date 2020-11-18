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

#ifndef CHOGORI_SQL_TABLE_INFO_HANDLER_H
#define CHOGORI_SQL_TABLE_INFO_HANDLER_H

#include <string>
#include <vector>

#include "yb/pggate/catalog/sql_catalog_defaults.h"
#include "yb/pggate/catalog/sql_catalog_entity.h"
#include "yb/pggate/k2_adapter.h"

namespace k2pg {
namespace sql {

using yb::Status;
using k2pg::gate::K2Adapter;
using k2pg::gate::K23SITxn;

struct CreateSysTablesResult {
    RStatus status;    
};

struct CheckSysTableResult {
    RStatus status;
};

struct CreateUpdateTableResult {
    RStatus status;
};

struct GetTableResult {
    RStatus status;
    std::shared_ptr<TableInfo> tableInfo;
};

struct ListTablesResult {
    RStatus status;
    std::vector<std::shared_ptr<TableInfo>> tableInfos;
};

struct CheckSchemaResult {
    RStatus status;
    bool exist;
};

class TableInfoHandler : public std::enable_shared_from_this<TableInfoHandler> {
    public:
    typedef std::shared_ptr<TableInfoHandler> SharedPtr;

    TableInfoHandler(std::shared_ptr<K2Adapter> k2_adapter);
    ~TableInfoHandler();

    // schema to store table information for a namespace
    static inline k2::dto::Schema sys_catalog_tablelist_schema {
        .name = sys_catalog_tablelist_schema_name,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "TableId", false, false},
                {k2::dto::FieldType::STRING, "TableName", false, false},
                {k2::dto::FieldType::INT32T, "TableOid", false, false},
                {k2::dto::FieldType::BOOL, "IsSysTable", false, false},
                {k2::dto::FieldType::BOOL, "IsTransactional", false, false},
                {k2::dto::FieldType::BOOL, "IsIndex", false, false},
                {k2::dto::FieldType::BOOL, "IsUnique", false, false},
                {k2::dto::FieldType::STRING, "IndexedTableId", false, false},
                {k2::dto::FieldType::INT16T, "IndexPermission", false, false},
                {k2::dto::FieldType::INT32T, "NextColumnId", false, false},
                {k2::dto::FieldType::INT64T, "SchemaVersion", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    // schema to store table column schema information
    static inline k2::dto::Schema sys_catalog_table_schema {
        .name = sys_catalog_table_schema_schema_name,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "TableId", false, false},
                {k2::dto::FieldType::INT32T, "ColumnId", false, false},
                {k2::dto::FieldType::STRING, "ColumnName", false, false},
                {k2::dto::FieldType::STRING, "ColumnType", false, false},
                {k2::dto::FieldType::BOOL, "IsNullable", false, false},
                {k2::dto::FieldType::BOOL, "IsPrimary", false, false},
                {k2::dto::FieldType::BOOL, "IsPartition", false, false},
                {k2::dto::FieldType::INT32T, "Order", false, false},
                {k2::dto::FieldType::INT16T, "SortingType", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 , 1},
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    // schema to store index column schema information
    static inline k2::dto::Schema sys_catalog_index_schema {
        .name = sys_catalog_index_schema_schema_name,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "TableId", false, false},
                {k2::dto::FieldType::INT32T, "ColumnId", false, false},
                {k2::dto::FieldType::STRING, "ColumnName", false, false},
                {k2::dto::FieldType::INT32T, "IndexedColumnId", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0, 1 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    CreateSysTablesResult CreateSysTablesIfNecessary(const Context& context, std::string collection_name);

    CreateUpdateTableResult CreateOrUpdateTable(const Context& context, std::string collection_name, std::string table_id, 
        uint32_t table_oid, std::shared_ptr<TableInfo> table);

    GetTableResult GetTable(const Context& context, std::string collection_name, std::string table_id);

    ListTablesResult ListTables(const Context& context, std::string collection_name, bool isSysTableIncluded);

    CheckSchemaResult CheckSchema(const Context& context, std::string collection_name, std::string schema_name);
    
    private:  
    CheckSysTableResult CheckAndCreateSysTable(const Context& context, std::string collection_name, std::string schema_name, 
        std::shared_ptr<k2::dto::Schema> schema);

    std::shared_ptr<K2Adapter> k2_adapter_;  
    std::string tablelist_schema_name_;
    std::string table_schema_name_;
    std::string index_schema_name_;
    std::shared_ptr<k2::dto::Schema> tablelist_schema_ptr;  
    std::shared_ptr<k2::dto::Schema> table_schema_ptr;  
    std::shared_ptr<k2::dto::Schema> index_schema_ptr;  
};

} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_TABLE_INFO_HANDLER_H