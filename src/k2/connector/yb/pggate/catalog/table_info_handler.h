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
namespace catalog {

using yb::Status;
using k2pg::gate::K2Adapter;
using k2pg::gate::K23SITxn;
using k2pg::gate::CreateScanReadResult;

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
    std::shared_ptr<k2::dto::Schema> schema;
};

struct CreateUpdateSKVSchemaResult {
    RStatus status;
};

struct PersistSysTableResult {
    RStatus status;
};

struct PersistIndexTableResult {
    RStatus status;
};

struct DeleteTableResult {
    RStatus status;
};

struct DeleteIndexResult {
    RStatus status;
};

struct GeBaseTableIdResult {
    RStatus status;
    std::string baseTableId;
};

class TableInfoHandler : public std::enable_shared_from_this<TableInfoHandler> {
    public:
    typedef std::shared_ptr<TableInfoHandler> SharedPtr;

    TableInfoHandler(std::shared_ptr<K2Adapter> k2_adapter);
    ~TableInfoHandler();

    // schema to store table information for a namespace
    static inline k2::dto::Schema sys_catalog_tablehead_schema {
        .name = sys_catalog_tablehead_schema_name,
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
                {k2::dto::FieldType::INT32T, "SchemaVersion", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    // schema to store table column schema information
    static inline k2::dto::Schema sys_catalog_tablecolumn_schema {
        .name = sys_catalog_tablecolumn_schema_schema_name,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "TableId", false, false},
                {k2::dto::FieldType::INT32T, "ColumnId", false, false},
                {k2::dto::FieldType::STRING, "ColumnName", false, false},
                {k2::dto::FieldType::INT16T, "ColumnType", false, false},
                {k2::dto::FieldType::BOOL, "IsNullable", false, false},
                {k2::dto::FieldType::BOOL, "IsPrimary", false, false},
                {k2::dto::FieldType::BOOL, "IsPartition", false, false},
                {k2::dto::FieldType::INT32T, "Order", false, false},
                {k2::dto::FieldType::INT16T, "SortingType", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 , 1},
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    // schema to store index column schema information
    static inline k2::dto::Schema sys_catalog_indexcolumn_schema {
        .name = sys_catalog_indexcolumn_schema_schema_name,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "TableId", false, false},
                {k2::dto::FieldType::INT32T, "ColumnId", false, false},
                {k2::dto::FieldType::STRING, "ColumnName", false, false},
                {k2::dto::FieldType::INT32T, "IndexedColumnId", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0, 1 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    CreateSysTablesResult CreateSysTablesIfNecessary(std::shared_ptr<Context> context, std::string collection_name);

    CreateUpdateTableResult CreateOrUpdateTable(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<TableInfo> table);

    GetTableResult GetTable(std::shared_ptr<Context> context, std::string namespace_id, std::string namespace_name, std::string table_id);

    ListTablesResult ListTables(std::shared_ptr<Context> context, std::string namespace_id, std::string namespace_name, bool isSysTableIncluded);

    CheckSchemaResult CheckSchema(std::shared_ptr<Context> context, std::string collection_name, std::string schema_name, uint32_t version);

    CreateUpdateSKVSchemaResult CreateOrUpdateTableSKVSchema(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<TableInfo> table);

    CreateUpdateSKVSchemaResult CreateOrUpdateIndexSKVSchema(std::shared_ptr<Context> context, std::string collection_name, 
        std::shared_ptr<TableInfo> table, const IndexInfo& index_info);

    PersistSysTableResult PersistSysTable(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<TableInfo> table);

    PersistIndexTableResult PersistIndexTable(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<TableInfo> table, const IndexInfo& index_info);

    DeleteTableResult DeleteTableMetadata(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<TableInfo> table);

    DeleteTableResult DeleteTableData(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<TableInfo> table);

    DeleteIndexResult DeleteIndexMetadata(std::shared_ptr<Context> context, std::string collection_name,  std::string& index_id);

    DeleteIndexResult DeleteIndexData(std::shared_ptr<Context> context, std::string collection_name,  std::string& index_id);

    GeBaseTableIdResult GeBaseTableId(std::shared_ptr<Context> context, std::string collection_name, std::string index_id);

    private:  
    CheckSysTableResult CheckAndCreateSysTable(std::shared_ptr<Context> context, std::string collection_name, std::string schema_name, 
        std::shared_ptr<k2::dto::Schema> schema);

    std::shared_ptr<k2::dto::Schema> DeriveSKVTableSchema(std::shared_ptr<TableInfo> table);

    std::vector<std::shared_ptr<k2::dto::Schema>> DeriveIndexSchemas(std::shared_ptr<TableInfo> table);

    std::shared_ptr<k2::dto::Schema> DeriveIndexSchema(const IndexInfo& index_info, const Schema& base_tablecolumn_schema);

    k2::dto::SKVRecord DeriveTableHeadRecord(std::string collection_name, std::shared_ptr<TableInfo> table);

    k2::dto::SKVRecord DeriveIndexHeadRecord(std::string collection_name, const IndexInfo& index, bool is_sys_table, int32_t next_column_id);

    std::vector<k2::dto::SKVRecord> DeriveTableColumnRecords(std::string collection_name, std::shared_ptr<TableInfo> table);
    
    std::vector<k2::dto::SKVRecord> DeriveIndexColumnRecords(std::string collection_name, const IndexInfo& index, const Schema& base_tablecolumn_schema);

    k2::dto::FieldType ToK2Type(std::shared_ptr<SQLType> type);

    DataType ToSqlType(k2::dto::FieldType type);

    void PersistSKVSchema(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<k2::dto::Schema> schema);

    void PersistSKVRecord(std::shared_ptr<Context> context, k2::dto::SKVRecord& record);

    k2::dto::SKVRecord FetchTableHeadSKVRecord(std::shared_ptr<Context> context, std::string collection_name, std::string table_id);

    std::vector<k2::dto::SKVRecord> FetchIndexHeadSKVRecords(std::shared_ptr<Context> context, std::string collection_name, std::string base_table_id);

    std::vector<k2::dto::SKVRecord> FetchTableColumnSchemaSKVRecords(std::shared_ptr<Context> context, std::string collection_name, std::string table_id);

    std::vector<k2::dto::SKVRecord> FetchIndexColumnSchemaSKVRecords(std::shared_ptr<Context> context, std::string collection_name, std::string table_id);

    std::shared_ptr<TableInfo> BuildTableInfo(std::string namespace_id, std::string namespace_name, k2::dto::SKVRecord& table_head, std::vector<k2::dto::SKVRecord>& table_columns);

    IndexInfo FetchAndBuildIndexInfo(std::shared_ptr<Context> context, std::string collection_name, k2::dto::SKVRecord& index_head);

    std::shared_ptr<K2Adapter> k2_adapter_;  
    std::string tablehead_schema_name_;
    std::string tablecolumn_schema_name_;
    std::string indexcolumn_schema_name_;
    std::shared_ptr<k2::dto::Schema> tablehead_schema_ptr;  
    std::shared_ptr<k2::dto::Schema> tablecolumn_schema_ptr;  
    std::shared_ptr<k2::dto::Schema> indexcolumn_schema_ptr;  
};

} // namespace catalog
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_TABLE_INFO_HANDLER_H