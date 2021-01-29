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

#include "yb/pggate/catalog/base_handler.h"
#include "catalog_log.h"

namespace k2pg {
namespace sql {
namespace catalog {

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

struct CopySKVTableResult {
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

struct ListTableIdsResult {
    RStatus status;
    std::vector<std::string> tableIds;
};

struct CopyTableResult {
    RStatus status;
    std::shared_ptr<TableInfo> tableInfo;
    int num_index = 0;
};

struct CheckSKVSchemaResult {
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

struct TableOrIndexResult {
    RStatus status;
    bool isIndex;
};

class TableInfoHandler : public BaseHandler {
    public:
    typedef std::shared_ptr<TableInfoHandler> SharedPtr;

    TableInfoHandler(std::shared_ptr<K2Adapter> k2_adapter);
    ~TableInfoHandler();

    // schema to store table information for a namespace
    k2::dto::Schema sys_catalog_tablehead_schema_ {
        .name = CatalogConsts::skv_schema_name_sys_catalog_tablehead,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "SchemaTableId", false, false},
                {k2::dto::FieldType::STRING, "SchemaIndexId", false, false},
                {k2::dto::FieldType::STRING, "TableId", false, false},
                {k2::dto::FieldType::STRING, "TableName", false, false},
                {k2::dto::FieldType::INT64T, "TableOid", false, false},
                {k2::dto::FieldType::BOOL, "IsSysTable", false, false},
                {k2::dto::FieldType::BOOL, "IsShared", false, false},
                {k2::dto::FieldType::BOOL, "IsTransactional", false, false},
                {k2::dto::FieldType::BOOL, "IsIndex", false, false},
                {k2::dto::FieldType::BOOL, "IsUnique", false, false},
                {k2::dto::FieldType::STRING, "IndexedTableId", false, false},
                {k2::dto::FieldType::INT16T, "IndexPermission", false, false},
                {k2::dto::FieldType::INT32T, "NextColumnId", false, false},
                {k2::dto::FieldType::INT32T, "SchemaVersion", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0, 1, 2},
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    // schema to store table column schema information
    k2::dto::Schema sys_catalog_tablecolumn_schema_ {
        .name = CatalogConsts::skv_schema_name_sys_catalog_tablecolumn,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "SchemaTableId", false, false},
                {k2::dto::FieldType::STRING, "SchemaIndexId", false, false},
                {k2::dto::FieldType::STRING, "TableId", false, false},
                {k2::dto::FieldType::INT32T, "ColumnId", false, false},
                {k2::dto::FieldType::STRING, "ColumnName", false, false},
                {k2::dto::FieldType::INT16T, "ColumnType", false, false},
                {k2::dto::FieldType::BOOL, "IsNullable", false, false},
                {k2::dto::FieldType::BOOL, "IsPrimary", false, false},
                {k2::dto::FieldType::BOOL, "IsHash", false, false},
                {k2::dto::FieldType::INT32T, "Order", false, false},
                {k2::dto::FieldType::INT16T, "SortingType", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 , 1, 2},
        .rangeKeyFields = std::vector<uint32_t> {3}
    };

    // schema to store index column schema information
    k2::dto::Schema sys_catalog_indexcolumn_schema_ {
        .name = CatalogConsts::skv_schema_name_sys_catalog_indexcolumn,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "SchemaTableId", false, false},
                {k2::dto::FieldType::STRING, "SchemaIndexId", false, false},
                {k2::dto::FieldType::STRING, "TableId", false, false},
                {k2::dto::FieldType::INT32T, "ColumnId", false, false},
                {k2::dto::FieldType::STRING, "ColumnName", false, false},
                {k2::dto::FieldType::INT16T, "ColumnType", false, false},
                {k2::dto::FieldType::BOOL, "IsNullable", false, false},
                {k2::dto::FieldType::BOOL, "IsHash", false, false},
                {k2::dto::FieldType::BOOL, "IsRange", false, false},
                {k2::dto::FieldType::INT32T, "Order", false, false},
                {k2::dto::FieldType::INT16T, "SortingType", false, false},
                {k2::dto::FieldType::INT32T, "IndexedColumnId", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0, 1, 2},
        .rangeKeyFields = std::vector<uint32_t> {3}
    };

    CreateSysTablesResult CheckAndCreateSystemTables(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id);

    CreateUpdateTableResult CreateOrUpdateTable(std::shared_ptr<SessionTransactionContext> context, const std::string& namespace_id, std::shared_ptr<TableInfo> table);

    GetTableResult GetTable(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, std::string namespace_name, std::string table_id);

    ListTablesResult ListTables(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, std::string namespace_name, bool isSysTableIncluded);

    ListTableIdsResult ListTableIds(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, bool isSysTableIncluded);

    CopyTableResult CopyTable(std::shared_ptr<SessionTransactionContext> target_context,
            const std::string& target_namespace_id,
            const std::string& target_namespace_name,
            uint32_t target_namespace_oid,
            std::shared_ptr<SessionTransactionContext> source_context,
            const std::string& source_namespace_id,
            const std::string& source_namespace_name,
            const std::string& source_table_id);

    CreateUpdateSKVSchemaResult CreateOrUpdateIndexSKVSchema(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id,
        std::shared_ptr<TableInfo> table, const IndexInfo& index_info);

    PersistIndexTableResult PersistIndexTable(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, std::shared_ptr<TableInfo> table, const IndexInfo& index_info);

    DeleteTableResult DeleteTableMetadata(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, std::shared_ptr<TableInfo> table);

    DeleteTableResult DeleteTableData(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, std::shared_ptr<TableInfo> table);

    DeleteIndexResult DeleteIndexMetadata(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id,  std::string& index_id);

    DeleteIndexResult DeleteIndexData(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id,  std::string& index_id);

    GeBaseTableIdResult GeBaseTableId(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, std::string index_id);

    TableOrIndexResult IsIndexTable(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, std::string table_id);

    private:
    CopySKVTableResult CopySKVTable(std::shared_ptr<SessionTransactionContext> target_context,
            const std::string& target_coll_name,
            const std::string& target_table_id,
            uint32_t target_version,
            std::shared_ptr<SessionTransactionContext> source_context,
            const std::string& source_coll_name,
            const std::string& source_table_id,
            uint32_t source_version);

    CheckSKVSchemaResult CheckSKVSchema(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string schema_name, uint32_t version);

    CreateUpdateSKVSchemaResult CreateOrUpdateTableSKVSchema(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::shared_ptr<TableInfo> table);

    PersistSysTableResult PersistSysTable(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::shared_ptr<TableInfo> table);

    CheckSysTableResult CheckAndCreateSysTable(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string schema_name,
        std::shared_ptr<k2::dto::Schema> schema);

    std::shared_ptr<k2::dto::Schema> DeriveSKVTableSchema(std::shared_ptr<TableInfo> table);

    std::vector<std::shared_ptr<k2::dto::Schema>> DeriveIndexSchemas(std::shared_ptr<TableInfo> table);

    std::shared_ptr<k2::dto::Schema> DeriveIndexSchema(const IndexInfo& index_info);

    k2::dto::SKVRecord DeriveTableHeadRecord(std::string collection_name, std::shared_ptr<TableInfo> table);

    k2::dto::SKVRecord DeriveIndexHeadRecord(std::string collection_name, const IndexInfo& index, bool is_sys_table, int32_t next_column_id);

    std::vector<k2::dto::SKVRecord> DeriveTableColumnRecords(std::string collection_name, std::shared_ptr<TableInfo> table);

    std::vector<k2::dto::SKVRecord> DeriveIndexColumnRecords(std::string collection_name, const IndexInfo& index, const Schema& base_tablecolumn_schema);

    k2::dto::FieldType ToK2Type(DataType type);

    DataType ToSqlType(k2::dto::FieldType type);

    k2::dto::SKVRecord FetchTableHeadSKVRecord(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string table_id);

    std::vector<k2::dto::SKVRecord> FetchIndexHeadSKVRecords(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string base_table_id);

    std::vector<k2::dto::SKVRecord> FetchTableColumnSchemaSKVRecords(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string table_id);

    std::vector<k2::dto::SKVRecord> FetchIndexColumnSchemaSKVRecords(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string table_id);

    std::shared_ptr<TableInfo> BuildTableInfo(std::string namespace_id, std::string namespace_name, k2::dto::SKVRecord& table_head, std::vector<k2::dto::SKVRecord>& table_columns);

    IndexInfo FetchAndBuildIndexInfo(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, k2::dto::SKVRecord& index_head);

    void AddDefaultPartitionKeys(std::shared_ptr<k2::dto::Schema> schema);

    k2::dto::SKVRecord buildRangeRecord(const std::string collection_name, std::shared_ptr<k2::dto::Schema> schema_ptr_, std::optional<std::string> table_id);

    std::string tablehead_schema_name_;
    std::string tablecolumn_schema_name_;
    std::string indexcolumn_schema_name_;
    std::shared_ptr<k2::dto::Schema> tablehead_schema_ptr_;
    std::shared_ptr<k2::dto::Schema> tablecolumn_schema_ptr_;
    std::shared_ptr<k2::dto::Schema> indexcolumn_schema_ptr_;
};

} // namespace catalog
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_TABLE_INFO_HANDLER_H
