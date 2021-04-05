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

#include "pggate/catalog/sql_catalog_defaults.h"
#include "pggate/catalog/sql_catalog_entity.h"
#include "pggate/k2_adapter.h"
#include "entities/entity_ids.h"
#include "catalog_log.h"

namespace k2pg {
namespace sql {
namespace catalog {

using k2pg::gate::CreateScanReadResult;
using k2pg::gate::K2Adapter;
using k2pg::sql::PgObjectId;
using yb::Status;

struct CreateSysTablesResult {
    Status status;
};

struct CreateSKVSchemaIfNotExistResult {
    Status status;
    bool created;
};

struct CreateUpdateTableResult {
    Status status;
};

struct CopySKVTableResult {
    Status status;
};

struct GetTableResult {
    Status status;
    std::shared_ptr<TableInfo> tableInfo;
};

struct ListTablesResult {
    Status status;
    std::vector<std::shared_ptr<TableInfo>> tableInfos;
};

struct ListTableIdsResult {
    Status status;
    std::vector<std::string> tableIds;
};

struct CopyTableResult {
    Status status;
    std::shared_ptr<TableInfo> tableInfo;
    int num_index = 0;
};

struct CreateUpdateSKVSchemaResult {
    Status status;
};

struct PersistSysTableResult {
    Status status;
};

struct PersistIndexTableResult {
    Status status;
};

struct DeleteTableResult {
    Status status;
};

struct DeleteIndexResult {
    Status status;
};

struct GetBaseTableIdResult {
    Status status;
    std::string baseTableId;
};

struct GetTableInfoResult {
    Status status;
    bool isShared;
    bool isIndex;
};

class TableInfoHandler {
    public:
    typedef std::shared_ptr<TableInfoHandler> SharedPtr;

    TableInfoHandler(std::shared_ptr<K2Adapter> k2_adapter);
    ~TableInfoHandler();

    // schema of table information 
    k2::dto::Schema sys_catalog_tablehead_schema_ {
        .name = CatalogConsts::skv_schema_name_sys_catalog_tablehead,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "SchemaTableId", false, false},
                {k2::dto::FieldType::STRING, "SchemaIndexId", false, false},
                {k2::dto::FieldType::STRING, "TableId", false, false},
                {k2::dto::FieldType::STRING, "TableName", false, false},
                {k2::dto::FieldType::INT64T, "TableOid", false, false},
                {k2::dto::FieldType::STRING, "TableUuid", false, false},
                {k2::dto::FieldType::BOOL, "IsSysTable", false, false},
                {k2::dto::FieldType::BOOL, "IsShared", false, false},
                {k2::dto::FieldType::BOOL, "IsTransactional", false, false},
                {k2::dto::FieldType::BOOL, "IsIndex", false, false},
                {k2::dto::FieldType::BOOL, "IsUnique", false, false},
                {k2::dto::FieldType::STRING, "BaseTableId", false, false},
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
                {k2::dto::FieldType::INT32T, "BaseColumnId", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0, 1, 2},
        .rangeKeyFields = std::vector<uint32_t> {3}
    };

    CreateSysTablesResult CheckAndCreateSystemTables(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name);

    CreateUpdateTableResult CreateOrUpdateTable(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    GetTableResult GetTable(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& database_name, const std::string& table_id);

    ListTablesResult ListTables(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& database_name, bool isSysTableIncluded);

    ListTableIdsResult ListTableIds(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, bool isSysTableIncluded);

    CopyTableResult CopyTable(std::shared_ptr<PgTxnHandler> target_txnHandler,
            const std::string& target_coll_name,
            const std::string& target_database_name,
            uint32_t target_database_oid,
            std::shared_ptr<PgTxnHandler> source_txnHandler,
            const std::string& source_coll_name,
            const std::string& source_database_name,
            const std::string& source_table_id);

    CreateUpdateSKVSchemaResult CreateOrUpdateIndexSKVSchema(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name,
        std::shared_ptr<TableInfo> table, const IndexInfo& index_info);

    PersistIndexTableResult PersistIndexTable(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table, const IndexInfo& index_info);

    DeleteTableResult DeleteTableMetadata(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    DeleteTableResult DeleteTableData(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    DeleteIndexResult DeleteIndexMetadata(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name,  const std::string& index_id);

    DeleteIndexResult DeleteIndexData(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name,  const std::string& index_id);

    GetBaseTableIdResult GetBaseTableId(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& index_id);

    GetTableInfoResult GetTableInfo(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id);

    private:
    CopySKVTableResult CopySKVTable(std::shared_ptr<PgTxnHandler> target_txnHandler,
            const std::string& target_coll_name,
            const std::string& target_table_id,
            uint32_t target_version,
            std::shared_ptr<PgTxnHandler> source_txnHandler,
            const std::string& source_coll_name,
            const std::string& source_table_id,
            uint32_t source_version);

    CreateUpdateSKVSchemaResult CreateOrUpdateTableSKVSchema(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    PersistSysTableResult PersistSysTable(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    CreateSKVSchemaIfNotExistResult CreateSKVSchemaIfNotExist(const std::string& collection_name, std::shared_ptr<k2::dto::Schema> Schema);

    std::shared_ptr<k2::dto::Schema> DeriveSKVTableSchema(std::shared_ptr<TableInfo> table);

    std::vector<std::shared_ptr<k2::dto::Schema>> DeriveIndexSchemas(std::shared_ptr<TableInfo> table);

    std::shared_ptr<k2::dto::Schema> DeriveIndexSchema(const IndexInfo& index_info);

    k2::dto::SKVRecord DeriveTableHeadRecord(const std::string& collection_name, std::shared_ptr<TableInfo> table);

    k2::dto::SKVRecord DeriveIndexHeadRecord(const std::string& collection_name, const IndexInfo& index, bool is_sys_table, int32_t next_column_id);

    std::vector<k2::dto::SKVRecord> DeriveTableColumnRecords(const std::string& collection_name, std::shared_ptr<TableInfo> table);

    std::vector<k2::dto::SKVRecord> DeriveIndexColumnRecords(const std::string& collection_name, const IndexInfo& index, const Schema& base_tablecolumn_schema);

    k2::dto::FieldType ToK2Type(DataType type);

    DataType ToSqlType(k2::dto::FieldType type);

    Status FetchTableHeadSKVRecord(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id, k2::dto::SKVRecord& resultSKVRecord);

    // TODO: change following API return Status instead of throw exception

    std::vector<k2::dto::SKVRecord> FetchIndexHeadSKVRecords(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& base_table_id);

    std::vector<k2::dto::SKVRecord> FetchTableColumnSchemaSKVRecords(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id);

    std::vector<k2::dto::SKVRecord> FetchIndexColumnSchemaSKVRecords(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id);

    std::shared_ptr<TableInfo> BuildTableInfo(const std::string& database_id, const std::string& database_name, k2::dto::SKVRecord& table_head, std::vector<k2::dto::SKVRecord>& table_columns);

    IndexInfo FetchAndBuildIndexInfo(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, k2::dto::SKVRecord& index_head);

    void AddDefaultPartitionKeys(std::shared_ptr<k2::dto::Schema> schema);

    k2::dto::SKVRecord buildRangeRecord(const std::string& collection_name, std::shared_ptr<k2::dto::Schema> schema_ptr, std::optional<std::string> table_id);

    std::shared_ptr<k2::dto::Schema> tablehead_schema_ptr_;
    std::shared_ptr<k2::dto::Schema> tablecolumn_schema_ptr_;
    std::shared_ptr<k2::dto::Schema> indexcolumn_schema_ptr_;

    std::shared_ptr<K2Adapter> k2_adapter_;
};

} // namespace catalog
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_TABLE_INFO_HANDLER_H
