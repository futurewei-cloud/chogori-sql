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
using k2pg::Status;

struct CreateMetaTablesResult {
    Status status;
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

struct GetTableSchemaResult {
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

struct CreateSKVSchemaResult {
    Status status;
};

struct PersistTableMetaResult {
    Status status;
};

struct PersistIndexMetaResult {
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

struct GetTableTypeInfoResult {
    Status status;
    bool isShared;
    bool isIndex;
};

struct CreateIndexTableResult {
    Status status;
    std::shared_ptr<IndexInfo> indexInfo;
};

struct CreateIndexTableParams {
    std::string index_name;
    uint32_t table_oid;
    Schema index_schema;
    bool is_unique;
    bool is_shared;
    bool is_not_exist;
    bool skip_index_backfill;
    IndexPermissions index_permissions;
};

class TableInfoHandler {
    public:
    typedef std::shared_ptr<TableInfoHandler> SharedPtr;

    TableInfoHandler(std::shared_ptr<K2Adapter> k2_adapter);
    ~TableInfoHandler();

    // Design Note: (tables mapping to SKV schema)
    // 1. Any table primary index(must have) is mapped to a SKV schema, and if it have secondary index(s), each one is mapped to its own SKV schema.
    // 2. Following three SKV schemas are for three system meta tables holding all table/index definition(aka. meta), i.e. tablemeta(table identities and basic info), tablecolumnmeta(column def), indexcolumnmeta(index column def)
    // 3. The schema name for meta tables are hardcoded constant, e.g. tablemeta's is CatalogConsts::skv_schema_name_table_meta
    // 4. The schema name for user table/secondary index are the table's TableId(), which is a string presentation of UUID containing tables's pguid (for details, see std::string PgObjectId::GetTableId(const PgOid& table_oid))
    // 5. As of now, before embedded table(s) are supported, all tables are flat in relationship with each other. Thus, all tables(meta or user) have two prefix fields "TableId" and "IndexId" in their SKV schema,
    //    so that all rows in a table and index are clustered together in K2.
    //      For a primary index, the TableId is the PgOid(uint32 but saved as int64_t in K2) of this table, and IndexId is 0
    //      For a secondary index, The TableId is the PgOid of base table(primary index), and IndexId is its own PgOid.
    //      For three system tables which is not defined in PostgreSQL originally, the PgOid of them are taken from unused system Pgoid range 4800-4803 (for detail, see CatalogConsts::oid_table_meta)

    // schema of table information
    k2::dto::Schema skv_schema_table_meta {
        .name = CatalogConsts::skv_schema_name_table_meta,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::INT64T, "SchemaTableId", false, false},        // const PgOid CatalogConsts::oid_table_meta = 4800;
                {k2::dto::FieldType::INT64T, "SchemaIndexId", false, false},        // 0
                {k2::dto::FieldType::STRING, "TableId", false, false},
                {k2::dto::FieldType::STRING, "TableName", false, false},
                {k2::dto::FieldType::INT64T, "TableOid", false, false},
                {k2::dto::FieldType::STRING, "TableUuid", false, false},
                {k2::dto::FieldType::BOOL, "IsSysTable", false, false},
                {k2::dto::FieldType::BOOL, "IsShared", false, false},
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
    k2::dto::Schema skv_schema_tablecolumn_meta {
        .name = CatalogConsts::skv_schema_name_tablecolumn_meta,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::INT64T, "SchemaTableId", false, false},    // const PgOid CatalogConsts::oid_tablecolumn_meta = 4801;
                {k2::dto::FieldType::INT64T, "SchemaIndexId", false, false},    // 0
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
    k2::dto::Schema skv_schema_indexcolumn_meta {
        .name = CatalogConsts::skv_schema_name_indexcolumn_meta,
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::INT64T, "SchemaTableId", false, false},    // const PgOid CatalogConsts::oid_indexcolumn_meta = 4802;
                {k2::dto::FieldType::INT64T, "SchemaIndexId", false, false},    // 0
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

    // create above three meta tables for a DB
    CreateMetaTablesResult CreateMetaTables(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name);

    // Create or update a user defined table fully, including all its secondary indexes if any.
    CreateUpdateTableResult CreateOrUpdateTable(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    GetTableResult GetTable(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& database_name, const std::string& table_id);
    GetTableSchemaResult GetTableSchema(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<DatabaseInfo> database_info, const std::string& table_id,
                std::shared_ptr<IndexInfo> index_info,
                std::function<std::shared_ptr<DatabaseInfo>(const std::string&)> fnc_db,
                std::function<std::shared_ptr<PgTxnHandler>()> fnc_tx);

    ListTablesResult ListTables(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& database_name, bool isSysTableIncluded);

    ListTableIdsResult ListTableIds(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, bool isSysTableIncluded);

    // CopyTable (meta and data) fully including secondary indexes, currently only support cross different database.
    CopyTableResult CopyTable(std::shared_ptr<PgTxnHandler> target_txnHandler,
            const std::string& target_coll_name,
            const std::string& target_database_name,
            uint32_t target_database_oid,
            std::shared_ptr<PgTxnHandler> source_txnHandler,
            const std::string& source_coll_name,
            const std::string& source_database_name,
            const std::string& source_table_id);

    CreateSKVSchemaResult CreateIndexSKVSchema(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name,
        std::shared_ptr<TableInfo> table, const IndexInfo& index_info);

    PersistIndexMetaResult PersistIndexMeta(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table, const IndexInfo& index_info);

    DeleteTableResult DeleteTableMetadata(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    DeleteTableResult DeleteTableData(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    DeleteIndexResult DeleteIndexMetadata(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name,  const std::string& index_id);

    DeleteIndexResult DeleteIndexData(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name,  const std::string& index_id);

    GetBaseTableIdResult GetBaseTableId(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& index_id);

    // check if passed id is that for a table or index, and if it is a shared table/index(just one instance shared by all databases and resides in primary cluster)
    GetTableTypeInfoResult GetTableTypeInfo(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id);

    // create index table (handle create if exists flag)
    CreateIndexTableResult CreateIndexTable(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<DatabaseInfo> database_info, std::shared_ptr<TableInfo> base_table_info, CreateIndexTableParams &index_params);

    private:
    CopySKVTableResult CopySKVTable(std::shared_ptr<PgTxnHandler> target_txnHandler,
            const std::string& target_coll_name,
            const std::string& target_schema_name,
            uint32_t target_schema_version,
            std::shared_ptr<PgTxnHandler> source_txnHandler,
            const std::string& source_coll_name,
            const std::string& source_schema_name,
            uint32_t source_schema_version,
            PgOid source_table_oid,
            PgOid source_index_oid);

    // A SKV Schema of perticular version is not mutable, thus, we only create a new specified version if that version doesn't exists yet
    CreateSKVSchemaResult CreateTableSKVSchema(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    // Persist (user) table's definition/meta into three sytem meta tables.
    PersistTableMetaResult PersistTableMeta(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    std::shared_ptr<k2::dto::Schema> DeriveSKVSchemaFromTableInfo(std::shared_ptr<TableInfo> table);

    std::vector<std::shared_ptr<k2::dto::Schema>> DeriveIndexSchemas(std::shared_ptr<TableInfo> table);

    std::shared_ptr<k2::dto::Schema> DeriveIndexSchema(const IndexInfo& index_info);

    k2::dto::SKVRecord DeriveTableMetaRecord(const std::string& collection_name, std::shared_ptr<TableInfo> table);

    k2::dto::SKVRecord DeriveTableMetaRecordOfIndex(const std::string& collection_name, const IndexInfo& index, bool is_sys_table, int32_t next_column_id);

    std::vector<k2::dto::SKVRecord> DeriveTableColumnMetaRecords(const std::string& collection_name, std::shared_ptr<TableInfo> table);

    std::vector<k2::dto::SKVRecord> DeriveIndexColumnMetaRecords(const std::string& collection_name, const IndexInfo& index, const Schema& base_tablecolumn_schema);

    k2::dto::FieldType ToK2Type(DataType type);

    DataType ToSqlType(k2::dto::FieldType type);

    Status FetchTableMetaSKVRecord(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id, k2::dto::SKVRecord& resultSKVRecord);

    // TODO: change following API return Status instead of throw exception

    std::vector<k2::dto::SKVRecord> FetchIndexMetaSKVRecords(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& base_table_id);

    std::vector<k2::dto::SKVRecord> FetchTableColumnMetaSKVRecords(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id);

    std::vector<k2::dto::SKVRecord> FetchIndexColumnMetaSKVRecords(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id);

    std::shared_ptr<TableInfo> BuildTableInfo(const std::string& database_id, const std::string& database_name, k2::dto::SKVRecord& table_meta, std::vector<k2::dto::SKVRecord>& table_columns);

    IndexInfo BuildIndexInfo(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, k2::dto::SKVRecord& index_table_meta);

    IndexInfo BuildIndexInfo(std::shared_ptr<TableInfo> base_table_info, std::string index_name, uint32_t table_oid, std::string index_uuid,
                const Schema& index_schema, bool is_unique, bool is_shared, IndexPermissions index_permissions);

    void AddDefaultPartitionKeys(std::shared_ptr<k2::dto::Schema> schema);

    // Build a range record for a scan, optionally using third param table_id when applicable(e.g. in sys table).
    k2::dto::SKVRecord buildRangeRecord(const std::string& collection_name, std::shared_ptr<k2::dto::Schema> schema, PgOid table_oid, PgOid index_oid, std::optional<std::string> table_id);

    std::shared_ptr<k2::dto::Schema> table_meta_SKVSchema_;
    std::shared_ptr<k2::dto::Schema> tablecolumn_meta_SKVSchema_;
    std::shared_ptr<k2::dto::Schema> indexcolumn_meta_SKVSchema_;

    std::shared_ptr<K2Adapter> k2_adapter_;
};

} // namespace catalog
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_TABLE_INFO_HANDLER_H