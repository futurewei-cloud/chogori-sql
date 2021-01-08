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

#include "yb/pggate/catalog/table_info_handler.h"

#include <stdexcept>

#include <glog/logging.h>

namespace k2pg {
namespace sql {
namespace catalog {

TableInfoHandler::TableInfoHandler(std::shared_ptr<K2Adapter> k2_adapter)
    : BaseHandler(k2_adapter),
    tablehead_schema_name_(CatalogConsts::skv_schema_name_sys_catalog_tablehead),
    tablecolumn_schema_name_(CatalogConsts::skv_schema_name_sys_catalog_tablecolumn),
    indexcolumn_schema_name_(CatalogConsts::skv_schema_name_sys_catalog_indexcolumn) {
    tablehead_schema_ptr_ = std::make_shared<k2::dto::Schema>(sys_catalog_tablehead_schema_);
    tablecolumn_schema_ptr_ = std::make_shared<k2::dto::Schema>(sys_catalog_tablecolumn_schema_);
    indexcolumn_schema_ptr_ = std::make_shared<k2::dto::Schema>(sys_catalog_indexcolumn_schema_);
}

TableInfoHandler::~TableInfoHandler() {
}

CreateSysTablesResult TableInfoHandler::CheckAndCreateSystemTables(std::shared_ptr<SessionTransactionContext> context, std::string collection_name) {
    CreateSysTablesResult response;
    // TODO: use sequential calls for now, could be optimized later for concurrent SKV api calls
    K2DEBUG("Creating SKV schema for collection: " << collection_name << ", name: " << tablehead_schema_name_);
    CheckSysTableResult result = CheckAndCreateSysTable(context, collection_name, tablehead_schema_name_, tablehead_schema_ptr_);
    if (!result.status.IsSucceeded()) {
        K2ERROR("Failed to create SKV schema for collection: " << collection_name << ", name: " << tablehead_schema_name_
            << " due to " << result.status.errorMessage);
        response.status = std::move(result.status);
        return response;
    }

    K2DEBUG("Creating SKV schema for collection: " << collection_name << ", name: " << tablecolumn_schema_name_);
    result = CheckAndCreateSysTable(context, collection_name, tablecolumn_schema_name_, tablecolumn_schema_ptr_);
    if (!result.status.IsSucceeded()) {
        K2ERROR("Failed to create SKV schema for collection: " << collection_name << ", name: " << tablecolumn_schema_name_
            << " due to " << result.status.errorMessage);
        response.status = std::move(result.status);
        return response;
    }

    K2DEBUG("Creating SKV schema for collection: " << collection_name << ", name: " << indexcolumn_schema_name_);
    result = CheckAndCreateSysTable(context, collection_name, indexcolumn_schema_name_, indexcolumn_schema_ptr_);
     if (!result.status.IsSucceeded()) {
        K2ERROR("Failed to create SKV schema for collection: " << collection_name << ", name: " << indexcolumn_schema_name_
            << " due to " << result.status.errorMessage);
        response.status = std::move(result.status);
        return response;
    }

    response.status.Succeed();
    K2DEBUG("Created SKV schema for system tables successfully");
    return response;
}

CheckSysTableResult TableInfoHandler::CheckAndCreateSysTable(std::shared_ptr<SessionTransactionContext> context, std::string collection_name,
        std::string schema_name, std::shared_ptr<k2::dto::Schema> schema) {
    // check if the schema already exists or not, which is an indication of whether if we have created the table or not
    std::future<k2::GetSchemaResult> schema_result_future = k2_adapter_->GetSchema(collection_name, schema_name, 1);
    k2::GetSchemaResult schema_result = schema_result_future.get();
    CheckSysTableResult response;
    // TODO: double check if this check is valid for schema
    if (schema_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        K2DEBUG("Table " << schema_name << " does not exist in " << collection_name);
        // create the table schema since it does not exist
        K2DEBUG("Creating SKV schema " << schema_name << " in " << collection_name);
        RStatus schema_result = CreateSKVSchema(collection_name, schema);
        if (!schema_result.IsSucceeded()) {
            K2ERROR("Failed to create SKV schema " << schema_name << " in " << collection_name << " due to " << schema_result.errorMessage);
            response.status = std::move(schema_result);
            return response;
        }
        K2DEBUG("Created SKV schema " << schema_name << " in " << collection_name);
    } else {
        K2WARN("SKV schema " << schema_name << " already exists in " << collection_name);
    }
    response.status.Succeed();
    return response;
}

CreateUpdateTableResult TableInfoHandler::CreateOrUpdateTable(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::shared_ptr<TableInfo> table) {
    CreateUpdateTableResult response;
    // persist system catalog entries to sys tables
    PersistSysTableResult sys_table_result = PersistSysTable(context, collection_name, table);
    if (!sys_table_result.status.IsSucceeded()) {
        response.status = std::move(sys_table_result.status);
        return response;
    }
    // persist SKV table and index schemas
    CreateUpdateSKVSchemaResult skv_schema_result = CreateOrUpdateTableSKVSchema(context, collection_name, table);
    if (skv_schema_result.status.IsSucceeded()) {
        response.status.Succeed();
    } else {
        response.status = std::move(skv_schema_result.status);
    }
    return response;
}

GetTableResult TableInfoHandler::GetTable(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, std::string namespace_name, std::string table_id) {
    K2DEBUG("Fetch table schema ns id: " << namespace_id << " ns name: " << namespace_name << ", table id: " << table_id);
    GetTableResult response;
    // use namespace_id, not namespace_name as the collection name
    // in this we could implement rename database easily by sticking to the same namespace_id
    // after the namespace_name change
    // same purpose for using table_id instead of table_name
    k2::dto::SKVRecord table_head_record = FetchTableHeadSKVRecord(context, namespace_id, table_id);
    std::vector<k2::dto::SKVRecord> table_column_records = FetchTableColumnSchemaSKVRecords(context, namespace_id, table_id);
    std::shared_ptr<TableInfo> table_info = BuildTableInfo(namespace_id, namespace_name, table_head_record, table_column_records);
    // check all the indexes whose IndexedTableId is table_id
    std::vector<k2::dto::SKVRecord> index_records = FetchIndexHeadSKVRecords(context, namespace_id, table_id);
    if (!index_records.empty()) {
        // table has indexes defined
        for (auto& index_record : index_records) {
            // Fetch and build each index table
            IndexInfo index_info = FetchAndBuildIndexInfo(context, namespace_id, index_record);
            // populate index to table_info
            table_info->add_secondary_index(index_info.table_id(), index_info);
        }
    }
    response.status.Succeed();
    response.tableInfo = table_info;
    return response;
}

ListTablesResult TableInfoHandler::ListTables(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, std::string namespace_name, bool isSysTableIncluded) {
    ListTablesResult response;
    ListTableIdsResult list_result = ListTableIds(context, namespace_id, isSysTableIncluded);
    if (!list_result.status.IsSucceeded()) {
        response.status = std::move(list_result.status);
        return response;
    }
    for (auto& table_id : list_result.tableIds) {
        GetTableResult result = GetTable(context, namespace_id, namespace_name, table_id);
        if (!result.status.IsSucceeded()) {
            response.status = std::move(result.status);
            return response;
        }
        response.tableInfos.push_back(std::move(result.tableInfo));
    }
    response.status.Succeed();
    return response;
}

ListTableIdsResult TableInfoHandler::ListTableIds(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, bool isSysTableIncluded) {
    ListTableIdsResult response;
    std::future<CreateScanReadResult> create_result_future = k2_adapter_->CreateScanRead(namespace_id, tablehead_schema_name_);
    CreateScanReadResult create_result = create_result_future.get();
    if (!create_result.status.is2xxOK()) {
        K2ERROR("Failed to create scan read due to error code " << create_result.status.code
            << " and message: " << create_result.status.message);
        std::ostringstream oss;
        oss << "Failed to create scan read for " << tablehead_schema_name_ <<  " in " << namespace_id << " due to " << create_result.status.message;
        response.status.code = StatusCode::INTERNAL_ERROR;
        response.status.errorMessage = std::move(oss.str());
        return response;
    }

    std::shared_ptr<k2::Query> query = create_result.query;
    std::vector<k2::dto::expression::Value> values;
    // TODO: consider to use the same additional table_id /index_id fields for the fixed system tables
    // find all the tables (not include indexes)
    values.emplace_back(k2::dto::expression::makeValueReference("IsIndex"));
    values.emplace_back(k2::dto::expression::makeValueLiteral<bool>(false));
    k2::dto::expression::Expression filterExpr = k2::dto::expression::makeExpression(k2::dto::expression::Operation::EQ, std::move(values), {});
    query->setFilterExpression(std::move(filterExpr));

    k2::dto::SKVRecord start_record(namespace_id, tablehead_schema_ptr_);
    // SchemaTableId
    start_record.serializeNext<k2::String>(tablehead_schema_ptr_->name);
    // SchemaIndexId
    start_record.serializeNext<k2::String>("");
    query->startScanRecord = std::move(start_record);

    k2::dto::SKVRecord end_record(namespace_id, tablehead_schema_ptr_);
    // SchemaTableId
    end_record.serializeNext<k2::String>(tablehead_schema_ptr_->name);
    // SchemaIndexId
    end_record.serializeNext<k2::String>("");
    query->endScanRecord = std::move(end_record);
    do {
        std::future<k2::QueryResult> query_result_future = context->GetTxn()->scanRead(query);
        k2::QueryResult query_result = query_result_future.get();
        if (!query_result.status.is2xxOK()) {
            K2ERROR("Failed to run scan read due to error code " << query_result.status.code
                << " and message: " << query_result.status.message);
            std::ostringstream oss;
            oss << "Failed to scan " << tablehead_schema_name_ <<  " in " << namespace_id << " due to " << query_result.status.message;
            response.status.code = StatusCode::INTERNAL_ERROR;
            response.status.errorMessage = std::move(oss.str());
            return response;
        }

        if (!query_result.records.empty()) {
            for (k2::dto::SKVRecord& record : query_result.records) {
                // deserialize table head
                // SchemaTableId
                record.skipNext();
                // SchemaIndexId
                record.skipNext();
                // TableId
                std::string table_id = record.deserializeNext<k2::String>().value();
                // TableName
                record.skipNext();
                // TableOid
                record.skipNext();
                // IsSysTable
                bool is_sys_table = record.deserializeNext<bool>().value();
                if (isSysTableIncluded) {
                    response.tableIds.push_back(std::move(table_id));
                } else {
                    if (!is_sys_table) {
                        response.tableIds.push_back(std::move(table_id));
                    }
                }
            }
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
    } while (!query->isDone());

    response.status.Succeed();
    return response;
}

CopyTableResult TableInfoHandler::CopyTable(std::shared_ptr<SessionTransactionContext> target_context,
            const std::string& target_namespace_id,
            const std::string& target_namespace_name,
            uint32_t target_namespace_oid,
            std::shared_ptr<SessionTransactionContext> source_context,
            const std::string& source_namespace_id,
            const std::string& source_namespace_name,
            const std::string& source_table_id) {
    CopyTableResult response;
    GetTableResult table_result = GetTable(source_context, source_namespace_id, source_namespace_name, source_table_id);
    if (!table_result.status.IsSucceeded()) {
        response.status = std::move(table_result.status);
        return response;
    }
    if (table_result.tableInfo == nullptr) {
        response.status.code = StatusCode::NOT_FOUND;
        response.status.errorMessage = "Cannot find table " + source_table_id;
        return response;
    }

    std::string target_table_id = GetPgsqlTableId(target_namespace_oid, table_result.tableInfo->pg_oid());
    std::shared_ptr<TableInfo> target_table = TableInfo::Clone(table_result.tableInfo, target_namespace_id,
            target_namespace_name, target_table_id, table_result.tableInfo->table_name());
    CreateUpdateTableResult create_result = CreateOrUpdateTable(target_context, target_namespace_id, target_table);
    if (!create_result.status.IsSucceeded()) {
        response.status = std::move(create_result.status);
        return response;
    }
    response.tableInfo = target_table;
    response.status.Succeed();
    return response;
}


CheckSKVSchemaResult TableInfoHandler::CheckSKVSchema(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string schema_name, uint32_t version) {
    CheckSKVSchemaResult response;
    // use the same schema version in PG for SKV schema version
    std::future<k2::GetSchemaResult> schema_result_future = k2_adapter_->GetSchema(collection_name, schema_name, version);
    k2::GetSchemaResult schema_result = schema_result_future.get();
    if (schema_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        response.schema = nullptr;
        response.status.Succeed();
    } else if (schema_result.status.is2xxOK()) {
        if(schema_result.schema != nullptr) {
            response.schema = std::move(schema_result.schema);
            response.status.Succeed();
        } else {
            response.schema = nullptr;
            response.status.Succeed();
        }
    } else {
        response.status.code = StatusCode::INTERNAL_ERROR;
        response.status.errorMessage = std::move(schema_result.status.message);
    }

    return response;
}

CreateUpdateSKVSchemaResult TableInfoHandler::CreateOrUpdateTableSKVSchema(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::shared_ptr<TableInfo> table) {
    CreateUpdateSKVSchemaResult response;
    // use table id (string) instead of table name as the schema name
    CheckSKVSchemaResult check_result = CheckSKVSchema(context, collection_name, table->table_id(), table->schema().version());
    if (check_result.status.IsSucceeded()) {
        if (check_result.schema == nullptr) {
            // build the SKV schema from TableInfo, i.e., PG table schema
            std::shared_ptr<k2::dto::Schema> tablecolumn_schema = DeriveSKVTableSchema(table);
            RStatus table_status = CreateSKVSchema(collection_name, tablecolumn_schema);
            if (!table_status.IsSucceeded()) {
                response.status = std::move(table_status);
                return response;
            }

            if (table->has_secondary_indexes()) {
                std::vector<std::shared_ptr<k2::dto::Schema>> index_schemas = DeriveIndexSchemas(table);
                for (std::shared_ptr<k2::dto::Schema> index_schema : index_schemas) {
                    // use sequential SKV writes for now, could optimize this later
                    RStatus index_status = CreateSKVSchema(collection_name, index_schema);
                    if (!index_status.IsSucceeded()) {
                        response.status = std::move(table_status);
                        return response;
                    }
                }
            }
        }
        response.status.Succeed();
    } else {
        response.status.code = check_result.status.code;
        response.status.errorMessage = std::move(check_result.status.errorMessage);
    }

    return response;
}

CreateUpdateSKVSchemaResult TableInfoHandler::CreateOrUpdateIndexSKVSchema(std::shared_ptr<SessionTransactionContext> context, std::string collection_name,
        std::shared_ptr<TableInfo> table, const IndexInfo& index_info) {
    CreateUpdateSKVSchemaResult response;
    std::shared_ptr<k2::dto::Schema> index_schema = DeriveIndexSchema(index_info, table->schema());
    response.status = CreateSKVSchema(collection_name, index_schema);
    return response;
}

PersistSysTableResult TableInfoHandler::PersistSysTable(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::shared_ptr<TableInfo> table) {
    PersistSysTableResult response;
    // use sequential SKV writes for now, could optimize this later
    k2::dto::SKVRecord tablelist_table_record = DeriveTableHeadRecord(collection_name, table);
    RStatus table_status = PersistSKVRecord(context, tablelist_table_record);
    if (!table_status.IsSucceeded()) {
        response.status = std::move(table_status);
        return response;
    }
    std::vector<k2::dto::SKVRecord> table_column_records = DeriveTableColumnRecords(collection_name, table);
    for (auto& table_column_record : table_column_records) {
        RStatus column_status = PersistSKVRecord(context, table_column_record);
        if (!column_status.IsSucceeded()) {
            response.status = std::move(column_status);
            return response;
        }
    }
    if (table->has_secondary_indexes()) {
        for( const auto& pair : table->secondary_indexes()) {
            PersistIndexTableResult index_result = PersistIndexTable(context, collection_name, table, pair.second);
            if (!index_result.status.IsSucceeded()) {
                response.status = std::move(index_result.status);
                return response;
            }
        }
    }
    response.status.Succeed();
    return response;
}

PersistIndexTableResult TableInfoHandler::PersistIndexTable(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::shared_ptr<TableInfo> table,
        const IndexInfo& index_info) {
    PersistIndexTableResult response;
    k2::dto::SKVRecord tablelist_index_record = DeriveIndexHeadRecord(collection_name, index_info, table->is_sys_table(), table->next_column_id());
    K2DEBUG("Persisting SKV record tablelist_index_record id: " << index_info.table_id() << ", name: " << index_info.table_name());
    RStatus table_status = PersistSKVRecord(context, tablelist_index_record);
    if (!table_status.IsSucceeded()) {
        response.status = std::move(table_status);
        return response;
    }
    std::vector<k2::dto::SKVRecord> index_column_records = DeriveIndexColumnRecords(collection_name, index_info, table->schema());
    for (auto& index_column_record : index_column_records) {
        K2DEBUG("Persisting SKV record index_column_record id: " << index_info.table_id() << ", name: " << index_info.table_name());
        RStatus index_status = PersistSKVRecord(context, index_column_record);
        if (!index_status.IsSucceeded()) {
            response.status = std::move(index_status);
            return response;
        }
    }
    response.status.Succeed();
    return response;
}

// Delete table_info and the related index_info from tablehead, tablecolumn, and indexcolumn system tables
DeleteTableResult TableInfoHandler::DeleteTableMetadata(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::shared_ptr<TableInfo> table) {
    DeleteTableResult response;
    // first delete indexes
    std::vector<k2::dto::SKVRecord> index_records = FetchIndexHeadSKVRecords(context, collection_name, table->table_id());
    if (!index_records.empty()) {
        for (auto& record : index_records) {
            // get table id for the index
            std::string index_id = record.deserializeNext<k2::String>().value();
            // delete index columns and the index head
            DeleteIndexResult index_result = DeleteIndexMetadata(context, collection_name, index_id);
            if (!index_result.status.IsSucceeded()) {
                response.status = std::move(index_result.status);
                return response;
            }
        }
    }

    // then delete the table metadata itself
    // first, fetch the table columns
    std::vector<k2::dto::SKVRecord> table_columns = FetchTableColumnSchemaSKVRecords(context, collection_name, table->table_id());
    RStatus columns_result = BatchDeleteSKVRecords(context, table_columns);
    if (!columns_result.IsSucceeded()) {
        response.status = std::move(columns_result);
    } else {
        // fetch table head
        k2::dto::SKVRecord table_head = FetchTableHeadSKVRecord(context, collection_name, table->table_id());
        // then delete table head record
        RStatus head_result = DeleteSKVRecord(context, table_head);
        if (!head_result.IsSucceeded()) {
            response.status = std::move(head_result);
        } else {
            response.status.Succeed();
        }
    }

    return response;
}

// Delete the actual table records from SKV that are stored with the SKV schema name to be table_id as in table_info
DeleteTableResult TableInfoHandler::DeleteTableData(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::shared_ptr<TableInfo> table) {
    DeleteTableResult response;
    // TODO: add a task to delete the actual data from SKV

    response.status.Succeed();
    return response;
}

// Delete index_info from tablehead and indexcolumn system tables
DeleteIndexResult TableInfoHandler::DeleteIndexMetadata(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string& index_id) {
    DeleteIndexResult response;
    // fetch index columns first
    std::vector<k2::dto::SKVRecord> index_columns = FetchIndexColumnSchemaSKVRecords(context, collection_name, index_id);
    // delete index columns first
    RStatus columns_result = BatchDeleteSKVRecords(context, index_columns);
    if (!columns_result.IsSucceeded()) {
        response.status = std::move(columns_result);
    } else {
        // fetch index head
        k2::dto::SKVRecord index_head = FetchTableHeadSKVRecord(context, collection_name, index_id);
        // then delete index head record
        RStatus head_result = DeleteSKVRecord(context, index_head);
        if (!head_result.IsSucceeded()) {
            response.status = std::move(head_result);
        } else {
            response.status.Succeed();
        }
    }
    return response;
}

// Delete the actual index records from SKV that are stored with the SKV schema name to be table_id as in index_info
DeleteIndexResult TableInfoHandler::DeleteIndexData(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string& index_id) {
    DeleteIndexResult response;
    // TODO: add a task to delete the actual data from SKV

    response.status.Succeed();
    return response;
}

GeBaseTableIdResult TableInfoHandler::GeBaseTableId(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string index_id) {
    GeBaseTableIdResult response;
    try {
        // exception would be thrown if the record could not be found
        k2::dto::SKVRecord index_head = FetchTableHeadSKVRecord(context, collection_name, index_id);
        // SchemaTableId
        index_head.skipNext();
        // SchemaIndexId
        index_head.skipNext();
        // TableId
        index_head.skipNext();
        // TableName
        index_head.skipNext();
        // TableOid
        index_head.skipNext();
        // IsSysTable
        index_head.skipNext();
        // IsTransactional
        index_head.skipNext();
        // IsIndex
        index_head.skipNext();
        // IsUnique
        index_head.skipNext();
        // IndexedTableId
        response.baseTableId = index_head.deserializeNext<k2::String>().value();
        response.status.Succeed();
    } catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }

    return response;
}

TableOrIndexResult TableInfoHandler::IsIndexTable(std::shared_ptr<SessionTransactionContext> context, std::string namespace_id, std::string table_id)
{
    TableOrIndexResult response;
    try {
        k2::dto::SKVRecord record = FetchTableHeadSKVRecord(context, namespace_id, table_id);
        // SchemaTableId
        record.skipNext();
        // SchemaIndexId
        record.skipNext();
        // TableId
        record.skipNext();
        // TableName
        record.skipNext();
        // TableOid
        record.skipNext();
        // IsSysTable
        record.skipNext();
        // IsTransactional
        record.skipNext();
        // IsIndex
        response.isIndex = record.deserializeNext<bool>().value();
        response.status.Succeed();
    } catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }

    return response;
}

void TableInfoHandler::AddDefaultPartitionKeys(std::shared_ptr<k2::dto::Schema> schema) {
    // "TableId"
    k2::dto::SchemaField table_id_field;
    table_id_field.type = k2::dto::FieldType::STRING;
    table_id_field.name = CatalogConsts::TABLE_ID_COLUMN_NAME;
    schema->fields.push_back(table_id_field);
    schema->partitionKeyFields.push_back(0);
    // "IndexId"
    k2::dto::SchemaField index_id_field;
    index_id_field.type = k2::dto::FieldType::STRING;
    index_id_field.name = CatalogConsts::INDEX_ID_COLUMN_NAME;
    schema->fields.push_back(index_id_field);
    schema->partitionKeyFields.push_back(1);
}

std::shared_ptr<k2::dto::Schema> TableInfoHandler::DeriveSKVTableSchema(std::shared_ptr<TableInfo> table) {
    K2DEBUG("Deriving SKV schema for table " << table->table_id());
    std::shared_ptr<k2::dto::Schema> schema = std::make_shared<k2::dto::Schema>();
    schema->name = table->table_id();
    schema->version = table->schema().version();
    // add two partitionkey fields
    AddDefaultPartitionKeys(schema);
    uint32_t count = 2;
    for (ColumnSchema col_schema : table->schema().columns()) {
        k2::dto::SchemaField field;
        field.type = ToK2Type(col_schema.type());
        field.name = col_schema.name();
        switch (col_schema.sorting_type()) {
            case ColumnSchema::SortingType::kAscending: {
                field.descending = false;
                field.nullLast = false;
            } break;
            case ColumnSchema::SortingType::kDescending: {
                field.descending = true;
                field.nullLast = false;
            } break;
             case ColumnSchema::SortingType::kAscendingNullsLast: {
                field.descending = false;
                field.nullLast = true;
            } break;
            case ColumnSchema::SortingType::kDescendingNullsLast: {
                field.descending = true;
                field.nullLast = true;
            } break;
            default: break;
       }
       schema->fields.push_back(field);
       if (col_schema.is_primary()) {
           schema->partitionKeyFields.push_back(count);
       }
       count++;
    }

    return schema;
}

std::vector<std::shared_ptr<k2::dto::Schema>> TableInfoHandler::DeriveIndexSchemas(std::shared_ptr<TableInfo> table) {
    std::vector<std::shared_ptr<k2::dto::Schema>> response;
    const IndexMap& index_map = table->secondary_indexes();
    for (const auto& pair : index_map) {
        response.push_back(DeriveIndexSchema(pair.second, table->schema()));
    }
    return response;
}

std::shared_ptr<k2::dto::Schema> TableInfoHandler::DeriveIndexSchema(const IndexInfo& index_info, const Schema& base_tablecolumn_schema) {
    K2DEBUG("Deriving SKV schema for index " << index_info.table_id());
    std::shared_ptr<k2::dto::Schema> schema = std::make_shared<k2::dto::Schema>();
    schema->name = index_info.table_id();
    schema->version = index_info.version();
    // add two partitionkey fields: base table id + index table id
    AddDefaultPartitionKeys(schema);
    uint32_t count = 2;
    for (IndexColumn indexcolumn_schema : index_info.columns()) {
        k2::dto::SchemaField field;
        field.name = indexcolumn_schema.column_name;
        if (indexcolumn_schema.indexed_column_id != -1) {
            int column_idx = base_tablecolumn_schema.find_column_by_id(indexcolumn_schema.indexed_column_id);
            if (column_idx == Schema::kColumnNotFound) {
                throw std::invalid_argument("Cannot find base column " + indexcolumn_schema.indexed_column_id);
            }
            const ColumnSchema& col_schema = base_tablecolumn_schema.column(column_idx);
            field.type = ToK2Type(col_schema.type());
            switch (col_schema.sorting_type()) {
                case ColumnSchema::SortingType::kAscending: {
                    field.descending = false;
                    field.nullLast = false;
                } break;
                case ColumnSchema::SortingType::kDescending: {
                    field.descending = true;
                    field.nullLast = false;
                } break;
                case ColumnSchema::SortingType::kAscendingNullsLast: {
                    field.descending = false;
                    field.nullLast = true;
                } break;
                case ColumnSchema::SortingType::kDescendingNullsLast: {
                    field.descending = true;
                    field.nullLast = true;
                } break;
                default: break;
            }
        } else {
            // extra index column such as "ybuniqueidxkeysuffix" or "ybidxbasectid"
            field.type = k2::dto::FieldType::STRING;
        }
        schema->fields.push_back(field);
        // all index columns should be treated as primary keys
        schema->partitionKeyFields.push_back(count);
        count++;
    }

    return schema;
}

k2::dto::SKVRecord TableInfoHandler::DeriveTableHeadRecord(std::string collection_name, std::shared_ptr<TableInfo> table) {
    k2::dto::SKVRecord record(collection_name, tablehead_schema_ptr_);
    // SchemaTableId
    record.serializeNext<k2::String>(tablehead_schema_ptr_->name);
    // SchemaIndexId
    record.serializeNext<k2::String>("");
    // TableId
    record.serializeNext<k2::String>(table->table_id());
    // TableName
    record.serializeNext<k2::String>(table->table_name());
    // TableOid
    record.serializeNext<int64_t>(table->pg_oid());
    // IsSysTable
    record.serializeNext<bool>(table->is_sys_table());
    // IsTransactional
    record.serializeNext<bool>(table->schema().table_properties().is_transactional());
    // IsIndex
    record.serializeNext<bool>(false);
    // IsUnique (for index)
    record.serializeNext<bool>(false);
    // IndexedTableId
    record.skipNext();
    // IndexPermission
    record.skipNext();
    // NextColumnId
    record.serializeNext<int32_t>(table->next_column_id());
    // SchemaVersion
    record.serializeNext<int32_t>(table->schema().version());

    return record;
}

k2::dto::SKVRecord TableInfoHandler::DeriveIndexHeadRecord(std::string collection_name, const IndexInfo& index, bool is_sys_table, int32_t next_column_id) {
    k2::dto::SKVRecord record(collection_name, tablehead_schema_ptr_);
    // SchemaTableId
    record.serializeNext<k2::String>(tablehead_schema_ptr_->name);
    // SchemaIndexId
    record.serializeNext<k2::String>("");
    // TableId
    record.serializeNext<k2::String>(index.table_id());
    // TableName
    record.serializeNext<k2::String>(index.table_name());
    // TableOid
    record.serializeNext<int64_t>(index.pg_oid());
    // IsSysTable
    record.serializeNext<bool>(is_sys_table);
    // IsTransactional
    record.serializeNext<bool>(true);
    // IsIndex
    record.serializeNext<bool>(true);
    // IsUnique (for index)
    record.serializeNext<bool>(index.is_unique());
    // IndexedTableId
    record.serializeNext<k2::String>(index.indexed_table_id());
    // IndexPermission
    record.serializeNext<int16_t>(index.index_permissions());
    // NextColumnId
    record.serializeNext<int32_t>(next_column_id);
    // SchemaVersion
    record.serializeNext<int32_t>(index.version());

    return record;
}

std::vector<k2::dto::SKVRecord> TableInfoHandler::DeriveTableColumnRecords(std::string collection_name, std::shared_ptr<TableInfo> table) {
    std::vector<k2::dto::SKVRecord> response;
    for (std::size_t i = 0; i != table->schema().columns().size(); ++i) {
        ColumnSchema col_schema = table->schema().columns()[i];
        int32_t column_id = table->schema().column_ids()[i];
        k2::dto::SKVRecord record(collection_name, tablecolumn_schema_ptr_);
        // SchemaTableId
        record.serializeNext<k2::String>(tablecolumn_schema_ptr_->name);
        // SchemaIndexId
        record.serializeNext<k2::String>("");
        // TableId
        record.serializeNext<k2::String>(table->table_id());
        // ColumnId
        record.serializeNext<int32_t>(column_id);
        // ColumnName
        record.serializeNext<k2::String>(col_schema.name());
        // ColumnType
        record.serializeNext<int16_t>(col_schema.type()->id());
        // IsNullable
        record.serializeNext<bool>(col_schema.is_nullable());
        // IsPrimary
        record.serializeNext<bool>(col_schema.is_primary());
        // IsPartition
        record.serializeNext<bool>(col_schema.is_partition());
        // Order
        record.serializeNext<int32_t>(col_schema.order());
        // SortingType
        record.serializeNext<int16_t>(col_schema.sorting_type());

        response.push_back(std::move(record));
    }
    return response;
}

std::vector<k2::dto::SKVRecord> TableInfoHandler::DeriveIndexColumnRecords(std::string collection_name, const IndexInfo& index, const Schema& base_tablecolumn_schema) {
    std::vector<k2::dto::SKVRecord> response;
    for (IndexColumn index_column : index.columns()) {
        k2::dto::SKVRecord record(collection_name, indexcolumn_schema_ptr_);
        // SchemaTableId
        record.serializeNext<k2::String>(indexcolumn_schema_ptr_->name);
        // SchemaIndexId
        record.serializeNext<k2::String>("");
        // TableId
        record.serializeNext<k2::String>(index.table_id());
        // ColumnId
        record.serializeNext<int32_t>(index_column.column_id);
        // ColumnName
        record.serializeNext<k2::String>(index_column.column_name);
        // IndexedColumnId
        record.serializeNext<int32_t>(index_column.indexed_column_id);
        response.push_back(std::move(record));
    }
    return response;
}

k2::dto::FieldType TableInfoHandler::ToK2Type(std::shared_ptr<SQLType> type) {
    k2::dto::FieldType field_type = k2::dto::FieldType::NOT_KNOWN;
    switch (type->id()) {
        case DataType::UINT8: {
            field_type = k2::dto::FieldType::INT64T;
        } break;
        case DataType::INT8: {
            field_type = k2::dto::FieldType::INT64T;
        } break;
        case DataType::UINT16: {
            field_type = k2::dto::FieldType::INT64T;
        } break;
        case DataType::INT16: {
            field_type = k2::dto::FieldType::INT64T;
        } break;
        case DataType::UINT32: {
            field_type = k2::dto::FieldType::INT64T;
        } break;
        case DataType::INT32: {
            field_type = k2::dto::FieldType::INT64T;
        } break;
        case DataType::UINT64: {
            field_type = k2::dto::FieldType::INT64T;
        } break;
        case DataType::INT64: {
            field_type = k2::dto::FieldType::INT64T;
        } break;
        case DataType::STRING: {
            field_type = k2::dto::FieldType::STRING;
        } break;
        case DataType::BOOL: {
            field_type = k2::dto::FieldType::BOOL;
        } break;
        case DataType::FLOAT: {
            field_type = k2::dto::FieldType::FLOAT;
        } break;
        case DataType::DOUBLE: {
            field_type = k2::dto::FieldType::DOUBLE;
        } break;
        case DataType::BINARY: {
            field_type = k2::dto::FieldType::STRING;
        } break;
        case DataType::TIMESTAMP: {
            field_type = k2::dto::FieldType::INT64T;
        } break;
        case DataType::DECIMAL: {
            field_type = k2::dto::FieldType::DECIMAL64;
        } break;
        default:
            throw std::invalid_argument("Unsupported type " + type->id());
    }
    return field_type;
}

DataType TableInfoHandler::ToSqlType(k2::dto::FieldType type) {
    // utility method, not used yet
    DataType sql_type = DataType::NOT_SUPPORTED;
    switch (type) {
        case k2::dto::FieldType::INT16T: {
            sql_type = DataType::INT16;
        } break;
        case k2::dto::FieldType::INT32T: {
            sql_type = DataType::INT32;
        } break;
        case k2::dto::FieldType::INT64T: {
            sql_type = DataType::INT64;
        } break;
        case k2::dto::FieldType::STRING: {
            sql_type = DataType::STRING;
        } break;
        case k2::dto::FieldType::BOOL: {
            sql_type = DataType::BOOL;
        } break;
        case k2::dto::FieldType::FLOAT: {
            sql_type = DataType::FLOAT;
        } break;
        case k2::dto::FieldType::DOUBLE: {
            sql_type = DataType::DOUBLE;
        } break;
        case k2::dto::FieldType::DECIMAL64: {
            sql_type = DataType::DECIMAL;
        } break;
        default: {
            std::ostringstream oss;
            oss << "Unsupported Type: " << type;
            throw std::invalid_argument(oss.str());
        }
    }
    return sql_type;
}

k2::dto::SKVRecord TableInfoHandler::FetchTableHeadSKVRecord(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string table_id) {
    k2::dto::SKVRecord record(collection_name, tablehead_schema_ptr_);
    // SchemaTableId
    record.serializeNext<k2::String>(tablehead_schema_ptr_->name);
    // SchemaIndexId
    record.serializeNext<k2::String>("");
    // table_id
    record.serializeNext<k2::String>(table_id);
    std::future<k2::ReadResult<k2::dto::SKVRecord>> result_future = context->GetTxn()->read(std::move(record));
    k2::ReadResult<k2::dto::SKVRecord> result = result_future.get();
    // TODO: add error handling and retry logic in catalog manager
    if (result.status == k2::dto::K23SIStatus::KeyNotFound) {
        throw std::runtime_error("Cannot find entry " + table_id + " in " + collection_name);
    } else if (!result.status.is2xxOK()) {
        std::ostringstream oss;
        oss << "Error fetching entry " << table_id <<  " in " << collection_name << " due to " << result.status.message;
        throw std::runtime_error(oss.str());
    }
    return std::move(result.value);
}

std::vector<k2::dto::SKVRecord> TableInfoHandler::FetchIndexHeadSKVRecords(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string base_table_id) {
    std::future<CreateScanReadResult> create_result_future = k2_adapter_->CreateScanRead(collection_name, tablehead_schema_name_);
    CreateScanReadResult create_result = create_result_future.get();
    if (!create_result.status.is2xxOK()) {
        K2ERROR("Failed to create scan read due to error code " << create_result.status.code
            << " and message: " << create_result.status.message);
        std::ostringstream oss;
        oss << "Failed to create scan read for " << base_table_id <<  " in " << collection_name << " due to " << create_result.status.message;
        throw std::runtime_error(oss.str());
    }

    std::vector<k2::dto::SKVRecord> records;
    std::shared_ptr<k2::Query> query = create_result.query;
    std::vector<k2::dto::expression::Value> values;
    std::vector<k2::dto::expression::Expression> exps;
    // find all the indexes for the base table, i.e., by IndexedTableId
    values.emplace_back(k2::dto::expression::makeValueReference(CatalogConsts::INDEXED_TABLE_ID_COLUMN_NAME));
    values.emplace_back(k2::dto::expression::makeValueLiteral<k2::String>(base_table_id));
    k2::dto::expression::Expression filterExpr = k2::dto::expression::makeExpression(k2::dto::expression::Operation::EQ, std::move(values), std::move(exps));
    query->setFilterExpression(std::move(filterExpr));

    k2::dto::SKVRecord start_record(collection_name, tablehead_schema_ptr_);
    // SchemaTableId
    start_record.serializeNext<k2::String>(tablehead_schema_ptr_->name);
    // SchemaIndexId
    start_record.serializeNext<k2::String>("");
    query->startScanRecord = std::move(start_record);

    k2::dto::SKVRecord end_record(collection_name, tablehead_schema_ptr_);
    // SchemaTableId
    end_record.serializeNext<k2::String>(tablehead_schema_ptr_->name);
    // SchemaIndexId
    end_record.serializeNext<k2::String>("");
    query->endScanRecord = std::move(end_record);
    do {
        std::future<k2::QueryResult> query_result_future = context->GetTxn()->scanRead(query);
        k2::QueryResult query_result = query_result_future.get();
        if (!query_result.status.is2xxOK()) {
            K2ERROR("Failed to run scan read due to error code " << query_result.status.code
                << " and message: " << query_result.status.message);
            std::ostringstream oss;
            oss << "Failed to create scan read for " << base_table_id <<  " in " << collection_name << " due to " << query_result.status.message;
            throw std::runtime_error(oss.str());
        }

        if (!query_result.records.empty()) {
            for (k2::dto::SKVRecord& record : query_result.records) {
                records.push_back(std::move(record));
            }
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
    } while (!query->isDone());

    return records;
}

std::vector<k2::dto::SKVRecord> TableInfoHandler::FetchTableColumnSchemaSKVRecords(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string table_id) {
    std::future<CreateScanReadResult> create_result_future = k2_adapter_->CreateScanRead(collection_name, tablecolumn_schema_name_);
    CreateScanReadResult create_result = create_result_future.get();
    if (!create_result.status.is2xxOK()) {
        K2ERROR("Failed to create scan read due to error code " << create_result.status.code
            << " and message: " << create_result.status.message);
        std::ostringstream oss;
        oss << "Failed to create scan read for " << table_id <<  " in " << collection_name << " due to " << create_result.status.message;
        throw std::runtime_error(oss.str());
    }

    std::vector<k2::dto::SKVRecord> records;
    auto& query = create_result.query;
    std::vector<k2::dto::expression::Value> values;
    std::vector<k2::dto::expression::Expression> exps;
    // find all the columns for a table by TableId
    values.emplace_back(k2::dto::expression::makeValueReference(CatalogConsts::TABLE_ID_COLUMN_NAME));
    values.emplace_back(k2::dto::expression::makeValueLiteral<k2::String>(table_id));
    k2::dto::expression::Expression filterExpr = k2::dto::expression::makeExpression(k2::dto::expression::Operation::EQ, std::move(values), std::move(exps));
    query->setFilterExpression(std::move(filterExpr));

    k2::dto::SKVRecord start_record(collection_name, tablecolumn_schema_ptr_);
    // SchemaTableId
    start_record.serializeNext<k2::String>(tablecolumn_schema_ptr_->name);
    // SchemaIndexId
    start_record.serializeNext<k2::String>("");
    // TableId
    start_record.serializeNext<k2::String>(table_id);
    query->startScanRecord = std::move(start_record);

    k2::dto::SKVRecord end_record(collection_name, tablecolumn_schema_ptr_);
    // SchemaTableId
    end_record.serializeNext<k2::String>(tablecolumn_schema_ptr_->name);
    // SchemaIndexId
    end_record.serializeNext<k2::String>("");
    // TableId
    end_record.serializeNext<k2::String>(table_id);
    query->endScanRecord = std::move(end_record);
    do {
        std::future<k2::QueryResult> query_result_future = context->GetTxn()->scanRead(query);
        k2::QueryResult query_result = query_result_future.get();
        if (!query_result.status.is2xxOK()) {
            K2ERROR("Failed to run scan read due to error code " << query_result.status.code
                << " and message: " << query_result.status.message);
            std::ostringstream oss;
            oss << "Failed to run scan read for " << table_id <<  " in " << collection_name << " due to " << query_result.status.message;
            throw std::runtime_error(oss.str());
        }

        if (!query_result.records.empty()) {
            for (k2::dto::SKVRecord& record : query_result.records) {
                records.push_back(std::move(record));
            }
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
    } while (!query->isDone());

    return records;
}

std::vector<k2::dto::SKVRecord> TableInfoHandler::FetchIndexColumnSchemaSKVRecords(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, std::string table_id) {
    std::future<CreateScanReadResult> create_result_future = k2_adapter_->CreateScanRead(collection_name, indexcolumn_schema_name_);
    CreateScanReadResult create_result = create_result_future.get();
    if (!create_result.status.is2xxOK()) {
        K2ERROR("Failed to create scan read due to error code " << create_result.status.code
            << " and message: " << create_result.status.message);
        std::ostringstream oss;
        oss << "Failed to create scan read for " << table_id <<  " in " << collection_name << " due to " << create_result.status.message;
        throw std::runtime_error(oss.str());
    }

    std::vector<k2::dto::SKVRecord> records;
    std::shared_ptr<k2::Query> query = create_result.query;
    std::vector<k2::dto::expression::Value> values;
    std::vector<k2::dto::expression::Expression> exps;
    // find all the columns for an index table by TableId
    values.emplace_back(k2::dto::expression::makeValueReference(CatalogConsts::TABLE_ID_COLUMN_NAME));
    values.emplace_back(k2::dto::expression::makeValueLiteral<k2::String>(table_id));
    k2::dto::expression::Expression filterExpr = k2::dto::expression::makeExpression(k2::dto::expression::Operation::EQ, std::move(values), std::move(exps));
    query->setFilterExpression(std::move(filterExpr));

    k2::dto::SKVRecord start_record(collection_name, indexcolumn_schema_ptr_);
    // SchemaTableId
    start_record.serializeNext<k2::String>(indexcolumn_schema_ptr_->name);
    // SchemaIndexId
    start_record.serializeNext<k2::String>("");
    // TableId
    start_record.serializeNext<k2::String>(table_id);
    query->startScanRecord = std::move(start_record);

    k2::dto::SKVRecord end_record(collection_name, indexcolumn_schema_ptr_);
    // SchemaTableId
    end_record.serializeNext<k2::String>(indexcolumn_schema_ptr_->name);
     // SchemaIndexId
    end_record.serializeNext<k2::String>("");
    // TableId
    end_record.serializeNext<k2::String>(table_id);
    query->endScanRecord = std::move(end_record);
    do {
        std::future<k2::QueryResult> query_result_future = context->GetTxn()->scanRead(query);
        k2::QueryResult query_result = query_result_future.get();
        if (!query_result.status.is2xxOK()) {
            K2ERROR("Failed to run scan read due to error code " << query_result.status.code
                << " and message: " << query_result.status.message);
            std::ostringstream oss;
            oss << "Failed to run scan read for " << table_id <<  " in " << collection_name << " due to " << query_result.status.message;
            throw std::runtime_error(oss.str());
        }

        if (!query_result.records.empty()) {
            for (k2::dto::SKVRecord& record : query_result.records) {
                records.push_back(std::move(record));
            }
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
    } while (!query->isDone());

    return records;
}

std::shared_ptr<TableInfo> TableInfoHandler::BuildTableInfo(std::string namespace_id, std::string namespace_name,
        k2::dto::SKVRecord& table_head, std::vector<k2::dto::SKVRecord>& table_columns) {
    // deserialize table head
    // SchemaTableId
    table_head.skipNext();
    // SchemaIndexId
    table_head.skipNext();
    // TableId
    std::string table_id = table_head.deserializeNext<k2::String>().value();
    // TableName
    std::string table_name = table_head.deserializeNext<k2::String>().value();
    // TableOid
    uint32_t table_oid = table_head.deserializeNext<int64_t>().value();
    // IsSysTable
    bool is_sys_table = table_head.deserializeNext<bool>().value();
    // IsTransactional
    bool is_transactional = table_head.deserializeNext<bool>().value();
    // IsIndex
    bool is_index = table_head.deserializeNext<bool>().value();
    if (is_index) {
        throw std::runtime_error("Table " + table_id + " should not be an index");
    }
    // IsUnique
    table_head.skipNext();
    // IndexedTableId
    table_head.skipNext();
    // IndexPermission
    table_head.skipNext();
    // NextColumnId
    int32_t next_column_id = table_head.deserializeNext<int32_t>().value();
     // SchemaVersion
    uint32_t version = table_head.deserializeNext<int32_t>().value();

    TableProperties table_properties;
    table_properties.SetTransactional(is_transactional);

    vector<ColumnSchema> cols;
    int key_columns = 0;
    vector<ColumnId> ids;
    // deserialize table columns
    for (auto& column : table_columns) {
        // SchemaTableId
        column.skipNext();
        // SchemaIndexId
        column.skipNext();
        // TableId
        std::string tb_id = column.deserializeNext<k2::String>().value();
        // ColumnId
        int32_t col_id = column.deserializeNext<int32_t>().value();
        // ColumnName
        std::string col_name = column.deserializeNext<k2::String>().value();
        // ColumnType, we persist SQL type directly as an integer
        int16_t col_type = column.deserializeNext<int16_t>().value();
        // IsNullable
        bool is_nullable = column.deserializeNext<bool>().value();
        // IsPrimary
        bool is_primary = column.deserializeNext<bool>().value();
        // IsPartition
        bool is_partition = column.deserializeNext<bool>().value();
        // Order
        int32 order = column.deserializeNext<int32_t>().value();
        // SortingType
        int16_t sorting_type = column.deserializeNext<int16_t>().value();
        ColumnSchema col_schema(col_name, static_cast<DataType>(col_type), is_nullable, is_primary, is_partition, order, static_cast<ColumnSchema::SortingType>(sorting_type));
        cols.push_back(std::move(col_schema));
        ids.push_back(col_id);
        if (is_primary) {
            key_columns++;
        }
    }
    Schema table_schema(cols, ids, key_columns, table_properties);
    table_schema.set_version(version);
    std::shared_ptr<TableInfo> table_info = std::make_shared<TableInfo>(namespace_id, namespace_name, table_id, table_name, table_schema);
    table_info->set_pg_oid(table_oid);
    table_info->set_next_column_id(next_column_id);
    table_info->set_is_sys_table(is_sys_table);
    return table_info;
}

IndexInfo TableInfoHandler::FetchAndBuildIndexInfo(std::shared_ptr<SessionTransactionContext> context, std::string collection_name, k2::dto::SKVRecord& index_head) {
    // deserialize index head
    // SchemaTableId
    index_head.skipNext();
    // SchemaIndexId
    index_head.skipNext();
    // TableId
    std::string table_id = index_head.deserializeNext<k2::String>().value();
    // TableName
    std::string table_name = index_head.deserializeNext<k2::String>().value();
    // TableOid
    uint32_t table_oid = index_head.deserializeNext<int64_t>().value();
    // IsSysTable
    index_head.skipNext();
    // IsTransactional
    index_head.skipNext();
    // IsIndex
    bool is_index = index_head.deserializeNext<bool>().value();
    if (!is_index) {
        throw std::runtime_error("Table " + table_id + " should be an index");
    }
    // IsUnique
    bool is_unique = index_head.deserializeNext<bool>().value();
    // IndexedTableId
    std::string indexed_table_id = index_head.deserializeNext<k2::String>().value();
    // IndexPermission
    IndexPermissions index_perm = static_cast<IndexPermissions>(index_head.deserializeNext<int16_t>().value());
    // NextColumnId
    index_head.skipNext();
    // SchemaVersion
    uint32_t version = index_head.deserializeNext<int32_t>().value();

    // Fetch index columns
    std::vector<k2::dto::SKVRecord> index_columns = FetchIndexColumnSchemaSKVRecords(context, collection_name, table_id);

    // deserialize index columns
    std::vector<IndexColumn> columns;
    for (auto& column : index_columns) {
        // SchemaTableId
        column.skipNext();
        // SchemaIndexId
        column.skipNext();
        // TableId
        std::string tb_id = column.deserializeNext<k2::String>().value();
        // ColumnId
        int32_t col_id = column.deserializeNext<int32_t>().value();
        // ColumnName
        std::string col_name = column.deserializeNext<k2::String>().value();
        // IndexedColumnId
        int32_t indexed_col_id = column.deserializeNext<int32_t>().value();
        // TODO: add support for expression in index
        IndexColumn index_column(col_id, col_name, indexed_col_id);
        columns.push_back(std::move(index_column));
    }

    IndexInfo index_info(table_id, table_name, table_oid, indexed_table_id, version, is_unique, columns, index_perm);
    return index_info;
}

} // namespace catalog
} // namespace sql
} // namespace k2pg
