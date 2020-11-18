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

#include <glog/logging.h>

namespace k2pg {
namespace sql {

TableInfoHandler::TableInfoHandler(std::shared_ptr<K2Adapter> k2_adapter) 
    : k2_adapter_(k2_adapter),  
    tablelist_schema_name_(sys_catalog_tablelist_schema_name),
    table_schema_name_(sys_catalog_table_schema_schema_name), 
    index_schema_name_(sys_catalog_index_schema_schema_name) {
    tablelist_schema_ptr = std::make_shared<k2::dto::Schema>();
    *(tablelist_schema_ptr.get()) = sys_catalog_tablelist_schema;
    table_schema_ptr = std::make_shared<k2::dto::Schema>();
    *(table_schema_ptr.get()) = sys_catalog_table_schema;
    index_schema_ptr = std::make_shared<k2::dto::Schema>();
    *(index_schema_ptr.get()) = sys_catalog_index_schema;
}

TableInfoHandler::~TableInfoHandler() {
}

CreateSysTablesResult TableInfoHandler::CreateSysTablesIfNecessary(std::shared_ptr<Context> context, std::string collection_name) {
    CreateSysTablesResult response;
    // TODO: use sequential calls for now, could be optimized later for concurrent SKV api calls
    CheckSysTableResult result = CheckAndCreateSysTable(context, collection_name, tablelist_schema_name_, tablelist_schema_ptr);
    if (!result.status.succeeded) {
        response.status = std::move(result.status);
        return response;
    }

    result = CheckAndCreateSysTable(context, collection_name, table_schema_name_, table_schema_ptr);
     if (!result.status.succeeded) {
        response.status = std::move(result.status);
        return response;
    }
   
    result = CheckAndCreateSysTable(context, collection_name, index_schema_name_, index_schema_ptr);
     if (!result.status.succeeded) {
        response.status = std::move(result.status);
        return response;
    }

    response.status.succeeded = true;
    return response;
}

CheckSysTableResult TableInfoHandler::CheckAndCreateSysTable(std::shared_ptr<Context> context, std::string collection_name, 
        std::string schema_name, std::shared_ptr<k2::dto::Schema> schema) {
    // check if the schema already exists or not, which is an indication of whether if we have created the table or not
    std::future<k2::GetSchemaResult> schema_result_future = k2_adapter_->GetSchema(collection_name, schema_name, 1);
    k2::GetSchemaResult schema_result = schema_result_future.get();   
    CheckSysTableResult response;
    // TODO: double check if this check is valid for schema
    if (schema_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        response.status.succeeded = true;
        LOG(INFO) << schema_name << " table does not exist in " << collection_name; 
        // create the table schema since it does not exist
        std::future<k2::CreateSchemaResult> result_future = k2_adapter_->CreateSchema(collection_name, schema);
        k2::CreateSchemaResult result = result_future.get();
        if (!result.status.is2xxOK()) {
            LOG(FATAL) << "Failed to create SKV schema for " << schema_name << "in" << collection_name
                << " due to error code " << result.status.code
                << " and message: " << result.status.message;
            response.status.succeeded = false;
            response.status.errorCode = result.status.code;
            response.status.errorMessage = std::move(result.status.message);
            return response;            
        }        
    }
    response.status.succeeded = true;
    return response;
}

CreateUpdateTableResult TableInfoHandler::CreateOrUpdateTable(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<TableInfo> table) {
    CreateUpdateTableResult response;
    // persist system catalog entries to sys tables
    PersistSysTableResult sys_table_result = PersistSysTable(context, collection_name, table);
    if (!sys_table_result.status.succeeded) {
        response.status = std::move(sys_table_result.status);
        return response;
    }
    // persist SKV table and index schemas
    CreateUpdateSKVSchemaResult skv_schema_result = CreateOrUpdateSKVSchema(context, collection_name, table);
    if (skv_schema_result.status.succeeded) {
        response.status.succeeded = true;
    } else {
        response.status = std::move(skv_schema_result.status);       
    }
    return response;
}

GetTableResult TableInfoHandler::GetTable(std::shared_ptr<Context> context, std::string collection_name, std::string table_id) {
    GetTableResult response;

    return response;
}
    
ListTablesResult TableInfoHandler::ListTables(std::shared_ptr<Context> context, std::string collection_name, bool isSysTableIncluded) {
    ListTablesResult response;

    return response;
}

CheckSchemaResult TableInfoHandler::CheckSchema(std::shared_ptr<Context> context, std::string collection_name, std::string schema_name, uint32_t version) {
    CheckSchemaResult response;
    // use the same schema version in PG for SKV schema version
    std::future<k2::GetSchemaResult> schema_result_future = k2_adapter_->GetSchema(collection_name, schema_name, version);
    k2::GetSchemaResult schema_result = schema_result_future.get();
    if (schema_result.status == k2::dto::K23SIStatus::KeyNotFound) {
        response.schema = nullptr;
        response.status.succeeded = true;
    } else if (schema_result.status.is2xxOK()) {
        if(schema_result.schema != nullptr) {
            response.schema = std::move(schema_result.schema);
            response.status.succeeded = true;
        } else {
            response.schema = nullptr;
            response.status.succeeded = true;          
        }
    } else {
        response.status.succeeded = false;          
        response.status.errorCode = schema_result.status.code;
        response.status.errorMessage = std::move(schema_result.status.message);    
    }

    return response;
}

CreateUpdateSKVSchemaResult TableInfoHandler::CreateOrUpdateSKVSchema(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<TableInfo> table) {
    CreateUpdateSKVSchemaResult response;
    // use table id (string) instead of table name as the schema name
    CheckSchemaResult check_result = CheckSchema(context, collection_name, table->table_id(), table->schema().version());
    if (check_result.status.succeeded) {
        if (check_result.schema == nullptr) {
            // build the SKV schema from TableInfo, i.e., PG table schema
            std::shared_ptr<k2::dto::Schema> table_schema = DeriveSKVTableSchema(table);
            PersistSKVSchema(context, collection_name, table_schema);

            if (table->has_secondary_indexes()) {
                std::vector<std::shared_ptr<k2::dto::Schema>> index_schemas = DeriveIndexSchemas(table);
                for (std::shared_ptr<k2::dto::Schema> index_schema : index_schemas) {
                    // use sequential SKV writes for now, could optimize this later
                    PersistSKVSchema(context, collection_name, index_schema);
                }
            }
        }
        response.status.succeeded = true;
    } else {
        response.status.succeeded = false;
        response.status.errorCode = check_result.status.errorCode;
        response.status.errorMessage = std::move(check_result.status.errorMessage);
    }

    return response;
}

PersistSysTableResult TableInfoHandler::PersistSysTable(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<TableInfo> table) {
    PersistSysTableResult response;
    // use sequential SKV writes for now, could optimize this later
    k2::dto::SKVRecord tablelist_table_record = DeriveTableListRecord(collection_name, table);
    PersistSKVRecord(context, tablelist_table_record);
    std::vector<k2::dto::SKVRecord> table_column_records = DeriveTableColumnRecords(collection_name, table);
    for (auto& table_column_record : table_column_records) {
        PersistSKVRecord(context, table_column_record);
    }
    if (table->has_secondary_indexes()) {
        for( const auto& pair : table->secondary_indexes()) {
            k2::dto::SKVRecord tablelist_index_record = DeriveIndexTableListRecord(collection_name, pair.second, table->is_sys_table(), table->next_column_id());
            PersistSKVRecord(context, tablelist_index_record);
            std::vector<k2::dto::SKVRecord> index_column_records = DeriveIndexColumnRecords(collection_name, pair.second, table->schema());
            for (auto& index_column_record : index_column_records) {
                PersistSKVRecord(context, index_column_record);    
            }
        }
    }
    response.status.succeeded = true;
    return response;
}

std::shared_ptr<k2::dto::Schema> TableInfoHandler::DeriveSKVTableSchema(std::shared_ptr<TableInfo> table) {
    std::shared_ptr<k2::dto::Schema> schema = std::make_shared<k2::dto::Schema>();
    schema->name = table->table_id();
    schema->version = table->schema().version();
    uint32_t count = 0;
    for (ColumnSchema col_schema : table->schema().columns()) {
        k2::dto::SchemaField field;
        field.type = GetType(col_schema.type());
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

std::shared_ptr<k2::dto::Schema> TableInfoHandler::DeriveIndexSchema(const IndexInfo& index_info, const Schema& base_table_schema) {
    std::shared_ptr<k2::dto::Schema> schema = std::make_shared<k2::dto::Schema>();
    schema->name = index_info.table_id();
    schema->version = index_info.version();
    uint32_t count = 0;
    for (IndexColumn index_schema : index_info.columns()) {
        k2::dto::SchemaField field;
        field.name = index_schema.column_name;
        int column_idx = base_table_schema.find_column_by_id(index_schema.indexed_column_id);
        if (column_idx == Schema::kColumnNotFound) {
            throw std::invalid_argument("Cannot find base column " + index_schema.indexed_column_id);
        }
        const ColumnSchema& col_schema = base_table_schema.column(column_idx);
        field.type = GetType(col_schema.type());
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
       // all index columns should be treated as primary keys
       schema->partitionKeyFields.push_back(count);
       count++;
    }

    return schema;
}

k2::dto::SKVRecord TableInfoHandler::DeriveTableListRecord(std::string collection_name, std::shared_ptr<TableInfo> table) {
    k2::dto::SKVRecord record;
    // TableId
    record.serializeNext<k2::String>(table->table_id());  
    // TableName
    record.serializeNext<k2::String>(table->table_name());
    // TableOid  
    record.serializeNext<int32_t>(table->pg_oid());  
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

k2::dto::SKVRecord TableInfoHandler::DeriveIndexTableListRecord(std::string collection_name, const IndexInfo& index, bool is_sys_table, int32_t next_column_id) {
    k2::dto::SKVRecord record;
    // TableId
    record.serializeNext<k2::String>(index.table_id());  
    // TableName
    record.serializeNext<k2::String>(index.table_name());
    // TableOid  
    record.serializeNext<int32_t>(index.pg_oid());  
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
        k2::dto::SKVRecord record;
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
    
std::vector<k2::dto::SKVRecord> TableInfoHandler::DeriveIndexColumnRecords(std::string collection_name, const IndexInfo& index, const Schema& base_table_schema) {
    std::vector<k2::dto::SKVRecord> response;
    int count = 0;
    for (IndexColumn index_column : index.columns()) {
        int column_idx = base_table_schema.find_column_by_id(index_column.indexed_column_id);
        if (column_idx == Schema::kColumnNotFound) {
            throw std::invalid_argument("Cannot find base column " + index_column.indexed_column_id);
        }
        const ColumnSchema& col_schema = base_table_schema.column(column_idx);

        k2::dto::SKVRecord record;
        // TableId
        record.serializeNext<k2::String>(index.table_id());  
        // ColumnId
        record.serializeNext<int32_t>(index_column.column_id);  
        // ColumnName
        record.serializeNext<k2::String>(index_column.column_name); 
        // ColumnType 
        record.serializeNext<int16_t>(col_schema.type()->id()); 
        // IsNullable 
        record.serializeNext<bool>(false); 
        // IsPrimary 
        record.serializeNext<bool>(true);  
        // IsPartition
        record.serializeNext<bool>(true); 
        // Order 
        record.serializeNext<int32_t>(count++);  
        // SortingType
        record.serializeNext<int16_t>(col_schema.sorting_type());  

        response.push_back(std::move(record));       
    }
    return response;
}

k2::dto::FieldType TableInfoHandler::GetType(std::shared_ptr<SQLType> type) {
    k2::dto::FieldType field_type = k2::dto::FieldType::NOT_KNOWN; 
    switch (type->id()) {
        case DataType::INT8: {
            field_type = k2::dto::FieldType::INT16T;
        } break;
        case DataType::INT16: {
            field_type = k2::dto::FieldType::INT16T;
        } break;
        case DataType::INT32: {
            field_type = k2::dto::FieldType::INT32T;
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
        default:                                                                                                             \
            throw std::invalid_argument("Unsupported Type" + type->id());       
    }
    return field_type;
}

void TableInfoHandler::PersistSKVSchema(std::shared_ptr<Context> context, std::string collection_name, std::shared_ptr<k2::dto::Schema> schema) {
    std::future<k2::CreateSchemaResult> result_future = k2_adapter_->CreateSchema(collection_name, schema);
    k2::CreateSchemaResult result = result_future.get();
    if (!result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to create SKV schema for " << schema->name << "in" << collection_name
            << " due to error code " << result.status.code
            << " and message: " << result.status.message;
        throw std::runtime_error("Failed to create SKV schema " + schema->name + " in " + collection_name + " due to " + result.status.message);
    }
}

void TableInfoHandler::PersistSKVRecord(std::shared_ptr<Context> context, k2::dto::SKVRecord& record) {
    std::future<k2::WriteResult> result_future = context->GetTxn()->write(std::move(record), true);
    k2::WriteResult result = result_future.get();
    if (!result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to persist SKV record "
            << " due to error code " << result.status.code
            << " and message: " << result.status.message;
        throw std::runtime_error("Failed to persist SKV record due to " + result.status.message);
    }  
}

} // namespace sql
} // namespace k2pg