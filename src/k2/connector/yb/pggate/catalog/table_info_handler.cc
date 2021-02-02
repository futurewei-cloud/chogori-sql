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

CreateSysTablesResult TableInfoHandler::CheckAndCreateSystemTables(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name) {
    CreateSysTablesResult response;
    try {
        // TODO: use sequential calls for now, could be optimized later for concurrent SKV api calls
        K2LOG_D(log::catalog, "Creating SKV schema for collection: {}, schema: {}", collection_name, tablehead_schema_name_);
        CheckSysTableResult result = CheckAndCreateSysTable(context, collection_name, tablehead_schema_name_, tablehead_schema_ptr_);
        if (!result.status.IsSucceeded()) {
            K2LOG_E(log::catalog, "Failed to create SKV schema for collection: {}, schema: {}, due to: {}", collection_name, tablehead_schema_name_, result.status);
            response.status = std::move(result.status);
            return response;
        }

        K2LOG_D(log::catalog, "Creating SKV schema for collection: {}, name: {}", collection_name, tablecolumn_schema_name_);
        result = CheckAndCreateSysTable(context, collection_name, tablecolumn_schema_name_, tablecolumn_schema_ptr_);
        if (!result.status.IsSucceeded()) {
            K2LOG_E(log::catalog, "Failed to create SKV schema for collection: {}, name: {}, due to: {}", collection_name, tablecolumn_schema_name_, result.status);
            response.status = std::move(result.status);
            return response;
        }

        K2LOG_D(log::catalog, "Creating SKV schema for collection: {}, name: {}", collection_name, indexcolumn_schema_name_);
        result = CheckAndCreateSysTable(context, collection_name, indexcolumn_schema_name_, indexcolumn_schema_ptr_);
        if (!result.status.IsSucceeded()) {
            K2LOG_E(log::catalog, "Failed to create SKV schema for collection: {}, name: {}, due to: {}", collection_name, indexcolumn_schema_name_, result.status);
            response.status = std::move(result.status);
            return response;
        }

        response.status.Succeed();
        K2LOG_D(log::catalog, "Created SKV schema for system tables successfully");
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

CheckSysTableResult TableInfoHandler::CheckAndCreateSysTable(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name,
        const std::string& schema_name, std::shared_ptr<k2::dto::Schema> schema) {
    CheckSysTableResult response;
    try {
        // check if the schema already exists or not, which is an indication of whether if we have created the table or not
        std::future<k2::GetSchemaResult> schema_result_future = k2_adapter_->GetSchema(collection_name, schema_name, 1);
        k2::GetSchemaResult schema_result = schema_result_future.get();
        // TODO: double check if this check is valid for schema
        if (schema_result.status == k2::dto::K23SIStatus::KeyNotFound) {
            K2LOG_D(log::catalog, "Table {}, does not exist in {}. Creating...", schema_name, collection_name);
            // create the table schema since it does not exist
            RStatus schema_result = CreateSKVSchema(collection_name, schema);
            if (!schema_result.IsSucceeded()) {
                K2LOG_E(log::catalog, "Failed to create SKV schema {} in {} due to {}",
                    schema_name, collection_name, schema_result);
                response.status = std::move(schema_result);
                return response;
            }
            K2LOG_D(log::catalog, "Created SKV schema {} in {}", schema_name, collection_name);
        } else {
            K2LOG_W(log::catalog, "SKV schema {} already exists in {}", schema_name, collection_name);
        }
        response.status.Succeed();
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

CreateUpdateTableResult TableInfoHandler::CreateOrUpdateTable(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    CreateUpdateTableResult response;
    try {
        // persist system catalog entries to sys tables on individual DB collection
        PersistSysTableResult sys_table_result = PersistSysTable(context, collection_name, table);
        if (!sys_table_result.status.IsSucceeded()) {
            response.status = std::move(sys_table_result.status);
            return response;
        }
        if (CatalogConsts::is_on_physical_collection(table->namespace_id(), table->is_shared())) {
            K2LOG_D(log::catalog, "Persisting table SKV schema id: {}, name: {} in {}", table->table_id(), table->table_name(), table->namespace_id());
            // persist SKV table and index schemas
            CreateUpdateSKVSchemaResult skv_schema_result = CreateOrUpdateTableSKVSchema(context, collection_name, table);
            if (!skv_schema_result.status.IsSucceeded()) {
                response.status = std::move(skv_schema_result.status);
                K2LOG_E(log::catalog, "Failed to persist table SKV schema id: {}, name: {}, in {} due to {}", table->table_id(), table->table_name(),
                    table->namespace_id(), response.status);
                return response;
            }
        } else {
            K2LOG_D(log::catalog, "Skip persisting table SKV schema id: {}, name: {} in {}, shared: {}", table->table_id(), table->table_name(),
                table->namespace_id(), table->is_shared());
        }
        response.status.Succeed();
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

GetTableResult TableInfoHandler::GetTable(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& namespace_name,
        const std::string& table_id) {
    GetTableResult response;
    try {
        K2LOG_D(log::catalog, "Fetch table schema ns id: {}, ns name: {}, table id: {}", collection_name, namespace_name, table_id);
        // table meta data on super tables that we owned are always on individual collection even for shared tables/indexes
        // (but their actual skv schema and table content are not)
        k2::dto::SKVRecord table_head_record = FetchTableHeadSKVRecord(context, collection_name, table_id);
        std::vector<k2::dto::SKVRecord> table_column_records = FetchTableColumnSchemaSKVRecords(context, collection_name, table_id);
        std::shared_ptr<TableInfo> table_info = BuildTableInfo(collection_name, namespace_name, table_head_record, table_column_records);
        // check all the indexes whose BaseTableId is table_id
        std::vector<k2::dto::SKVRecord> index_records = FetchIndexHeadSKVRecords(context, collection_name, table_id);
        if (!index_records.empty()) {
            // table has indexes defined
            for (auto& index_record : index_records) {
                // Fetch and build each index table
                IndexInfo index_info = FetchAndBuildIndexInfo(context, collection_name, index_record);
                // populate index to table_info
                table_info->add_secondary_index(index_info.table_id(), index_info);
            }
        }
        response.status.Succeed();
        response.tableInfo = table_info;
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

ListTablesResult TableInfoHandler::ListTables(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& namespace_name, bool isSysTableIncluded) {
    ListTablesResult response;
    try {
        ListTableIdsResult list_result = ListTableIds(context, collection_name, isSysTableIncluded);
        if (!list_result.status.IsSucceeded()) {
            response.status = std::move(list_result.status);
            return response;
        }
        for (auto& table_id : list_result.tableIds) {
            GetTableResult result = GetTable(context, collection_name, namespace_name, table_id);
            if (!result.status.IsSucceeded()) {
                response.status = std::move(result.status);
                return response;
            }
            response.tableInfos.push_back(std::move(result.tableInfo));
        }
        response.status.Succeed();
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }

    return response;
}

ListTableIdsResult TableInfoHandler::ListTableIds(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, bool isSysTableIncluded) {
    ListTableIdsResult response;
    try {
        std::future<CreateScanReadResult> create_result_future = k2_adapter_->CreateScanRead(collection_name, tablehead_schema_name_);
        CreateScanReadResult create_result = create_result_future.get();
        if (!create_result.status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to create scan read due to {}", create_result.status);

            response.status.errorMessage = fmt::format("Failed to create scan read for {} in {} due to {}", tablehead_schema_name_, collection_name, create_result.status);
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
        query->startScanRecord = buildRangeRecord(collection_name, tablehead_schema_ptr_, std::nullopt);
        query->endScanRecord = buildRangeRecord(collection_name, tablehead_schema_ptr_, std::nullopt);
        do {
            std::future<k2::QueryResult> query_result_future = context->GetTxn()->scanRead(query);
            k2::QueryResult query_result = query_result_future.get();
            if (!query_result.status.is2xxOK()) {
                K2LOG_E(log::catalog, "Failed to run scan read due to {}", query_result.status);
                response.status.code = StatusCode::INTERNAL_ERROR;
                response.status.errorMessage = fmt::format("Failed to scan {} in {} due to {}",
                tablehead_schema_name_, collection_name, query_result.status);
                return response;
            }

            if (!query_result.records.empty()) {
                for (k2::dto::SKVRecord& record : query_result.records) {
                    // deserialize table head
                    // SchemaTableId
                    record.deserializeNext<k2::String>();
                    // SchemaIndexId
                    record.deserializeNext<k2::String>();
                    // TableId
                    std::string table_id = record.deserializeNext<k2::String>().value();
                    // TableName
                    record.deserializeNext<k2::String>();
                    // TableOid
                    record.deserializeNext<int64_t>();
                    // TableUuid
                    record.deserializeNext<k2::String>();
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
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

CopyTableResult TableInfoHandler::CopyTable(std::shared_ptr<SessionTransactionContext> target_context,
            const std::string& target_coll_name,
            const std::string& target_namespace_name,
            uint32_t target_namespace_oid,
            std::shared_ptr<SessionTransactionContext> source_context,
            const std::string& source_coll_name,
            const std::string& source_namespace_name,
            const std::string& source_table_id) {
    CopyTableResult response;
    try {
        GetTableResult table_result = GetTable(source_context, source_coll_name, source_namespace_name, source_table_id);
        if (!table_result.status.IsSucceeded()) {
            response.status = std::move(table_result.status);
            return response;
        }
        if (table_result.tableInfo == nullptr) {
            response.status.code = StatusCode::NOT_FOUND;
            response.status.errorMessage = "Cannot find table " + source_table_id;
            return response;
        }

        std::shared_ptr<TableInfo> source_table = table_result.tableInfo;
        std::string target_table_uuid = PgObjectId::GetTableUuid(target_namespace_oid, table_result.tableInfo->table_oid());
        std::shared_ptr<TableInfo> target_table = TableInfo::Clone(table_result.tableInfo, target_coll_name,
                target_namespace_name, target_table_uuid, table_result.tableInfo->table_name());

        CreateUpdateTableResult create_result = CreateOrUpdateTable(target_context, target_coll_name, target_table);
        if (!create_result.status.IsSucceeded()) {
            response.status = std::move(create_result.status);
            return response;
        }

        if(source_table->is_shared()) {
            K2LOG_D(log::catalog, "Skip copying shared table {} in {}", source_table_id, source_coll_name);
            if(source_table->has_secondary_indexes()) {
                for (std::pair<TableId, IndexInfo> secondary_index : source_table->secondary_indexes()) {
                    K2ASSERT(log::catalog, secondary_index.second.is_shared(), "Index for a shared table must be shared");
                    K2LOG_D(log::catalog, "Skip copying shared index {} in {}", secondary_index.first, source_coll_name);
                }
            }
        } else {
            CopySKVTableResult copy_skv_table_result = CopySKVTable(target_context, target_coll_name, target_table->table_id(), target_table->schema().version(),
                source_context, source_coll_name, source_table_id, source_table->schema().version());
            if (!copy_skv_table_result.status.IsSucceeded()) {
                response.status = std::move(copy_skv_table_result.status);
                return response;
            }
            // the indexes for a shared table should be shared as well
            if(source_table->has_secondary_indexes()) {
                std::unordered_map<std::string, IndexInfo*> target_index_name_map;
                for (std::pair<TableId, IndexInfo> secondary_index : target_table->secondary_indexes()) {
                    target_index_name_map[secondary_index.second.table_name()] = &secondary_index.second;
                }
                for (std::pair<TableId, IndexInfo> secondary_index : source_table->secondary_indexes()) {
                    K2ASSERT(log::catalog, !secondary_index.second.is_shared(), "Index for a non-shared table must not be shared");
                    // search for target index by name
                    auto found = target_index_name_map.find(secondary_index.second.table_name());
                    if (found == target_index_name_map.end()) {
                        response.status.code = StatusCode::NOT_FOUND;
                        response.status.errorMessage = "Cannot find target index " + secondary_index.second.table_name();
                        return response;
                    }
                    IndexInfo* target_index = found->second;
                    CopySKVTableResult copy_skv_index_result = CopySKVTable(target_context, target_coll_name, secondary_index.first, secondary_index.second.version(),
                        source_context, source_coll_name, target_index->table_id(), target_index->version());
                    if (!copy_skv_index_result.status.IsSucceeded()) {
                        response.status = std::move(copy_skv_index_result.status);
                        return response;
                    }
                    response.num_index++;
                }
            }
        }

        K2LOG_D(log::catalog, "Copied table from {} in {} to {} in {}", source_table_id, source_coll_name, target_table->table_id(), target_coll_name);
        response.tableInfo = target_table;
        response.status.Succeed();
    } catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

CopySKVTableResult TableInfoHandler::CopySKVTable(std::shared_ptr<SessionTransactionContext> target_context,
            const std::string& target_coll_name,
            const std::string& target_table_id,
            uint32_t target_version,
            std::shared_ptr<SessionTransactionContext> source_context,
            const std::string& source_coll_name,
            const std::string& source_table_id,
            uint32_t source_version) {
    CopySKVTableResult response;
    // check target SKV schema
    CheckSKVSchemaResult target_schema_result = CheckSKVSchema(target_context, target_coll_name, target_table_id, target_version);
    if (!target_schema_result.status.IsSucceeded()) {
        K2LOG_E(log::catalog, "Failed to get SKV schema for table {} in {} with version {} due to {}",
            target_table_id, target_coll_name, target_version, target_schema_result.status.errorMessage);
        response.status = std::move(target_schema_result.status);
        return response;
    }

    // check the source SKV schema
    CheckSKVSchemaResult source_schema_result = CheckSKVSchema(source_context, source_coll_name, source_table_id, source_version);
    if (!source_schema_result.status.IsSucceeded()) {
        K2LOG_E(log::catalog, "Failed to get SKV schema for table {} in {} with version {} due to {}",
            source_table_id, source_coll_name, source_version, source_schema_result.status.errorMessage);
        response.status = std::move(source_schema_result.status);
        return response;
    }

    // create scan for source table
    CreateScanReadResult create_source_scan_result = k2_adapter_->CreateScanRead(source_coll_name, source_schema_result.schema->name).get();
    if (!create_source_scan_result.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create scan read for {} in {} due to {}", source_table_id, source_coll_name, create_source_scan_result.status.message);
        response.status.errorMessage = create_source_scan_result.status.message;
        response.status.code = StatusCode::RUNTIME_ERROR;
        return response;
    }

    // scan the source table
    std::shared_ptr<k2::Query> query = create_source_scan_result.query;
    query->startScanRecord = buildRangeRecord(source_coll_name, source_schema_result.schema, std::nullopt);
    query->endScanRecord = buildRangeRecord(source_coll_name, source_schema_result.schema, std::nullopt);
    int count = 0;
    do {
        k2::QueryResult query_result = source_context->GetTxn()->scanRead(query).get();
        if (!query_result.status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to run scan read for table {} in {} due to {}", source_table_id, source_coll_name, query_result.status.message);
            response.status.code = StatusCode::RUNTIME_ERROR;
            response.status.errorMessage = query_result.status.message;
            return response;
        }

        if (!query_result.records.empty()) {
            for (k2::dto::SKVRecord& record : query_result.records) {
                // clone and persist SKV record to target table
                k2::dto::SKVRecord target_record = record.cloneToOtherSchema(target_coll_name, target_schema_result.schema);
                RStatus table_status = PersistSKVRecord(target_context, target_record);
                if (!table_status.IsSucceeded()) {
                    K2LOG_E(log::catalog, "Failed to persist SKV record to table {} in {} due to {}", target_table_id, target_coll_name, table_status.errorMessage);
                    response.status = std::move(table_status);
                    return response;
                }
                count++;
            }
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
    } while (!query->isDone());
    K2LOG_I(log::catalog, "Finished copying {} in {} to {} in {} with {} records", source_table_id, source_coll_name, target_table_id, target_coll_name, count);
    response.status.Succeed();
    return response;
}

CheckSKVSchemaResult TableInfoHandler::CheckSKVSchema(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& schema_name, uint32_t version) {
    CheckSKVSchemaResult response;
    try {
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
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

CreateUpdateSKVSchemaResult TableInfoHandler::CreateOrUpdateTableSKVSchema(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    CreateUpdateSKVSchemaResult response;
    try {
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
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }

    return response;
}

CreateUpdateSKVSchemaResult TableInfoHandler::CreateOrUpdateIndexSKVSchema(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name,
        std::shared_ptr<TableInfo> table, const IndexInfo& index_info) {
    CreateUpdateSKVSchemaResult response;
    try {
        std::shared_ptr<k2::dto::Schema> index_schema = DeriveIndexSchema(index_info);
        response.status = CreateSKVSchema(collection_name, index_schema);
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

PersistSysTableResult TableInfoHandler::PersistSysTable(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    PersistSysTableResult response;
    try {
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
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

PersistIndexTableResult TableInfoHandler::PersistIndexTable(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, std::shared_ptr<TableInfo> table,
        const IndexInfo& index_info) {
    PersistIndexTableResult response;
    try {
        k2::dto::SKVRecord tablelist_index_record = DeriveIndexHeadRecord(collection_name, index_info, table->is_sys_table(), table->next_column_id());
        K2LOG_D(log::catalog, "Persisting SKV record tablelist_index_record id: {}, name: {}",
            index_info.table_id(), index_info.table_name());
        RStatus table_status = PersistSKVRecord(context, tablelist_index_record);
        if (!table_status.IsSucceeded()) {
            response.status = std::move(table_status);
            return response;
        }
        std::vector<k2::dto::SKVRecord> index_column_records = DeriveIndexColumnRecords(collection_name, index_info, table->schema());
        for (auto& index_column_record : index_column_records) {
            K2LOG_D(log::catalog, "Persisting SKV record index_column_record id: {}, name: {}",
                index_info.table_id(), index_info.table_name());
            RStatus index_status = PersistSKVRecord(context, index_column_record);
            if (!index_status.IsSucceeded()) {
                response.status = std::move(index_status);
                return response;
            }
        }
        response.status.Succeed();
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

// Delete table_info and the related index_info from tablehead, tablecolumn, and indexcolumn system tables
DeleteTableResult TableInfoHandler::DeleteTableMetadata(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    DeleteTableResult response;
    try {
        // first delete indexes
        std::vector<k2::dto::SKVRecord> index_records = FetchIndexHeadSKVRecords(context, collection_name, table->table_id());
        if (!index_records.empty()) {
            for (auto& record : index_records) {
                // SchemaTableId
                record.deserializeNext<k2::String>();
                // SchemaIndexId
                record.deserializeNext<k2::String>();
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

    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

// Delete the actual table records from SKV that are stored with the SKV schema name to be table_id as in table_info
DeleteTableResult TableInfoHandler::DeleteTableData(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    DeleteTableResult response;
    try {
        // TODO: add a task to delete the actual data from SKV

        response.status.Succeed();
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

// Delete index_info from tablehead and indexcolumn system tables
DeleteIndexResult TableInfoHandler::DeleteIndexMetadata(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& index_id) {
    DeleteIndexResult response;
    try {
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
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

// Delete the actual index records from SKV that are stored with the SKV schema name to be table_id as in index_info
DeleteIndexResult TableInfoHandler::DeleteIndexData(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& index_id) {
    DeleteIndexResult response;
    try {
        // TODO: add a task to delete the actual data from SKV

        response.status.Succeed();
    }
    catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }
    return response;
}

GetBaseTableIdResult TableInfoHandler::GetBaseTableId(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& index_id) {
    GetBaseTableIdResult response;
    try {
        // exception would be thrown if the record could not be found
        k2::dto::SKVRecord index_head = FetchTableHeadSKVRecord(context, collection_name, index_id);
        // SchemaTableId
        index_head.deserializeNext<k2::String>();
        // SchemaIndexId
        index_head.deserializeNext<k2::String>();
        // TableId
        index_head.deserializeNext<k2::String>();
        // TableName
        index_head.deserializeNext<k2::String>();
        // TableOid
        index_head.deserializeNext<int64_t>();
        // TableUuid
        index_head.deserializeNext<k2::String>();
        // IsSysTable
        index_head.deserializeNext<bool>();
        // IsShared
        index_head.deserializeNext<bool>();
        // IsTransactional
        index_head.deserializeNext<bool>();
        // IsIndex
        index_head.deserializeNext<bool>();
        // IsUnique
        index_head.deserializeNext<bool>();
        // BaseTableId
        response.baseTableId = index_head.deserializeNext<k2::String>().value();
        response.status.Succeed();
    } catch (const std::exception& e) {
        response.status.code = StatusCode::RUNTIME_ERROR;
        response.status.errorMessage = e.what();
    }

    return response;
}

GetTableInfoResult TableInfoHandler::GetTableInfo(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& table_id)
{
    GetTableInfoResult response;
    try {
        k2::dto::SKVRecord record = FetchTableHeadSKVRecord(context, collection_name, table_id);
        // SchemaTableId
        record.deserializeNext<k2::String>();
        // SchemaIndexId
        record.deserializeNext<k2::String>();
        // TableId
        record.deserializeNext<k2::String>();
        // TableName
        record.deserializeNext<k2::String>();
        // TableOid
        record.deserializeNext<int64_t>();
        // TableUuid
        record.deserializeNext<k2::String>();
        // IsSysTable
        record.deserializeNext<bool>();
        // IsShared
        response.isShared = record.deserializeNext<bool>().value();
        // IsTransactional
        record.deserializeNext<bool>();
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
    std::shared_ptr<k2::dto::Schema> schema = std::make_shared<k2::dto::Schema>();
    schema->name = table->table_id();
    schema->version = table->schema().version();
    // add two partitionkey fields
    AddDefaultPartitionKeys(schema);
    uint32_t count = 2;
    for (ColumnSchema col_schema : table->schema().columns()) {
        k2::dto::SchemaField field;
        field.type = ToK2Type(col_schema.type()->id());
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
        response.push_back(DeriveIndexSchema(pair.second));
    }
    return response;
}

std::shared_ptr<k2::dto::Schema> TableInfoHandler::DeriveIndexSchema(const IndexInfo& index_info) {
    std::shared_ptr<k2::dto::Schema> schema = std::make_shared<k2::dto::Schema>();
    schema->name = index_info.table_id();
    schema->version = index_info.version();
    // add two partitionkey fields: base table id + index table id
    AddDefaultPartitionKeys(schema);
    uint32_t count = 2;
    for (IndexColumn indexcolumn_schema : index_info.columns()) {
        k2::dto::SchemaField field;
        field.name = indexcolumn_schema.column_name;
        field.type = ToK2Type(indexcolumn_schema.type);
        switch (indexcolumn_schema.sorting_type) {
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
        // use the keys from PG as the partition keys
        if (indexcolumn_schema.is_hash || indexcolumn_schema.is_range) {
            schema->partitionKeyFields.push_back(count);
        }
        count++;
    }

    return schema;
}

k2::dto::SKVRecord TableInfoHandler::DeriveTableHeadRecord(const std::string& collection_name, std::shared_ptr<TableInfo> table) {
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
    record.serializeNext<int64_t>(table->table_oid());
    // TableUuid
    record.serializeNext<k2::String>(table->table_uuid());
    // IsSysTable
    record.serializeNext<bool>(table->is_sys_table());
    // IsShared
    record.serializeNext<bool>(table->is_shared());
    // IsTransactional
    record.serializeNext<bool>(table->schema().table_properties().is_transactional());
    // IsIndex
    record.serializeNext<bool>(false);
    // IsUnique (for index)
    record.serializeNext<bool>(false);
    // BaseTableId
    record.serializeNull();
    // IndexPermission
    record.serializeNull();
    // NextColumnId
    record.serializeNext<int32_t>(table->next_column_id());
    // SchemaVersion
    record.serializeNext<int32_t>(table->schema().version());
    return record;
}

k2::dto::SKVRecord TableInfoHandler::DeriveIndexHeadRecord(const std::string& collection_name, const IndexInfo& index, bool is_sys_table, int32_t next_column_id) {
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
    record.serializeNext<int64_t>(index.table_oid());
    // TableUuid
    record.serializeNext<k2::String>(index.table_uuid());
    // IsSysTable
    record.serializeNext<bool>(is_sys_table);
    // IsShared
    record.serializeNext<bool>(index.is_shared());
    // IsTransactional
    record.serializeNext<bool>(true);
    // IsIndex
    record.serializeNext<bool>(true);
    // IsUnique (for index)
    record.serializeNext<bool>(index.is_unique());
    // BaseTableId
    record.serializeNext<k2::String>(index.base_table_id());
    // IndexPermission
    record.serializeNext<int16_t>(index.index_permissions());
    // NextColumnId
    record.serializeNext<int32_t>(next_column_id);
    // SchemaVersion
    record.serializeNext<int32_t>(index.version());

    return record;
}

std::vector<k2::dto::SKVRecord> TableInfoHandler::DeriveTableColumnRecords(const std::string& collection_name, std::shared_ptr<TableInfo> table) {
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
        // IsHash
        record.serializeNext<bool>(col_schema.is_hash());
        // Order
        record.serializeNext<int32_t>(col_schema.order());
        // SortingType
        record.serializeNext<int16_t>(col_schema.sorting_type());

        response.push_back(std::move(record));
    }
    return response;
}

std::vector<k2::dto::SKVRecord> TableInfoHandler::DeriveIndexColumnRecords(const std::string& collection_name, const IndexInfo& index, const Schema& base_tablecolumn_schema) {
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
        // ColumnType, we persist SQL type directly as an integer
        record.serializeNext<int16_t>(index_column.type);
        // IsNullable
        record.serializeNext<bool>(index_column.is_nullable);
        // IsHash
        record.serializeNext<bool>(index_column.is_hash);
        // IsRange
        record.serializeNext<bool>(index_column.is_range);
        // Order
        record.serializeNext<int32_t>(index_column.order);
        // SortingType
        record.serializeNext<int16_t>(index_column.sorting_type);
        // BaseColumnId
        record.serializeNext<int32_t>(index_column.base_column_id);
        response.push_back(std::move(record));
    }
    return response;
}

k2::dto::FieldType TableInfoHandler::ToK2Type(DataType type) {
    k2::dto::FieldType field_type = k2::dto::FieldType::NOT_KNOWN;
    switch (type) {
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
            throw std::invalid_argument("Unsupported type " + type);
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
            throw std::invalid_argument(fmt::format("unsupported type: {}", type));
        }
    }
    return sql_type;
}

k2::dto::SKVRecord TableInfoHandler::FetchTableHeadSKVRecord(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& table_id) {
    k2::dto::SKVRecord record(collection_name, tablehead_schema_ptr_);
    // SchemaTableId
    record.serializeNext<k2::String>(tablehead_schema_ptr_->name);
    // SchemaIndexId
    record.serializeNext<k2::String>("");
    // table_id
    record.serializeNext<k2::String>(table_id);
    K2LOG_D(log::catalog, "Fetching Tablehead SKV record for table {}", table_id);
    std::future<k2::ReadResult<k2::dto::SKVRecord>> result_future = context->GetTxn()->read(std::move(record));
    k2::ReadResult<k2::dto::SKVRecord> result = result_future.get();
    // TODO: add error handling and retry logic in catalog manager
    if (result.status == k2::dto::K23SIStatus::KeyNotFound) {
        throw std::runtime_error("Cannot find entry " + table_id + " in " + collection_name);
    } else if (!result.status.is2xxOK()) {
        throw std::runtime_error(fmt::format("Error fetching entry {} in {} due to {}",
            table_id, collection_name, result.status
        ));
    }
    return std::move(result.value);
}

std::vector<k2::dto::SKVRecord> TableInfoHandler::FetchIndexHeadSKVRecords(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& base_table_id) {
    std::future<CreateScanReadResult> create_result_future = k2_adapter_->CreateScanRead(collection_name, tablehead_schema_name_);
    CreateScanReadResult create_result = create_result_future.get();
    if (!create_result.status.is2xxOK()) {
        auto msg = fmt::format("Failed to create scan read for {} in {} due to {}",
        base_table_id, collection_name, create_result.status);
        K2LOG_E(log::catalog, "{}", msg);
        throw std::runtime_error(msg);
    }

    std::vector<k2::dto::SKVRecord> records;
    std::shared_ptr<k2::Query> query = create_result.query;
    std::vector<k2::dto::expression::Value> values;
    std::vector<k2::dto::expression::Expression> exps;
    // find all the indexes for the base table, i.e., by BaseTableId
    values.emplace_back(k2::dto::expression::makeValueReference(CatalogConsts::BASE_TABLE_ID_COLUMN_NAME));
    values.emplace_back(k2::dto::expression::makeValueLiteral<k2::String>(base_table_id));
    k2::dto::expression::Expression filterExpr = k2::dto::expression::makeExpression(k2::dto::expression::Operation::EQ, std::move(values), std::move(exps));
    query->setFilterExpression(std::move(filterExpr));
    query->startScanRecord = buildRangeRecord(collection_name, tablehead_schema_ptr_, std::nullopt);
    query->endScanRecord = buildRangeRecord(collection_name, tablehead_schema_ptr_, std::nullopt);
    do {
        K2LOG_D(log::catalog, "Fetching Tablehead SKV records for indexes on base table {}", base_table_id);
        std::future<k2::QueryResult> query_result_future = context->GetTxn()->scanRead(query);
        k2::QueryResult query_result = query_result_future.get();
        if (!query_result.status.is2xxOK()) {
            auto msg = fmt::format("Failed to run scan read for {} in {} due to {}",
            base_table_id, collection_name, query_result.status);
            K2LOG_E(log::catalog, "{}", msg);
            throw std::runtime_error(msg);
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

std::vector<k2::dto::SKVRecord> TableInfoHandler::FetchTableColumnSchemaSKVRecords(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& table_id) {
    std::future<CreateScanReadResult> create_result_future = k2_adapter_->CreateScanRead(collection_name, tablecolumn_schema_name_);
    CreateScanReadResult create_result = create_result_future.get();
    if (!create_result.status.is2xxOK()) {
        auto msg = fmt::format("Failed to create scan read for {} in {} due to {}",
        table_id, collection_name, create_result.status);
        K2LOG_E(log::catalog, "{}", msg);
        throw std::runtime_error(msg);
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
    query->startScanRecord = buildRangeRecord(collection_name, tablecolumn_schema_ptr_, std::make_optional(table_id));
    query->endScanRecord = buildRangeRecord(collection_name, tablecolumn_schema_ptr_, std::make_optional(table_id));
    do {
        std::future<k2::QueryResult> query_result_future = context->GetTxn()->scanRead(query);
        k2::QueryResult query_result = query_result_future.get();
        if (!query_result.status.is2xxOK()) {
            auto msg = fmt::format("Failed to run scan read for {} in {} due to {}",
            table_id, collection_name, query_result.status);
            K2LOG_E(log::catalog, "{}", msg);
            throw std::runtime_error(msg);
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

std::vector<k2::dto::SKVRecord> TableInfoHandler::FetchIndexColumnSchemaSKVRecords(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, const std::string& table_id) {
    std::future<CreateScanReadResult> create_result_future = k2_adapter_->CreateScanRead(collection_name, indexcolumn_schema_name_);
    CreateScanReadResult create_result = create_result_future.get();
    if (!create_result.status.is2xxOK()) {
        auto msg = fmt::format("Failed to create scan read for {} in {} due to {}",
        table_id, collection_name, create_result.status);
        K2LOG_E(log::catalog, "{}", msg);
        throw std::runtime_error(msg);
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
    query->startScanRecord = buildRangeRecord(collection_name, indexcolumn_schema_ptr_, std::make_optional(table_id));
    query->endScanRecord = buildRangeRecord(collection_name, indexcolumn_schema_ptr_, std::make_optional(table_id));
    do {
        std::future<k2::QueryResult> query_result_future = context->GetTxn()->scanRead(query);
        k2::QueryResult query_result = query_result_future.get();
        if (!query_result.status.is2xxOK()) {
            auto msg = fmt::format("Failed to run scan read for {} in {} due to {}",
            table_id, collection_name, query_result.status);
            K2LOG_E(log::catalog, "{}", msg);
            throw std::runtime_error(msg);
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

std::shared_ptr<TableInfo> TableInfoHandler::BuildTableInfo(const std::string& namespace_id, const std::string& namespace_name,
        k2::dto::SKVRecord& table_head, std::vector<k2::dto::SKVRecord>& table_columns) {
    // deserialize table head
    // SchemaTableId
    table_head.deserializeNext<k2::String>();
    // SchemaIndexId
    table_head.deserializeNext<k2::String>();
    // TableId
    std::string table_id = table_head.deserializeNext<k2::String>().value();
    // TableName
    std::string table_name = table_head.deserializeNext<k2::String>().value();
    // TableOid
    uint32_t table_oid = table_head.deserializeNext<int64_t>().value();
    // TableUuid
    std::string table_uuid = table_head.deserializeNext<k2::String>().value();
    // IsSysTable
    bool is_sys_table = table_head.deserializeNext<bool>().value();
    // IsShared
    bool is_shared = table_head.deserializeNext<bool>().value();
    // IsTransactional
    bool is_transactional = table_head.deserializeNext<bool>().value();
    // IsIndex
    bool is_index = table_head.deserializeNext<bool>().value();
    if (is_index) {
        throw std::runtime_error("Table " + table_id + " should not be an index");
    }
    // IsUnique
    table_head.deserializeNext<bool>();
    // BaseTableId
    table_head.deserializeNext<k2::String>();
    // IndexPermission
    table_head.deserializeNext<int16_t>();
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
        column.deserializeNext<k2::String>();
        // SchemaIndexId
        column.deserializeNext<k2::String>();
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
        // IsHash
        bool is_hash = column.deserializeNext<bool>().value();
        // Order
        int32 col_order = column.deserializeNext<int32_t>().value();
        // SortingType
        int16_t sorting_type = column.deserializeNext<int16_t>().value();
        ColumnSchema col_schema(col_name, static_cast<DataType>(col_type), is_nullable, is_primary, is_hash,
                col_order, static_cast<ColumnSchema::SortingType>(sorting_type));
        cols.push_back(std::move(col_schema));
        ids.push_back(col_id);
        if (is_primary) {
            key_columns++;
        }
    }
    Schema table_schema(cols, ids, key_columns, table_properties);
    table_schema.set_version(version);
    std::shared_ptr<TableInfo> table_info = std::make_shared<TableInfo>(namespace_id, namespace_name, table_oid, table_name, table_uuid, table_schema);
    table_info->set_next_column_id(next_column_id);
    table_info->set_is_sys_table(is_sys_table);
    table_info->set_is_shared_table(is_shared);
    return table_info;
}

IndexInfo TableInfoHandler::FetchAndBuildIndexInfo(std::shared_ptr<SessionTransactionContext> context, const std::string& collection_name, k2::dto::SKVRecord& index_head) {
    // deserialize index head
    // SchemaTableId
    index_head.deserializeNext<k2::String>();
    // SchemaIndexId
    index_head.deserializeNext<k2::String>();
    // TableId
    std::string table_id = index_head.deserializeNext<k2::String>().value();
    // TableName
    std::string table_name = index_head.deserializeNext<k2::String>().value();
    // TableOid
    uint32_t table_oid = index_head.deserializeNext<int64_t>().value();
    // TableUuid
    std::string table_uuid = index_head.deserializeNext<k2::String>().value();
    // IsSysTable
    index_head.deserializeNext<bool>();
    // IsShared
    bool is_shared = index_head.deserializeNext<bool>().value();
    // IsTransactional
    index_head.deserializeNext<bool>();
    // IsIndex
    bool is_index = index_head.deserializeNext<bool>().value();
    if (!is_index) {
        throw std::runtime_error("Table " + table_id + " should be an index");
    }
    // IsUnique
    bool is_unique = index_head.deserializeNext<bool>().value();
    // BaseTableId
    std::string base_table_id = index_head.deserializeNext<k2::String>().value();
    // IndexPermission
    IndexPermissions index_perm = static_cast<IndexPermissions>(index_head.deserializeNext<int16_t>().value());
    // NextColumnId
    index_head.deserializeNext<int32_t>();
    // SchemaVersion
    uint32_t version = index_head.deserializeNext<int32_t>().value();

    // Fetch index columns
    std::vector<k2::dto::SKVRecord> index_columns = FetchIndexColumnSchemaSKVRecords(context, collection_name, table_id);

    // deserialize index columns
    std::vector<IndexColumn> columns;
    for (auto& column : index_columns) {
        // SchemaTableId
        column.deserializeNext<k2::String>();
        // SchemaIndexId
        column.deserializeNext<k2::String>();
        // TableId
        std::string tb_id = column.deserializeNext<k2::String>().value();
        // ColumnId
        int32_t col_id = column.deserializeNext<int32_t>().value();
        // ColumnName
        std::string col_name = column.deserializeNext<k2::String>().value();
        // ColumnType
        int16_t col_type = column.deserializeNext<int16_t>().value();
        // IsNullable
        bool is_nullable = column.deserializeNext<bool>().value();
        // IsHash
        bool is_hash = column.deserializeNext<bool>().value();
        // IsRange
        bool is_range = column.deserializeNext<bool>().value();
        // Order
        int32_t col_order = column.deserializeNext<int32_t>().value();
        // SortingType
        int16_t sorting_type = column.deserializeNext<int16_t>().value();
        // BaseColumnId
        int32_t base_col_id = column.deserializeNext<int32_t>().value();
        // TODO: add support for expression in index
        IndexColumn index_column(col_id, col_name, static_cast<DataType>(col_type), is_nullable, is_hash, is_range,
                col_order, static_cast<ColumnSchema::SortingType>(sorting_type), base_col_id);
        columns.push_back(std::move(index_column));
    }

    IndexInfo index_info(table_name, table_oid, table_uuid, base_table_id, version, is_unique, is_shared, columns, index_perm);
    return index_info;
}

k2::dto::SKVRecord TableInfoHandler::buildRangeRecord(const std::string& collection_name, std::shared_ptr<k2::dto::Schema> schema_ptr_, std::optional<std::string> table_id) {
    k2::dto::SKVRecord record(collection_name, schema_ptr_);
    // SchemaTableId
    record.serializeNext<k2::String>(schema_ptr_->name);
    // SchemaIndexId
    record.serializeNext<k2::String>("");
    if (table_id != std::nullopt) {
        record.serializeNext<k2::String>(table_id.value());
    }
    return record;
}

} // namespace catalog
} // namespace sql
} // namespace k2pg
