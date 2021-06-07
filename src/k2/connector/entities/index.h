// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
// Copyright(c) 2020 Futurewei Cloud
//
// Permission is hereby granted,
//        free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "entities/entity_ids.h"
#include "entities/schema.h"
#include "entities/expr.h"


namespace k2pg {
    namespace sql {

        enum IndexPermissions {
            INDEX_PERM_DELETE_ONLY = 0,
            INDEX_PERM_WRITE_AND_DELETE = 2,
            INDEX_PERM_DO_BACKFILL = 4,
            // This is the "success" state, where the index is completely usable.
            INDEX_PERM_READ_WRITE_AND_DELETE = 6,
            // Used while removing an index -- either due to backfill failure, or
            // due to a client requested "drop index".
            INDEX_PERM_WRITE_AND_DELETE_WHILE_REMOVING = 8,
            INDEX_PERM_DELETE_ONLY_WHILE_REMOVING = 10,
            INDEX_PERM_INDEX_UNUSED = 12,
            // Used as a sentinel value.
            INDEX_PERM_NOT_USED = 14
        };

        struct IndexColumn {
            ColumnId column_id;             // Column id in the index table.
            std::string column_name;        // Column name in the index table - colexpr.MangledName().
            DataType type;                  // data type
            bool is_nullable;               // can be null or not
            bool is_hash;                   // is hash key
            bool is_range;                  // is range key
            int32_t order;                  // attr_num
            ColumnSchema::SortingType sorting_type;       // sort type
            ColumnId base_column_id;      // Corresponding column id in base table.
            std::shared_ptr<PgExpr> colexpr = nullptr;    // Index expression.

            explicit IndexColumn(ColumnId in_column_id, std::string in_column_name,
                DataType in_type, bool in_is_nullable, bool in_is_hash, bool in_is_range,
                int32_t in_order, ColumnSchema::SortingType in_sorting_type,
                ColumnId in_base_column_id, std::shared_ptr<PgExpr> in_colexpr)
                : column_id(in_column_id), column_name(std::move(in_column_name)),
                    type(in_type), is_nullable(in_is_nullable), is_hash(in_is_hash),
                    is_range(in_is_range), order(in_order), sorting_type(in_sorting_type),
                    base_column_id(in_base_column_id), colexpr(in_colexpr) {
            }

            explicit IndexColumn(ColumnId in_column_id, std::string in_column_name,
                DataType in_type, bool in_is_nullable, bool in_is_hash, bool in_is_range,
                int32_t in_order, ColumnSchema::SortingType in_sorting_type,
                ColumnId in_base_column_id)
                : column_id(in_column_id), column_name(std::move(in_column_name)),
                    type(in_type), is_nullable(in_is_nullable), is_hash(in_is_hash),
                    is_range(in_is_range), order(in_order), sorting_type(in_sorting_type),
                    base_column_id(in_base_column_id) {
            }
        };

        class IndexInfo {
        public:
            explicit IndexInfo(std::string table_name, uint32_t table_oid, std::string table_uuid,
                std::string base_table_id, uint32_t schema_version, bool is_unique,
                bool is_shared, std::vector<IndexColumn> columns, size_t hash_column_count,
                size_t range_column_count, std::vector<ColumnId> indexed_hash_column_ids,
                std::vector<ColumnId> indexed_range_column_ids, IndexPermissions index_permissions,
                bool use_mangled_column_name)
                : table_name_(table_name),
                table_oid_(table_oid),
                table_id_(PgObjectId::GetTableId(table_oid)),
                table_uuid_(table_uuid),
                base_table_id_(base_table_id),
                schema_version_(schema_version),
                is_unique_(is_unique),
                is_shared_(is_shared),
                columns_(std::move(columns)),
                hash_column_count_(hash_column_count),
                range_column_count_(range_column_count),
                indexed_hash_column_ids_(std::move(indexed_hash_column_ids)),
                indexed_range_column_ids_(std::move(indexed_range_column_ids)),
                index_permissions_(index_permissions) {
            }

            explicit IndexInfo(std::string table_name,
                uint32_t table_oid,
                std::string table_uuid,
                std::string base_table_id,
                uint32_t schema_version,
                bool is_unique,
                bool is_shared,
                std::vector<IndexColumn> columns,
                IndexPermissions index_permissions)
                : table_name_(table_name),
                    table_oid_(table_oid),
                    table_id_(PgObjectId::GetTableId(table_oid)),
                    table_uuid_(table_uuid),
                    base_table_id_(base_table_id),
                    schema_version_(schema_version),
                    is_unique_(is_unique),
                    is_shared_(is_shared),
                    columns_(std::move(columns)),
                    index_permissions_(index_permissions) {
                    for (auto& column : columns_) {
                        if (column.is_hash) {
                            hash_column_count_++;
                        } else if (column.is_range) {
                            range_column_count_++;
                        }
                    }
            }

            const std::string& table_id() const {
                return table_id_;
            }

            const std::string& table_name() const {
                return table_name_;
            }

            const uint32_t table_oid() const {
                return table_oid_;
            }

            const std::string& table_uuid() const {
                return table_uuid_;
            }

            const std::string& base_table_id() const {
                return base_table_id_;
            }

            const uint32_t base_table_oid() const {
                return PgObjectId::GetTableOidByTableUuid(base_table_id_);
            }

            bool is_unique() const {
                return is_unique_;
            }

            bool is_shared() const {
                return is_shared_;
            }

            const uint32_t version() const {
                return schema_version_;
            }

            const std::vector<IndexColumn>& columns() const {
                return columns_;
            }

            const IndexColumn& column(const size_t idx) const {
                return columns_[idx];
            }

            size_t num_columns() const {
                return columns_.size();
            }

            size_t hash_column_count() const {
                return hash_column_count_;
            }

            size_t range_column_count() const {
                return range_column_count_;
            }

            size_t key_column_count() const {
                return hash_column_count_ + range_column_count_;
            }

            const std::vector<ColumnId>& indexed_hash_column_ids() const {
                return indexed_hash_column_ids_;
            }

            const std::vector<ColumnId>& indexed_range_column_ids() const {
                return indexed_range_column_ids_;
            }

            const IndexPermissions index_permissions() const {
                return index_permissions_;
            }

            // Return column ids that are primary key columns of the base table.
            std::vector<ColumnId> index_key_column_ids() const;

            // Check if this index is dependent on the given column.
            bool CheckColumnDependency(ColumnId column_id) const;

            // Index primary key columns of the base table only?
            bool PrimaryKeyColumnsOnly(const Schema& indexed_schema) const;

            // Are read operations allowed to use the index?  During CREATE INDEX, reads are not allowed until
            // the index backfill is successfully completed.
            bool HasReadPermission() const {
                return index_permissions_ == INDEX_PERM_READ_WRITE_AND_DELETE;
            }

            // Should write operations to the index update the index table?  This includes INSERT and UPDATE.
            bool HasWritePermission() const {
                return index_permissions_ >= INDEX_PERM_WRITE_AND_DELETE &&
                    index_permissions_ <= INDEX_PERM_WRITE_AND_DELETE_WHILE_REMOVING;
            }

            // Should delete operations to the index update the index table?  This includes DELETE and UPDATE.
            bool HasDeletePermission() const {
                return index_permissions_ >= INDEX_PERM_DELETE_ONLY &&
                    index_permissions_ <= INDEX_PERM_DELETE_ONLY_WHILE_REMOVING;
            }

            // Is the index being backfilled?
            bool IsBackfilling() const {
                return index_permissions_ == INDEX_PERM_DO_BACKFILL;
            }

            int32_t FindKeyIndex(const std::string& key_name) const;

        private:
            const std::string table_name_;      // Index table name.
            const uint32_t table_oid_;
            const std::string table_id_;            // Index table id.
            const std::string table_uuid_;
            const std::string base_table_id_;    // Base table id.
            const uint32_t schema_version_ = 0; // Index table's schema version.
            const bool is_unique_ = false;      // Whether this is a unique index.
            const bool is_shared_ = false;      // whether this is a shared index
            const std::vector<IndexColumn> columns_; // Index columns.
            size_t hash_column_count_ = 0;     // Number of hash columns in the index.
            size_t range_column_count_ = 0;    // Number of range columns in the index.
            const std::vector<ColumnId> indexed_hash_column_ids_;  // Hash column ids in the base table.
            const std::vector<ColumnId> indexed_range_column_ids_; // Range column ids in the base table.
            const IndexPermissions index_permissions_ = INDEX_PERM_READ_WRITE_AND_DELETE;
        };

        class IndexMap : public std::unordered_map<std::string, IndexInfo> {
        public:
            IndexMap() {}

            Result<const IndexInfo*> FindIndex(const std::string& index_id) const;
        };

    }  // namespace sql
}  // namespace k2pg
