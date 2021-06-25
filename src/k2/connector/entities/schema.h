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
// Portions Copyright (c) 2021 Futurewei Cloud
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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "entities/type.h"
#include "common/result.h"

namespace k2pg {
namespace sql {
    using k2pg::Result;
    using k2pg::Status;

    typedef int32_t ColumnId;
    constexpr ColumnId kFirstColumnId = 0;

    enum PermissionType {
        ALTER = 0,
        CREATE = 1,
        DROP = 2,
        SELECT = 3,
        MODIFY = 4,
        AUTHORIZE = 5,
        DESCRIBE = 6,
        ALL = 999999999
    };

    class ColumnSchema {
        public:
        enum SortingType : uint8_t {
            kNotSpecified = 0,
            kAscending,          // ASC, NULLS FIRST
            kDescending,         // DESC, NULLS FIRST
            kAscendingNullsLast, // ASC, NULLS LAST
            kDescendingNullsLast // DESC, NULLS LAST
        };

        ColumnSchema(string name,
                const std::shared_ptr<SQLType>& type,
                bool is_nullable,
                bool is_primary,
                bool is_hash,
                int32_t order,
                SortingType sorting_type)
            : name_(std::move(name)),
            type_(type),
            is_nullable_(is_nullable),
            is_primary_(is_primary),
            is_hash_(is_hash),
            order_(order),
            sorting_type_(sorting_type) {
        }

        // convenience constructor for creating columns with simple (non-parametric) data types
        ColumnSchema(string name,
                DataType type,
                bool is_nullable,
                bool is_primary,
                bool is_hash,
                int32_t order,
                SortingType sorting_type)
            : ColumnSchema(name, SQLType::Create(type), is_nullable, is_primary, is_hash, order, sorting_type) {
        }

        const std::shared_ptr<SQLType>& type() const {
            return type_;
        }

        void set_type(const std::shared_ptr<SQLType>& type) {
            type_ = type;
        }

        bool is_nullable() const {
            return is_nullable_;
        }

        bool is_key() const {
            return is_hash_ || is_primary_;
        }

        bool is_hash() const {
            return is_hash_;
        }

        bool is_primary() const {
            return is_primary_;
        }

        int32_t order() const {
            return order_;
        }

        SortingType sorting_type() const {
            return sorting_type_;
        }

        void set_sorting_type(SortingType sorting_type) {
            sorting_type_ = sorting_type;
        }

        const std::string sorting_type_string() const {
            switch (sorting_type_) {
                case kNotSpecified:
                    return "none";
                case kAscending:
                    return "asc";
                case kDescending:
                    return "desc";
                case kAscendingNullsLast:
                    return "asc nulls last";
                case kDescendingNullsLast:
                    return "desc nulls last";
            }
            LOG (FATAL) << "Invalid sorting type: " << sorting_type_;
        }

        const std::string &name() const {
            return name_;
        }

        const TypeInfo* type_info() const {
            return type_->type_info();
        }

        // Return a string identifying this column, including its
        // name.
        std::string ToString() const;

        // Same as above, but only including the type information.
        // For example, "STRING NOT NULL".
        std::string TypeToString() const;

        bool EqualsType(const ColumnSchema &other) const {
            return is_nullable_ == other.is_nullable_ &&
                   is_primary_ == other.is_primary_ &&
                   is_hash_ == other.is_hash_ &&
                   sorting_type_ == other.sorting_type_ &&
                   type_info()->type() == other.type_info()->type();
        }

        bool Equals(const ColumnSchema &other) const {
            return EqualsType(other) && this->name_ == other.name_;
        }

        int Compare(const void *lhs, const void *rhs) const {
            return type_info()->Compare(lhs, rhs);
        }

        private:
        friend class SchemaBuilder;

        void set_name(const std::string& name) {
            name_ = name;
        }

        std::string name_;
        std::shared_ptr<SQLType> type_;
        bool is_nullable_;
        bool is_primary_;
        bool is_hash_;
        int32_t order_;
        SortingType sorting_type_;
    };


    // The schema for a set of rows.
    //
    // A Schema is simply a set of columns, along with information about
    // which prefix of columns makes up the primary key.
    //
    // Note that, while Schema is copyable and assignable, it is a complex
    // object that is not inexpensive to copy. You should generally prefer
    // passing by pointer or reference, and functions that create new
    // Schemas should generally prefer taking a Schema pointer and using
    // Schema::swap() or Schema::Reset() rather than returning by value.
    class Schema {
        public:

        static const int kColumnNotFound = -1;

        Schema()
        : num_key_columns_(0),
            num_hash_key_columns_(0),
            has_nullables_(false) {
        }

        Schema(const Schema& other);
        Schema& operator=(const Schema& other);

        void swap(Schema& other); // NOLINT(build/include_what_you_use)

        void CopyFrom(const Schema& other);

        // Construct a schema with the given information.
        //
        // NOTE: if the schema is user-provided, it's better to construct an
        // empty schema and then use Reset(...)  so that errors can be
        // caught. If an invalid schema is passed to this constructor, an
        // assertion will be fired!
        Schema(const std::vector<ColumnSchema>& cols,
                int key_columns) {
                CHECK_OK(Reset(cols, key_columns));
        }

        // Construct a schema with the given information.
        //
        // NOTE: if the schema is user-provided, it's better to construct an
        // empty schema and then use Reset(...)  so that errors can be
        // caught. If an invalid schema is passed to this constructor, an
        // assertion will be fired!
        Schema(const std::vector<ColumnSchema>& cols,
                const std::vector<ColumnId>& ids,
                int key_columns) {
                CHECK_OK(Reset(cols, ids, key_columns));
        }

        // Reset this Schema object to the given schema.
        // If this fails, the Schema object is left in an inconsistent
        // state and may not be used.
        CHECKED_STATUS Reset(const std::vector<ColumnSchema>& cols, int key_columns) {
            std::vector <ColumnId> ids;
            return Reset(cols, ids, key_columns);
        }

        // Reset this Schema object to the given schema.
        // If this fails, the Schema object is left in an inconsistent
        // state and may not be used.
        CHECKED_STATUS Reset(const std::vector<ColumnSchema>& cols,
            const std::vector<ColumnId>& ids,
            int key_columns);

        // Return the number of columns in this schema
        size_t num_columns() const {
            return cols_.size();
        }

        // Return the length of the key prefix in this schema.
        size_t num_key_columns() const {
            return num_key_columns_;
        }

        // Number of hash key columns.
        size_t num_hash_key_columns() const {
            return num_hash_key_columns_;
        }

        // Number of range key columns.
        size_t num_range_key_columns() const {
            return num_key_columns_ - num_hash_key_columns_;
        }

        // Return the ColumnSchema corresponding to the given column index.
        inline const ColumnSchema &column(size_t idx) const {
            DCHECK_LT(idx, cols_.size());
            return cols_[idx];
        }

        // Return the ColumnSchema corresponding to the given column ID.
        inline Result<const ColumnSchema&> column_by_id(ColumnId id) const {
            int idx = find_column_by_id(id);
            if (idx < 0) {
                return STATUS_FORMAT(InvalidArgument, "Column id {} not found", id);
            }
            return cols_[idx];
        }

        // Return the column ID corresponding to the given column index
        ColumnId column_id(size_t idx) const {
            DCHECK(has_column_ids());
            DCHECK_LT(idx, cols_.size());
            return col_ids_[idx];
        }

        // Return true if the schema contains an ID mapping for its columns.
        // In the case of an empty schema, this is false.
        bool has_column_ids() const {
            return !col_ids_.empty();
        }

        const std::vector<ColumnSchema>& columns() const {
            return cols_;
        }

        const std::vector<ColumnId>& column_ids() const {
            return col_ids_;
        }

        const std::vector<std::string> column_names() const {
            std::vector<string> column_names;
            for (const auto& col : cols_) {
                column_names.push_back(col.name());
            }
            return column_names;
        }

        // Return the column index corresponding to the given column,
        // or kColumnNotFound if the column is not in this schema.
        int find_column(const std::string col_name) const {
            auto iter = name_to_index_.find(col_name);
            if (PREDICT_FALSE(iter == name_to_index_.end())) {
                return kColumnNotFound;
            } else {
                return (*iter).second;
            }
        }

        Result<ColumnId> ColumnIdByName(const std::string& name) const;

        std::pair<bool, ColumnId> FindColumnIdByName(const std::string& col_name) const;

        // Returns true if the schema contains nullable columns
        bool has_nullables() const {
            return has_nullables_;
        }

        // Returns true if the specified column (by index) is a key
        bool is_key_column(size_t idx) const {
            return idx < num_key_columns_;
        }

        // Returns true if the specified column (by column id) is a key
        bool is_key_column(ColumnId column_id) const {
            return is_key_column(find_column_by_id(column_id));
        }

        // Returns true if the specified column (by name) is a key
        bool is_key_column(const std::string col_name) const {
            return is_key_column(find_column(col_name));
        }

        // Returns true if the specified column (by index) is a hash key
        bool is_hash_key_column(size_t idx) const {
            return idx < num_hash_key_columns_;
        }

        // Returns true if the specified column (by column id) is a hash key
        bool is_hash_key_column(ColumnId column_id) const {
            return is_hash_key_column(find_column_by_id(column_id));
        }

        // Returns true if the specified column (by name) is a hash key
        bool is_hash_key_column(const std::string col_name) const {
            return is_hash_key_column(find_column(col_name));
        }

        // Returns true if the specified column (by index) is a range column
        bool is_range_column(size_t idx) const {
            return is_key_column(idx) && !is_hash_key_column(idx);
        }

        // Returns true if the specified column (by column id) is a range column
        bool is_range_column(ColumnId column_id) const {
            return is_range_column(find_column_by_id(column_id));
        }

        // Returns true if the specified column (by name) is a range column
        bool is_range_column(const std::string col_name) const {
            return is_range_column(find_column(col_name));
        }

        // Return true if this Schema is initialized and valid.
        bool initialized() const {
            return !col_offsets_.empty();
        }

        // Returns the highest column id in this Schema.
        ColumnId max_col_id() const {
            return max_col_id_;
        }

        // Stringify this Schema. This is not particularly efficient,
        // so should only be used when necessary for output.
        std::string ToString() const;

        // Return true if the schemas have exactly the same set of columns
        // and respective types, and the same table properties.
        bool Equals(const Schema &other) const {
            if (this == &other) return true;
            if (this->num_key_columns_ != other.num_key_columns_) return false;
            if (this->cols_.size() != other.cols_.size()) return false;

            for (size_t i = 0; i < other.cols_.size(); i++) {
                if (!this->cols_[i].Equals(other.cols_[i])) return false;
            }

            return true;
        }

        // Returns the column index given the column ID.
        // If no such column exists, returns kColumnNotFound.
        int find_column_by_id(ColumnId id) const {
            DCHECK(cols_.empty() || has_column_ids());
            auto iter = id_to_index_.find(id);
            if (iter == id_to_index_.end()) {
               return kColumnNotFound;
            }
            return (*iter).second;
        }

        static ColumnId first_column_id();

        uint32_t version() const {
            return version_;
        }

        void set_version(uint32_t version) {
            version_ = version;
        }

        private:
        friend class SchemaBuilder;

        std::vector<ColumnSchema> cols_;
        size_t num_key_columns_;
        size_t num_hash_key_columns_;
        ColumnId max_col_id_;
        std::vector<ColumnId> col_ids_;
        std::vector<size_t> col_offsets_;

        std::unordered_map<std::string, size_t> name_to_index_;
        std::unordered_map<int, int> id_to_index_;

        // Cached indicator whether any columns are nullable.
        bool has_nullables_;

        // initialie schema version to zero
        uint32_t version_ = 0;
    };

    // Helper used for schema creation/editing.
    //
    // Example:
    //   Status s;
    //   SchemaBuilder builder(base_schema);
    //   s = builder.RemoveColumn("value");
    //   s = builder.AddKeyColumn("key2", STRING);
    //   s = builder.AddColumn("new_c1", UINT32);
    //   ...
    //   Schema new_schema = builder.Build();
    //
    class SchemaBuilder {
        public:
        SchemaBuilder() { Reset(); }
        explicit SchemaBuilder(const Schema& schema) { Reset(schema); }
        SchemaBuilder(const SchemaBuilder&) = delete;
        SchemaBuilder& operator=(const Variant&) = delete;

        void Reset();
        void Reset(const Schema& schema);

        bool is_valid() const { return cols_.size() > 0; }

        // Set the next column ID to be assigned to columns added with
        // AddColumn.
        void set_next_column_id(ColumnId next_id) {
            DCHECK_GE(next_id, ColumnId(0));
            next_id_ = next_id;
        }

        // Return the next column ID that would be assigned with AddColumn.
        ColumnId next_column_id() const {
            return next_id_;
        }

        Schema Build() const {
            return Schema(cols_, col_ids_, num_key_columns_);
        }

        Schema BuildWithoutIds() const {
            return Schema(cols_, num_key_columns_);
        }

        CHECKED_STATUS AddColumn(const ColumnSchema& column, bool is_key);

        CHECKED_STATUS AddColumn(const std::string& name, const std::shared_ptr<SQLType>& type) {
            return AddColumn(name, type, false, false, 0,
                             ColumnSchema::SortingType::kNotSpecified);
        }

        // convenience function for adding columns with simple (non-parametric) data types
        CHECKED_STATUS AddColumn(const std::string& name, DataType type) {
            return AddColumn(name, SQLType::Create(type));
        }

        CHECKED_STATUS AddNullableColumn(const std::string& name, const std::shared_ptr<SQLType>& type) {
            return AddColumn(name, type, true, false, 0,
                             ColumnSchema::SortingType::kNotSpecified);
        }

        // convenience function for adding columns with simple (non-parametric) data types
        CHECKED_STATUS AddNullableColumn(const std::string& name, DataType type) {
            return AddNullableColumn(name, SQLType::Create(type));
        }

        CHECKED_STATUS AddColumn(const std::string& name,
            const std::shared_ptr<SQLType>& type,
            bool is_nullable,
            bool is_key,
            int32_t order,
            ColumnSchema::SortingType sorting_type);

        // convenience function for adding columns with simple (non-parametric) data types
        CHECKED_STATUS AddColumn(const std::string& name,
            DataType type,
            bool is_nullable,
            bool is_key,
            int32_t order,
            ColumnSchema::SortingType sorting_type) {
            return AddColumn(name, SQLType::Create(type), is_nullable, is_key, order, sorting_type);
        }

        CHECKED_STATUS RemoveColumn(const std::string& name);
        CHECKED_STATUS RenameColumn(const std::string& old_name, const std::string& new_name);

        private:

        ColumnId next_id_;
        std::vector<ColumnId> col_ids_;
        std::vector<ColumnSchema> cols_;
        std::unordered_set<string> col_names_;
        size_t num_key_columns_;
    };
}  // namespace sql
}  // namespace k2pg

typedef class k2pg::sql::Schema PgSchema;
