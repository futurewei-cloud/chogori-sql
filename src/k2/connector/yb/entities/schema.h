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

#ifndef CHOGORI_SQL_SCHEMA_H
#define CHOGORI_SQL_SCHEMA_H

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "yb/entities/type.h"
#include "yb/common/id_mapping.h"
#include "yb/common/result.h"
#include "yb/common/strings/stringpiece.h"
#include "yb/common/util/stl_util.h"

// Check that two schemas are equal, yielding a useful error message in the case that
// they are not.
#define DCHECK_SCHEMA_EQ(s1, s2) \
  do { \
    DCHECK((s1).Equals((s2))) << "Schema " << (s1).ToString() \
                              << " does not match " << (s2).ToString(); \
  } while (0);

#define DCHECK_KEY_PROJECTION_SCHEMA_EQ(s1, s2) \
  do { \
    DCHECK((s1).KeyEquals((s2))) << "Key-Projection Schema " \
                                 << (s1).ToString() << " does not match " \
                                 << (s2).ToString(); \
  } while (0);

#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)
#endif

namespace k2pg {
namespace sql {
    using yb::Result;
    using yb::Status;
    using yb::IdMapping;

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
                bool is_partition,
                int32_t order,
                SortingType sorting_type)
            : name_(std::move(name)),
            type_(type),
            is_nullable_(is_nullable),
            is_primary_(is_primary),
            is_partition_(is_partition),
            order_(order),
            sorting_type_(sorting_type) {
        }

        // convenience constructor for creating columns with simple (non-parametric) data types
        ColumnSchema(string name,
                DataType type,
                bool is_nullable,
                bool is_primary,
                bool is_partition,
                int32_t order,
                SortingType sorting_type)
            : ColumnSchema(name, SQLType::Create(type), is_nullable, is_primary, is_partition, order, sorting_type) {
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
            return is_partition_ || is_primary_;
        }

        bool is_partition() const {
            return is_partition_;
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
                   is_partition_ == other.is_partition_ &&
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
        bool is_partition_;
        int32_t order_;
        SortingType sorting_type_;
    };

    class TableProperties {
        public:
        TableProperties() = default;
        ~TableProperties() = default;

        bool operator==(const TableProperties& other) const {
            return default_time_to_live_ == other.default_time_to_live_;
        }

        bool operator!=(const TableProperties& other) const {
            return !(*this == other);
        }

        bool HasDefaultTimeToLive() const {
            return (default_time_to_live_ != kNoDefaultTtl);
        }

        void SetDefaultTimeToLive(uint64_t default_time_to_live) {
            default_time_to_live_ = default_time_to_live;
        }

        int64_t DefaultTimeToLive() const {
            return default_time_to_live_;
        }

        bool is_transactional() const {
            return is_transactional_;
        }

        void SetTransactional(bool is_transactional) {
            is_transactional_ = is_transactional;
        }

        void Reset();

        std::string ToString() const;

        private:
        static const int kNoDefaultTtl = -1;
        bool is_transactional_ = false;
        int64_t default_time_to_live_ = kNoDefaultTtl;
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
            name_to_index_bytes_(0),
            // TODO: C++11 provides a single-arg constructor
            name_to_index_(10,
                NameToIndexMap::hasher(),
                NameToIndexMap::key_equal(),
                NameToIndexMapAllocator(&name_to_index_bytes_)),
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
        Schema(const vector<ColumnSchema>& cols,
                int key_columns,
                const TableProperties& table_properties = TableProperties())
            : name_to_index_bytes_(0),
            // TODO: C++11 provides a single-arg constructor
            name_to_index_(10,
                NameToIndexMap::hasher(),
                NameToIndexMap::key_equal(),
                NameToIndexMapAllocator(&name_to_index_bytes_)) {
                CHECK_OK(Reset(cols, key_columns, table_properties));
        }

        // Construct a schema with the given information.
        //
        // NOTE: if the schema is user-provided, it's better to construct an
        // empty schema and then use Reset(...)  so that errors can be
        // caught. If an invalid schema is passed to this constructor, an
        // assertion will be fired!
        Schema(const vector<ColumnSchema>& cols,
                const vector<ColumnId>& ids,
                int key_columns,
                const TableProperties& table_properties = TableProperties())
            : name_to_index_bytes_(0),
            name_to_index_(10,
                NameToIndexMap::hasher(),
                NameToIndexMap::key_equal(),
                NameToIndexMapAllocator(&name_to_index_bytes_)) {
                CHECK_OK(Reset(cols, ids, key_columns, table_properties));
        }

        // Reset this Schema object to the given schema.
        // If this fails, the Schema object is left in an inconsistent
        // state and may not be used.
        CHECKED_STATUS Reset(const vector<ColumnSchema>& cols, int key_columns,
            const TableProperties& table_properties = TableProperties()) {
            std::vector <ColumnId> ids;
            return Reset(cols, ids, key_columns, table_properties);
        }

        // Reset this Schema object to the given schema.
        // If this fails, the Schema object is left in an inconsistent
        // state and may not be used.
        CHECKED_STATUS Reset(const vector<ColumnSchema>& cols,
            const vector<ColumnId>& ids,
            int key_columns,
            const TableProperties& table_properties = TableProperties());

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
                return STATUS_FORMAT(InvalidArgument, "Column id $0 not found", id);
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

        const std::vector<string> column_names() const {
            vector<string> column_names;
            for (const auto& col : cols_) {
                column_names.push_back(col.name());
            }
            return column_names;
        }

        const TableProperties& table_properties() const {
            return table_properties_;
        }

        void SetDefaultTimeToLive(const uint64_t& ttl_msec) {
            table_properties_.SetDefaultTimeToLive(ttl_msec);
        }

        // Return the column index corresponding to the given column,
        // or kColumnNotFound if the column is not in this schema.
        int find_column(const GStringPiece col_name) const {
            auto iter = name_to_index_.find(col_name);
            if (PREDICT_FALSE(iter == name_to_index_.end())) {
                return kColumnNotFound;
            } else {
                return (*iter).second;
            }
        }

        Result<ColumnId> ColumnIdByName(const std::string& name) const;

        Result<int> ColumnIndexByName(GStringPiece col_name) const;

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
        bool is_key_column(const GStringPiece col_name) const {
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
        bool is_hash_key_column(const GStringPiece col_name) const {
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
        bool is_range_column(const GStringPiece col_name) const {
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
            if (this->table_properties_ != other.table_properties_) return false;
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
            int ret = id_to_index_[id];
            if (ret == -1) {
                return kColumnNotFound;
            }
            return ret;
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

        vector<ColumnSchema> cols_;
        size_t num_key_columns_;
        size_t num_hash_key_columns_;
        ColumnId max_col_id_;
        vector<ColumnId> col_ids_;
        vector<size_t> col_offsets_;

        // The keys of this map are GStringPiece references to the actual name members of the
        // ColumnSchema objects inside cols_. This avoids an extra copy of those strings,
        // and also allows us to do lookups on the map using GStringPiece keys, sometimes
        // avoiding copies.
        //
        // The map is instrumented with a counting allocator so that we can accurately
        // measure its memory footprint.
        int64_t name_to_index_bytes_;

        typedef STLCountingAllocator<std::pair<const GStringPiece, size_t> > NameToIndexMapAllocator;

        typedef std::unordered_map<GStringPiece, size_t, std::hash<GStringPiece>,
            std::equal_to<GStringPiece>, NameToIndexMapAllocator> NameToIndexMap;

        NameToIndexMap name_to_index_;

        IdMapping id_to_index_;

        // Cached indicator whether any columns are nullable.
        bool has_nullables_;

        TableProperties table_properties_;

        uint32_t version_;
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

        void SetTableProperties(TableProperties& table_properties) {
            table_properties_ = table_properties;
        }

        Schema Build() const {
            return Schema(cols_, col_ids_, num_key_columns_, table_properties_);
        }

        Schema BuildWithoutIds() const {
            return Schema(cols_, num_key_columns_, table_properties_);
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
        vector<ColumnId> col_ids_;
        vector<ColumnSchema> cols_;
        std::unordered_set<string> col_names_;
        size_t num_key_columns_;
        TableProperties table_properties_;

        DISALLOW_COPY_AND_ASSIGN(SchemaBuilder);
    };
}  // namespace sql
}  // namespace k2pg

typedef class k2pg::sql::Schema PgSchema;

#endif //CHOGORI_SQL_SCHEMA_H
