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

#include "entities/schema.h"

#include <set>
#include <algorithm>

#include "common/strings/substitute.h"
#include "common/strings/strcat.h"
#include "common/strings/join.h"
#include "common/format.h"
#include "common/port.h"
#include "common/status.h"
#include "common/util/map-util.h"

namespace k2pg {
namespace sql {

    using std::shared_ptr;
    using std::set;
    using std::unordered_map;
    using std::unordered_set;
    using yb::Result;
    using yb::Status;
    using yb::IdMapping;

    string ColumnSchema::ToString() const {
        return strings::Substitute("$0[$1]",
                                   name_,
                                   TypeToString());
    }

    string ColumnSchema::TypeToString() const {
        return strings::Substitute("$0, $1, $2, $3, $4",
                                   type_info()->name(),
                                   is_nullable_ ? "NULLABLE" : "NOT NULL",
                                   is_primary_ ? "PRIMARY KEY" : "NOT A PRIMARY KEY",
                                   is_hash_ ? "HASH KEY" : "NOT A HASH KEY",
                                   order_);
    }

    Schema::Schema(const Schema& other)
            : name_to_index_bytes_(0),
              name_to_index_(10,
                             NameToIndexMap::hasher(),
                             NameToIndexMap::key_equal(),
                             NameToIndexMapAllocator(&name_to_index_bytes_)) {
        CopyFrom(other);
    }

    Schema& Schema::operator=(const Schema& other) {
        if (&other != this) {
            CopyFrom(other);
        }
        return *this;
    }

    void Schema::CopyFrom(const Schema& other) {
        num_key_columns_ = other.num_key_columns_;
        num_hash_key_columns_ = other.num_hash_key_columns_;
        cols_ = other.cols_;
        col_ids_ = other.col_ids_;
        col_offsets_ = other.col_offsets_;
        id_to_index_ = other.id_to_index_;

        // We can't simply copy name_to_index_ since the GStringPiece keys
        // reference the other Schema's ColumnSchema objects.
        name_to_index_.clear();
        int i = 0;
        for (const ColumnSchema &col : cols_) {
            // The map uses the 'name' string from within the ColumnSchema object.
            name_to_index_[col.name()] = i++;
        }

        has_nullables_ = other.has_nullables_;
    }

    void Schema::swap(Schema& other) {
        std::swap(num_key_columns_, other.num_key_columns_);
        std::swap(num_hash_key_columns_, other.num_hash_key_columns_);
        cols_.swap(other.cols_);
        col_ids_.swap(other.col_ids_);
        col_offsets_.swap(other.col_offsets_);
        name_to_index_.swap(other.name_to_index_);
        id_to_index_.swap(other.id_to_index_);
        std::swap(has_nullables_, other.has_nullables_);
    }

    Status Schema::Reset(const vector<ColumnSchema>& cols,
                         const vector<ColumnId>& ids,
                         int key_columns) {
        cols_ = cols;
        num_key_columns_ = key_columns;
        num_hash_key_columns_ = 0;

        // Determine whether any column is nullable or static, and count number of hash columns.
        has_nullables_ = false;
        for (const ColumnSchema& col : cols_) {
            if (col.is_key()) {
                num_hash_key_columns_++;
            }
            if (col.is_nullable()) {
                has_nullables_ = true;
            }
        }

        if (PREDICT_FALSE(key_columns > cols_.size())) {
            return STATUS(InvalidArgument,
                          "Bad schema", "More key columns than columns");
        }

        if (PREDICT_FALSE(key_columns < 0)) {
            return STATUS(InvalidArgument,
                          "Bad schema", "Cannot specify a negative number of key columns");
        }

        if (PREDICT_FALSE(!ids.empty() && ids.size() != cols_.size())) {
            return STATUS(InvalidArgument, "Bad schema",
                          "The number of ids does not match with the number of columns");
        }

        // Verify that the key columns are not nullable nor static
        for (int i = 0; i < key_columns; ++i) {
            if (PREDICT_FALSE(cols_[i].is_nullable())) {
                return STATUS(InvalidArgument,
                              "Bad schema", strings::Substitute("Nullable key columns are not "
                                                                "supported: $0", cols_[i].name()));
            }

        }

        // Calculate the offset of each column in the row format.
        col_offsets_.reserve(cols_.size() + 1);  // Include space for total byte size at the end.
        size_t off = 0;
        size_t i = 0;
        name_to_index_.clear();
        for (const ColumnSchema &col : cols_) {
            // The map uses the 'name' string from within the ColumnSchema object.
            if (!InsertIfNotPresent(&name_to_index_, col.name(), i++)) {
                return STATUS(InvalidArgument, "Duplicate column name", col.name());
            }

            col_offsets_.push_back(off);
            off += col.type_info()->size();
        }

        // Add an extra element on the end for the total
        // byte size
        col_offsets_.push_back(off);

        // Initialize IDs mapping
        col_ids_ = ids;
        id_to_index_.clear();
        max_col_id_ = 0;
        for (int i = 0; i < ids.size(); ++i) {
            if (ids[i] > max_col_id_) {
                max_col_id_ = ids[i];
            }
            id_to_index_.set(ids[i], i);
        }

        // Ensure clustering columns have a default sorting type of 'ASC' if not specified.
        for (int i = num_hash_key_columns_; i < num_key_columns(); i++) {
            ColumnSchema& col = cols_[i];
            if (col.sorting_type() == ColumnSchema::SortingType::kNotSpecified) {
                col.set_sorting_type(ColumnSchema::SortingType::kAscending);
            }
        }

        return Status::OK();
    }

    string Schema::ToString() const {
        vector<string> col_strs;
        if (has_column_ids()) {
            for (int i = 0; i < cols_.size(); ++i) {
                col_strs.push_back(strings::Substitute("$0:$1", col_ids_[i], cols_[i].ToString()));
            }
        } else {
            for (const ColumnSchema &col : cols_) {
                col_strs.push_back(col.ToString());
            }
        }

        return StrCat("Schema [\n\t",
                      JoinStrings(col_strs, ",\n\t"));
    }

    Result<int> Schema::ColumnIndexByName(GStringPiece col_name) const {
        auto index = find_column(col_name);
        if (index == kColumnNotFound) {
            return STATUS_FORMAT(Corruption, "$0 not found in schema $1", col_name, name_to_index_);
        }
        return index;
    }

    Result<ColumnId> Schema::ColumnIdByName(const std::string& column_name) const {
        size_t column_index = find_column(column_name);
        if (column_index == Schema::kColumnNotFound) {
            return STATUS_FORMAT(NotFound, "Couldn't find column $0 in the schema", column_name);
        }
        return ColumnId(column_id(column_index));
    }

    std::pair<bool, ColumnId> Schema::FindColumnIdByName(const std::string& column_name) const {
        size_t column_index = find_column(column_name);
        if (column_index == Schema::kColumnNotFound) {
            return std::make_pair<bool, ColumnId>(false, -1);
        }
        return std::make_pair<bool, ColumnId>(true, ColumnId(column_id(column_index)));
    }

    ColumnId Schema::first_column_id() {
        return kFirstColumnId;
    }

    void SchemaBuilder::Reset() {
        cols_.clear();
        col_ids_.clear();
        col_names_.clear();
        num_key_columns_ = 0;
        next_id_ = kFirstColumnId;
    }

    void SchemaBuilder::Reset(const Schema& schema) {
        cols_ = schema.cols_;
        col_ids_ = schema.col_ids_;
        num_key_columns_ = schema.num_key_columns_;
        for (const auto& column : cols_) {
            col_names_.insert(column.name());
        }

        if (col_ids_.empty()) {
            for (int32_t i = 0; i < cols_.size(); ++i) {
                col_ids_.push_back(ColumnId(kFirstColumnId + i));
            }
        }
        if (col_ids_.empty()) {
            next_id_ = kFirstColumnId;
        } else {
            next_id_ = *std::max_element(col_ids_.begin(), col_ids_.end()) + 1;
        }
    }

    Status SchemaBuilder::AddColumn(const string& name,
                                    const std::shared_ptr<SQLType>& type,
                                    bool is_nullable,
                                    bool is_key,
                                    int32_t order,
                                    ColumnSchema::SortingType sorting_type) {
        return AddColumn(ColumnSchema(name, type, is_nullable, is_key, /*is_partition*/ false, order, sorting_type), is_key);
    }

    Status SchemaBuilder::RemoveColumn(const string& name) {
        unordered_set<string>::const_iterator it_names;
        if ((it_names = col_names_.find(name)) == col_names_.end()) {
            return STATUS(NotFound, "The specified column does not exist", name);
        }

        col_names_.erase(it_names);
        for (int i = 0; i < cols_.size(); ++i) {
            if (name == cols_[i].name()) {
                cols_.erase(cols_.begin() + i);
                col_ids_.erase(col_ids_.begin() + i);
                if (i < num_key_columns_) {
                    num_key_columns_--;
                }
                return Status::OK();
            }
        }

        LOG(FATAL) << "Should not reach here";
        return STATUS(Corruption, "Unable to remove existing column");
    }

    Status SchemaBuilder::RenameColumn(const string& old_name, const string& new_name) {
        unordered_set<string>::const_iterator it_names;

        // check if 'new_name' is already in use
        if ((it_names = col_names_.find(new_name)) != col_names_.end()) {
            return STATUS(AlreadyPresent, "The column already exists", new_name);
        }

        // check if the 'old_name' column exists
        if ((it_names = col_names_.find(old_name)) == col_names_.end()) {
            return STATUS(NotFound, "The specified column does not exist", old_name);
        }

        col_names_.erase(it_names);   // TODO: Should this one stay and marked as alias?
        col_names_.insert(new_name);

        for (ColumnSchema& col_schema : cols_) {
            if (old_name == col_schema.name()) {
                col_schema.set_name(new_name);
                return Status::OK();
            }
        }

        LOG(FATAL) << "Should not reach here";
        return STATUS(IllegalState, "Unable to rename existing column");
    }

    Status SchemaBuilder::AddColumn(const ColumnSchema& column, bool is_key) {
        if (ContainsKey(col_names_, column.name())) {
            return STATUS(AlreadyPresent, "The column already exists", column.name());
        }

        col_names_.insert(column.name());
        if (is_key) {
            cols_.insert(cols_.begin() + num_key_columns_, column);
            col_ids_.insert(col_ids_.begin() + num_key_columns_, next_id_);
            num_key_columns_++;
        } else {
            cols_.push_back(column);
            col_ids_.push_back(next_id_);
        }

        next_id_ = ColumnId(next_id_ + 1);
        return Status::OK();
    }

}  // namespace sql
}  // namespace k2pg
