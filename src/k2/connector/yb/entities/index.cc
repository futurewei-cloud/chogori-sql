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

#include "yb/entities/index.h"
#include "yb/common/format.h"

using std::vector;
using std::unordered_map;

namespace k2 {
namespace sql {

    vector<ColumnId> IndexInfo::index_key_column_ids() const {
        unordered_map<ColumnId, ColumnId> map;
        for (const auto column : columns_) {
            map[column.indexed_column_id] = column.column_id;
        }
        vector<ColumnId> ids;
        ids.reserve(indexed_hash_column_ids_.size() + indexed_range_column_ids_.size());
        for (const auto id : indexed_hash_column_ids_) {
            ids.push_back(map[id]);
        }
        for (const auto id : indexed_range_column_ids_) {
            ids.push_back(map[id]);
        }
        return ids;
    }

    bool IndexInfo::PrimaryKeyColumnsOnly(const Schema& indexed_schema) const {
        for (size_t i = 0; i < hash_column_count_ + range_column_count_; i++) {
            if (!indexed_schema.is_key_column(columns_[i].indexed_column_id)) {
                return false;
            }
        }
        return true;
    }

    // Check for dependency is used for DDL operations, so it does not need to be fast. As a result,
    // the dependency list does not need to be cached in a member id list for fast access.
    bool IndexInfo::CheckColumnDependency(ColumnId column_id) const {
        for (const IndexColumn &index_col : columns_) {
            // The protobuf data contains IDs of all columns that this index is referencing.
            // Examples:
            // 1. Index by column
            // - INDEX ON tab (a_column)
            // - The ID of "a_column" is included in protobuf data.
            //
            // 2. Index by expression of column:
            // - INDEX ON tab (j_column->>'field')
            // - The ID of "j_column" is included in protobuf data.
            if (index_col.indexed_column_id == column_id) {
                return true;
            }
        }
        return false;
    }

    int32_t IndexInfo::FindKeyIndex(const string& key_expr_name) const {
        for (int32_t idx = 0; idx < key_column_count(); idx++) {
            const auto& col = columns_[idx];
            if (!col.column_name.empty() && key_expr_name.find(col.column_name) != key_expr_name.npos) {
                // Return the found key column that is referenced by the expression.
                return idx;
            }
        }

        return -1;
    }

    Result<const IndexInfo*> IndexMap::FindIndex(const TableId& index_id) const {
        const auto itr = find(index_id);
        if (itr == end()) {
            return STATUS(NotFound, yb::Format("Index id $0 not found", index_id));
        }
        return &itr->second;
    }

}  // namespace sql
}  // namespace k2
