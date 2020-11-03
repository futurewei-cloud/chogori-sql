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

#ifndef CHOGORI_SQL_TABLE_H
#define CHOGORI_SQL_TABLE_H

#include <memory>
#include <string>

#include "yb/common/status.h"
#include "yb/entities/entity_ids.h"
#include "yb/entities/schema.h"
#include "yb/entities/index.h"

namespace k2pg {
namespace sql {
    struct TableIdentifier {
        NamespaceName namespace_name; // Can be empty, that means the namespace has not been set yet.
        TableName table_name;  
        TableIdentifier(NamespaceName ns, TableName tn) : namespace_name(ns), table_name(tn) {
        }
    };

    class TableInfo {
        public: 
          
        typedef std::shared_ptr<TableInfo> SharedPtr;

        TableInfo(NamespaceName namespace_name, TableName table_name, Schema schema) : 
            table_id_(namespace_name, table_name), schema_(std::move(schema)) {
        }

        const NamespaceName& namespace_name() const {
            return table_id_.namespace_name;
        }

        const TableName& table_name() const {
            return table_id_.table_name;
        }

        const TableIdentifier& table_identifier() {
            return table_id_;
        }
        
        const Schema& schema() const {
            return schema_;
        }

        const bool has_secondary_indexes() {
            return !index_map_.empty();
        }

         // Return the number of columns in this table
        size_t num_columns() const {
            return schema_.num_columns();
        }

        // Return the length of the key prefix in this table.
        size_t num_key_columns() const {
            return schema_.num_key_columns();
        }

        // Number of hash key columns.
        size_t num_hash_key_columns() const {
            return schema_.num_hash_key_columns();
        }

        // Number of range key columns.
        size_t num_range_key_columns() const {
            return schema_.num_range_key_columns();
        }

        void add_secondary_index(const TableId& index_id, const IndexInfo& index_info) {
            index_map_.emplace(index_id, index_info);
        }

        Result<const IndexInfo*> FindIndex(const TableId& index_id) const;

        private: 
        
        TableIdentifier table_id_;
        Schema schema_;
        IndexMap index_map_;
    };

}  // namespace sql
}  // namespace k2pg

#endif //CHOGORI_SQL_TABLE_H
