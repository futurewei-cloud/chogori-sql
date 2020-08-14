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

#include "yb/common/concurrent/ref_counted.h"
#include "yb/entities/entity_ids.h"
#include "yb/entities/schema.h"
#include "yb/entities/index.h"

namespace k2 {
namespace sql {

    using yb::RefCountedThreadSafe;

    class TableInfo : public RefCountedThreadSafe<TableInfo> {
        public: 
          
        typedef scoped_refptr<TableInfo> ScopedRefPtr;

        TableInfo(TableId table_id, TableName table_name, Schema schema, IndexMap index_map) : 
            table_id_(table_id), table_name_(table_name), schema_(std::move(schema)), index_map_(std::move(index_map)) {
        }

        const TableId table_id() const {
            return table_id_;
        }

        const TableName table_name() const {
            return table_name_;
        }

        const Schema& schema() const {
            return schema_;
        }

        const bool has_secondary_indexes() {
            return !index_map_.empty();
        }

        Result<const IndexInfo*> FindIndex(const TableId& index_id) const;

        private: 
        TableId table_id_;
        TableName table_name_;
        Schema schema_;
        IndexMap index_map_;
        // TODO: add partition information if necessary for PG
    };

}  // namespace sql
}  // namespace k2

#endif //CHOGORI_SQL_TABLE_H