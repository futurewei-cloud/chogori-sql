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

#include "entities/table.h"

namespace k2pg {
namespace sql {

    Result<const IndexInfo*> TableInfo::FindIndex(const std::string& index_id) const {
        return index_map_.FindIndex(index_id);
    }

    std::shared_ptr<TableInfo> TableInfo::Clone(std::shared_ptr<TableInfo> table_info, std::string database_id,
            std::string database_name, std::string table_uuid, std::string table_name) {
        std::shared_ptr<TableInfo> new_table_info = std::make_shared<TableInfo>(database_id, database_name, table_info->table_oid(), table_name, table_uuid, table_info->schema());
        new_table_info->set_next_column_id(table_info->next_column_id());
        new_table_info->set_is_sys_table(table_info->is_sys_table());
        if (table_info->has_secondary_indexes()) {
            for (std::pair<std::string, IndexInfo> secondary_index : table_info->secondary_indexes()) {
                new_table_info->add_secondary_index(secondary_index.first, secondary_index.second);
            }
        }

        return new_table_info;
    }
}  // namespace sql
}  // namespace k2pg
