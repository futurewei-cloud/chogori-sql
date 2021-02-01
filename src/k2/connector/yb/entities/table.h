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

    class TableInfo {
        public:

        typedef std::shared_ptr<TableInfo> SharedPtr;

        TableInfo(std::string namespace_id, std::string namespace_name, uint32_t table_oid, std::string table_name, std::string table_uuid, Schema schema) :
            namespace_id_(namespace_id), namespace_name_(namespace_name), table_oid_(table_oid), table_id_(PgObjectId::GetTableId(table_oid)), table_name_(table_name),
            table_uuid_(table_uuid), schema_(std::move(schema)) {
        }

        const std::string& namespace_id() const {
            return namespace_id_;
        }

        const std::string& namespace_name() const {
            return namespace_name_;
        }

        const std::string& table_id() const {
            return table_id_;
        }

        const std::string& table_name() const {
            return table_name_;
        }

        void set_table_oid(uint32_t table_oid) {
            table_oid_ = table_oid;
        }

        uint32_t table_oid() {
            return table_oid_;
        }

        const std::string table_uuid() {
            return table_uuid_;
        }

        void set_next_column_id(int32_t next_column_id) {
            next_column_id_ = next_column_id;
        }

        int32_t next_column_id() {
            return next_column_id_;
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

        void add_secondary_index(const std::string& index_id, const IndexInfo& index_info) {
            index_map_.emplace(index_id, index_info);
        }

        const IndexMap& secondary_indexes() {
            return index_map_;
        }

        void drop_index(const std::string& index_id) {
            index_map_.erase(index_id);
        }

        Result<const IndexInfo*> FindIndex(const std::string& index_id) const;

        void set_is_sys_table(bool is_sys_table) {
            is_sys_table_ = is_sys_table;
        }

        bool is_sys_table() {
            return is_sys_table_;
        }

        void set_is_shared_table(bool is_shared_table) {
            is_shared_table_ = is_shared_table;
        }

        bool is_shared() {
            return is_shared_table_;
        }

        static std::shared_ptr<TableInfo> Clone(std::shared_ptr<TableInfo> table_info, std::string namespace_id,
            std::string namespace_name, std::string table_uuid, std::string table_name);

        private:
        std::string namespace_id_;
        std::string namespace_name_; // Can be empty, that means the namespace has not been set yet.
        // PG internal object id
        uint32_t table_oid_;
        std::string table_id_;
        std::string table_name_;
        // cache key and it is unique cross databases
        std::string table_uuid_;
        Schema schema_;
        IndexMap index_map_;
        int32_t next_column_id_ = 0;
        bool is_sys_table_ = false;
        bool is_shared_table_ = false;
    };

}  // namespace sql
}  // namespace k2pg

#endif //CHOGORI_SQL_TABLE_H
