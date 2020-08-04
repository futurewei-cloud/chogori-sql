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
#ifndef CHOGORI_SQL_SCHEMA_H
#define CHOGORI_SQL_SCHEMA_H

#include <string>

namespace k2 {
namespace sql {
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
                bool is_nullable = false,
                bool is_key = false,
                SortingType sorting_type = SortingType::kNotSpecified)
            : name_(std::move(name)),
            type_(type),
            is_nullable_(is_nullable),
            is_key_(is_key),
            sorting_type_(sorting_type) {
        }

        // convenience constructor for creating columns with simple (non-parametric) data types
        ColumnSchema(string name,
                DataType type,
                bool is_nullable = false,
                bool is_key = false,
                SortingType sorting_type = SortingType::kNotSpecified)
            : ColumnSchema(name, QLType::Create(type), is_nullable, is_key, sorting_type) {
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
            return is_hash_key_;
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

        // Return a string identifying this column, including its
        // name.
        std::string ToString() const;

        // Same as above, but only including the type information.
        // For example, "STRING NOT NULL".
        std::string TypeToString() const;

        bool EqualsType(const ColumnSchema &other) const {
            return is_nullable_ == other.is_nullable_ &&
                   is_hash_key_ == other.is_hash_key_ &&
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
        std::string name_;
        bool is_nullable_;
        bool is_key_;
        SortingType sorting_type_;
    }
}  // namespace sql
}  // namespace k2

#endif //CHOGORI_SQL_SCHEMA_H
