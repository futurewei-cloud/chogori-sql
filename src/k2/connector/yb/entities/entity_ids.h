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

#ifndef CHOGORI_SQL_ENTITY_IDS_H
#define CHOGORI_SQL_ENTITY_IDS_H

#include <string>
#include <set>

namespace k2 {

    using NamespaceName = std::string;
    using TableName = std::string;
    using UDTypeName = std::string;
    using RoleName = std::string;

    using NamespaceId = std::string;
    using TableId = std::string;
    using UDTypeId = std::string;

    using NamespaceIdTableNamePair = std::pair<NamespaceId, TableName>;

    static const uint32_t kPgSequencesDataTableOid = 0xFFFF;
    static const uint32_t kPgSequencesDataDatabaseOid = 0xFFFF;

    static const uint32_t kPgIndexTableOid = 2610;  // Hardcoded for pg_index. (in pg_index.h)

    extern const TableId kPgProcTableId;

}  // namespace k2

#endif //CHOGORI_SQL_ENTITY_IDS_H
