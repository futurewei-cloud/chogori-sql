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

#ifndef CHOGORI_SQL_DATA_TYPE_H
#define CHOGORI_SQL_DATA_TYPE_H

namespace k2 {
namespace sql {

    // this type definition is synced with the PgDataType in ybc_pg_typedefs.h. We might not support all of them
    typedef enum PgSqlDataType {
        NOT_SUPPORTED = -1,
        UNKNOWN_DATA = 999,
        NULL_VALUE_TYPE = 0,
        INT8 = 1,
        INT16 = 2,
        INT32 = 3,
        INT64 = 4,
        STRING = 5,
        BOOL = 6,
        FLOAT = 7,
        DOUBLE = 8,
        BINARY = 9,
        TIMESTAMP = 10,
        DECIMAL = 11,
        VARINT = 12,
        INET = 13,
        LIST = 14,
        MAP = 15,
        SET = 16,
        UUID = 17,
        TIMEUUID = 18,
        TUPLE = 19,
        TYPEARGS = 20,
        USER_DEFINED_TYPE = 21,
        FROZEN = 22,
        DATE = 23,
        TIME = 24,
        JSONB = 25,
    } DataType;

}  // namespace sql
}  // namespace k2

#endif //CHOGORI_SQL_DATA_TYPE_H
