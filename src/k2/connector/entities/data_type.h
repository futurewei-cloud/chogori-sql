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

// this type definition is synced with the PgDataType in pg_gate_typedefs.h. We might not support all of them
typedef enum K2SqlDataType {
    K2SQL_DATA_TYPE_NOT_SUPPORTED = -1,
    K2SQL_DATA_TYPE_UNKNOWN_DATA = 999,
    K2SQL_DATA_TYPE_NULL_VALUE_TYPE = 0,
    K2SQL_DATA_TYPE_INT8 = 1,
    K2SQL_DATA_TYPE_INT16 = 2,
    K2SQL_DATA_TYPE_INT32 = 3,
    K2SQL_DATA_TYPE_INT64 = 4,
    K2SQL_DATA_TYPE_STRING = 5,
    K2SQL_DATA_TYPE_BOOL = 6,
    K2SQL_DATA_TYPE_FLOAT = 7,
    K2SQL_DATA_TYPE_DOUBLE = 8,
    K2SQL_DATA_TYPE_BINARY = 9,
    K2SQL_DATA_TYPE_TIMESTAMP = 10,
    K2SQL_DATA_TYPE_DECIMAL = 11,
    K2SQL_DATA_TYPE_VARINT = 12,
    K2SQL_DATA_TYPE_INET = 13,
    K2SQL_DATA_TYPE_LIST = 14,
    K2SQL_DATA_TYPE_MAP = 15,
    K2SQL_DATA_TYPE_SET = 16,
    K2SQL_DATA_TYPE_UUID = 17,
    K2SQL_DATA_TYPE_TIMEUUID = 18,
    K2SQL_DATA_TYPE_TUPLE = 19,
    K2SQL_DATA_TYPE_TYPEARGS = 20,
    K2SQL_DATA_TYPE_USER_DEFINED_TYPE = 21,
    K2SQL_DATA_TYPE_FROZEN = 22,
    K2SQL_DATA_TYPE_DATE = 23,
    K2SQL_DATA_TYPE_TIME = 24,
    K2SQL_DATA_TYPE_JSONB = 25,
    K2SQL_DATA_TYPE_UINT8 = 100,
    K2SQL_DATA_TYPE_UINT16 = 101,
    K2SQL_DATA_TYPE_UINT32 = 102,
    K2SQL_DATA_TYPE_UINT64 = 103
} DataType;

#endif //CHOGORI_SQL_DATA_TYPE_H
