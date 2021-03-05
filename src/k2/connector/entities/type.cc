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

#include "entities/type.h"
#include "common/macros.h"

namespace k2pg {
namespace sql {

    using std::shared_ptr;
    using std::vector;
    using std::string;

    //--------------------------------------------------------------------------------------------------
    // The following functions are to construct QLType objects.

    shared_ptr<SQLType> SQLType::Create(DataType data_type, const vector<shared_ptr<SQLType>>& params) {
        switch (data_type) {
            case DataType::LIST:
                DCHECK_EQ(params.size(), 1);
                return CreateCollectionType<DataType::LIST>(params);
            case DataType::MAP:
                DCHECK_EQ(params.size(), 2);
                return CreateCollectionType<DataType::MAP>(params);
            case DataType::SET:
                DCHECK_EQ(params.size(), 1);
                return CreateCollectionType<DataType::SET>(params);
            case DataType::TUPLE:
                return CreateCollectionType<DataType::TUPLE>(params);
                // User-defined types cannot be created like this
            case DataType::USER_DEFINED_TYPE:
                LOG(FATAL) << "Unsupported constructor for user-defined type";
                return nullptr;
            default:
                DCHECK_EQ(params.size(), 0);
                return Create(data_type);
        }
    }

    shared_ptr<SQLType> SQLType::Create(DataType data_type) {
        switch (data_type) {
            case DataType::UNKNOWN_DATA:
                return CreatePrimitiveType<DataType::UNKNOWN_DATA>();
            case DataType::NULL_VALUE_TYPE:
                return CreatePrimitiveType<DataType::NULL_VALUE_TYPE>();
            case DataType::UINT8:
                return CreatePrimitiveType<DataType::UINT8>();
            case DataType::INT8:
                return CreatePrimitiveType<DataType::INT8>();
            case DataType::UINT16:
                return CreatePrimitiveType<DataType::UINT16>();
            case DataType::INT16:
                return CreatePrimitiveType<DataType::INT16>();
            case DataType::UINT32:
                return CreatePrimitiveType<DataType::UINT32>();
            case DataType::INT32:
                return CreatePrimitiveType<DataType::INT32>();
            case DataType::UINT64:
                return CreatePrimitiveType<DataType::UINT64>();
            case DataType::INT64:
                return CreatePrimitiveType<DataType::INT64>();
            case DataType::STRING:
                return CreatePrimitiveType<DataType::STRING>();
            case DataType::BOOL:
                return CreatePrimitiveType<DataType::BOOL>();
            case DataType::FLOAT:
                return CreatePrimitiveType<DataType::FLOAT>();
            case DataType::DOUBLE:
                return CreatePrimitiveType<DataType::DOUBLE>();
            case DataType::BINARY:
                return CreatePrimitiveType<DataType::BINARY>();
            case DataType::TIMESTAMP:
                return CreatePrimitiveType<DataType::TIMESTAMP>();
            case DataType::DECIMAL:
                return CreatePrimitiveType<DataType::DECIMAL>();
            case DataType::VARINT:
                return CreatePrimitiveType<DataType::VARINT>();
            case DataType::INET:
                return CreatePrimitiveType<DataType::INET>();
            case DataType::JSONB:
                return CreatePrimitiveType<DataType::JSONB>();
            case DataType::UUID:
                return CreatePrimitiveType<DataType::UUID>();
            case DataType::TIMEUUID:
                return CreatePrimitiveType<DataType::TIMEUUID>();
            case DataType::DATE:
                return CreatePrimitiveType<DataType::DATE>();
            case DataType::TIME:
                return CreatePrimitiveType<DataType::TIME>();

                // Create empty parametric types and raise error during semantic check.
            case DataType::LIST:
                return CreateTypeList();
            case DataType::MAP:
                return CreateTypeMap();
            case DataType::SET:
                return CreateTypeSet();
            case DataType::TUPLE:
                return CreateCollectionType<DataType::TUPLE>({});

                // Datatype for variadic builtin function.
            case TYPEARGS:
                return CreatePrimitiveType<DataType::TYPEARGS>();

                // User-defined types cannot be created like this
            case DataType::USER_DEFINED_TYPE:
                LOG(FATAL) << "Unsupported constructor for user-defined type";
                return nullptr;

            default:
                LOG(FATAL) << "Not supported datatype " << SQLType::ToDataTypeString(data_type);
                return nullptr;
        }
    }

    bool SQLType::IsValidPrimaryType(DataType type) {
        switch (type) {
            case DataType::MAP: FALLTHROUGH_INTENDED;
            case DataType::SET: FALLTHROUGH_INTENDED;
            case DataType::LIST: FALLTHROUGH_INTENDED;
            case DataType::TUPLE: FALLTHROUGH_INTENDED;
            case DataType::JSONB: FALLTHROUGH_INTENDED;
            case DataType::USER_DEFINED_TYPE:
                return false;

            default:
                // Let all other types go. Because we already process column datatype before getting here,
                // just assume that they are all valid types.
                return true;
        }
    }

    shared_ptr<SQLType> SQLType::CreateTypeMap(std::shared_ptr<SQLType> key_type,
                                             std::shared_ptr<SQLType> value_type) {
        vector<shared_ptr<SQLType>> params = {key_type, value_type};
        return CreateCollectionType<DataType::MAP>(params);
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeMap(DataType key_type, DataType value_type) {
        return CreateTypeMap(SQLType::Create(key_type), SQLType::Create(value_type));
    }

    shared_ptr<SQLType> SQLType::CreateTypeList(std::shared_ptr<SQLType> value_type) {
        vector<shared_ptr<SQLType>> params(1, value_type);
        return CreateCollectionType<DataType::LIST>(params);
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeList(DataType value_type) {
        return CreateTypeList(SQLType::Create(value_type));
    }

    shared_ptr<SQLType> SQLType::CreateTypeSet(std::shared_ptr<SQLType> value_type) {
        vector<shared_ptr<SQLType>> params(1, value_type);
        return CreateCollectionType<DataType::SET>(params);
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeSet(DataType value_type) {
        return CreateTypeSet(SQLType::Create(value_type));
    }

    const string SQLType::ToDataTypeString(const DataType& datatype) {
        switch (datatype) {
            case DataType::UNKNOWN_DATA: return "unknown";
            case DataType::NULL_VALUE_TYPE: return "anytype";
            case DataType::INT8: return "tinyint";
            case DataType::INT16: return "smallint";
            case DataType::INT32: return "int";
            case DataType::INT64: return "bigint";
            case DataType::UINT32: return "uint32";
            case DataType::STRING: return "text";
            case DataType::BOOL: return "boolean";
            case DataType::FLOAT: return "float";
            case DataType::DOUBLE: return "double";
            case DataType::BINARY: return "blob";
            case DataType::TIMESTAMP: return "timestamp";
            case DataType::DECIMAL: return "decimal";
            case DataType::VARINT: return "varint";
            case DataType::INET: return "inet";
            case DataType::JSONB: return "jsonb";
            case DataType::LIST: return "list";
            case DataType::MAP: return "map";
            case DataType::SET: return "set";
            case DataType::UUID: return "uuid";
            case DataType::TIMEUUID: return "timeuuid";
            case DataType::TUPLE: return "tuple";
            case DataType::TYPEARGS: return "typeargs";
            case DataType::FROZEN: return "frozen";
            case DataType::USER_DEFINED_TYPE: return "user_defined_type";
            case DataType::DATE: return "date";
            case DataType::TIME: return "time";
            default: return "unknown";
        }
        LOG (FATAL) << "Invalid datatype: " << datatype;
        return "Undefined Type";
    }

    std::ostream& operator<<(std::ostream& os, const DataType& data_type) {
         switch (data_type) {
            case DataType::UNKNOWN_DATA: return os << "unknown";
            case DataType::NULL_VALUE_TYPE: return os << "anytype";
            case DataType::INT8: return os << "tinyint";
            case DataType::INT16: return os << "smallint";
            case DataType::INT32: return os << "int";
            case DataType::INT64: return os << "bigint";
            case DataType::UINT32: return os << "uint32";
            case DataType::STRING: return os << "text";
            case DataType::BOOL: return os << "boolean";
            case DataType::FLOAT: return os << "float";
            case DataType::DOUBLE: return os << "double";
            case DataType::BINARY: return os << "blob";
            case DataType::TIMESTAMP: return os << "timestamp";
            case DataType::DECIMAL: return os << "decimal";
            case DataType::VARINT: return os << "varint";
            case DataType::INET: return os << "inet";
            case DataType::JSONB: return os << "jsonb";
            case DataType::LIST: return os << "list";
            case DataType::MAP: return os << "map";
            case DataType::SET: return os << "set";
            case DataType::UUID: return os << "uuid";
            case DataType::TIMEUUID: return os << "timeuuid";
            case DataType::TUPLE: return os << "tuple";
            case DataType::TYPEARGS: return os << "typeargs";
            case DataType::FROZEN: return os << "frozen";
            case DataType::USER_DEFINED_TYPE: return os << "user_defined_type";
            case DataType::DATE: return os << "date";
            case DataType::TIME: return os << "time";
            default: return os << "unknown";
        }
    }

    std::ostream& operator<<(std::ostream& os, const SQLType& sql_type) {
        os << sql_type.id_;
        if (!sql_type.params_.empty()) {
            os << "<";
            for (int i = 0; i < sql_type.params_.size(); i++) {
                if (i > 0) {
                    os << ", ";
                }
                os << (*sql_type.params_[i].get());
            }
            os << ">";
        }
        return os;
    }
}  // namespace sql
}  // namespace k2pg
