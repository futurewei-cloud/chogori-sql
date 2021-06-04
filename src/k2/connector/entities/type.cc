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

namespace k2pg {
namespace sql {

    using std::shared_ptr;
    using std::vector;
    using std::string;

    //--------------------------------------------------------------------------------------------------
    // The following functions are to construct QLType objects.

    shared_ptr<SQLType> SQLType::Create(DataType data_type, const std::vector<shared_ptr<SQLType>>& params) {
        switch (data_type) {
            case DataType::K2SQL_DATA_TYPE_LIST:
                DCHECK_EQ(params.size(), 1);
                return CreateCollectionType<DataType::K2SQL_DATA_TYPE_LIST>(params);
            case DataType::K2SQL_DATA_TYPE_MAP:
                DCHECK_EQ(params.size(), 2);
                return CreateCollectionType<DataType::K2SQL_DATA_TYPE_MAP>(params);
            case DataType::K2SQL_DATA_TYPE_SET:
                DCHECK_EQ(params.size(), 1);
                return CreateCollectionType<DataType::K2SQL_DATA_TYPE_SET>(params);
            default:
                DCHECK_EQ(params.size(), 0);
                return Create(data_type);
        }
    }

    shared_ptr<SQLType> SQLType::Create(DataType data_type) {
        switch (data_type) {
            case DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA>();
            case DataType::K2SQL_DATA_TYPE_NULL_VALUE_TYPE:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_NULL_VALUE_TYPE>();
            case DataType::K2SQL_DATA_TYPE_UINT8:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_UINT8>();
            case DataType::K2SQL_DATA_TYPE_INT8:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_INT8>();
            case DataType::K2SQL_DATA_TYPE_UINT16:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_UINT16>();
            case DataType::K2SQL_DATA_TYPE_INT16:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_INT16>();
            case DataType::K2SQL_DATA_TYPE_UINT32:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_UINT32>();
            case DataType::K2SQL_DATA_TYPE_INT32:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_INT32>();
            case DataType::K2SQL_DATA_TYPE_UINT64:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_UINT64>();
            case DataType::K2SQL_DATA_TYPE_INT64:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_INT64>();
            case DataType::K2SQL_DATA_TYPE_STRING:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_STRING>();
            case DataType::K2SQL_DATA_TYPE_BOOL:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_BOOL>();
            case DataType::K2SQL_DATA_TYPE_FLOAT:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_FLOAT>();
            case DataType::K2SQL_DATA_TYPE_DOUBLE:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_DOUBLE>();
            case DataType::K2SQL_DATA_TYPE_BINARY:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_BINARY>();
            case DataType::K2SQL_DATA_TYPE_TIMESTAMP:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_TIMESTAMP>();
            case DataType::K2SQL_DATA_TYPE_DECIMAL:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_DECIMAL>();
            case DataType::K2SQL_DATA_TYPE_DATE:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_DATE>();
            case DataType::K2SQL_DATA_TYPE_TIME:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_TIME>();

                // Create empty parametric types and raise error during semantic check.
            case DataType::K2SQL_DATA_TYPE_LIST:
                return CreateTypeList();
            case DataType::K2SQL_DATA_TYPE_MAP:
                return CreateTypeMap();
            case DataType::K2SQL_DATA_TYPE_SET:
                return CreateTypeSet();

            default:
                LOG(FATAL) << "Not supported datatype " << SQLType::ToDataTypeString(data_type);
                return nullptr;
        }
    }

    bool SQLType::IsValidPrimaryType(DataType type) {
        switch (type) {
            case DataType::K2SQL_DATA_TYPE_MAP: [[fallthrough]];
            case DataType::K2SQL_DATA_TYPE_SET: [[fallthrough]];
            case DataType::K2SQL_DATA_TYPE_LIST:
                return false;

            default:
                // Let all other types go. Because we already process column datatype before getting here,
                // just assume that they are all valid types.
                return true;
        }
    }

    shared_ptr<SQLType> SQLType::CreateTypeMap(std::shared_ptr<SQLType> key_type,
                                             std::shared_ptr<SQLType> value_type) {
        std::vector<shared_ptr<SQLType>> params = {key_type, value_type};
        return CreateCollectionType<DataType::K2SQL_DATA_TYPE_MAP>(params);
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeMap(DataType key_type, DataType value_type) {
        return CreateTypeMap(SQLType::Create(key_type), SQLType::Create(value_type));
    }

    shared_ptr<SQLType> SQLType::CreateTypeList(std::shared_ptr<SQLType> value_type) {
        std::vector<shared_ptr<SQLType>> params(1, value_type);
        return CreateCollectionType<DataType::K2SQL_DATA_TYPE_LIST>(params);
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeList(DataType value_type) {
        return CreateTypeList(SQLType::Create(value_type));
    }

    shared_ptr<SQLType> SQLType::CreateTypeSet(std::shared_ptr<SQLType> value_type) {
        std::vector<shared_ptr<SQLType>> params(1, value_type);
        return CreateCollectionType<DataType::K2SQL_DATA_TYPE_SET>(params);
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeSet(DataType value_type) {
        return CreateTypeSet(SQLType::Create(value_type));
    }

    const string SQLType::ToDataTypeString(const DataType& datatype) {
        switch (datatype) {
            case DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA: return "unknown";
            case DataType::K2SQL_DATA_TYPE_NULL_VALUE_TYPE: return "anytype";
            case DataType::K2SQL_DATA_TYPE_INT8: return "tinyint";
            case DataType::K2SQL_DATA_TYPE_INT16: return "smallint";
            case DataType::K2SQL_DATA_TYPE_INT32: return "int";
            case DataType::K2SQL_DATA_TYPE_INT64: return "bigint";
            case DataType::K2SQL_DATA_TYPE_UINT32: return "uint32";
            case DataType::K2SQL_DATA_TYPE_STRING: return "text";
            case DataType::K2SQL_DATA_TYPE_BOOL: return "boolean";
            case DataType::K2SQL_DATA_TYPE_FLOAT: return "float";
            case DataType::K2SQL_DATA_TYPE_DOUBLE: return "double";
            case DataType::K2SQL_DATA_TYPE_BINARY: return "blob";
            case DataType::K2SQL_DATA_TYPE_TIMESTAMP: return "timestamp";
            case DataType::K2SQL_DATA_TYPE_DECIMAL: return "decimal";
            case DataType::K2SQL_DATA_TYPE_LIST: return "list";
            case DataType::K2SQL_DATA_TYPE_MAP: return "map";
            case DataType::K2SQL_DATA_TYPE_SET: return "set";
            case DataType::K2SQL_DATA_TYPE_DATE: return "date";
            case DataType::K2SQL_DATA_TYPE_TIME: return "time";
            default: return "unknown";
        }
        LOG (FATAL) << "Invalid datatype: " << datatype;
        return "Undefined Type";
    }

    std::ostream& operator<<(std::ostream& os, const DataType& data_type) {
         switch (data_type) {
            case DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA: return os << "unknown";
            case DataType::K2SQL_DATA_TYPE_NULL_VALUE_TYPE: return os << "anytype";
            case DataType::K2SQL_DATA_TYPE_INT8: return os << "tinyint";
            case DataType::K2SQL_DATA_TYPE_INT16: return os << "smallint";
            case DataType::K2SQL_DATA_TYPE_INT32: return os << "int";
            case DataType::K2SQL_DATA_TYPE_INT64: return os << "bigint";
            case DataType::K2SQL_DATA_TYPE_UINT32: return os << "uint32";
            case DataType::K2SQL_DATA_TYPE_STRING: return os << "text";
            case DataType::K2SQL_DATA_TYPE_BOOL: return os << "boolean";
            case DataType::K2SQL_DATA_TYPE_FLOAT: return os << "float";
            case DataType::K2SQL_DATA_TYPE_DOUBLE: return os << "double";
            case DataType::K2SQL_DATA_TYPE_BINARY: return os << "blob";
            case DataType::K2SQL_DATA_TYPE_TIMESTAMP: return os << "timestamp";
            case DataType::K2SQL_DATA_TYPE_DECIMAL: return os << "decimal";
            case DataType::K2SQL_DATA_TYPE_LIST: return os << "list";
            case DataType::K2SQL_DATA_TYPE_MAP: return os << "map";
            case DataType::K2SQL_DATA_TYPE_SET: return os << "set";
            case DataType::K2SQL_DATA_TYPE_DATE: return os << "date";
            case DataType::K2SQL_DATA_TYPE_TIME: return os << "time";
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
