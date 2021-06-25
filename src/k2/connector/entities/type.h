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
// Portions Copyright (c) 2021 Futurewei Cloud
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
#pragma once

#include <string>
#include <memory>
#include <vector>

#include <glog/logging.h>
#include "entities/data_type.h"
#include "entities/types.h"

namespace k2pg {
namespace sql {
    class SQLType {
        public:
        typedef std::shared_ptr<SQLType> SharedPtr;

        template<DataType data_type>
        static const std::shared_ptr<SQLType>& CreatePrimitiveType() {
            static std::shared_ptr<SQLType> sql_type = std::make_shared<SQLType>(data_type);
            return sql_type;
        }

        template<DataType data_type>
        static std::shared_ptr<SQLType> CreateCollectionType(
            const std::vector<std::shared_ptr<SQLType>>& params) {
            return std::make_shared<SQLType>(data_type, params);
        }

        // Create all builtin types including collection.
        static std::shared_ptr<SQLType> Create(DataType data_type, const std::vector<std::shared_ptr<SQLType>>& params);

        // Create primitive types, all builtin types except collection.
        static std::shared_ptr<SQLType> Create(DataType data_type);

        // Check type methods.
        static bool IsValidPrimaryType(DataType type);

        const TypeInfo* type_info() const {
            return GetTypeInfo(id_);
        }

        // Create map datatype.
        static std::shared_ptr<SQLType> CreateTypeMap(std::shared_ptr<SQLType> key_type,
                std::shared_ptr<SQLType> value_type);
        static std::shared_ptr<SQLType> CreateTypeMap(DataType key_type, DataType value_type);
        static std::shared_ptr<SQLType> CreateTypeMap() {
            // Create default map type: MAP <UNKNOWN -> UNKNOWN>.
            static const std::shared_ptr<SQLType> default_map =
            CreateTypeMap(SQLType::Create(DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA),
                      SQLType::Create(DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA));
            return default_map;
        }

        // Create list datatype.
        static std::shared_ptr<SQLType> CreateTypeList(std::shared_ptr<SQLType> value_type);
        static std::shared_ptr<SQLType> CreateTypeList(DataType val_type);
        static std::shared_ptr<SQLType> CreateTypeList() {
            // Create default list type: LIST <UNKNOWN>.
            static const std::shared_ptr<SQLType> default_list = CreateTypeList(DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA);
            return default_list;
        }

        // Create set datatype.
        static std::shared_ptr<SQLType> CreateTypeSet(std::shared_ptr<SQLType> value_type);
        static std::shared_ptr<SQLType> CreateTypeSet(DataType value_type);
        static std::shared_ptr<SQLType> CreateTypeSet() {
            // Create default set type: SET <UNKNOWN>.
            static const std::shared_ptr<SQLType> default_set = CreateTypeSet(DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA);
            return default_set;
        }

        //------------------------------------------------------------------------------------------------
        // Constructors.

        // Constructor for elementary types
        explicit SQLType(DataType sql_typeid) : id_(sql_typeid), params_(0) {
        }

        // Constructor for collection types
        SQLType(DataType sql_typeid, const std::vector<std::shared_ptr<SQLType>>& params)
            : id_(sql_typeid), params_(params) {
        }

        virtual ~SQLType() {
        }

        const DataType id() const {
            return id_;
        }

        const std::vector<std::shared_ptr<SQLType>>& params() const {
            return params_;
        }

        std::shared_ptr<SQLType> keys_type() const {
            switch (id_) {
                case K2SQL_DATA_TYPE_MAP:
                    return params_[0];
                case K2SQL_DATA_TYPE_LIST:
                    return SQLType::Create(K2SQL_DATA_TYPE_INT32);
                case K2SQL_DATA_TYPE_SET:
                    // set has no keys, only values
                    return nullptr;

                default:
                    // elementary types have no keys or values
                    return nullptr;
            }
        }

        std::shared_ptr<SQLType> values_type() const {
            switch (id_) {
                case K2SQL_DATA_TYPE_MAP:
                    return params_[1];
                case K2SQL_DATA_TYPE_LIST:
                    return params_[0];
                case K2SQL_DATA_TYPE_SET:
                    return params_[0];

                default:
                    // other types have no keys or values
                    return nullptr;
            }
        }

        const SQLType::SharedPtr& param_type(int member_index = 0) const {
            // TODO: add index validation
            return params_[member_index];
        }

        //------------------------------------------------------------------------------------------------
        // Predicates.

        bool IsCollection() const {
            return id_ == K2SQL_DATA_TYPE_MAP || id_ == K2SQL_DATA_TYPE_SET || id_ == K2SQL_DATA_TYPE_LIST;
        }

        bool IsUnknown() const {
            return IsUnknown(id_);
        }

        bool IsAnyType() const {
            return IsNull(id_);
        }

        bool IsInteger() const {
            return IsInteger(id_);
        }

        bool IsElementary() const {
            return !IsCollection();
        }

        bool IsValid() const {
            if (IsElementary()) {
                return params_.empty();
            } else {
                // checking number of params
                if (id_ == K2SQL_DATA_TYPE_MAP && params_.size() != 2) {
                    return false; // expect two type parameters for maps
                } else if ((id_ == K2SQL_DATA_TYPE_SET || id_ == K2SQL_DATA_TYPE_LIST) && params_.size() != 1) {
                    return false; // expect one type parameter for set and list
                }
                // recursively checking params
                for (const auto &param : params_) {
                    if (!param->IsValid()) return false;
                }
                return true;
            }
        }

        bool Contains(DataType id) const {
            for (const std::shared_ptr<SQLType>& param : params_) {
                if (param->Contains(id)) {
                    return true;
                }
            }
            return id_ == id;
        }

        bool operator ==(const SQLType& other) const {
            if (id_ == other.id_ && params_.size() == other.params_.size()) {
                for (int i = 0; i < params_.size(); i++) {
                    if (*params_[i] == *other.params_[i]) {
                        continue;
                    }
                    return false;
                }
                return true;
            }

            return false;
        }

        bool operator !=(const SQLType& other) const {
            return !(*this == other);
        }

        //------------------------------------------------------------------------------------------------
        // Logging supports.
        static const std::string ToDataTypeString(const DataType& datatype);

        friend std::ostream& operator<<(std::ostream& os, const DataType& type);
        friend std::ostream& operator<<(std::ostream& os, const SQLType& sql_type);

        //------------------------------------------------------------------------------------------------
        // static methods
        static bool IsInteger(DataType t) {
            return (t >= K2SQL_DATA_TYPE_INT8 && t <= K2SQL_DATA_TYPE_INT64); // || t == K2SQL_DATA_TYPE_VARINT;
        }

        static bool IsNumeric(DataType t) {
            return IsInteger(t) || t == K2SQL_DATA_TYPE_FLOAT || t == K2SQL_DATA_TYPE_DOUBLE || t == K2SQL_DATA_TYPE_DECIMAL;
        }

        // NULL_VALUE_TYPE represents type of a null value.
        static bool IsNull(DataType t) {
            return t == K2SQL_DATA_TYPE_NULL_VALUE_TYPE;
        }

        // Type is not yet set (VOID).
        static bool IsUnknown(DataType t) {
            return t == DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA;
        }

        private:
        //------------------------------------------------------------------------------------------------
        // Data members.
        DataType id_;
        std::vector<std::shared_ptr<SQLType>> params_;
    };

}  // namespace sql
}  // namespace k2pg
