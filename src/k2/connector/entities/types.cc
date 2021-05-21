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

#include "entities/types.h"

#include <memory>
#include <unordered_map>

#include "common/singleton.h"

using std::shared_ptr;
using std::unordered_map;

namespace k2pg {
namespace sql {

template<typename TypeTraitsClass>
TypeInfo::TypeInfo(TypeTraitsClass t)
  : type_(TypeTraitsClass::type),
    physical_type_(TypeTraitsClass::physical_type),
    name_(TypeTraitsClass::name()),
    size_(TypeTraitsClass::size),
    min_value_(TypeTraitsClass::min_value()),
    compare_func_(TypeTraitsClass::Compare) {
}

int TypeInfo::Compare(const void *lhs, const void *rhs) const {
  return compare_func_(lhs, rhs);
}

class TypeInfoResolver {
 public:
  const TypeInfo* GetTypeInfo(DataType t) {
    const TypeInfo *type_info = mapping_[t].get();
    CHECK(type_info != nullptr) <<
      "Bad type: " << t;
    return type_info;
  }

 private:
  TypeInfoResolver() {
    AddMapping<K2SQL_DATA_TYPE_UINT8>();
    AddMapping<K2SQL_DATA_TYPE_INT8>();
    AddMapping<K2SQL_DATA_TYPE_UINT16>();
    AddMapping<K2SQL_DATA_TYPE_INT16>();
    AddMapping<K2SQL_DATA_TYPE_UINT32>();
    AddMapping<K2SQL_DATA_TYPE_INT32>();
    AddMapping<K2SQL_DATA_TYPE_UINT64>();
    AddMapping<K2SQL_DATA_TYPE_INT64>();
    AddMapping<K2SQL_DATA_TYPE_TIMESTAMP>();
    AddMapping<K2SQL_DATA_TYPE_DATE>();
    AddMapping<K2SQL_DATA_TYPE_TIME>();
    AddMapping<K2SQL_DATA_TYPE_STRING>();
    AddMapping<K2SQL_DATA_TYPE_BOOL>();
    AddMapping<K2SQL_DATA_TYPE_FLOAT>();
    AddMapping<K2SQL_DATA_TYPE_DOUBLE>();
    AddMapping<K2SQL_DATA_TYPE_BINARY>();
    AddMapping<K2SQL_DATA_TYPE_MAP>();
    AddMapping<K2SQL_DATA_TYPE_SET>();
    AddMapping<K2SQL_DATA_TYPE_LIST>();
    AddMapping<K2SQL_DATA_TYPE_DECIMAL>();
  }

  template<DataType type> void AddMapping() {
    TypeTraits<type> traits;
    mapping_.insert(make_pair(type, shared_ptr<TypeInfo>(new TypeInfo(traits))));
  }

  unordered_map<DataType,
                shared_ptr<const TypeInfo>,
                std::hash<size_t> > mapping_;

  friend class Singleton<TypeInfoResolver>;
  DISALLOW_COPY_AND_ASSIGN(TypeInfoResolver);
};

const TypeInfo* GetTypeInfo(DataType type) {
  return Singleton<TypeInfoResolver>::get()->GetTypeInfo(type);
}

}  // namespace sql
}  // namespace k2pg
