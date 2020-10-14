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

#ifndef CHOGORI_SQL_TYPES_H
#define CHOGORI_SQL_TYPES_H

#include <stdint.h>
#include <string>

#include <glog/logging.h>

#include "yb/entities/data_type.h"
#include "yb/common/type/mathlimits.h"
#include "yb/common/type/slice.h"

using std::string;
using yb::Slice;

namespace k2pg {
namespace sql {

// The size of the in-memory format of the largest
// type we support.
const int kLargestTypeSize = sizeof(Slice);

class TypeInfo;

// This is the important bit of this header:
// given a type enum, get the TypeInfo about it.
extern const TypeInfo* GetTypeInfo(DataType type);

// Information about a given type.
// This is a runtime equivalent of the TypeTraits template below.
class TypeInfo {
 public:
  // Returns the type mentioned in the schema.
  DataType type() const { return type_; }
  // Returns the type used to actually store the data.
  DataType physical_type() const { return physical_type_; }
  const std::string& name() const { return name_; }
  const size_t size() const { return size_; }
  int Compare(const void *lhs, const void *rhs) const;
  void CopyMinValue(void* dst) const {
    memcpy(dst, min_value_, size_);
  }

 private:
  friend class TypeInfoResolver;
  template<typename Type> TypeInfo(Type t);

  const DataType type_;
  const DataType physical_type_;
  const std::string name_;
  const size_t size_;
  const void* const min_value_;

  typedef int (*CompareFunc)(const void *, const void *);
  const CompareFunc compare_func_;
};

template<DataType Type> struct DataTypeTraits {};

template<DataType Type>
static int GenericCompare(const void *lhs, const void *rhs) {
  typedef typename DataTypeTraits<Type>::cpp_type CppType;
  CppType lhs_int = *reinterpret_cast<const CppType *>(lhs);
  CppType rhs_int = *reinterpret_cast<const CppType *>(rhs);
  if (lhs_int < rhs_int) {
    return -1;
  } else if (lhs_int > rhs_int) {
    return 1;
  } else {
    return 0;
  }
}

template<>
struct DataTypeTraits<INT8> {
  static const DataType physical_type = INT8;
  typedef int8_t cpp_type;

  static const char *name() {
    return "int8";
  }

  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<INT8>(lhs, rhs);
  }

  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
};

template<>
struct DataTypeTraits<INT16> {
  static const DataType physical_type = INT16;
  typedef int16_t cpp_type;

  static const char *name() {
    return "int16";
  }

  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<INT16>(lhs, rhs);
  }

  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
};

template<>
struct DataTypeTraits<INT32> {
  static const DataType physical_type = INT32;
  typedef int32_t cpp_type;

  static const char *name() {
    return "int32";
  }

  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<INT32>(lhs, rhs);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
};

template<>
struct DataTypeTraits<INT64> {
  static const DataType physical_type = INT64;
  typedef int64_t cpp_type;

  static const char *name() {
    return "int64";
  }

  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<INT64>(lhs, rhs);
  }

  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
};

template<>
struct DataTypeTraits<FLOAT> {
  static const DataType physical_type = FLOAT;
  typedef float cpp_type;

  static const char *name() {
    return "float";
  }

  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<FLOAT>(lhs, rhs);
  }

  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
};

template<>
struct DataTypeTraits<DOUBLE> {
  static const DataType physical_type = DOUBLE;
  typedef double cpp_type;

  static const char *name() {
    return "double";
  }

  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<DOUBLE>(lhs, rhs);
  }

  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
};

template<>
struct DataTypeTraits<BINARY> {
  static const DataType physical_type = BINARY;
  typedef Slice cpp_type;

  static const char *name() {
    return "binary";
  }

  static int Compare(const void *lhs, const void *rhs) {
    const Slice *lhs_slice = reinterpret_cast<const Slice *>(lhs);
    const Slice *rhs_slice = reinterpret_cast<const Slice *>(rhs);
    return lhs_slice->compare(*rhs_slice);
  }

  static const cpp_type* min_value() {
    static Slice s("");
    return &s;
  }
};

template<>
struct DataTypeTraits<BOOL> {
  static const DataType physical_type = BOOL;
  typedef bool cpp_type;

  static const char* name() {
    return "bool";
  }

  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<BOOL>(lhs, rhs);
  }

  static const cpp_type* min_value() {
    static bool b = false;
    return &b;
  }
};

// Base class for types that are derived, that is that have some other type as the
// physical representation.
template<DataType PhysicalType>
struct DerivedTypeTraits {
  typedef typename DataTypeTraits<PhysicalType>::cpp_type cpp_type;
  static const DataType physical_type = PhysicalType;

  static int Compare(const void *lhs, const void *rhs) {
    return DataTypeTraits<PhysicalType>::Compare(lhs, rhs);
  }

  static const cpp_type* min_value() {
    return DataTypeTraits<PhysicalType>::min_value();
  }
};

template<>
struct DataTypeTraits<STRING> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "string";
  }
};

template<>
struct DataTypeTraits<INET> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "inet";
  }
};

template<>
struct DataTypeTraits<JSONB> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "jsonb";
  }
};

template<>
struct DataTypeTraits<UUID> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "uuid";
  }
};

template<>
struct DataTypeTraits<TIMEUUID> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "timeuuid";
  }
};

template<>
struct DataTypeTraits<MAP> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "map";
  }
};

template<>
struct DataTypeTraits<SET> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "set";
  }
};

template<>
struct DataTypeTraits<LIST> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "list";
  }
};

template<>
struct DataTypeTraits<USER_DEFINED_TYPE> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "user_defined_type";
  }
};

template<>
struct DataTypeTraits<FROZEN> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "frozen";
  }
};

template<>
struct DataTypeTraits<DECIMAL> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "decimal";
  }
};

template<>
struct DataTypeTraits<VARINT> : public DerivedTypeTraits<BINARY>{
  static const char* name() {
    return "varint";
  }
};

template<>
struct DataTypeTraits<TIMESTAMP> : public DerivedTypeTraits<INT64>{
  static const int US_TO_S = 1000L * 1000L;

  static const char* name() {
    return "timestamp";
  }
};

// XXX: use signed int32 for DATE because SQL standard does not support unsigned integers
template<>
struct DataTypeTraits<DATE> : public DerivedTypeTraits<INT32>{
  static const char* name() {
    return "date";
  }
};

template<>
struct DataTypeTraits<TIME> : public DerivedTypeTraits<INT64>{
  static const char* name() {
    return "time";
  }
};

// Instantiate this template to get static access to the type traits.
template<DataType datatype>
struct TypeTraits : public DataTypeTraits<datatype> {
  typedef typename DataTypeTraits<datatype>::cpp_type cpp_type;

  static const DataType type = datatype;
  static const size_t size = sizeof(cpp_type);
};

class Variant {
 public:
  Variant(DataType type, const void *value) {
    Reset(type, value);
  }

  ~Variant() {
    Clear();
  }

  template<DataType Type>
  void Reset(const typename DataTypeTraits<Type>::cpp_type& value) {
    Reset(Type, &value);
  }

  // Set the variant to the specified type/value.
  // The value must be of the relative type.
  // In case of strings, the value must be a pointer to a Slice, and the data block
  // will be copied, and released by the variant on the next set/clear call.
  //
  //  Examples:
  //      Slice slice("Hello World");
  //      variant.set(UINT16, &u16);
  //      variant.set(STRING, &slice);
  void Reset(DataType type, const void *value) {
    CHECK(value != NULL) << "Variant value must be not NULL";
    Clear();
    type_ = type;
    switch (type_) {
      case UNKNOWN_DATA:
        LOG(FATAL) << "Unreachable";
      case BOOL:
        numeric_.b1 = *static_cast<const bool *>(value);
        break;
      case INT8:
        numeric_.i8 = *static_cast<const int8_t *>(value);
        break;
      case INT16:
        numeric_.i16 = *static_cast<const int16_t *>(value);
        break;
      case INT32:
        numeric_.i32 = *static_cast<const int32_t *>(value);
        break;
      case DATE:
        numeric_.i32 = *static_cast<const uint32_t *>(value);
        break;
      case TIMESTAMP:
      case TIME:
      case INT64:
        numeric_.i64 = *static_cast<const int64_t *>(value);
        break;
      case FLOAT:
        numeric_.float_val = *static_cast<const float *>(value);
        break;
      case DOUBLE:
        numeric_.double_val = *static_cast<const double *>(value);
        break;
      case STRING: FALLTHROUGH_INTENDED;
      case INET: FALLTHROUGH_INTENDED;
      case UUID: FALLTHROUGH_INTENDED;
      case TIMEUUID: FALLTHROUGH_INTENDED;
      case FROZEN: FALLTHROUGH_INTENDED;
      case JSONB: FALLTHROUGH_INTENDED;
      case BINARY:
        {
          const Slice *str = static_cast<const Slice *>(value);
          // In the case that str->size() == 0, then the 'Clear()' above has already
          // set vstr_ to Slice(""). Otherwise, we need to allocate and copy the
          // user's data.
          if (str->size() > 0) {
            auto blob = new uint8_t[str->size()];
            memcpy(blob, str->data(), str->size());
            vstr_ = Slice(blob, str->size());
          }
        }
        break;
      case MAP: FALLTHROUGH_INTENDED;
      case SET: FALLTHROUGH_INTENDED;
      case LIST:
        LOG(FATAL) << "Default values for collection types not supported, found: "
                   << type_;
      case DECIMAL: FALLTHROUGH_INTENDED;
      case USER_DEFINED_TYPE:
        LOG(FATAL) << "Unsupported data type: " << type_;

      default: LOG(FATAL) << "Unknown data type: " << type_;
    }
  }

  // Set the variant to a STRING type.
  // The specified data block will be copied, and released by the variant
  // on the next set/clear call.
  void Reset(const std::string& data) {
    Slice slice(data);
    Reset(STRING, &slice);
  }

  // Set the variant to a STRING type.
  // The specified data block will be copied, and released by the variant
  // on the next set/clear call.
  void Reset(const char *data, size_t size) {
    Slice slice(data, size);
    Reset(STRING, &slice);
  }

  // Returns the type of the Variant
  DataType type() const {
    return type_;
  }

  // Returns a pointer to the internal variant value
  // The return value can be casted to the relative type()
  // The return value will be valid until the next set() is called.
  //
  //  Examples:
  //    static_cast<const int32_t *>(variant.value())
  //    static_cast<const Slice *>(variant.value())
  const void *value() const {
    switch (type_) {
      case UNKNOWN_DATA: LOG(FATAL) << "Attempted to access value of unknown data type";
      case BOOL:         return &(numeric_.b1);
      case INT8:         return &(numeric_.i8);
      case INT16:        return &(numeric_.i16);
      case INT32:        return &(numeric_.i32);
      case INT64:        return &(numeric_.i64);
      case FLOAT:        return (&numeric_.float_val);
      case DOUBLE:       return (&numeric_.double_val);
      case STRING:       FALLTHROUGH_INTENDED;
      case INET:         FALLTHROUGH_INTENDED;
      case UUID:         FALLTHROUGH_INTENDED;
      case TIMEUUID:     FALLTHROUGH_INTENDED;
      case FROZEN:       FALLTHROUGH_INTENDED;
      case BINARY:       return &vstr_;
      case MAP: FALLTHROUGH_INTENDED;
      case SET: FALLTHROUGH_INTENDED;
      case LIST:
        LOG(FATAL) << "Default values for collection types not supported, found: "
                   << type_;

      case DECIMAL: FALLTHROUGH_INTENDED;
      case USER_DEFINED_TYPE:
        LOG(FATAL) << "Unsupported data type: " << type_;

      default: LOG(FATAL) << "Unknown data type: " << type_;
    }
    CHECK(false) << "not reached!";
    return NULL;
  }

  bool Equals(const Variant *other) const {
    if (other == NULL || type_ != other->type_)
      return false;
    return GetTypeInfo(type_)->Compare(value(), other->value()) == 0;
  }

 private:
  void Clear() {
    // No need to delete[] zero-length vstr_, because we always ensure that
    // such a string would point to a constant "" rather than an allocated piece
    // of memory.
    if (vstr_.size() > 0) {
      delete[] vstr_.mutable_data();
      vstr_.clear();
    }
  }

  union NumericValue {
    bool     b1;
    int8_t   i8;
    int16_t  i16;
    int32_t  i32;
    int64_t  i64;
    float    float_val;
    double   double_val;
  };

  DataType type_;
  NumericValue numeric_;
  Slice vstr_;

  DISALLOW_COPY_AND_ASSIGN(Variant);
};

}  // namespace sql
}  // namespace k2pg

#endif  // CHOGORI_SQL_TYPES_H
