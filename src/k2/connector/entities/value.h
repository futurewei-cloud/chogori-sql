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

#include <assert.h>
#include <cmath>

#include "common/type/slice.h"
#include "pggate/pg_gate_typedefs.h"
#include <k2/common/Common.h>

namespace k2pg {
namespace sql {

struct Data {
    union {
        bool bool_val_;
        int64_t int_val_;
        float float_val_;
        double double_val_;
    };
    std::string slice_val_;
};

class SqlValue {
public:
  enum ValueType {
      BOOL,
      INT,
      FLOAT,
      DOUBLE,
      SLICE,
      UNKNOWN
  };

  SqlValue(bool b) {
      type_ = ValueType::BOOL;
      data_.bool_val_ = b;
      null_value_ = false;
  }

  SqlValue(int64_t v) {
      type_ = ValueType::INT;
      data_.int_val_ = v;
      null_value_ = false;
  }

  SqlValue(float f) {
      type_ = ValueType::FLOAT;
      data_.float_val_ = f;
      null_value_ = false;
  }

  SqlValue(double d) {
      type_ = ValueType::DOUBLE;
      data_.double_val_ = d;
      null_value_ = false;
  }

  SqlValue(k2pg::Slice s) {
      type_ = ValueType::SLICE;
      data_.slice_val_ = std::string(s.cdata(), s.size());
      null_value_ = false;
  }

  SqlValue(std::string s) {
      type_ = ValueType::SLICE;
      data_.slice_val_ = std::move(s);
      null_value_ = false;
  }

  SqlValue(const K2PgTypeEntity* type_entity, uint64_t datum, bool is_null);

  SqlValue(const SqlValue& val) = default;

  bool IsBoolean() {
      return type_ == ValueType::BOOL;
  }

  bool IsInteger() {
      return type_ == ValueType::INT;
  }

  bool IsMaxInteger() {
      if (!IsInteger()) {
          return false;
      }

      // null values are not handled here since SQL has its own way to handle nulls
      assert(!IsNull());
      return data_.int_val_ == std::numeric_limits<int64_t>::max();
  }

  // get a value that is higher than the current one
  SqlValue UpperBound() {
    // null values are not handled here since SQL has its own way to handle nulls
    assert(!IsNull());
    switch (type_) {
        case ValueType::INT: {
            return SqlValue(data_.int_val_ + 1);
        } break;
        default: {
            throw std::invalid_argument("Unsupported data type: " + type_);
        } break;
    }

    throw std::invalid_argument("Unsupported data type: " + type_);
  }

  int Compare(const SqlValue& val) {
    // null values are not considered here since their comparison is based on column sorting type
    assert((!IsNull()) && (!val.IsNull()));
    // types must be the same for comparison
    assert(type_ == val.type_);

    switch (type_) {
        case ValueType::BOOL: {
            if (data_.bool_val_ == val.data_.bool_val_) {
                return 0;
            } else if (data_.bool_val_ < val.data_.bool_val_) {
                return -1;
            } else {
                return 1;
            }
        } break;
        case ValueType::INT: {
            if (data_.int_val_ == val.data_.int_val_) {
                return 0;
            } else if (data_.int_val_ < val.data_.int_val_) {
                return -1;
            } else {
                return 1;
            }
        } break;
        case ValueType::FLOAT: {
            if (data_.float_val_ == val.data_.float_val_) {
                return 0;
            } else if (data_.float_val_ < val.data_.float_val_) {
                return -1;
            } else {
                return 1;
            }
        } break;
        case ValueType::DOUBLE: {
            if (data_.double_val_ == val.data_.double_val_) {
                return 0;
            } else if (data_.double_val_ < val.data_.double_val_) {
                return -1;
            } else {
                return 1;
            }
        } break;
        case ValueType::SLICE: {
            return data_.slice_val_.compare(val.data_.slice_val_);
        } break;
        default:
            throw std::invalid_argument("Unknown data type");
        break;
    }

    throw std::invalid_argument("Unknown data type");
  }

  static SqlValue* CopySlice(k2pg::Slice s);

  friend std::ostream& operator<<(std::ostream& os, const SqlValue& sql_value) {
    os << "{type: " << sql_value.type_ << ", isNull: " << sql_value.null_value_ << ", value: ";
    if (sql_value.null_value_) {
        os << "NULL";
    } else {
        switch (sql_value.type_) {
            case ValueType::BOOL: {
                os << sql_value.data_.bool_val_;
            } break;
            case ValueType::INT: {
                os << sql_value.data_.int_val_;
            } break;
            case ValueType::FLOAT: {
                os << sql_value.data_.float_val_;
            } break;
            case ValueType::DOUBLE: {
                os << sql_value.data_.double_val_;
            } break;
            case ValueType::SLICE: {
                os << k2::HexCodec::encode(sql_value.data_.slice_val_);
            } break;
            default: {
                os << "Unknown";
            } break;
        }
    }
    os << "}";
    return os;
  }

  bool IsNull() const {
      return null_value_;
  }

  bool isBinaryValue() const {
      return type_ == ValueType::SLICE;
  }

  void set_bool_value(bool value, bool is_null);
  void set_int8_value(int8_t value, bool is_null);
  void set_int16_value(int16_t value, bool is_null);
  void set_int32_value(int32_t value, bool is_null);
  void set_int64_value(int64_t value, bool is_null);
  void set_float_value(float value, bool is_null);
  void set_double_value(double value, bool is_null);
  void set_string_value(const char *value, bool is_null);
  void set_binary_value(const char *value, size_t bytes, bool is_null);

  ValueType type_ = ValueType::UNKNOWN;
  Data data_;

  private:
  void Clear();

  bool null_value_ = true;
};


}  // namespace sql
}  // namespace k2pg
