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

#include <glog/logging.h>

#include "entities/value.h"
#include "common/type/slice.h"
#include "common/type/decimal.h"

namespace k2pg {
namespace sql {

void SqlValue::Clear() {
    null_value_ = true;
}

SqlValue::SqlValue(const YBCPgTypeEntity* type_entity, uint64_t datum, bool is_null) {
  null_value_ = is_null;

 switch (type_entity->yb_type) {
    case YB_YQL_DATA_TYPE_INT8:
      type_ = ValueType::INT;
      if (!is_null) {
        int8_t value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_INT16:
      type_ = ValueType::INT;
      if (!is_null) {
        int16_t value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_INT32:
      type_ = ValueType::INT;
      if (!is_null) {
        int32_t value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_INT64:
      type_ = ValueType::INT;
      if (!is_null) {
        int64_t value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_UINT32:
      type_ = ValueType::INT;
      if (!is_null) {
        uint32_t value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_UINT8:
      type_ = ValueType::INT;
      if (!is_null) {
        uint8_t value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_UINT16:
      type_ = ValueType::INT;
      if (!is_null) {
        uint16_t value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_UINT64:
      type_ = ValueType::INT;
      if (!is_null) {
        uint64_t value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_STRING:
      type_ = ValueType::SLICE;
      if (!is_null) {
        char *value;
        int64_t bytes = type_entity->datum_fixed_size;
        type_entity->datum_to_yb(datum, &value, &bytes);
        data_.slice_val_ = std::string(value, bytes);
      }
      break;

    case YB_YQL_DATA_TYPE_BOOL:
      type_ = ValueType::BOOL;
      if (!is_null) {
        bool value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.bool_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_FLOAT:
      type_ = ValueType::FLOAT;
      if (!is_null) {
        float value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.float_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_DOUBLE:
      type_ = ValueType::DOUBLE;
      if (!is_null) {
        double value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.double_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_BINARY:
      type_ = ValueType::SLICE;
      if (!is_null) {
        uint8_t *value;
        int64_t bytes = type_entity->datum_fixed_size;
        type_entity->datum_to_yb(datum, &value, &bytes);
        data_.slice_val_ = std::string((char*)value, bytes);
      }
      break;

    case YB_YQL_DATA_TYPE_TIMESTAMP:
      type_ = ValueType::INT;
      if (!is_null) {
        int64_t value;
        type_entity->datum_to_yb(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case YB_YQL_DATA_TYPE_DECIMAL:
      type_ = ValueType::SLICE;
      if (!is_null) {
        char* plaintext;
        // Calls YBCDatumToDecimalText in ybctype.c
        type_entity->datum_to_yb(datum, &plaintext, nullptr);
        yb::util::Decimal yb_decimal(plaintext);
        data_.slice_val_ = yb_decimal.EncodeToComparable();
      }
      break;

    case YB_YQL_DATA_TYPE_VARINT:
    case YB_YQL_DATA_TYPE_INET:
    case YB_YQL_DATA_TYPE_LIST:
    case YB_YQL_DATA_TYPE_MAP:
    case YB_YQL_DATA_TYPE_SET:
    case YB_YQL_DATA_TYPE_UUID:
    case YB_YQL_DATA_TYPE_TIMEUUID:
    case YB_YQL_DATA_TYPE_TUPLE:
    case YB_YQL_DATA_TYPE_TYPEARGS:
    case YB_YQL_DATA_TYPE_USER_DEFINED_TYPE:
    case YB_YQL_DATA_TYPE_FROZEN:
    case YB_YQL_DATA_TYPE_DATE: // Not used for PG storage
    case YB_YQL_DATA_TYPE_TIME: // Not used for PG storage
    case YB_YQL_DATA_TYPE_JSONB:
    default:
      LOG(DFATAL) << "Internal error: unsupported type " << type_entity->yb_type;
  }
}

SqlValue* SqlValue::CopySlice(yb::Slice s) {
  auto copy = new uint8_t[s.size()];
  memcpy(copy, s.data(), s.size());
  auto slice_val = yb::Slice(copy, s.size());

  return new SqlValue(slice_val);
}

void SqlValue::set_bool_value(bool value, bool is_null) {
    type_ = ValueType::BOOL;
    if(is_null) {
        Clear();
    } else {
        data_.bool_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_int8_value(int8_t value, bool is_null) {
    type_ = ValueType::INT;
    if(is_null) {
        Clear();
    } else {
        data_.int_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_int16_value(int16_t value, bool is_null) {
    type_ = ValueType::INT;
    if(is_null) {
        Clear();
    } else {
        data_.int_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_int32_value(int32_t value, bool is_null) {
    type_ = ValueType::INT;
    if(is_null) {
        Clear();
    } else {
        data_.int_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_int64_value(int64_t value, bool is_null) {
    type_ = ValueType::INT;
    if(is_null) {
        Clear();
    } else {
        data_.int_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_float_value(float value, bool is_null) {
    type_ = ValueType::FLOAT;
    if(is_null) {
        Clear();
    } else {
        data_.float_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_double_value(double value, bool is_null) {
    type_ = ValueType::DOUBLE;
    if(is_null) {
        Clear();
    } else {
        data_.double_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_string_value(const char *value, bool is_null) {
    type_ = ValueType::SLICE;
    if(is_null) {
        Clear();
    } else {
        data_.slice_val_ = std::string(value);
        null_value_ = false;
    }
}

void SqlValue::set_binary_value(const char *value, size_t bytes, bool is_null) {
    type_ = ValueType::SLICE;
    if(is_null) {
        Clear();
    } else {
        data_.slice_val_ = std::string(value, bytes);
        null_value_ = false;
    }
}

}  // namespace sql
}  // namespace k2pg
