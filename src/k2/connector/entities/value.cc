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

SqlValue::SqlValue(const K2PgTypeEntity* type_entity, uint64_t datum, bool is_null) {
  null_value_ = is_null;

 switch (type_entity->k2pg_type) {
    case K2SQL_DATA_TYPE_INT8:
      type_ = ValueType::INT;
      if (!is_null) {
        int8_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_INT16:
      type_ = ValueType::INT;
      if (!is_null) {
        int16_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_INT32:
      type_ = ValueType::INT;
      if (!is_null) {
        int32_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_INT64:
      type_ = ValueType::INT;
      if (!is_null) {
        int64_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_UINT32:
      type_ = ValueType::INT;
      if (!is_null) {
        uint32_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_UINT8:
      type_ = ValueType::INT;
      if (!is_null) {
        uint8_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_UINT16:
      type_ = ValueType::INT;
      if (!is_null) {
        uint16_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_UINT64:
      type_ = ValueType::INT;
      if (!is_null) {
        uint64_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_STRING:
      type_ = ValueType::SLICE;
      if (!is_null) {
        char *value;
        int64_t bytes = type_entity->datum_fixed_size;
        type_entity->datum_to_k2pg(datum, &value, &bytes);
        data_.slice_val_ = std::string(value, bytes);
      }
      break;

    case K2SQL_DATA_TYPE_BOOL:
      type_ = ValueType::BOOL;
      if (!is_null) {
        bool value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.bool_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_FLOAT:
      type_ = ValueType::FLOAT;
      if (!is_null) {
        float value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.float_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_DOUBLE:
      type_ = ValueType::DOUBLE;
      if (!is_null) {
        double value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.double_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_BINARY:
      type_ = ValueType::SLICE;
      if (!is_null) {
        uint8_t *value;
        int64_t bytes = type_entity->datum_fixed_size;
        type_entity->datum_to_k2pg(datum, &value, &bytes);
        data_.slice_val_ = std::string((char*)value, bytes);
      }
      break;

    case K2SQL_DATA_TYPE_TIMESTAMP:
      type_ = ValueType::INT;
      if (!is_null) {
        int64_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_DECIMAL:
      type_ = ValueType::SLICE;
      if (!is_null) {
        char* plaintext;
        type_entity->datum_to_k2pg(datum, &plaintext, nullptr);
        k2pg::Decimal k2pg_decimal(plaintext);
        data_.slice_val_ = k2pg_decimal.EncodeToComparable();
      }
      break;

    case K2SQL_DATA_TYPE_LIST:
    case K2SQL_DATA_TYPE_MAP:
    case K2SQL_DATA_TYPE_SET:
    case K2SQL_DATA_TYPE_DATE: // Not used for PG storage
    case K2SQL_DATA_TYPE_TIME: // Not used for PG storage
    default:
      LOG(DFATAL) << "Internal error: unsupported type " << type_entity->k2pg_type;
  }
}

SqlValue* SqlValue::CopySlice(k2pg::Slice s) {
  auto copy = new uint8_t[s.size()];
  memcpy(copy, s.data(), s.size());
  auto slice_val = k2pg::Slice(copy, s.size());

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
