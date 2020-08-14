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

#ifndef CHOGORI_SQL_VALUE_H
#define CHOGORI_SQL_VALUE_H

#include "yb/common/macros.h"
#include "yb/common/type/slice.h"
#include "yb/pggate/ybc_pg_typedefs.h"

namespace k2 {
namespace sql {

using yb::Slice;

struct Data {
    union {
        bool bool_val_;
        int64_t int_val_;
        float float_val_;
        double double_val_;
    };
    Slice slice_val_;
};  

class SqlValue {
public:
  enum ValueType {
      BOOL,  
      INT,
      FLOAT,
      DOUBLE,
      SLICE
  };

  SqlValue(ValueType type, Data* data) {
      type_ = type;
      data_ = data;
  }

  SqlValue(bool b) {
      type_ = ValueType::BOOL;
      data_ = new Data();
      data_->bool_val_ = b;
  }

  SqlValue(int64_t v) {
      type_ = ValueType::INT;
      data_ = new Data();
      data_->int_val_ = v;
  }

  SqlValue(float f) {
      type_ = ValueType::FLOAT;
      data_ = new Data();
      data_->float_val_ = f;
  }

  SqlValue(double d) {
      type_ = ValueType::DOUBLE;
      data_ = new Data();
      data_->double_val_ = d;
  }

  SqlValue(Slice s) {
      type_ = ValueType::SLICE;
      data_ = new Data();
      data_->slice_val_ = s;
  }

  SqlValue(const YBCPgTypeEntity* type_entity, uint64_t datum, bool is_null);

  // Return a new identical SQLValue object.
  SqlValue* Clone() const;

  // Construct a SQLValue by copying the value of the given Slice.
  static SqlValue* CopySlice(Slice s);

  ~SqlValue();

  private: 
  ValueType type_;
  Data* data_;
};


}  // namespace sql
}  // namespace k2

#endif //CHOGORI_SQL_VALUE_H