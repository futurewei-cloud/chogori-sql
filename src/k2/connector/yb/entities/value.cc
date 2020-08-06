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

#include "yb/entities/value.h"
#include "yb/common/type/slice.h"

namespace k2 {
namespace sql {

using yb::Slice;

SQLValue::~SQLValue() {
    if (type_ == ValueType::SLICE) {
        delete[] data_->slice_val_.data();
    }
    delete data_;
}

SQLValue* SQLValue::Clone() const {
   switch (type_) {
        case ValueType::BOOL:
            return new SQLValue(data_->bool_val_);
        case ValueType::INT:
            return new SQLValue(data_->int_val_);
        case ValueType::DOUBLE:
            return new SQLValue(data_->double_val_);
        case ValueType::FLOAT:
            return new SQLValue(data_->float_val_);
        case ValueType::SLICE:
            return CopySlice(data_->slice_val_);
        default:
            LOG(FATAL) << "Invalid type " << type_;            
  }
}

SQLValue* SQLValue::CopySlice(Slice s) {
  auto copy = new uint8_t[s.size()];
  memcpy(copy, s.data(), s.size());
  auto slice_val = Slice(copy, s.size());

  return new SQLValue(slice_val);
}


}  // namespace sql
}  // namespace k2
