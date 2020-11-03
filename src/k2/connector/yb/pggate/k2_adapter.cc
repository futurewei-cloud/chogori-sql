//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

#include "yb/pggate/k2_adapter.h"

namespace k2pg {
namespace gate {

using k2::K2TxnOptions;


Status K2Adapter::Init() {
  // TODO: add implementation                                   
  return Status::OK();
}

Status K2Adapter::Shutdown() {
  // TODO: add implementation                                   
  return Status::OK();
}

std::future<k2::CreateSchemaResult> K2Adapter::CreateSchema(const std::string& collectionName, k2::dto::Schema schema) {
  throw new std::logic_error("Not implemented yet");    
}

std::future<Status> K2Adapter::Exec(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgOpTemplate> op) {
  // TODO: add implementation  
  // 1) check the request in op and construct the SKV request based on the op type, i.e., READ or WRITE
  // 2) call read or write on k23SITxn
  // 3) create create a runner in a thread pool to check the response of the SKV call
  // 4) return a promise and return the future as the response for this method
  // 5) once the response from SKV returns
  //   a) populate the response object in op
  //   b) populate the data field in op as result set
  //   c) set the value for future 
  throw new std::logic_error("Not implemented yet");   
}

std::future<Status> K2Adapter::BatchExec(std::shared_ptr<K23SITxn> k23SITxn, const std::vector<std::shared_ptr<PgOpTemplate>>& ops) {
  // same as the above except that send multiple requests and need to handle multiple futures from SKV
  // but only return a single future to this method caller 
  // TODO: add implementation  
  throw new std::logic_error("Not implemented yet");   
}

std::string K2Adapter::GetRowId(std::shared_ptr<SqlOpWriteRequest> request) {
  // TODO: add implementation 
  // either use the virtual row id defined in ybctid_column_value field
  // if it has been set or calculate the row id based on primary key values 
  // in key_column_values in the request   
  throw new std::logic_error("Not implemented yet");                                
}

std::future<K23SITxn> K2Adapter::beginTransaction() {
  return k23si_->beginTxn(k2::K2TxnOptions{});
}

}  // namespace gate
}  // namespace k2pg
