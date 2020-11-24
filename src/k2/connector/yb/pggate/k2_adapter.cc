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

std::future<k2::GetSchemaResult> K2Adapter::GetSchema(const std::string& collectionName, const std::string& schemaName, uint64_t schemaVersion) {
  return k23si_->getSchema(collectionName, schemaName, schemaVersion);     
}

std::future<k2::CreateSchemaResult> K2Adapter::CreateSchema(const std::string& collectionName, std::shared_ptr<k2::dto::Schema> schema) {
  return k23si_->createSchema(collectionName, *schema.get());
}

std::future<CreateScanReadResult> K2Adapter::CreateScanRead(const std::string& collectionName, 
                                                     const std::string& schemaName) {
  return k23si_->createScanRead(collectionName, schemaName);   
}

// delete one SKV record
std::future<Status> K2Adapter::DeleteSKVRecord(std::shared_ptr<K23SITxn> k23SITxn, k2::dto::SKVRecord& record) {
  throw std::logic_error("Not implemented yet");
}

// delete a batch of SKV records
std::future<Status> K2Adapter::BatchDeleteSKVRecords(std::shared_ptr<K23SITxn> k23SITxn, std::vector<k2::dto::SKVRecord>& records) {
  throw std::logic_error("Not implemented yet");
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
  throw std::logic_error("Not implemented yet");
}

std::future<Status> K2Adapter::BatchExec(std::shared_ptr<K23SITxn> k23SITxn, const std::vector<std::shared_ptr<PgOpTemplate>>& ops) {
  // same as the above except that send multiple requests and need to handle multiple futures from SKV
  // but only return a single future to this method caller
  // TODO: add implementation
  throw std::logic_error("Not implemented yet");
}

std::string K2Adapter::GetRowId(std::shared_ptr<SqlOpWriteRequest> request) {
  // TODO: add implementation
  // either use the virtual row id defined in ybctid_column_value field
  // if it has been set or calculate the row id based on primary key values
  // in key_column_values in the request
  throw std::logic_error("Not implemented yet");
}

std::future<K23SITxn> K2Adapter::beginTransaction() {
  return k23si_->beginTxn(k2::K2TxnOptions{});
}

K2Adapter::~K2Adapter() {
}

}  // namespace gate
}  // namespace k2pg
