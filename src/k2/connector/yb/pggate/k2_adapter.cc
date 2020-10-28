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


yb::Status K2Adapter::Init() {
  // TODO: add implementation                                   
  return yb::Status::OK();
}

yb::Status K2Adapter::Shutdown() {
  // TODO: add implementation                                   
  return yb::Status::OK();
}

yb::Status K2Adapter::Apply(std::shared_ptr<PgOpTemplate> op, std::shared_ptr<K23SITxn> k23SITxn) {
  // TODO: add implementation  
  // could add the op to a batch and then process the batch in FlushAsync()                                 
  return yb::Status::OK();
}

void K2Adapter::FlushAsync(StatusFunctor callback) {
  // TODO: add implementation  
  // send one or batch of operations asynchronously                                 
}

yb::Status K2Adapter::ReadSync(std::shared_ptr<PgOpTemplate> pg_op, std::shared_ptr<K23SITxn> k23SITxn) {
  Synchronizer s;
  ReadAsync(std::move(pg_op), k23SITxn, s.AsStatusFunctor());
  return s.Wait();
}

void K2Adapter::ReadAsync(std::shared_ptr<PgOpTemplate> pg_op, std::shared_ptr<K23SITxn> k23SITxn, StatusFunctor callback) {
  CHECK(pg_op->read_only());
  CHECK_OK(Apply(std::move(pg_op), k23SITxn));
  FlushAsync(std::move(callback));
}

std::string K2Adapter::GetRowId(std::shared_ptr<SqlOpReadRequest> request) {
  // TODO: add implementation   
  return nullptr;                                
}
        
std::string K2Adapter::GetRowId(std::shared_ptr<SqlOpWriteRequest> request) {
  // TODO: add implementation   
  return nullptr;                                
}

std::future<K23SITxn> K2Adapter::beginTransaction() {
  return k23si->beginTxn(k2::K2TxnOptions{});
}

}  // namespace gate
}  // namespace k2pg
