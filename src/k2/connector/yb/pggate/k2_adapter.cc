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

namespace k2 {
namespace gate {

Status K2Adapter::CreateNamespace(const std::string& namespace_name,
                                 const std::string& creator_role_name,
                                 const std::string& namespace_id,
                                 const std::string& source_namespace_id,
                                 const std::optional<uint32_t>& next_pg_oid) {
  // TODO: add implementation                                   
  return Status::OK();
}

Status K2Adapter::DeleteNamespace(const std::string& namespace_name,
                                 const std::string& namespace_id) {
  // TODO: add implementation                                   
  return Status::OK();
}

Status K2Adapter::CreateTable(NamespaceId& namespace_id, NamespaceName& namespace_name, TableName& table_name, const PgObjectId& table_id, 
    Schema& schema, std::vector<std::string>& range_columns, std::vector<std::vector<SqlValue>>& split_rows, 
    bool is_pg_catalog_table, bool is_shared_table, bool if_not_exist) {

  // TODO: add implementation                                   
  return Status::OK();
}

Status K2Adapter::DeleteTable(const string& table_id, bool wait) {
  // TODO: add implementation                                   
  return Status::OK();
}

Status K2Adapter::ReservePgsqlOids(const std::string& namespace_id,
                                  const uint32_t next_oid, const uint32_t count,
                                  uint32_t* begin_oid, uint32_t* end_oid) {
  // TODO: add implementation                                   
  return Status::OK();
}

Status K2Adapter::GetYsqlCatalogMasterVersion(uint64_t *ysql_catalog_version) {
  // TODO: add implementation                                   
  return Status::OK();
}

Status K2Adapter::Init() {
  // TODO: add implementation                                   
  return Status::OK();
}

Status K2Adapter::Shutdown() {
  // TODO: add implementation                                   
  return Status::OK();
}

Status K2Adapter::Apply(std::shared_ptr<PgOpTemplate> op) {
  // TODO: add implementation  
  // could add the op to a batch and then process the batch in FlushAsync()                                 
  return Status::OK();
}

void K2Adapter::FlushAsync(StatusFunctor callback) {
  // TODO: add implementation  
  // send one or batch of operations asynchronously                                 
}

std::string K2Adapter::getDocKey(SqlOpReadRequest& request) {
  // TODO: add implementation   
  return nullptr;                                
}
        
std::string K2Adapter::getDocKey(SqlOpWriteRequest& request) {
  // TODO: add implementation   
  return nullptr;                                
}

}  // namespace gate
}  // namespace k2
