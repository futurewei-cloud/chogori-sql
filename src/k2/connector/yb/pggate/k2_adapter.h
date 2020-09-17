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

#ifndef CHOGORI_GATE_K2_ADAPTER_H
#define CHOGORI_GATE_K2_ADAPTER_H

#include <boost/function.hpp>

#include "yb/common/concurrent/async_util.h"
#include "yb/pggate/pg_op_api.h"
#include "yb/pggate/pg_env.h"
#include "yb/common/status.h"

namespace k2 {
namespace gate {

using namespace yb;

// an adapter between SQL layer operations and K2 SKV storage
class K2Adapter {
 public:
  K2Adapter() {
  };

  ~K2Adapter();

  // Create a new namespace with the given name.
  CHECKED_STATUS CreateNamespace(const std::string& namespace_name,
                                 const std::string& creator_role_name = "",
                                 const std::string& namespace_id = "",
                                 const std::string& source_namespace_id = "",
                                 const std::optional<uint32_t>& next_pg_oid = std::nullopt);
                                 
  // Delete namespace with the given name.
  CHECKED_STATUS DeleteNamespace(const std::string& namespace_name,
                                 const std::string& namespace_id = "");

  CHECKED_STATUS CreateTable(NamespaceId& namespace_id, NamespaceName& namespace_name, TableName& table_name, const PgObjectId& table_id, 
    Schema& schema, std::vector<std::string>& range_columns, std::vector<std::vector<SqlValue>>& split_rows, 
    bool is_pg_catalog_table, bool is_shared_table, bool if_not_exist);

  // Delete the specified table.
  // Set 'wait' to true if the call must wait for the table to be fully deleted before returning.
  CHECKED_STATUS DeleteTable(const std::string& table_id, bool wait = true);    

  // For Postgres: reserve oids for a Postgres database.
  CHECKED_STATUS ReservePgsqlOids(const std::string& namespace_id,
                                  uint32_t next_oid, uint32_t count,
                                  uint32_t* begin_oid, uint32_t* end_oid);

  CHECKED_STATUS GetYsqlCatalogMasterVersion(uint64_t *ysql_catalog_version);

  CHECKED_STATUS Init();

  CHECKED_STATUS Shutdown();

  CHECKED_STATUS Run(std::shared_ptr<SqlOpCall> op);

  void FlushAsync(StatusFunctor callback);

  std::string getDocKey(SqlOpReadRequest& request);
        
  std::string getDocKey(SqlOpWriteRequest& request);
};

}  // namespace gate
}  // namespace k2

#endif //CHOGORI_GATE_K2_ADAPTER_H