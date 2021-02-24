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
#include "k2_includes.h"
#include <boost/function.hpp>

#include "yb/common/concurrent/async_util.h"
#include "yb/common/status.h"
#include "yb/entities/schema.h"
#include "yb/pggate/k23si_gate.h"
#include "yb/pggate/k23si_txn.h"
#include "yb/pggate/pg_op_api.h"
#include "yb/pggate/pg_env.h"

#include "k2_thread_pool.h"
#include "k2_log.h"

namespace k2pg {
namespace gate {

using yb::Status;

// an adapter between SQL layer operations and K2 SKV storage
class K2Adapter {
 public:
  // TODO make thead pool size configurable and investigate best number of threads
  K2Adapter():threadPool_(2) {
    k23si_ = std::make_shared<K23SIGate>();
  };

  CHECKED_STATUS Init();

  CHECKED_STATUS Shutdown();

  CBFuture<k2::GetSchemaResult> GetSchema(const std::string& collectionName, const std::string& schemaName, uint64_t schemaVersion);

  CBFuture<k2::CreateSchemaResult> CreateSchema(const std::string& collectionName, std::shared_ptr<k2::dto::Schema> schema);

  CBFuture<k2::Status> CreateCollection(const std::string& collection_name, const std::string& nsName);
  // TODO: Add DeleteColection later when it is supported on CPO/K23si

  CBFuture<CreateScanReadResult> CreateScanRead(const std::string& collectionName,
                                                     const std::string& schemaName);

  CBFuture<Status> Exec(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgOpTemplate> op);

  CBFuture<Status> BatchExec(std::shared_ptr<K23SITxn> k23SITxn, const std::vector<std::shared_ptr<PgOpTemplate>>& ops);

  std::string GetRowId(std::shared_ptr<SqlOpWriteRequest> request);
  std::string GetRowId(const std::string& collection_name, const std::string& table_id, uint32_t schema_version, std::vector<std::shared_ptr<SqlValue>> key_values);
  static std::string GetRowIdFromReadRecord(k2::dto::SKVRecord& record);

  CBFuture<K23SITxn> beginTransaction();

  private:
  std::shared_ptr<K23SIGate> k23si_;
  ThreadPool threadPool_;

  CBFuture<Status> handleReadOp(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgReadOpTemplate> op);
  CBFuture<Status> handleWriteOp(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgWriteOpTemplate> op);
  // Helper funcxtion for handleReadOp when ybctid is set in the request
  void handleReadByRowIds(std::shared_ptr<K23SITxn> k23SITxn,
                           std::shared_ptr<PgReadOpTemplate> op,
                           std::shared_ptr<std::promise<Status>> prom);

  template <class T> // Works with SqlOpWriteRequest and SqlOpReadRequest types
  std::pair<k2::dto::SKVRecord, Status> MakeSKVRecordWithKeysSerialized(T& request, bool existYbctids, bool ignoreYBCTID=false);

  // Sorts values by field index, serializes values into SKVRecord, and returns skv indexes of written fields
  std::vector<uint32_t> SerializeSKVValueFields(k2::dto::SKVRecord& record,
                                                std::vector<ColumnValue>& values);

  // Column ID of the virtual column which is not stored in k2 data
  static constexpr int32_t VIRTUAL_COLUMN = -8;

  public:
  static void SerializeValueToSKVRecord(const SqlValue& value, k2::dto::SKVRecord& record);
  static Status K2StatusToYBStatus(const k2::Status& status);
  static SqlOpResponse::RequestStatus K2StatusToPGStatus(const k2::Status& status);
  static std::string YBCTIDToString(std::shared_ptr<SqlOpExpr> ybctid_column_value);

  // We have two implicit fields (tableID and indexID) in the SKV, so this is the offset to get a user field
  static constexpr uint32_t SKV_FIELD_OFFSET = 2;
};

}  // namespace gate
}  // namespace k2pg
