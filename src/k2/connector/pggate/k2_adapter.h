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
#include <boost/function.hpp>

#include "common/concurrent/async_util.h"
#include "common/status.h"
#include "entities/schema.h"
#include "entities/expr.h"
#include "k2_config.h"
#include "k2_gate.h"
#include "k2_includes.h"
#include "k2_log.h"
#include "k2_thread_pool.h"
#include "k2_txn.h"
#include "pg_env.h"
#include "pg_op_api.h"

namespace k2pg {
namespace gate {

using yb::Status;
using k2pg::sql::PgExpr;
using k2pg::sql::PgConstant;
using k2pg::sql::PgOperator;

// An adapter between SQL/Connector layer operations and K2 SKV storage, designed to be the ONLY interface in between.
// It contains 5 sub-groups of APIs
//  1) SKV Schema APIs (CRUD of Collection, Schema, similar to DDL)
//  2) K2-3SI transaction APIs (Begin, Commit, Abort)
//  3) Transactional SKV Record/data API (CRUD, QueryScan, etc, similar to DML)
//  4) (static) Utility functions, e.g. K2 type conversion to PG types
//  5) K2Adapter self-managment APIs, e.g. ctor, dtor, Init() etc.
class K2Adapter {
public:
  // 1/5 SKV Schema and collection APIs (CPO operations)
  CBFuture<k2::CreateSchemaResult> CreateSchema(const std::string& collectionName, std::shared_ptr<k2::dto::Schema> schema)
    { return k23si_->createSchema(collectionName, *schema.get()); };

  CBFuture<k2::GetSchemaResult> GetSchema(const std::string& collectionName, const std::string& schemaName, uint64_t schemaVersion)
    { return k23si_->getSchema(collectionName, schemaName, schemaVersion); }

  CBFuture<k2::Status> CreateCollection(const std::string& collection_name, const std::string& nsName);
  CBFuture<k2::Status> DropCollection(const std::string& collection_name)
    { return k23si_->dropCollection(collection_name); }

  // 2/5 K2-3SI transaction APIs
  CBFuture<K23SITxn> BeginTransaction();
  // Async EndTransaction shouldCommit - true to commit the transaction, false to abort the transaction
  CBFuture<k2::EndResult> EndTransaction(std::shared_ptr<K23SITxn> k23SITxn, bool shouldCommit) { return k23SITxn->endTxn(shouldCommit);}

  // 3/5 SKV data APIs
  // Read a record based on recordKey(which will be consumed/moved).
  CBFuture<k2::ReadResult<k2::dto::SKVRecord>> ReadRecord(std::shared_ptr<K23SITxn> k23SITxn, k2::dto::SKVRecord& recordKey)
    { return k23SITxn->read(std::move(recordKey)); }
  // param record will be consumed/moved
  CBFuture<k2::WriteResult> UpsertRecord(std::shared_ptr<K23SITxn> k23SITxn, k2::dto::SKVRecord& record)
      { return WriteRecord(k23SITxn, record, false/*isDelete*/); }
  // param record will be consumed/moved
  CBFuture<k2::WriteResult> DeleteRecord(std::shared_ptr<K23SITxn> k23SITxn, k2::dto::SKVRecord& record)
    { return WriteRecord(k23SITxn, record, true/*isDelete*/); }

  CBFuture<CreateScanReadResult> CreateScanRead(const std::string& collectionName, const std::string& schemaName);
  CBFuture<k2::QueryResult> ScanRead(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<k2::Query> query)
    { return k23SITxn->scanRead(query); }

  CBFuture<Status> Exec(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgOpTemplate> op);

  CBFuture<Status> BatchExec(std::shared_ptr<K23SITxn> k23SITxn, const std::vector<std::shared_ptr<PgOpTemplate>>& ops);

  // 4/5 Utility APIs and Misc.
  std::string GetRowId(std::shared_ptr<SqlOpWriteRequest> request);
  std::string GetRowId(const std::string& collection_name, const std::string& schema_name, uint32_t schema_version,
    k2pg::sql::PgOid base_table_oid, k2pg::sql::PgOid index_oid, std::vector<SqlValue *>& key_values);
  static std::string GetRowIdFromReadRecord(k2::dto::SKVRecord& record);

  static void SerializeValueToSKVRecord(const SqlValue& value, k2::dto::SKVRecord& record);
  static Status K2StatusToYBStatus(const k2::Status& status);
  static SqlOpResponse::RequestStatus K2StatusToPGStatus(const k2::Status& status);

  // We have two implicit fields (tableID and indexID) in the SKV, so this is the offset to get a user field
  static constexpr uint32_t SKV_FIELD_OFFSET = 2;

  // 5/5 Self managment APIs
  // TODO make thead pool size configurable and investigate best number of threads
  K2Adapter():threadPool_(conf_.get("thread_pool_size", 2)) {
    k23si_ = std::make_shared<K23SIGate>();
  };

  CHECKED_STATUS Init();
  CHECKED_STATUS Shutdown();

  static k2::dto::expression::Expression ToK2Expression(PgExpr* pg_expr);
  static k2::dto::expression::Expression ToK2AndOrOperator(k2::dto::expression::Operation op, std::vector<PgExpr*> args);
  static k2::dto::expression::Expression ToK2BinaryLogicOperator(PgOperator* pg_opr);
  static k2::dto::expression::Expression ToK2BetweenOperator(PgOperator* pg_opr);
  static k2::dto::expression::Operation ToK2OperationType(PgExpr* pg_expr) ;
  static k2::dto::expression::Value ToK2Value(PgConstant* pg_const);
  static k2::dto::expression::Value ToK2ColumnRef(PgColumnRef* pg_colref);

  private:
  std::shared_ptr<K23SIGate> k23si_;
  Config conf_;

  ThreadPool threadPool_;

  // will consume/move record param
  CBFuture<k2::WriteResult> WriteRecord(std::shared_ptr<K23SITxn> k23SITxn, k2::dto::SKVRecord& record, bool isDelete)
    { return k23SITxn->write(std::move(record), isDelete); }

  CBFuture<Status> handleReadOp(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgReadOpTemplate> op);
  CBFuture<Status> handleWriteOp(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgWriteOpTemplate> op);

  Status HandleRangeConditions(PgExpr *range_conds, std::vector<PgExpr *>& leftover_exprs, k2::dto::SKVRecord& start, k2::dto::SKVRecord& end);

  // Helper funcxtion for handleReadOp when ybctid is set in the request
  void handleReadByRowIds(std::shared_ptr<K23SITxn> k23SITxn,
                           std::shared_ptr<PgReadOpTemplate> op,
                           std::shared_ptr<std::promise<Status>> prom);

  template <class T> // Works with SqlOpWriteRequest and SqlOpReadRequest types
  std::pair<k2::dto::SKVRecord, Status> MakeSKVRecordWithKeysSerialized(T& request, bool existYbctids, bool ignoreYBCTID=false);

  // Sorts values by field index, serializes values into SKVRecord, and returns skv indexes of written fields
  std::vector<uint32_t> SerializeSKVValueFields(k2::dto::SKVRecord& record,
                                                std::vector<std::shared_ptr<BindVariable>>& values);

  static std::string YBCTIDToString(std::shared_ptr<BindVariable> ybctid_column_value);
  static std::string SerializeSKVRecordToString(k2::dto::SKVRecord& record);
  static k2::dto::SKVRecord YBCTIDToRecord(const std::string& collection,
                                      std::shared_ptr<k2::dto::Schema> schema,
                                      std::shared_ptr<BindVariable> ybctid_column_value);

  // Column ID of the virtual column which is not stored in k2 data
  static constexpr int32_t VIRTUAL_COLUMN = -8;
};

}  // namespace gate
}  // namespace k2pg
