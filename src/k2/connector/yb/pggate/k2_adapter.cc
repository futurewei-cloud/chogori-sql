//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
#include "k2_adapter.h"
#include "k2_config.h"

#include "yb/pggate/pg_gate_defaults.h"
#include <seastar/core/resource.hh>
#include <seastar/core/memory.hh>

namespace k2pg {
namespace gate {

using k2::K2TxnOptions;


Status K2Adapter::Init() {
    K2LOG_I(log::pg, "Initialize adapter");
    Config conf;
    size_t mem = conf()["thread_mem_mb"].get<size_t>();
    mem *= 1024 * 1024; // to bytes
    // TODO NUMA and hugepages
    seastar::resource::memory mem_config{.bytes = mem, .nodeid = 0};
    std::vector<seastar::resource::memory> mem_configs;
    mem_configs.emplace_back(mem_config);
    seastar::memory::configure(std::move(mem_configs), false, false);

    return Status::OK();
}

Status K2Adapter::Shutdown() {
    // TODO: add implementation
    K2LOG_I(log::pg, "Shutdown adapter");
    return Status::OK();
}

template<typename T>
void FieldCopy(std::optional<T> field, const k2::String& fieldName, k2::dto::SKVRecord& dest) {
    (void) fieldName;

    if (!field.has_value()) {
        dest.serializeNull();
    } else {
        dest.serializeNext<T>(*field);
    }
}

// A RowId can't be created directly from returned record from a read/scan request
// because the key fields weren't serialized directly. This function copies them to a
// a new record and gets the RowId
std::string K2Adapter::GetRowIdFromReadRecord(k2::dto::SKVRecord& record) {
    k2::dto::SKVRecord copyRec(record.collectionName, record.schema);

    record.seekField(0);
    for (int i=0; i < record.schema->partitionKeyFields.size(); ++i) {
        DO_ON_NEXT_RECORD_FIELD(record, FieldCopy, copyRec);
    }
    record.seekField(0);

    return copyRec.getKey().partitionKey;
}

// this helper method processes the given leaf condition, and sets the bounds start/end accordingly.
// didBranch: output param. If this condition causes a branch (e.g. it has some sort of inequality),
// then we set the didBranch output param so that the top-level processor knows that we can't build
// more of the key prefix.
// lastColId: is an input/output param. We check against it to make sure we haven't seen the current field yet,
// and we set it to the current field if we are about to process it.
Status handleLeafCondition(std::shared_ptr<SqlOpCondition> cond,
                           k2::dto::SKVRecord& start, k2::dto::SKVRecord& end,
                           bool& didBranch, int& lastColId) {
    auto& ops = cond->getOperands();
    // we expect 2 nested expressions except for BETWEEN which has 3
    int expectedExpressions = cond->getOp() == PgExpr::Opcode::PG_EXPR_BETWEEN ? 3: 2;
    if (ops.size() != expectedExpressions) {
        const char* msg = "leaf condition wrong number of operands";
        K2LOG_E(log::pg, "{}, got={}, expected={}", msg, ops.size(), expectedExpressions);
        return STATUS(InvalidCommand, msg);
    }
    // first operand is col reference expression
    if (ops[0]->getType() != SqlOpExpr::ExprType::COLUMN_ID) {
        const char* msg = "1st expression in leaf condition must be a column reference";
        K2LOG_E(log::pg, "{}, got {}", msg, ops[0]->getType());
        return STATUS(InvalidCommand, msg);
    }

    int colId = ops[0]->getId();
    if (colId < 0) {
        const char* msg = "column reference is for a system field";
        K2LOG_E(log::pg, "{}, got=", msg, colId);
        return STATUS(InvalidCommand, msg);
    }

    // make sure we're working on a prefix of the key
    if (colId <= lastColId) {
        const char* msg = "column reference in leaf condition is for an already processed field";
        K2LOG_E(log::pg, "{}, got={}, lastColId={}", msg, colId, lastColId);
        return STATUS(InvalidCommand, msg);
    }
    // update the output param
    lastColId = colId;
    int skvId = colId + K2Adapter::SKV_FIELD_OFFSET;
    // rewind the cursor if necessary. Ideally, the fields come in order so this should be a no-op
    if (start.getFieldCursor() != skvId || end.getFieldCursor() != skvId) {
        const char* msg = "column reference in leaf condition refers to non-consecutive field";
        K2LOG_E(log::pg, "{}, got={}, start={}, end={}", msg, skvId, start.getFieldCursor(), end.getFieldCursor());
        return STATUS(InvalidCommand, msg);
    }

    switch (cond->getOp()) {
        case PgExpr::Opcode::PG_EXPR_EQ: {
            if (ops[1]->getType() != SqlOpExpr::ExprType::VALUE) {
                const char* msg = "2nd expression in EQ leaf condition must be a value";
                K2LOG_E(log::pg, "{}, got={}", msg, ops[1]->getType());
                return STATUS(InvalidCommand, msg);
            }
            // non-branching case. Set the value here in both start and end keys as we're limiting both bounds
            K2Adapter::SerializeValueToSKVRecord(*(ops[1]->getValue()), start);
            K2Adapter::SerializeValueToSKVRecord(*(ops[1]->getValue()), end);
            break;
        }
        case PgExpr::Opcode::PG_EXPR_GE: {
            if (ops[1]->getType() != SqlOpExpr::ExprType::VALUE) {
                const char* msg = "2nd expression in GE leaf condition must be a value";
                K2LOG_E(log::pg, "{}, got={}", msg, ops[1]->getType());
                return STATUS(InvalidCommand, msg);
            }
            // branching case. we can only set the lower bound
            didBranch = true;
            K2Adapter::SerializeValueToSKVRecord(*(ops[1]->getValue()), start);
            break;
        }
        case PgExpr::Opcode::PG_EXPR_LE: {
            if (ops[1]->getType() != SqlOpExpr::ExprType::VALUE) {
                const char* msg = "2nd expression in LE leaf condition must be a value";
                K2LOG_E(log::pg, "{}, got={}", msg, ops[1]->getType());
                return STATUS(InvalidCommand, msg);
            }
            // branching case. we can only set the upper bound
            didBranch = true;
            K2Adapter::SerializeValueToSKVRecord(*(ops[1]->getValue()), end);
            break;
        }
        case PgExpr::Opcode::PG_EXPR_BETWEEN: {
            if (ops[1]->getType() != SqlOpExpr::ExprType::VALUE || ops[2]->getType() != SqlOpExpr::ExprType::VALUE) {
                const char* msg = "2nd and 3rd expressions in BETWEEN leaf condition must be values";
                K2LOG_E(log::pg, "{}, got={}, and {}", msg, ops[1]->getType(), ops[2]->getType());
                return STATUS(InvalidCommand, msg);
            }
            // branching case. we can set both bounds but no further prefix is possible
            didBranch = true;
            K2Adapter::SerializeValueToSKVRecord(*(ops[1]->getValue()), start);
            K2Adapter::SerializeValueToSKVRecord(*(ops[2]->getValue()), end);
            break;
        }
        default: {
            const char* msg = "Expression Condition must be one of [BETWEEN, EQ, GE, LE]";
            K2LOG_E(log::pg, "{}", msg);
            return STATUS(InvalidCommand, msg);
        }
    }
    return Status();
}

// this method processes the given top-level condition and sets the start/end boundaries based on the condition
Status parseCondExprAsRange_(std::shared_ptr<SqlOpCondition> condition_expr,
                           k2::dto::SKVRecord& start, k2::dto::SKVRecord& end) {
    if (!condition_expr) {
        return Status::OK();
    }

    // the top level condition must be AND
    if (condition_expr->getOp() != PgExpr::Opcode::PG_EXPR_AND) {
        const char* msg = "Only AND top-level condition is supported in condition expression";
        K2LOG_E(log::pg, "{}", msg);
        return STATUS(InvalidCommand, msg);
    }

    int lastColId = -1; // make sure we're setting the key fields in increasing order
    bool didBranch = false;
    for (auto& expr: condition_expr->getOperands()) {
        if (didBranch) {
            // there was a branch in the processing of previous condition and we cannot continue.
            // Ideally, this shouldn't happen if the query parser did its job well.
            // This is not an error, and so we can still process the request. PG would down-filter the result set after
            K2LOG_W(log::pg, "Condition branched at previous key field. Cannot process further expressions");
            continue; // keep going so that we log all skipped expressions;
        }
        if (expr->getType() != SqlOpExpr::ExprType::CONDITION) {
            const char* msg = "First-level nested expression must be of type Condition";
            K2LOG_E(log::pg, "{}", msg);
            return STATUS(InvalidCommand, msg);
        }
        auto cond = expr->getCondition();
        auto status = handleLeafCondition(expr->getCondition(), start, end, didBranch, lastColId);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

// Helper function for handleReadOp when a vector of ybctids are set in the request
void K2Adapter::handleReadByRowIds(std::shared_ptr<K23SITxn> k23SITxn,
                                    std::shared_ptr<PgReadOpTemplate> op,
                                    std::shared_ptr<std::promise<Status>> prom) {
    std::shared_ptr<SqlOpReadRequest> request = op->request();
    SqlOpResponse& response = op->response();

    k2::Status status;
    std::vector<std::future<k2::ReadResult<k2::SKVRecord>>> result_futures;
    for (auto& ybctid_column_value : request->ybctid_column_values) {
        k2::dto::Key key {.schemaName=request->table_id, .partitionKey=YBCTIDToString(ybctid_column_value), .rangeKey=""};
        result_futures.push_back(k23SITxn->read(std::move(key), request->namespace_id));
    }

    int idx = 0;
    for (auto& result_future : result_futures) {
        k2::ReadResult<k2::SKVRecord> read = result_future.get();
        if (read.status.is2xxOK()) {
            op->mutable_rows_data()->emplace_back(std::move(read.value));
            // use the last read response as the batch response
            status = std::move(read.status);
            idx++;
        } else {
            // If any read failed, abort and fail the batch
            K2LOG_E(log::pg, "Failed to read for {}, due to {}", YBCTIDToString(request->ybctid_column_values[idx]), read.status.message);
            status = std::move(read.status);
            break;
        }
    }

    response.paging_state = nullptr;
    K2LOG_D(log::pg, "handleReadByRowIds set response paging state to null for read op tid={}", op->request()->table_id);
    response.status = K2StatusToPGStatus(status);
    prom->set_value(K2StatusToYBStatus(status));
}

std::future<Status> K2Adapter::handleReadOp(std::shared_ptr<K23SITxn> k23SITxn,
                                            std::shared_ptr<PgReadOpTemplate> op) {
    auto prom = std::make_shared<std::promise<Status>>();
    auto result = prom->get_future();

    threadPool_.enqueue([this, k23SITxn, op, prom] () {
        try {

        std::shared_ptr<SqlOpReadRequest> request = op->request();
        SqlOpResponse& response = op->response();
        response.skipped = false;

        if (request->ybctid_column_values.size() > 0) {
            // TODO SKV doesn't support ybctid_column_value on query yet so no projection or filtering
            return handleReadByRowIds(k23SITxn, op, prom);
        }

        std::shared_ptr<k2::Query> scan = nullptr;

        if (request->paging_state && request->paging_state->query) {
            scan = request->paging_state->query;
        } else {
            std::future<CreateScanReadResult> scan_f = k23si_->createScanRead(request->namespace_id, request->table_id);
            CreateScanReadResult scan_create_result = scan_f.get();
            if (!scan_create_result.status.is2xxOK()) {
                K2LOG_E(log::pg, "Unable to create scan read request");
                response.rows_affected_count = 0;
                response.status = K2StatusToPGStatus(scan_create_result.status);
                prom->set_value(K2StatusToYBStatus(scan_create_result.status));
                return;
            }

            scan = scan_create_result.query;
            scan->setReverseDirection(!request->is_forward_scan);

            std::shared_ptr<k2::dto::Schema> schema = scan->startScanRecord.schema;
            // Projections must include key fields so that ybctid/rowid can be created from the resulting
            // record
            if (request->targets.size()) {
                for (uint32_t keyIdx : schema->partitionKeyFields) {
                    scan->addProjection(schema->fields[keyIdx].name);
                }
            }
            for (const std::shared_ptr<SqlOpExpr>& target : request->targets) {
                if (target->getType() != SqlOpExpr::ExprType::COLUMN_ID) {
                    prom->set_exception(std::make_exception_ptr(std::logic_error("Non-projection type in read targets")));
                    return;
                }

                // Skip the virtual column which is not stored in K2
                if (target->getId() == VIRTUAL_COLUMN) {
                    continue;
                }

                k2::String name = target->getName();
                // Skip key fields which were already projected above
                bool skip = false;
                for (uint32_t keyIdx : schema->partitionKeyFields) {
                    if (name == schema->fields[keyIdx].name) {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    continue;
                }

                scan->addProjection(name);
                K2LOG_D(log::pg, "Projection added for name={}", name);
            }

            // create the start/end records based on the data found in the request and the hard-coded tableid/idxid
            auto [startRecord, startStatus] = MakeSKVRecordWithKeysSerialized(*request, false);
            auto [endRecord, endStatus] = MakeSKVRecordWithKeysSerialized(*request, false);

            if (!startStatus.ok() || !endStatus.ok()) {
                // An error here means the schema could not be retrieved, which shouldn't happen
                // because the schema would have been used to make the original query
                K2LOG_E(log::pg, "Scan request cannot create SKVRecords due to: {} ;;; {}", startStatus, endStatus);
                response.status = SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
                response.rows_affected_count = 0;
                prom->set_value(std::move(startStatus.ok() ? endStatus : startStatus));
                return;
            }

            // update the records based on the condition expression found in the request
            auto parseStatus = parseCondExprAsRange_(request->condition_expr, startRecord, endRecord);
            if (!parseStatus.ok()) {
                response.status = SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
                response.rows_affected_count = 0;
                prom->set_value(std::move(parseStatus));
                return;
            }

            scan->startScanRecord = std::move(startRecord);
            scan->endScanRecord = std::move(endRecord);

            // TODO apply the where clause as a filter expression in the scan
            if (request->where_expr) {
                K2LOG_E(log::pg, "Read request with where_expr is not supported.");
                response.status = SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
                response.rows_affected_count = 0;
                prom->set_value(std::move(startStatus));
                return;
            }

            // this is a total limit.
            if (request->limit > 0) {
                scan->setLimit(request->limit);
            }
        }

        k2::QueryResult scan_result = k23SITxn->scanRead(scan).get();
        if (scan->isDone()) {
            response.paging_state = nullptr;
            K2LOG_D(log::pg, "Scan is done, set response paging state to null for request {}", request->table_id);
        } else if (request->paging_state) {
            response.paging_state = request->paging_state;
            response.paging_state->total_num_rows_read += scan_result.records.size();
            K2LOG_D(log::pg, "Request paging state is null? {}, for request {}", (request->paging_state == nullptr), request->table_id);
        } else {
            response.paging_state = std::make_shared<SqlOpPagingState>();
            response.paging_state->query = scan;
            response.paging_state->total_num_rows_read += scan_result.records.size();
            K2LOG_D(log::pg, "Created paging state for request {}, total rows read={}", request->table_id,  response.paging_state->total_num_rows_read);
        }

        *(op->mutable_rows_data()) = std::move(scan_result.records);

        response.status = K2StatusToPGStatus(scan_result.status);
        prom->set_value(K2StatusToYBStatus(scan_result.status));

        } catch (const std::exception& e) {
            K2LOG_W(log::pg, "Throw in handleReadOp", e.what());
            prom->set_exception(std::current_exception());
        }
    });

    return result;
}

std::future<Status> K2Adapter::handleWriteOp(std::shared_ptr<K23SITxn> k23SITxn,
                                             std::shared_ptr<PgWriteOpTemplate> op) {
    auto prom = std::make_shared<std::promise<Status>>();
    auto result = prom->get_future();

    threadPool_.enqueue([this, k23SITxn, op, prom] () {
        try {

        std::shared_ptr<SqlOpWriteRequest> writeRequest = op->request();
        SqlOpResponse& response = op->response();
        response.skipped = false;

        if (writeRequest->targets.size() || writeRequest->where_expr || writeRequest->condition_expr) {
            throw std::logic_error("Targets, where, and condition expressions are not supported for write");
        }

        bool ignoreYBCTID = writeRequest->stmt_type != SqlOpWriteRequest::StmtType::PGSQL_UPDATE;
        auto [record, status] = MakeSKVRecordWithKeysSerialized(*writeRequest, writeRequest->ybctid_column_value != nullptr, ignoreYBCTID);
        if (!status.ok()) {
            response.status = SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
            response.rows_affected_count = 0;
            prom->set_value(std::move(status));
            return;
        }

        // These two are INSERT, UPSERT, and DELETE only
        bool erase = writeRequest->stmt_type == SqlOpWriteRequest::StmtType::PGSQL_DELETE;
        bool rejectIfExists = writeRequest->stmt_type == SqlOpWriteRequest::StmtType::PGSQL_INSERT;
        // UDPATE only
        std::string cachedKey = YBCTIDToString(writeRequest->ybctid_column_value); // aka ybctid or rowid

        // populate the data, fieldsForUpdate is only relevant for UPDATE
        std::vector<ColumnValue>& values = writeRequest->stmt_type != SqlOpWriteRequest::StmtType::PGSQL_UPDATE
                ? writeRequest->column_values : writeRequest->column_new_values;
        std::vector<uint32_t> fieldsForUpdate = SerializeSKVValueFields(record, values);

        k2::Status writeStatus;

        // block-write
        if (writeRequest->stmt_type != SqlOpWriteRequest::StmtType::PGSQL_UPDATE) {
            k2::WriteResult writeResult = k23SITxn->write(std::move(record), erase, rejectIfExists).get();
            writeStatus = std::move(writeResult.status);
        } else {
            k2::PartialUpdateResult updateResult = k23SITxn->partialUpdate(std::move(record),
                            std::move(fieldsForUpdate), std::move(cachedKey)).get();
            writeStatus = std::move(updateResult.status);
        }

        if (writeStatus.is2xxOK()) {
            response.rows_affected_count = 1;
        } else {
            response.rows_affected_count = 0;
            response.error_message = writeStatus.message;
            // TODO pg_error_code or txn_error_code in response?
            K2LOG_E(log::pg, "K2 write failed due to {}", response.error_message);
        }
        K2LOG_D(log::pg, "K2 write status: {}", writeStatus);
        response.status = K2StatusToPGStatus(writeStatus);
        prom->set_value(K2StatusToYBStatus(writeStatus));

        } catch (const std::exception& e) {
            K2LOG_W(log::pg, "Throw in handlewrite: ", e.what());
            prom->set_exception(std::current_exception());
        }
    });

    return result;
}

std::future<k2::GetSchemaResult> K2Adapter::GetSchema(const std::string& collectionName, const std::string& schemaName, uint64_t schemaVersion) {
  return k23si_->getSchema(collectionName, schemaName, schemaVersion);
}

std::future<k2::CreateSchemaResult> K2Adapter::CreateSchema(const std::string& collectionName, std::shared_ptr<k2::dto::Schema> schema) {
  return k23si_->createSchema(collectionName, *schema.get());
}

std::future<k2::Status> K2Adapter::CreateCollection(const std::string& collection_name, const std::string& nsName)
{
    K2LOG_I(log::pg, "Create collection: name={}, ns={}", collection_name, nsName);
    Config conf;

    // Working around json conversion to/from k2::String which uses b64
    std::vector<std::string> stdRangeEnds = conf()["create_collections"][nsName]["range_ends"];
    std::vector<k2::String> rangeEnds;
    for (const std::string& end : stdRangeEnds) {
        rangeEnds.emplace_back(end);
    }

    std::vector<std::string> stdEndpoints = conf()["create_collections"][nsName]["endpoints"];
    std::vector<k2::String> endpoints;
    for (const std::string& ep : stdEndpoints) {
        endpoints.emplace_back(ep);
    }

    k2::dto::HashScheme scheme = rangeEnds.size() ? k2::dto::HashScheme::Range : k2::dto::HashScheme::HashCRC32C;

    auto createCollectionReq = k2::dto::CollectionCreateRequest{
        .metadata{
            .name = collection_name,
            .hashScheme = scheme,
            .storageDriver = k2::dto::StorageDriver::K23SI,
            .capacity{
                // TODO: get capacity from config or pass in from param
                //.dataCapacityMegaBytes = 1000,
                //.readIOPs = 100000,
                //.writeIOPs = 100000
            },
            .retentionPeriod = k2::Duration(1h) * 90 * 24  //TODO: get this from config or from param in
        },
        .clusterEndpoints = std::move(endpoints),
        .rangeEnds = std::move(rangeEnds)
    };

    return k23si_->createCollection(std::move(createCollectionReq));
}

std::future<CreateScanReadResult> K2Adapter::CreateScanRead(const std::string& collectionName,
                                                     const std::string& schemaName) {
  return k23si_->createScanRead(collectionName, schemaName);
}

std::future<Status> K2Adapter::Exec(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgOpTemplate> op) {
    // 1) check the request in op and construct the SKV request based on the op type, i.e., READ or WRITE
    // 2) call read or write on k23SITxn
    // 3) create create a runner in a thread pool to check the response of the SKV call
    // 4) return a promise and return the future as the response for this method
    // 5) once the response from SKV returns
    //   a) populate the response object in op
    //   b) populate the data field in op as result set
    //   c) set the value for future
    op->allocateResponse();
    switch (op->type()) {
        case PgOpTemplate::WRITE:
            K2LOG_I(log::pg, "Executing writing operation");
            return handleWriteOp(k23SITxn, std::static_pointer_cast<PgWriteOpTemplate>(op));
        case PgOpTemplate::READ:
            K2LOG_I(log::pg, "Executing reading operation");
            return handleReadOp(k23SITxn, std::static_pointer_cast<PgReadOpTemplate>(op));
        default:
          throw new std::logic_error("Unsupported op template type");
    }
}

std::future<Status> K2Adapter::BatchExec(std::shared_ptr<K23SITxn> k23SITxn, const std::vector<std::shared_ptr<PgOpTemplate>>& ops) {
    // same as the above except that send multiple requests and need to handle multiple futures from SKV
    // but only return a single future to this method caller. Return Status will be OK all Execs are
    // successful, otherwise Status will be one of the failed Execs

    // This only works if the Exec calls are executed in order by the threadpool, otherwise we could
    // deadlock if the waiting thread below is executed first
    auto op_futures = std::make_shared<std::vector<std::future<Status>>>();
    for (const std::shared_ptr<PgOpTemplate>& op : ops) {
        op_futures->emplace_back(Exec(k23SITxn, op));
    }

    auto prom = std::make_shared<std::promise<Status>>();
    auto result = prom->get_future();
    threadPool_.enqueue([op_futures, prom] () {
        Status status = Status::OK();
        std::exception_ptr e = nullptr;

        for (std::future<Status>& op : *op_futures) {
            try {
                Status exec_status = op.get();
                if (!exec_status.ok()) {
                    status = std::move(exec_status);
                }
            } catch (...) {
                e = std::current_exception();
            }
        }

        if (!e) {
            prom->set_value(std::move(status));
        } else {
            prom->set_exception(e);
        }
    });

    return result;
}

std::string K2Adapter::GetRowId(std::shared_ptr<SqlOpWriteRequest> request) {
    // either use the virtual row id defined in ybctid_column_value field
    // if it has been set or calculate the row id based on primary key values
    // in key_column_values in the request

    if (request->ybctid_column_value) {
        return YBCTIDToString(request->ybctid_column_value);
    }

    auto [record, status] = MakeSKVRecordWithKeysSerialized(*request, request->ybctid_column_value != nullptr);
    if (!status.ok()) {
        throw std::runtime_error("MakeSKVRecordWithKeysSerialized failed for GetRowId");
    }

    k2::dto::Key key = record.getKey();
    // No range keys in SQL and row id only has to be unique within a table, so only need partitionKey
    return key.partitionKey;
}

std::string K2Adapter::GetRowId(const std::string& namespace_id, const std::string& table_id, uint32_t schema_version, std::vector<std::shared_ptr<SqlValue>> key_values) {
    std::future<k2::GetSchemaResult> schema_f = k23si_->getSchema(namespace_id, table_id, schema_version);
    k2::GetSchemaResult schema_result = schema_f.get();
    if (!schema_result.status.is2xxOK()) {
        throw std::runtime_error("Failed to get schema for " + table_id + " in " + namespace_id + " due to " + schema_result.status.message.c_str());
    }
    std::shared_ptr<k2::dto::Schema>& schema = schema_result.schema;
    k2::dto::SKVRecord record(namespace_id, schema);

    // Serialize key data into SKVRecord
    record.serializeNext<k2::String>(table_id);
    record.serializeNext<k2::String>(""); // TODO index ID needs to be added to the request
    for (std::shared_ptr<SqlValue> value : key_values) {
        K2Adapter::SerializeValueToSKVRecord(*(value.get()), record);
    }

    k2::dto::Key key = record.getKey();
    // No range keys in SQL and row id only has to be unique within a table, so only need partitionKey
    K2LOG_D(log::pg, "Returning row id for table {} from SKV partition key: {}", table_id, key.partitionKey);
    return key.partitionKey;
}

std::future<K23SITxn> K2Adapter::beginTransaction() {
    k2::K2TxnOptions options{};
    // use default values for now
    // TODO: read from configuration/env files
    // Actual partition request deadline is min of this and command line option
    options.deadline = k2::Duration(60000s);
    //options.priority = k2::dto::TxnPriority::Medium;
    return k23si_->beginTxn(options);
}

void K2Adapter::SerializeValueToSKVRecord(const SqlValue& value, k2::dto::SKVRecord& record) {
    if (value.IsNull()) {
        K2LOG_D(log::pg, "null value for field: {}", record.schema->fields[record.getFieldCursor()])
        record.serializeNull();
        return;
    }

    switch (value.type_) {
        case SqlValue::ValueType::BOOL:
            K2LOG_D(log::pg, "bool value for field: {}", record.schema->fields[record.getFieldCursor()])
            record.serializeNext<bool>(value.data_.bool_val_);
            break;
        case SqlValue::ValueType::INT:
            K2LOG_D(log::pg, "int value for field: {}", record.schema->fields[record.getFieldCursor()])
            record.serializeNext<int64_t>(value.data_.int_val_);
            break;
        case SqlValue::ValueType::FLOAT:
            K2LOG_D(log::pg, "float value for field: {}", record.schema->fields[record.getFieldCursor()])
            record.serializeNext<float>(value.data_.float_val_);
            break;
        case SqlValue::ValueType::DOUBLE:
            K2LOG_D(log::pg, "double value for field: {}", record.schema->fields[record.getFieldCursor()])
            record.serializeNext<double>(value.data_.double_val_);
            break;
        case SqlValue::ValueType::SLICE:
            K2LOG_D(log::pg, "slice value for field: {}", record.schema->fields[record.getFieldCursor()])
            record.serializeNext<k2::String>(k2::String(value.data_.slice_val_));
            break;
        default:
            throw std::logic_error("Unknown SqlValue type");
    }
}

template <class T> // Works with SqlOpWriteRequest and SqlOpReadRequest types
std::pair<k2::dto::SKVRecord, Status> K2Adapter::MakeSKVRecordWithKeysSerialized(T& request, bool existYbctids, bool ignoreYBCTID) {
    std::future<k2::GetSchemaResult> schema_f = k23si_->getSchema(request.namespace_id, request.table_id,
                                                                  request.schema_version);
    // TODO Schemas are cached by SKVClient but we can add a cache to K2 adapter to reduce
    // cross-thread traffic
    k2::GetSchemaResult schema_result = schema_f.get();
    if (!schema_result.status.is2xxOK()) {
        return std::make_pair(k2::dto::SKVRecord(), K2StatusToYBStatus(schema_result.status));
    }

    std::shared_ptr<k2::dto::Schema>& schema = schema_result.schema;
    k2::dto::SKVRecord record(request.namespace_id, schema);

    if (existYbctids && !ignoreYBCTID) {
        // Using a pre-stored and pre-serialized key, just need to skip key fields
        // Note, not using range keys for SQL
        for (size_t i=0; i < schema->partitionKeyFields.size(); ++i) {
            record.serializeNull();
        }
    } else {
        // Serialize key data into SKVRecord
        record.serializeNext<k2::String>(request.table_id);
        record.serializeNext<k2::String>(""); // TODO index ID needs to be added to the request
        for (const std::shared_ptr<SqlOpExpr>& expr : request.key_column_values) {
            if (!expr->isValueType()) {
                throw std::logic_error("Non value type in key_column_values");
            }

            K2Adapter::SerializeValueToSKVRecord(*(expr->getValue()), record);
        }
    }

    return std::make_pair(std::move(record), Status());
}
// Sorts values by field index, serializes values into SKVRecord, and returns skv indexes of written fields
std::vector<uint32_t> K2Adapter::SerializeSKVValueFields(k2::dto::SKVRecord& record,
                                                         std::vector<ColumnValue>& values) {
    std::vector<uint32_t> fieldsForUpdate;

    std::sort(values.begin(), values.end(), [] (const ColumnValue& a, const ColumnValue& b) {
        return a.column_id < b.column_id; }
    );

    for (const ColumnValue& column : values) {
        if (!column.expr->isValueType()) {
            throw std::logic_error("Non value type in column_values");
        }

        // Assumes field ids need to be offset for the implicit tableID and indexID SKV fields
        uint32_t skvIndex = column.column_id + SKV_FIELD_OFFSET;

        while (record.getFieldCursor() != skvIndex) {
            record.serializeNull();
        }

        // TODO support update on key fields
        fieldsForUpdate.push_back(skvIndex);
        K2Adapter::SerializeValueToSKVRecord(*(column.expr->getValue()), record);
    }

    return fieldsForUpdate;
}

std::string K2Adapter::YBCTIDToString(std::shared_ptr<SqlOpExpr> ybctid_column_value) {
    if (!ybctid_column_value) {
        return "";
    }

    if (!ybctid_column_value->isValueType()) {
        K2LOG_W(log::pg, "ybctid_column_value value is not a Slice");
        throw std::invalid_argument("Non value type in ybctid_column_value");
    }

    std::shared_ptr<SqlValue> value = ybctid_column_value->getValue();
    if (value->type_ != SqlValue::ValueType::SLICE) {
        K2LOG_W(log::pg, "ybctid_column_value value is not a Slice");
        throw std::invalid_argument("ybctid_column_value value is not a Slice");
    }

    return value->data_.slice_val_;
}


Status K2Adapter::K2StatusToYBStatus(const k2::Status& status) {
    // TODO verify this translation with how the upper layers use the Status,
    // especially the Aborted status
    switch (status.code) {
        case 200: // OK Codes
        case 201:
        case 202:
            return Status();
        case 400: // Bad request
            return STATUS(InvalidCommand, status.message.c_str());
        case 403: // Forbidden, used to indicate AbortRequestTooOld in K23SI
            return STATUS(Aborted, status.message.c_str());
        case 404: // Not found
            return STATUS(NotFound, status.message.c_str());
        case 405: // Not allowed, indicates a bug in K2 usage or operation
        case 406: // Not acceptable, used to indicate BadFilterExpression
            return STATUS(InvalidArgument, status.message.c_str());
        case 408: // Timeout
            return STATUS(TimedOut, status.message.c_str());
        case 409: // Conflict, used to indicate K23SI transaction conflicts
            return STATUS(Aborted, status.message.c_str());
        case 410: // Gone, indicates a partition map error
            return STATUS(ServiceUnavailable, status.message.c_str());
        case 412: // Precondition failed, indicates a failed K2 insert operation
            return STATUS(AlreadyPresent, status.message.c_str());
        case 422: // Unprocessable entity, BadParameter in K23SI, indicates a bug in usage or operation
            return STATUS(InvalidArgument, status.message.c_str());
        case 500: // Internal error, indicates a bug in K2 code
            return STATUS(Corruption, status.message.c_str());
        case 503: // Service unavailable, indicates a partition is not assigned
            return STATUS(ServiceUnavailable, status.message.c_str());
        default:
            return STATUS(Corruption, "Unknown K2 status code");
    }
}

SqlOpResponse::RequestStatus K2Adapter::K2StatusToPGStatus(const k2::Status& status) {
    switch (status.code) {
        case 200: // OK Codes
        case 201:
        case 202:
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_OK;
        case 400: // Bad request
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_USAGE_ERROR;
        case 403: // Forbidden, used to indicate AbortRequestTooOld in K23SI
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RESTART_REQUIRED_ERROR;
        case 404: // Not found
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_OK; // TODO: correct?
        case 405: // Not allowed, indicates a bug in K2 usage or operation
        case 406: // Not acceptable, used to indicate BadFilterExpression
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_USAGE_ERROR;
        case 408: // Timeout
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        case 409: // Conflict, used to indicate K23SI transaction conflicts
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RESTART_REQUIRED_ERROR;
        case 410: // Gone, indicates a partition map error
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        case 412: // Precondition failed, indicates a failed K2 insert operation
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_DUPLICATE_KEY_ERROR;
        case 422: // Unprocessable entity, BadParameter in K23SI, indicates a bug in usage or operation
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_USAGE_ERROR;
        case 500: // Internal error, indicates a bug in K2 code
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        case 503: // Service unavailable, indicates a partition is not assigned
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        default:
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
    }
}

}  // namespace gate
}  // namespace k2pg
