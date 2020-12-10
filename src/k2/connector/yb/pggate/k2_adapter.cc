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

std::future<Status> K2Adapter::handleReadOp(std::shared_ptr<K23SITxn> k23SITxn, 
                                            std::shared_ptr<PgReadOpTemplate> op) {
    throw new std::logic_error("Unsupported op template type");
}

std::future<Status> K2Adapter::handleWriteOp(std::shared_ptr<K23SITxn> k23SITxn, 
                                             std::shared_ptr<PgWriteOpTemplate> op) {
    auto prom = std::make_shared<std::promise<Status>>();
    auto result = prom->get_future();

    _threadPool.enqueue([this, k23SITxn, op, prom] () {
        std::shared_ptr<SqlOpWriteRequest> writeRequest = op->request();
        SqlOpResponse& response = op->response();
        response.skipped = false;

        if (writeRequest->targets.size() || writeRequest->where_expr || writeRequest->condition_expr) {
            throw std::logic_error("Targets, where, and condition expressions are not supported for write");
        }

        auto [record, status] = MakeSKVRecordWithKeysSerialized(*writeRequest);
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
        std::string cachedKey = YBCTIDToString(*writeRequest); // aka ybctid or rowid

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
        }
        response.status = K2StatusToPGStatus(writeStatus);
        prom->set_value(K2StatusToYBStatus(writeStatus));
    });

    return result;
}

std::future<k2::GetSchemaResult> K2Adapter::GetSchema(const std::string& collectionName, const std::string& schemaName, uint64_t schemaVersion) {
  return _k23si->getSchema(collectionName, schemaName, schemaVersion);     
}

std::future<k2::CreateSchemaResult> K2Adapter::CreateSchema(const std::string& collectionName, std::shared_ptr<k2::dto::Schema> schema) {
  return _k23si->createSchema(collectionName, *schema.get());
}

std::future<CreateScanReadResult> K2Adapter::CreateScanRead(const std::string& collectionName, 
                                                     const std::string& schemaName) {
  return _k23si->createScanRead(collectionName, schemaName);   
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
    switch (op->type()) {
        case PgOpTemplate::WRITE:
            return handleWriteOp(k23SITxn, std::static_pointer_cast<PgWriteOpTemplate>(op));
        case PgOpTemplate::READ:
            return handleReadOp(k23SITxn, std::static_pointer_cast<PgReadOpTemplate>(op));
        default:
          throw new std::logic_error("Unsupported op template type");
    }
}

std::future<Status> K2Adapter::BatchExec(std::shared_ptr<K23SITxn> k23SITxn, const std::vector<std::shared_ptr<PgOpTemplate>>& ops) {
    // same as the above except that send multiple requests and need to handle multiple futures from SKV
    // but only return a single future to this method caller
    // TODO: add implementation
    throw std::logic_error("Not implemented yet");
}

std::string K2Adapter::GetRowId(std::shared_ptr<SqlOpWriteRequest> request) {
    // either use the virtual row id defined in ybctid_column_value field
    // if it has been set or calculate the row id based on primary key values
    // in key_column_values in the request

    if (request->ybctid_column_value) {
        return YBCTIDToString(*request);
    }

    auto [record, status] = MakeSKVRecordWithKeysSerialized(*request);
    if (!status.ok()) {
        throw std::runtime_error("MakeSKVRecordWithKeysSerialized failed for GetRowId");
    }

    k2::dto::Key key = record.getKey();
    // No range keys in SQL and row id only has to be unique within a table, so only need partitionKey
    return key.partitionKey;
}

std::future<K23SITxn> K2Adapter::beginTransaction() {
    return _k23si->beginTxn(k2::K2TxnOptions{});
}

void K2Adapter::SerializeValueToSKVRecord(const SqlValue& value, k2::dto::SKVRecord& record) {
    if (value.IsNull()) {
        record.skipNext();
        return;
    }

    switch (value.type_) {
        case SqlValue::ValueType::BOOL:
            record.serializeNext<bool>(value.data_->bool_val_);
            break;
        case SqlValue::ValueType::INT:
            record.serializeNext<int64_t>(value.data_->int_val_);
            break;
        case SqlValue::ValueType::FLOAT:
            record.serializeNext<float>(value.data_->float_val_);
            break;
        case SqlValue::ValueType::DOUBLE:
            record.serializeNext<double>(value.data_->double_val_);
            break;
        case SqlValue::ValueType::SLICE:
            record.serializeNext<k2::String>(k2::String(value.data_->slice_val_.ToBuffer()));
            break;
        default:
            throw std::logic_error("Unknown SqlValue type");
    }
}

std::pair<k2::dto::SKVRecord, Status> K2Adapter::MakeSKVRecordWithKeysSerialized(SqlOpWriteRequest& request) {
    // TODO names need to be replaced with IDs in the request
    std::future<k2::GetSchemaResult> schema_f = _k23si->getSchema(request.namespace_name, request.table_name,
                                                                  request.schema_version);
    // TODO Schemas are cached by SKVClient but we can add a cache to K2 adapter to reduce
    // cross-thread traffic
    k2::GetSchemaResult schema_result = schema_f.get();
    if (!schema_result.status.is2xxOK()) {
        return std::make_pair(k2::dto::SKVRecord(), K2StatusToYBStatus(schema_result.status));
    }

    std::shared_ptr<k2::dto::Schema>& schema = schema_result.schema;
    k2::dto::SKVRecord record(request.namespace_name, schema);

    if (request.ybctid_column_value) {
        // Using a pre-stored and pre-serialized key, just need to skip key fields
        record.skipNext(); // For table name
        record.skipNext(); // For index id
        // Note, not using range keys for SQL
        for (size_t i=0; i < schema->partitionKeyFields.size(); ++i) {
            record.skipNext();
        }
    } else {
        // Serialize key data into SKVRecord
        record.serializeNext<k2::String>(request.table_name);
        record.serializeNext<k2::String>(""); // TODO index ID needs to be added to the request
        for (const std::shared_ptr<SqlOpExpr>& expr : request.key_column_values) {
            if (!expr->isValueType()) {
                throw std::logic_error("Non value type in key_column_values");
            }

            SerializeValueToSKVRecord(*(expr->getValue()), record);
        }
    }

    return std::make_pair(std::move(record), Status());
}

// Sorts values by field index, serializes values into SKVRecord, and returns skv indexes of written fields
std::vector<uint32_t> K2Adapter::SerializeSKVValueFields(k2::dto::SKVRecord& record, 
                                                         std::vector<ColumnValue>& values) {
    std::vector<uint32_t> fieldsForUpdate;
    uint32_t nextIndex = record.schema->partitionKeyFields.size() + record.schema->rangeKeyFields.size();

    std::sort(values.begin(), values.end(), [] (const ColumnValue& a, const ColumnValue& b) { 
        return a.column_id < b.column_id; }
    );

    for (const ColumnValue& column : values) {
        if (!column.expr->isValueType()) {
            throw std::logic_error("Non value type in column_values");
        }

        // Assumes field ids need to be offset for the implicit tableID and indexID SKV fields
        uint32_t skvIndex = column.column_id + SKV_FIELD_OFFSET;

        while (nextIndex != skvIndex) {
            record.skipNext();
            ++nextIndex;
        }

        // TODO support update on key fields
        fieldsForUpdate.push_back(skvIndex);
        SerializeValueToSKVRecord(*(column.expr->getValue()), record);
    }

    return fieldsForUpdate;
}

std::string K2Adapter::YBCTIDToString(SqlOpWriteRequest& request) {
    if (!request.ybctid_column_value) {
        return "";
    }

    if (!request.ybctid_column_value->isValueType()) {
        throw std::logic_error("Non value type in ybctid_column_value");
    }

    std::shared_ptr<SqlValue> value = request.ybctid_column_value->getValue();
    if (value->type_ != SqlValue::ValueType::SLICE) {
        throw std::logic_error("ybctid_column_value value is not a Slice");
    }

    return value->data_->slice_val_.ToBuffer();
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
