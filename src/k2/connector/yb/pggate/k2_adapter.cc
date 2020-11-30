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

std::future<Status> handleReadOp(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgReadOpTemplate> op) {
    throw new std::logic_error("Unsupported op template type");
}

std::future<Status> handleWriteOp(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgWriteOpTemplate> op) {
    std::promise<Status> prom;
    auto result = prom.get_future();
    _tp.enqueue([this, k23SITxn, op, prom = std::move(prom)] {
        auto writeRequest = op->request();

        auto collection = writeRequest->namespace_name;
        auto schemaName = writeRequest->table_name;
        auto schemaVersion = writeRequest->schema_version;
        bool erase = writeRequest->stmt_type == SqlOpWriteRequest::StmtType::PGSQL_DELETE;

        // block-get the schema
        auto schemaResult = _k23si->getSchema(collection, schemaName, schemaVersion).get();
        if (!schemaResult.status.is2xxOK()) {
            // set failure response
        }

        // see if we have a cached key (ybctid)

        // error if there is a where/condition clause

        // handle insert/upsert/update
        PGSQL_INSERT = 1,
        PGSQL_UPDATE = 2,
        PGSQL_DELETE = 3,
        PGSQL_UPSERT = 4,

        k2::dto::SKVRecord rec(collection, schemaResult.schema);
        // populate the data

        // block-write
        auto writeResult = k23SITxn->write(rec, erase).get();
    });
    return result;
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
        if (!request->ybctid_column_value->isValueType()) {
            throw std::logic_error("Non value type in ybctid_column_value");
        }

        std::shared_ptr<SqlValue> value = request->ybctid_column_value->getValue();
        if (value->type_ != SqlValue::ValueType::SLICE) {
            throw std::logic_error("ybctid_column_value value is not a Slice");
        }

        return value->data_->slice_val_.ToBuffer();
    }

    k2::dto::SKVRecord record = MakeSKVRecordWithKeysSerialized(*request);
    k2::dto::Key key = record.getKey();
    // No range keys in SQL and row id only has to be unique within a table, so only need partitionKey
    return key.partitionKey;
}

std::future<K23SITxn> K2Adapter::beginTransaction() {
    return k23si_->beginTxn(k2::K2TxnOptions{});
}

k2::dto::SKVRecord K2Adapter::MakeSKVRecordWithKeysSerialized(SqlOpWriteRequest& request) {
    // TODO use namespace name and table name directly? How does secondary index fit into this?
    std::future<k2::GetSchemaResult> schema_f = k23si_->getSchema(request.namespace_name, request.table_name,
                                                                  request.schema_version);
    // TODO Schemas are cached by SKVClient but we can add a cache to K2 adapter to reduce
    // cross-thread traffic
    k2::GetSchemaResult schema_result = schema_f.get();
    if (!schema_result.status.is2xxOK()) {
        throw std::runtime_error("Failed to get schema");
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
        record.serializeNext<int16_t>(0); // TODO how to get index id?
        for (const std::shared_ptr<SqlOpExpr>& expr : request.key_column_values) {
            if (!expr->isValueType()) {
                throw std::logic_error("Non value type in key_column_values");
            }

            std::shared_ptr<SqlValue> value = expr->getValue();
            if (value->IsNull()) {
                record.skipNext();
                continue;
            }

            // TODO can make a macro for this when we have another use case
            switch (value->type_) {
                case SqlValue::ValueType::BOOL:
                    record.serializeNext<bool>(value->data_->bool_val_);
                    break;
                case SqlValue::ValueType::INT:
                    record.serializeNext<int64_t>(value->data_->int_val_);
                    break;
                case SqlValue::ValueType::FLOAT:
                    record.serializeNext<float>(value->data_->float_val_);
                    break;
                case SqlValue::ValueType::DOUBLE:
                    record.serializeNext<double>(value->data_->double_val_);
                    break;
                case SqlValue::ValueType::SLICE:
                    record.serializeNext<k2::String>(k2::String(value->data_->slice_val_.ToBuffer()));
                    break;
                default:
                    throw std::logic_error("Unknown SqlValue type");
            }
        }
    }

    return record;
}

K2Adapter::~K2Adapter() {
}

}  // namespace gate
}  // namespace k2pg
