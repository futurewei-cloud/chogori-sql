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
