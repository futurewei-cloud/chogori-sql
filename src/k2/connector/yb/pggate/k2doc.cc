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

#include "yb/pggate/k2doc.h"
#include "yb/common/endian.h"

namespace k2 {
namespace gate {   

    PgDocResult::PgDocResult(string&& data) : data_(move(data)) {
        LoadCache(data_, &row_count_, &row_iterator_);
    }

    PgDocResult::PgDocResult(string&& data, std::list<int64_t>&& row_orders)
        : data_(move(data)), row_orders_(move(row_orders)) {
        LoadCache(data_, &row_count_, &row_iterator_);
    }

    PgDocResult::~PgDocResult() {
    }

    void PgDocResult::LoadCache(const string& cache, int64_t *total_row_count, Slice *cursor) {
        // Setup the buffer to read the next set of tuples.
        CHECK(cursor->empty()) << "Existing cache is not yet fully read";
        *cursor = cache;

        // Read the number row_count in this set.
        int64_t this_count;
        size_t read_size = ReadNumber(cursor, &this_count);
        *total_row_count = this_count;
        cursor->remove_prefix(read_size);
    }

    //--------------------------------------------------------------------------------------------------
    // Read numbers.

    // This is not called ReadBool but ReadNumber because it is invoked from the TranslateNumber
    // template function similarly to the rest of numeric types.
    size_t PgDocResult::ReadNumber(Slice *cursor, bool *value) {
        *value = !!*reinterpret_cast<const bool*>(cursor->data());
        return sizeof(bool);
    }

    size_t PgDocResult::ReadNumber(Slice *cursor, int8_t *value) {
        *value = *reinterpret_cast<const int8_t*>(cursor->data());
        return sizeof(int8_t);
    }

    size_t PgDocResult::ReadNumber(Slice *cursor, uint8_t *value) {
        *value = *reinterpret_cast<const uint8*>(cursor->data());
        return sizeof(uint8_t);
    }

    size_t PgDocResult::ReadNumber(Slice *cursor, uint16 *value) {
        return ReadNumericValue(NetworkByteOrder::Load16, cursor, value);
    }

    size_t PgDocResult::ReadNumber(Slice *cursor, int16 *value) {
        return ReadNumericValue(NetworkByteOrder::Load16, cursor, reinterpret_cast<uint16*>(value));
    }

    size_t PgDocResult::ReadNumber(Slice *cursor, uint32 *value) {
        return ReadNumericValue(NetworkByteOrder::Load32, cursor, value);
    }

    size_t PgDocResult::ReadNumber(Slice *cursor, int32 *value) {
        return ReadNumericValue(NetworkByteOrder::Load32, cursor, reinterpret_cast<uint32*>(value));
    }

    size_t PgDocResult::ReadNumber(Slice *cursor, uint64 *value) {
        return ReadNumericValue(NetworkByteOrder::Load64, cursor, value);
    }

    size_t PgDocResult::ReadNumber(Slice *cursor, int64 *value) {
        return ReadNumericValue(NetworkByteOrder::Load64, cursor, reinterpret_cast<uint64*>(value));
    }

    size_t PgDocResult::ReadNumber(Slice *cursor, float *value) {
        uint32 int_value;
        size_t read_size = ReadNumericValue(NetworkByteOrder::Load32, cursor, &int_value);
        *value = *reinterpret_cast<float*>(&int_value);
        return read_size;
    }

    size_t PgDocResult::ReadNumber(Slice *cursor, double *value) {
        uint64 int_value;
        size_t read_size = ReadNumericValue(NetworkByteOrder::Load64, cursor, &int_value);
        *value = *reinterpret_cast<double*>(&int_value);
        return read_size;
    }

    // Read Text Data
    size_t PgDocResult::ReadBytes(Slice *cursor, char *value, int64_t bytes) {
        memcpy(value, cursor->data(), bytes);
        return bytes;
    }

    int64_t PgDocResult::NextRowOrder() {
        return row_orders_.size() > 0 ? row_orders_.front() : -1;
    }

    Status PgDocResult::WritePgTuple(const std::vector<PgExpr*>& targets, PgTuple *pg_tuple,
                                    int64_t *row_order) {
        int attr_num = 0;
        for (const PgExpr *target : targets) {
            if (!target->is_colref() && !target->is_aggregate()) {
                return STATUS(InternalError,
                            "Unexpected expression, only column refs or aggregates supported here");
            }
            if (target->opcode() == PgColumnRef::Opcode::PG_EXPR_COLREF) {
                attr_num = static_cast<const PgColumnRef *>(target)->attr_num();
            } else {
                attr_num++;
            }

            TranslateData(target, &row_iterator_, attr_num - 1, pg_tuple);
        }

        if (row_orders_.size()) {
            *row_order = row_orders_.front();
            row_orders_.pop_front();
        } else {
            *row_order = -1;
        }
        return Status::OK();
    }

    Status PgDocResult::ProcessSystemColumns() {
        if (syscol_processed_) {
            return Status::OK();
        }
        syscol_processed_ = true;

        for (int i = 0; i < row_count_; i++) {
            int64_t data_size;
            size_t read_size = ReadNumber(&row_iterator_, &data_size);
            row_iterator_.remove_prefix(read_size);

            ybctids_.emplace_back(row_iterator_.data(), data_size);
            row_iterator_.remove_prefix(data_size);
        }
        return Status::OK();
    }

    DocOp::DocOp(const std::shared_ptr<TableInfo>& table)  : table_(table) {
    }

    DocOp::~DocOp() {}
 
    DocWriteOp::DocWriteOp(const shared_ptr<TableInfo>& table)
            : DocOp(table), write_request_(new DocWriteRequest()) {
    }

    DocWriteOp::~DocWriteOp() {}

    bool DocWriteOp::IsTransactional() const {
        return !is_single_row_txn_ && table_->schema().table_properties().is_transactional();
    }

    std::string DocWriteOp::ToString() const {
        return "PGSQL WRITE: " + write_request_->stmt_id;
    }

    DocReadOp::DocReadOp(const shared_ptr<TableInfo>& table)
        : DocOp(table), read_request_(new DocReadRequest()) {
    }
    
    std::string DocReadOp::ToString() const {
        return "PGSQL READ: " + read_request_->stmt_id;
    }

    std::string DocApi::getDocKey(DocReadRequest& request) {
        return "Not implemented";
    }
        
    std::string DocApi::getDocKey(DocWriteRequest& request) {
        return "Not implemented";
    }

    SaveOrUpdateSchemaResponse DocApi::saveOrUpdateSchema(DocKey& key, Schema& schema) {
        return SaveOrUpdateSchemaResponse();
    }

    SaveOrUpdateRecordResponse DocApi::saveOrUpdateRecord(DocKey& key, RowRecord& record) {
        return SaveOrUpdateRecordResponse();
    }

    RowRecord DocApi::getRecord(DocKey& key) {
        return RowRecord();
    }

    RowRecords DocApi::batchGetRecords(DocKey& key, std::vector<PgExpr>& filters, std::string& token) {
        return RowRecords();
    }

    DeleteRecordResponse DocApi::deleteRecord(DocKey& key) {
        return DeleteRecordResponse();
    }

    DeleteRecordsResponse DocApi::batchDeleteRecords(DocKey& key, std::vector<PgExpr>& filters) {
        return DeleteRecordsResponse();
    }

    DeleteRecordsResponse DocApi::deleteAllRecordsAndSchema(DocKey& key) {
        return DeleteRecordsResponse();
    }

}  // namespace gate
}  // namespace k2
