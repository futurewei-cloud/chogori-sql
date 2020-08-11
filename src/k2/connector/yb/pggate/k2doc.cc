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

#include "k2doc.h"

namespace k2 {
namespace gate {   

    SaveOrUpdateSchemaResponse DocApi::saveOrUpdateSchema(DocKey& key, Schema& schema) {
        return SaveOrUpdateSchemaResponse();
    }

    SaveOrUpdateRecordResponse DocApi::saveOrUpdateRecord(DocKey& key, RowRecord& record) {
        return SaveOrUpdateRecordResponse();
    }

    RowRecord DocApi::getRecord(DocKey& key) {
        return RowRecord();
    }

    RowRecords DocApi::batchGetRecords(DocKey& key, std::vector<SqlExpr>& filters, std::string& token) {
        return RowRecords();
    }

    DeleteRecordResponse DocApi::deleteRecord(DocKey& key) {
        return DeleteRecordResponse();
    }

    DeleteRecordsResponse DocApi::batchDeleteRecords(DocKey& key, std::vector<SqlExpr>& filters) {
        return DeleteRecordsResponse();
    }

    DeleteRecordsResponse DocApi::deleteAllRecordsAndSchema(DocKey& key) {
        return DeleteRecordsResponse();
    }

}  // namespace gate
}  // namespace k2
