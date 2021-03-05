/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#include "pggate/catalog/base_handler.h"

#include <glog/logging.h>

namespace k2pg {
namespace sql {
namespace catalog {
BaseHandler::BaseHandler(std::shared_ptr<K2Adapter> k2_adapter) : k2_adapter_(k2_adapter) {
}

BaseHandler::~BaseHandler() {
}

RStatus BaseHandler::CreateSKVSchema(std::string collection_name, std::shared_ptr<k2::dto::Schema> schema) {
    RStatus response;
    auto result = k2_adapter_->CreateSchema(collection_name, schema).get();
    if (!result.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create SKV schema for {} in {}, due to {}", schema->name, collection_name, result.status);
        response.code = StatusCode::INTERNAL_ERROR;
        response.errorMessage = std::move(result.status.message);
    } else {
        K2LOG_D(log::catalog, "Created SKV Schema for {} in ns {} as: {}", schema->name, collection_name, (*schema.get()))
        response.Succeed();
    }
    return response;
}

RStatus BaseHandler::PersistSKVRecord(std::shared_ptr<SessionTransactionContext> context, k2::dto::SKVRecord& record) {
    return SaveOrUpdateSKVRecord(context, record, false);
}

RStatus BaseHandler::DeleteSKVRecord(std::shared_ptr<SessionTransactionContext> context, k2::dto::SKVRecord& record) {
    return SaveOrUpdateSKVRecord(context, record, true);
}

RStatus BaseHandler::BatchDeleteSKVRecords(std::shared_ptr<SessionTransactionContext> context, std::vector<k2::dto::SKVRecord>& records) {
    RStatus response;
    for (auto& record : records) {
        RStatus result = DeleteSKVRecord(context, record);
        if (!result.IsSucceeded()) {
            return result;
        }
    }
    response.Succeed();
    return response;
}

RStatus BaseHandler::SaveOrUpdateSKVRecord(std::shared_ptr<SessionTransactionContext> context, k2::dto::SKVRecord& record, bool isDelete) {
    RStatus response;
    auto result = context->GetTxn()->write(std::move(record), isDelete).get();
    if (!result.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to {} SKV record due to {}", (isDelete ? "Delete" : "Save"), result.status);
        response.code = StatusCode::INTERNAL_ERROR;
        response.errorMessage = std::move(result.status.message);
    } else {
        response.Succeed();
    }
    return response;
}

} // namespace sql
} // namespace sql
} // namespace k2pg
