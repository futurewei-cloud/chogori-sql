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

#include "yb/pggate/catalog/base_handler.h"

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
    std::future<k2::CreateSchemaResult> result_future = k2_adapter_->CreateSchema(collection_name, schema);
    k2::CreateSchemaResult result = result_future.get();
    if (!result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to create SKV schema for " << schema->name << "in" << collection_name
            << " due to error code " << result.status.code
            << " and message: " << result.status.message;
        response.code = StatusCode::INTERNAL_ERROR;
        response.errorMessage = std::move(result.status.message);
    } else {
        response.Succeed();
    }
    return response;
}

RStatus BaseHandler::PersistSKVRecord(std::shared_ptr<SessionTransactionContext> context, k2::dto::SKVRecord& record) {
    RStatus response;
    std::future<k2::WriteResult> result_future = context->GetTxn()->write(std::move(record), true);
    k2::WriteResult result = result_future.get();
    if (!result.status.is2xxOK()) {
        LOG(FATAL) << "Failed to persist SKV record "
            << " due to error code " << result.status.code
            << " and message: " << result.status.message;
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
