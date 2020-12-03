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

#ifndef CHOGORI_SQL_BASE_HANDLER_H
#define CHOGORI_SQL_BASE_HANDLER_H

#include <string>

#include "yb/pggate/catalog/sql_catalog_defaults.h"
#include "yb/pggate/catalog/sql_catalog_entity.h"
#include "yb/pggate/k2_adapter.h"

namespace k2pg {
namespace sql {
namespace catalog {

using k2pg::gate::K2Adapter;
using k2pg::gate::K23SITxn;

class BaseHandler : public std::enable_shared_from_this<BaseHandler> {
    public:
    BaseHandler(std::shared_ptr<K2Adapter> k2_adapter);
    ~BaseHandler();

    RStatus CreateSKVSchema(std::string collection_name, std::shared_ptr<k2::dto::Schema> schema);

    RStatus PersistSKVRecord(std::shared_ptr<SessionTransactionContext> context, k2::dto::SKVRecord& record);

    protected:
    std::shared_ptr<K2Adapter> k2_adapter_;  
};

} // namespace sql
} // namespace sql
} // namespace k2pg

#endif //CHOGORI_SQL_BASE_HANDLER_H