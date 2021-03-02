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

#include "yb/pggate/catalog/sql_catalog_entity.h"

#include <glog/logging.h>

namespace k2pg {
namespace sql {
namespace catalog {

ClusterInfo::ClusterInfo() {
};

ClusterInfo::ClusterInfo(std::string cluster_id, uint64_t catalog_version, bool initdb_done) :
    cluster_id_(cluster_id), catalog_version_(catalog_version), initdb_done_(initdb_done) {
};

ClusterInfo::~ClusterInfo() {
};

SessionTransactionContext::SessionTransactionContext(std::shared_ptr<K23SITxn> txn) {
    txn_ = txn;
    finished_ = false;
}

SessionTransactionContext::~SessionTransactionContext() {
    if (!finished_) {
        K2LOG_E(log::catalog, "Session is not ended");
        // abort the transaction if it has been committed or aborted
        EndTransaction(false);
        finished_ = true;
    }
}

void SessionTransactionContext::EndTransaction(bool should_commit) {
    auto txn_result = txn_->endTxn(should_commit).get();
    if (!txn_result.status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to commit transaction due to {}", txn_result.status);
        throw std::runtime_error("Failed to end transaction, should_commit: " + should_commit);
    }
}

} // namespace catalog
} // namespace sql
} // namespace k2pg
