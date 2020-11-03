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
#pragma once
#include <k2/module/k23si/client/k23si_client.h>
#include "k23si_txn.h"
#include <atomic>
#include <future>
#include <queue>

namespace k2pg {
namespace gate {
struct BeginTxnRequest {
    k2::K2TxnOptions opts;
    std::promise<K23SITxn> prom;
};

struct EndTxnRequest {
    k2::dto::K23SI_MTR mtr;
    bool shouldCommit;
    std::promise<k2::EndResult> prom;
};

struct SchemaGetRequest {
    k2::String collectionName;
    k2::String schemaName;
    uint64_t schemaVersion;
    std::promise<k2::GetSchemaResult> prom;
};

struct SchemaCreateRequest {
    k2::String collectionName;
    k2::dto::Schema schema;
    std::promise<k2::CreateSchemaResult> prom;
};

struct ScanReadCreateRequest {
    k2::String collectionName;
    k2::String schemaName;
    std::promise<k2::CreateQueryResult> prom;
};

struct ScanReadRequest {
    k2::dto::K23SI_MTR mtr;
    std::shared_ptr<k2::Query> query;
    std::promise<k2::QueryResult> prom;
};

struct ReadRequest {
    k2::dto::K23SI_MTR mtr;
    k2::SKVRecord record;
    std::promise<k2::ReadResult<k2::SKVRecord>> prom;
};

struct WriteRequest {
    k2::dto::K23SI_MTR mtr;
    bool erase = false;
    k2::SKVRecord record;
    std::promise<k2::WriteResult> prom;
};

// Lock-free mutex
class LFMutex {
private:
    std::atomic_flag _flag = ATOMIC_FLAG_INIT;

public:
    void lock() {
        while (_flag.test_and_set(std::memory_order_acquire));
    }
    void unlock() {
        _flag.clear(std::memory_order_release);
    }
};


// mutex for locking outgoing request queues
extern LFMutex requestQMutex;

// Shared queues for submitting requests to the seastar thread
extern std::queue<BeginTxnRequest> beginTxQ;
extern std::queue<EndTxnRequest> endTxQ;
extern std::queue<SchemaGetRequest> schemaGetTxQ;
extern std::queue<SchemaCreateRequest> schemaCreateTxQ;
extern std::queue<ScanReadCreateRequest> scanReadCreateTxQ;
extern std::queue<ScanReadRequest> scanReadTxQ;
extern std::queue<ReadRequest> readTxQ;
extern std::queue<WriteRequest> writeTxQ;

// Helper function used to push an item onto a request queue safely.
template <typename Q, typename Request>
void pushQ(Q& queue, Request&& r) {
    std::lock_guard lock{requestQMutex};
    queue.push(std::forward<Request>(r));
}

}  // namespace gate
}  // namespace k2pg
