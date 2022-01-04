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

#include <atomic>
#include <future>
#include <queue>

#include "k2_includes.h"
#include "k2_txn.h"

namespace k2pg {
namespace gate {
struct BeginTxnRequest {
    k2::K2TxnOptions opts;
    std::promise<K23SITxn> prom;
    k2::TimePoint startTime;
    K2_DEF_FMT(BeginTxnRequest, opts);
};

struct EndTxnRequest {
    k2::dto::K23SI_MTR mtr;
    bool shouldCommit;
    std::promise<k2::EndResult> prom;
    K2_DEF_FMT(EndTxnRequest, mtr, shouldCommit);
};

struct SchemaGetRequest {
    k2::String collectionName;
    k2::String schemaName;
    uint64_t schemaVersion;
    std::promise<k2::GetSchemaResult> prom;
    K2_DEF_FMT(SchemaGetRequest, collectionName, schemaName, schemaVersion);
};

struct SchemaCreateRequest {
    k2::String collectionName;
    k2::dto::Schema schema;
    std::promise<k2::CreateSchemaResult> prom;
    K2_DEF_FMT(SchemaCreateRequest, collectionName, schema);
};

struct CollectionCreateRequest {
    k2::dto::CollectionCreateRequest ccr;
    std::promise<k2::Status> prom;
    K2_DEF_FMT(CollectionCreateRequest, ccr);
};

struct CollectionDropRequest {
    k2::String collectionName;
    std::promise<k2::Status> prom;
    K2_DEF_FMT(CollectionDropRequest, collectionName);
};

struct CreateScanReadResult {
    k2::Status status;
    std::shared_ptr<k2::Query> query;
    K2_DEF_FMT(CreateScanReadResult, status);
};

struct ScanReadCreateRequest {
    k2::String collectionName;
    k2::String schemaName;
    std::promise<CreateScanReadResult> prom;
    K2_DEF_FMT(ScanReadCreateRequest, collectionName, schemaName);
};

struct ScanReadRequest {
    k2::dto::K23SI_MTR mtr;
    std::shared_ptr<k2::Query> query;
    std::promise<k2::QueryResult> prom;
    K2_DEF_FMT(ScanReadRequest, mtr);
};

struct ReadRequest {
    k2::dto::K23SI_MTR mtr;
    k2::dto::SKVRecord record;
    // For key-oriented read without SKVRecord
    k2::dto::Key key = k2::dto::Key();
    std::string collectionName = "";

    std::promise<k2::ReadResult<k2::dto::SKVRecord>> prom;
    K2_DEF_FMT(ReadRequest, mtr, key, collectionName);
};

struct WriteRequest {
    k2::dto::K23SI_MTR mtr;
    bool erase = false;
    k2::dto::ExistencePrecondition precondition = k2::dto::ExistencePrecondition::None;
    k2::dto::SKVRecord record;
    std::promise<k2::WriteResult> prom;
    K2_DEF_FMT(WriteRequest, mtr, erase, precondition);
};

struct UpdateRequest {
    k2::dto::K23SI_MTR mtr;
    k2::dto::SKVRecord record;
    std::vector<uint32_t> fieldsForUpdate;
    k2::dto::Key key = k2::dto::Key();
    std::promise<k2::PartialUpdateResult> prom;
    K2_DEF_FMT(UpdateRequest, mtr, fieldsForUpdate, key);
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
inline LFMutex requestQMutex;

// signal between consumer and producers that the queue processing is shutdown
inline volatile bool shutdown{false};

// Shared queues for submitting requests to the seastar thread
inline std::queue<BeginTxnRequest> beginTxQ;
inline std::queue<EndTxnRequest> endTxQ;
inline std::queue<SchemaGetRequest> schemaGetTxQ;
inline std::queue<SchemaCreateRequest> schemaCreateTxQ;
inline std::queue<CollectionCreateRequest> collectionCreateTxQ;
inline std::queue<CollectionDropRequest> collectionDropTxQ;
inline std::queue<ScanReadCreateRequest> scanReadCreateTxQ;
inline std::queue<ScanReadRequest> scanReadTxQ;
inline std::queue<ReadRequest> readTxQ;
inline std::queue<WriteRequest> writeTxQ;
inline std::queue<UpdateRequest> updateTxQ;

// Helper function used to push an item onto a request queue safely.
template <typename Q, typename Request>
void pushQ(Q& queue, Request&& r) {
    std::lock_guard lock{requestQMutex};
    if (shutdown) {
        r.prom.set_exception(std::make_exception_ptr(std::runtime_error("queue processing has been shutdown")));
    }
    else {
        queue.push(std::forward<Request>(r));
    }
}

}  // namespace gate
}  // namespace k2pg
