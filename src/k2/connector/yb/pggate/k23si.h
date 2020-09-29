#pragma once
#include <k2/module/k23si/client/k23si_client.h>

#include <atomic>
#include <condition_variable>
#include <future>
#include <queue>

namespace k2pggate {

struct TxnId{
    uint64_t id;
};

class K23SITxn {
public:
    K23SITxn(TxnId id);
    std::future<k2::ReadResult<k2::SKVRecord>> read(k2::dto::SKVRecord&& rec);
    std::future<k2::WriteResult> write(k2::dto::SKVRecord&& rec, bool erase = false);
    std::future<k2::EndResult> endTxn(bool shouldCommit);

private:
    TxnId _id;
 };  // class K23SITxn

class K23SIGate {
public:
    K23SIGate();
    std::future<K23SITxn> beginTxn(const k2::K2TxnOptions& txnOpts);
    std::future<k2::Schema> getSchema(const k2::String& collectionName, const k2::String& schemaName, uint64_t schemaVersion);
};  // class K23SIGate


struct BeginTxnRequest{
    k2::K2TxnOptions opts;
    std::promise<K23SITxn> prom;
};

struct EndTxnRequest {
    TxnId txnid;
    bool shouldCommit;
    std::promise<k2::EndResult> prom;
};

struct SchemaRequest {
    k2::String collectionName;
    k2::String schemaName;
    uint64_t schemaVersion;
    std::promise<k2::Schema> prom;
};

struct QueryRequest {
    TxnId txnid;
    std::promise<void*> prom;
};

struct ReadRequest {
    TxnId txnid;
    k2::SKVRecord record;
    std::promise<k2::ReadResult<k2::SKVRecord>> prom;
};

struct WriteRequest {
    TxnId txnid;
    bool erase=false;
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

//// Shared queues

// mutex for locking outgoing request queues
extern LFMutex requestQMutex;
extern std::queue<BeginTxnRequest> beginTxQ;
extern std::queue<EndTxnRequest> endTxQ;
extern std::queue<SchemaRequest> schemaTxQ;
extern std::queue<QueryRequest> queryTxQ;
extern std::queue<ReadRequest> readTxQ;
extern std::queue<WriteRequest> writeTxQ;

}  // namespace k2pggate
