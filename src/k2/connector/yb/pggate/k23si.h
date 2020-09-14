#pragma once
#include <k2/module/k23si/client/k23si_client.h>

#include <atomic>
#include <condition_variable>
#include <future>
#include <queue>

namespace k2gate {

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
private:
    void poller();
    bool _stopPoller = false;

    // this polls for completed requests across all of the response queues.
    std::future<void> pollerFut = std::async([this]{poller();});
};  // class K23SIGate


struct BeginTxnQTx{
    k2::K2TxnOptions opts;
    std::promise<K23SITxn> prom;
};
struct BeginTxnQRx {
    TxnId txnid;
    std::promise<K23SITxn> prom;
};

struct EndTxnQTx {
    TxnId txnid;
    bool shouldCommit;
    std::promise<k2::EndResult> prom;
};
struct EndTxnQRx {
    k2::EndResult result;
    std::promise<k2::EndResult> prom;
};

struct ReadQTx {
    TxnId txnid;
    k2::SKVRecord record;
    std::promise<k2::ReadResult<k2::SKVRecord>> prom;
};
struct ReadQRx {
    k2::ReadResult<k2::dto::SKVRecord> result;
    std::promise<k2::ReadResult<k2::dto::SKVRecord>> prom;
};

struct WriteQTx {
    TxnId txnid;
    bool erase;
    k2::SKVRecord record;
    std::promise<k2::WriteResult> prom;
};
struct WriteQRx {
    k2::WriteResult result;
    std::promise<k2::WriteResult> prom;
};

// Lock-free mutex
class LFMutex {
private:
    std::atomic_flag _flag = ATOMIC_FLAG_INIT;

public:
    void lock() {
        while (_flag.test_and_set(std::memory_order_acquire))
            ;
    }
    void unlock() {
        _flag.clear(std::memory_order_release);
    }
};

//// Shared queues

// mutex for locking outgoing request queues
extern LFMutex requestQMutex;
extern std::queue<BeginTxnQTx> beginTxQ;
extern std::queue<EndTxnQTx> endTxQ;
extern std::queue<ReadQTx> readTxQ;
extern std::queue<WriteQTx> writeTxQ;
extern std::atomic<bool> pg_ready;

// mutex for locking incoming result queue and the conditional variable
extern LFMutex resultQMutex;
// condvar used by the 3si lib to signal that results are ready for pickup
extern std::condition_variable_any resultReady;
extern std::queue<BeginTxnQRx> beginRxQ;
extern std::queue<EndTxnQRx> endRxQ;
extern std::queue<ReadQRx> readRxQ;
extern std::queue<WriteQRx> writeRxQ;

}  // namespace k2gate
