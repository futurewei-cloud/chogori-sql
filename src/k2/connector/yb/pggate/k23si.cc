#include "k23si.h"
#include <k2/dto/Collection.h>
namespace k2gate {
using namespace k2;

LFMutex requestQMutex;
std::queue<BeginTxnQTx> beginTxQ;
std::queue<EndTxnQTx> endTxQ;
std::queue<ReadQTx> readTxQ;
std::queue<WriteQTx> writeTxQ;
std::atomic<bool> pg_ready{false};

LFMutex resultQMutex;
std::condition_variable_any resultReady;
std::queue<BeginTxnQRx> beginRxQ;
std::queue<EndTxnQRx> endRxQ;
std::queue<ReadQRx> readRxQ;
std::queue<WriteQRx> writeRxQ;

K23SITxn::K23SITxn(TxnId id):_id(id){}

std::future<EndResult> K23SITxn::endTxn(bool shouldCommit) {
    EndTxnQTx qr{.txnid=_id, .shouldCommit = shouldCommit, .prom={}};

    auto result = qr.prom.get_future();
    std::lock_guard lock{requestQMutex};
    endTxQ.push(std::move(qr));
    return result;
}

std::future<ReadResult<dto::SKVRecord>> K23SITxn::read(dto::SKVRecord&& rec) {
    ReadQTx qr { .txnid = _id, .record=std::move(rec), .prom={}};

    auto result = qr.prom.get_future();
    std::lock_guard lock{requestQMutex};
    readTxQ.push(std::move(qr));
    return result;
}

std::future<WriteResult> K23SITxn::write(dto::SKVRecord&& rec, bool erase) {
    WriteQTx qr { .txnid = _id, .erase=erase, .record=std::move(rec), .prom={}};

    auto result = qr.prom.get_future();
    std::lock_guard lock{requestQMutex};
    writeTxQ.push(std::move(qr));
    return result;
}

K23SIGate::K23SIGate() {
    // the gate has been constructed and we can notify the k2 side to connect to the queues
    pg_ready.store(true);
}

std::future<K23SITxn> K23SIGate::beginTxn(const K2TxnOptions& txnOpts) {
    BeginTxnQTx qr{.opts=txnOpts, .prom={}};

    auto result = qr.prom.get_future();
    std::lock_guard lock{requestQMutex};
    beginTxQ.push(std::move(qr));
    return result;
}

void K23SIGate::poller() {
    while(!_stopPoller) {
        std::unique_lock lock{resultQMutex};
        resultReady.wait(lock, [this](){return _stopPoller;});

        while (!beginRxQ.empty()) {
            auto& resp = beginRxQ.front();
            resp.prom.set_value(K23SITxn{std::move(resp.txnid)});
            beginRxQ.pop();
        }
        while (!endRxQ.empty()) {
            auto& resp = endRxQ.front();
            resp.prom.set_value(std::move(resp.result));
            endRxQ.pop();
        }
        while (!readRxQ.empty()) {
            auto& resp = readRxQ.front();
            resp.prom.set_value(std::move(resp.result));
            readRxQ.pop();
        }
        while (!writeRxQ.empty()) {
            auto& resp = writeRxQ.front();
            resp.prom.set_value(std::move(resp.result));
            writeRxQ.pop();
        }
    }
}

} // ns k2gate
