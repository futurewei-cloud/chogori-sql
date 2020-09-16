#include "k23si.h"
#include <k2/dto/Collection.h>
namespace k2gate {
using namespace k2;

template <typename Q, typename Request>
void pushQ(Q& queue, Request&& r) {
    std::lock_guard lock{requestQMutex};
    queue.push(std::forward<Request>(r));
}

LFMutex requestQMutex;
std::queue<BeginTxnRequest> beginTxQ;
std::queue<SchemaRequest> schemaTxQ;
std::queue<EndTxnRequest> endTxQ;
std::queue<ReadRequest> readTxQ;
std::queue<QueryRequest> queryTxQ;
std::queue<WriteRequest> writeTxQ;

K23SITxn::K23SITxn(TxnId id):_id(id){}

std::future<EndResult> K23SITxn::endTxn(bool shouldCommit) {
    EndTxnRequest qr{.txnid=_id, .shouldCommit = shouldCommit, .prom={}};

    auto result = qr.prom.get_future();
    pushQ(endTxQ, std::move(qr));
    return result;
}

std::future<ReadResult<dto::SKVRecord>> K23SITxn::read(dto::SKVRecord&& rec) {
    ReadRequest qr { .txnid = _id, .record=std::move(rec), .prom={}};

    auto result = qr.prom.get_future();
    pushQ(readTxQ, std::move(qr));
    return result;
}

std::future<WriteResult> K23SITxn::write(dto::SKVRecord&& rec, bool erase) {
    WriteRequest qr { .txnid = _id, .erase=erase, .record=std::move(rec), .prom={}};

    auto result = qr.prom.get_future();
    pushQ(writeTxQ, std::move(qr));
    return result;
}

K23SIGate::K23SIGate() {
}

std::future<K23SITxn> K23SIGate::beginTxn(const K2TxnOptions& txnOpts) {
    BeginTxnRequest qr{.opts=txnOpts, .prom={}};

    auto result = qr.prom.get_future();
    pushQ(beginTxQ, std::move(qr));
    return result;
}

std::future<k2::Schema> K23SIGate::getSchema(const k2::String& collectionName, const k2::String& schemaName, uint64_t schemaVersion) {
    SchemaRequest qr{.collectionName = collectionName, .schemaName = schemaName, .schemaVersion = schemaVersion, .prom={}};

    auto result = qr.prom.get_future();
    pushQ(schemaTxQ, std::move(qr));
    return result;
}

} // ns k2gate
