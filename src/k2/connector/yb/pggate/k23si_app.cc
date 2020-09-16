#include "k23si_app.h"
#include "k23si.h"

namespace k2gate {

PGK2Client::PGK2Client():
    _client(k2::K23SIClientConfig()) {
}

seastar::future<> PGK2Client::gracefulStop() {
    return std::move(_poller);
}

seastar::future<> PGK2Client::start() {
    // start polling the request queues only on core 0
    if (seastar::engine().cpu_id() == 0) {
        _poller = _poller.then([this] {
            return seastar::do_until(
                [] {
                    return false; // TODO break out of poller pool if asked to exit
                },
                [this] {
                    return _pollForWork();
                }
            );
        });
    }
    return seastar::make_ready_future();
}

template <typename Q, typename Func>
seastar::future<> pollQ(Q& q, Func&& func) {
    std::vector<seastar::future<>> futs;
    futs.reserve(q.size());

    while (!q.empty()) {
        futs.push_back(
            seastar::do_with(std::move(q.front()), std::forward<Func>(func), [](auto& req, auto& func) {
                return func(req)
                    .handle_exception([&req](auto exc) {
                        req.prom.set_exception(exc);
                    });
            }));
        q.pop();
    }
    return seastar::when_all_succeed(futs.begin(), futs.end());
}

seastar::future<> PGK2Client::_pollBeginQ() {
    return pollQ(beginTxQ, [this] (auto& req) {
        return _client.beginTxn(req.opts)
            .then([this, &req](auto&& txn) {
                _txns[_txnidCounter] = std::move(txn);
                req.prom.set_value(K23SITxn(TxnId{_txnidCounter}));
                _txnidCounter++;
            });
    });
}

seastar::future<> PGK2Client::_pollEndQ() {
    return pollQ(endTxQ, [this](auto& req) {
        auto fiter = _txns.find(req.txnid.id);
        if (fiter == _txns.end()) {
            req.prom.set_value(k2::EndResult(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id")));
            return seastar::make_ready_future();
        }

        return fiter->second.end(req.shouldCommit)
            .then([this, &req](auto&& endResult) {
                _txns.erase(req.txnid.id);
                req.prom.set_value(std::move(endResult));
            });
    });
}

seastar::future<> PGK2Client::_pollSchemaQ() {
    return pollQ(schemaTxQ, [this](auto& req) {
        return _client.getSchema(req.collectionName, req.schemaName, req.schemaVersion)
            .then([this, &req](auto&& schema) {
                req.prom.set_value(std::move(schema));
            });
    });
}

seastar::future<> PGK2Client::_pollReadQ() {
    return pollQ(readTxQ, [this](auto& req) mutable {
        auto fiter = _txns.find(req.txnid.id);
        if (fiter == _txns.end()) {
            req.prom.set_value(k2::ReadResult<k2::dto::SKVRecord>(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id"), k2::dto::SKVRecord()));
            return seastar::make_ready_future();
        }
        return seastar::make_ready_future();
        /*
        pending fix in the client library to correctly construct an SKVRecord
        return fiter->second.read(std::move(req.record))
            .then([this, &req](auto&& readResult) {
                req.prom.set_value(std::move(readResult));
            });
        */
    });
}

seastar::future<> PGK2Client::_pollQueryQ() {
    return seastar::make_ready_future();
}

seastar::future<> PGK2Client::_pollWriteQ() {
    return pollQ(writeTxQ, [this](auto& req) mutable {
        auto fiter = _txns.find(req.txnid.id);
        if (fiter == _txns.end()) {
            req.prom.set_value(k2::WriteResult(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id"), k2::dto::K23SIWriteResponse{}));
            return seastar::make_ready_future();
        }
        return fiter->second.write(req.record, req.erase)
            .then([this, &req](auto&& writeResult) {
                req.prom.set_value(std::move(writeResult));
            });
    });
}

seastar::future<> PGK2Client::_pollForWork() {
    std::unique_lock lock(requestQMutex);

    return seastar::when_all_succeed(
        _pollBeginQ(), _pollEndQ(), _pollSchemaQ(), _pollQueryQ(), _pollReadQ(), _pollWriteQ());
}

}  // namespace k2gate
