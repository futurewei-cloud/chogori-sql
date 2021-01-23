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

#include "k23si_seastar_app.h"
#include "k23si_queue_defs.h"
#include "k23si_txn.h"
#include <k2/module/k23si/client/k23si_client.h>

namespace k2pg {
namespace gate {

PGK2Client::PGK2Client() {
    K2LOG_I(log::k2ss, "Ctor");
    _client = new k2::K23SIClient(k2::K23SIClientConfig());
    _txns = new std::unordered_map<k2::dto::K23SI_MTR, k2::K2TxnHandle>();
}

PGK2Client::~PGK2Client() {
    delete _client;
    delete _txns;
}

seastar::future<> PGK2Client::gracefulStop() {
    K2LOG_I(log::k2ss, "Stopping");
    _stop = true;
    return std::move(_poller).then([this] () { return _client->gracefulStop(); });
}

seastar::future<> PGK2Client::start() {
    K2LOG_I(log::k2ss, "Starting");
    // start polling the request queues only on core 0
    if (seastar::engine().cpu_id() == 0) {
        K2LOG_I(log::k2ss, "Poller starting");
        _poller = _poller.then([this] {
            return seastar::do_until(
                [this] {
                    return _stop;
                },
                [this] {
                    return _pollForWork();
                }
            );
        });
    }
    return _client->start();
}

// Helper function used to poll a given queue. The given Func visitor is called with each element
// pulled off the queue in sequence
template <typename Q, typename Func>
seastar::future<> pollQ(Q& q, Func&& visitor) {
    // lock the mutex before manipulating the queue
    std::unique_lock lock(requestQMutex);

    std::vector<seastar::future<>> futs;
    futs.reserve(q.size());

    while (!q.empty()) {
        K2LOG_V(log::k2ss, "Found op in queue");
        futs.push_back(
            seastar::do_with(std::move(q.front()), std::forward<Func>(visitor), [](auto& req, auto& visitor) {
                try {
                    return visitor(req)
                        .handle_exception([&req](auto exc) {
                            K2LOG_W_EXC(log::k2ss, exc, "caught exception");
                            req.prom.set_exception(exc);
                        });
                }
                catch (const std::exception& exc) {
                    K2LOG_W(log::k2ss, "Caught exception during poll of {}: {}", typeid(Q).name(), exc.what());
                    req.prom.set_exception(std::current_exception());
                    return seastar::make_ready_future();
                }
                catch (...) {
                    K2LOG_W(log::k2ss, "Caught unknown exception during poll of {}", typeid(Q).name());
                    req.prom.set_exception(std::current_exception());
                    return seastar::make_ready_future();
                }
            }));
        q.pop();
    }
    return seastar::when_all_succeed(futs.begin(), futs.end());
}

seastar::future<> PGK2Client::_pollBeginQ() {
    return pollQ(beginTxQ, [this](auto& req) {
        K2LOG_D(log::k2ss, "Begin txn...");

        return _client->beginTxn(req.opts)
            .then([this, &req](auto&& txn) {
                K2LOG_D(log::k2ss, "txn: {}", txn.mtr());
                req.prom.set_value(K23SITxn(txn.mtr()));  // send a copy to the promise

                (*_txns)[txn.mtr()] = std::move(txn);
            });
    });
}

seastar::future<> PGK2Client::_pollEndQ() {
    return pollQ(endTxQ, [this](auto& req) {
        K2LOG_D(log::k2ss, "End txn...");
        auto fiter = _txns->find(req.mtr);
        if (fiter == _txns->end()) {
            K2LOG_W(log::k2ss, "invalid txn id: {}", req.mtr);
            req.prom.set_value(k2::EndResult(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id")));
            return seastar::make_ready_future();
        }
        K2LOG_D(log::k2ss, "Ending txn: {}, with commit={}", req.mtr, req.shouldCommit);
        return fiter->second.end(req.shouldCommit)
            .then([this, &req](auto&& endResult) {
                K2LOG_D(log::k2ss, "Ended txn: {}, with result: {}", req.mtr, endResult);
                _txns->erase(req.mtr);
                req.prom.set_value(std::move(endResult));
            });
    });
}

seastar::future<> PGK2Client::_pollSchemaGetQ() {
    return pollQ(schemaGetTxQ, [this](auto& req) {
        K2LOG_D(log::k2ss, "Schema get {}", req);
        return _client->getSchema(req.collectionName, req.schemaName, req.schemaVersion)
            .then([this, &req](auto&& result) {
                K2LOG_D(log::k2ss, "Schema get received {}", result);
                req.prom.set_value(std::move(result));
            });
    });
}

seastar::future<> PGK2Client::_pollSchemaCreateQ() {
    return pollQ(schemaCreateTxQ, [this](auto& req) {
        K2LOG_D(log::k2ss, "Schema create... {}", req);
        return _client->createSchema(req.collectionName, req.schema)
            .then([this, &req](auto&& result) {
                K2LOG_D(log::k2ss, "Schema create received {}", result);
                req.prom.set_value(std::move(result));
            });
    });
}

seastar::future<> PGK2Client::_pollCreateCollectionQ() {
    return pollQ(collectionCreateTxQ, [this](auto& req) {
        K2LOG_D(log::k2ss, "Collection create... {}", req);
        return _client->makeCollection(std::move(req.ccr.metadata), std::move(req.ccr.clusterEndpoints), std::move(req.ccr.rangeEnds))
            .then([this, &req](auto&& result) {
                K2LOG_D(log::k2ss, "Collection create received {}", result);
                req.prom.set_value(std::move(result));
            });
    });
}

seastar::future<> PGK2Client::_pollReadQ() {
    return pollQ(readTxQ, [this](auto& req) mutable {
        K2LOG_D(log::k2ss, "Read... {}", req);
        auto fiter = _txns->find(req.mtr);
        if (fiter == _txns->end()) {
            K2LOG_W(log::k2ss, "invalid txn id: {}", req.mtr);
            req.prom.set_value(k2::ReadResult<k2::dto::SKVRecord>(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id"), k2::dto::SKVRecord()));
            return seastar::make_ready_future();
        }

        if (!req.key.partitionKey.empty()) {
            return fiter->second.read(std::move(req.key), std::move(req.collectionName))
            .then([this, &req](auto&& readResult) {
                K2LOG_D(log::k2ss, "Key Read received: {}", readResult);
                req.prom.set_value(std::move(readResult));
            });
        }
        return fiter->second.read(std::move(req.record))
            .then([this, &req](auto&& readResult) {
                K2LOG_D(log::k2ss, "Read received: {}", readResult);
                req.prom.set_value(std::move(readResult));
            });
    });
}

seastar::future<> PGK2Client::_pollCreateScanReadQ() {
    return pollQ(scanReadCreateTxQ, [this](auto& req) {
        K2LOG_D(log::k2ss, "Create scan... {}", req);
        return _client->createQuery(req.collectionName, req.schemaName)
            .then([this, &req](auto&& result) {
                K2LOG_D(log::k2ss, "Created scan... {}", result);
                CreateScanReadResult response {
                    .status = std::move(result.status),
                    .query = std::make_shared<k2::Query>(std::move(result.query))
                };
                req.prom.set_value(std::move(response));
            });
    });
}

seastar::future<> PGK2Client::_pollScanReadQ() {
    return pollQ(scanReadTxQ, [this](auto& req) mutable {
        K2LOG_D(log::k2ss, "Scan... {}", req);
        auto fiter = _txns->find(req.mtr);
        if (fiter == _txns->end()) {
            K2LOG_W(log::k2ss, "invalid txn id: {}", req.mtr);
            req.prom.set_value(k2::QueryResult(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id")));
            return seastar::make_ready_future();
        }
        return fiter->second.query(*req.query)
            .then([this, &req](auto&& queryResult) {
                K2LOG_D(log::k2ss, "Scanned... {}", queryResult);
                req.prom.set_value(std::move(queryResult));
            });
    });
}

seastar::future<> PGK2Client::_pollWriteQ() {
    return pollQ(writeTxQ, [this](auto& req) mutable {
        K2LOG_D(log::k2ss, "Write... {}", req);
        auto fiter = _txns->find(req.mtr);
        if (fiter == _txns->end()) {
            K2LOG_W(log::k2ss, "invalid txn id: {}", req.mtr);
            req.prom.set_value(k2::WriteResult(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id"), k2::dto::K23SIWriteResponse{}));
            return seastar::make_ready_future();
        }
        return fiter->second.write(req.record, req.erase, req.rejectIfExists)
            .then([this, &req](auto&& writeResult) {
                K2LOG_D(log::k2ss, "Written... {}", writeResult);
                req.prom.set_value(std::move(writeResult));
            });
    });
}

seastar::future<> PGK2Client::_pollUpdateQ() {
    return pollQ(updateTxQ, [this](auto& req) mutable {
        K2LOG_D(log::k2ss, "Update... {}", req);
        auto fiter = _txns->find(req.mtr);
        if (fiter == _txns->end()) {
            K2LOG_W(log::k2ss, "invalid txn id: {}", req.mtr);
            req.prom.set_value(k2::PartialUpdateResult(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id")));
            return seastar::make_ready_future();
        }
        return fiter->second.partialUpdate(req.record, std::move(req.fieldsForUpdate), std::move(req.key))
            .then([this, &req](auto&& updateResult) {
                K2LOG_D(log::k2ss, "Updated... {}", updateResult);
                req.prom.set_value(std::move(updateResult));
            });
    });
}

seastar::future<> PGK2Client::_pollForWork() {
    return seastar::when_all_succeed(
        _pollBeginQ(), _pollEndQ(), _pollSchemaGetQ(), _pollSchemaCreateQ(), _pollScanReadQ(), _pollReadQ(), _pollWriteQ(), _pollCreateScanReadQ(), _pollUpdateQ(),
        _pollCreateCollectionQ());  // TODO: collection creation is rare, maybe consider some optimization later on to pull on demand only.
}

}  // namespace gate
}  // namespace k2pg
