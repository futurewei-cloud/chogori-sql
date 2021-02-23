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

#include "k2_config.h"
#include "k23si_seastar_app.h"
#include "k23si_queue_defs.h"
#include "k23si_txn.h"
#include <k2/module/k23si/client/k23si_client.h>

namespace k2pg {
namespace gate {

PGK2Client::PGK2Client() {
    K2LOG_I(log::k2ss, "Ctor");
    _client = std::make_unique<k2::K23SIClient>(k2::K23SIClient(k2::K23SIClientConfig()));
    Config conf;
    // Sync finalize is used to make the heavy load of initDB and TPCC load phase more
    // reliable especially with RDMA, so concurrentWrites is tied to this value
    _concurrentWrites = !(conf()["force_sync_finalize"]);
}

seastar::future<> PGK2Client::gracefulStop() {
    K2LOG_I(log::k2ss, "Stopping");
    _stop = true;
    return std::move(_poller).then([this] () { return _client->gracefulStop(); });
}

seastar::future<> PGK2Client::start() {
    K2LOG_I(log::k2ss, "Starting");
    // start polling the request queues only on core 0
    if (seastar::this_shard_id() == 0) {
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
                auto mtr = txn.mtr();
                _txns[txn.mtr()] = std::move(txn);
                _activeWrites.insert({mtr, seastar::make_ready_future()});
                req.prom.set_value(K23SITxn(mtr));  // send a copy to the promise
            });
    });
}

seastar::future<> PGK2Client::_pollEndQ() {
    return pollQ(endTxQ, [this](auto& req) {
        K2LOG_D(log::k2ss, "End txn...");
        auto fiter = _txns.find(req.mtr);
        if (fiter == _txns.end()) {
            K2LOG_W(log::k2ss, "invalid txn id: {}", req.mtr);
            req.prom.set_value(k2::EndResult(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id")));
            return seastar::make_ready_future();
        }

        K2LOG_D(log::k2ss, "Ending txn: {}, with commit={}", req.mtr, req.shouldCommit);
        seastar::future<>& active_writes = _activeWrites.find(req.mtr)->second;
        return active_writes
        .then([this, mtr=req.mtr, shouldCommit=req.shouldCommit] () {
            // We are relying on the SKV client to change the commit to an abort if there was an error
            // on one of the writes. In that case the error will be progated to the response of the end() call
            return _txns[mtr].end(shouldCommit);
        })
        .then([this, &req](auto&& endResult) {
            K2LOG_D(log::k2ss, "Ended txn: {}, with result: {}", req.mtr, endResult);
            _txns.erase(req.mtr);
            _activeWrites.erase(req.mtr);
            req.prom.set_value(std::move(endResult));
        })
        .handle_exception([this, &req](auto exc) {
            K2LOG_W_EXC(log::k2ss, exc, "caught exception");
            _txns.erase(req.mtr);
            _activeWrites.erase(req.mtr);
            req.prom.set_exception(exc);
        });
    });
}

seastar::future<> PGK2Client::_pollSchemaGetQ() {
    return pollQ(schemaGetTxQ, [this](auto& req) {
        K2LOG_D(log::k2ss, "Schema get {}", req);
        // Strings will be copied into a payload by transport so will be RDMA safe without extra copy
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
        // Parameters will be copied into a payload by transport so will be RDMA safe without extra copy
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
        return _client->makeCollection(std::move(req.ccr.metadata), std::move(req.ccr.clusterEndpoints),
                                       std::move(req.ccr.rangeEnds))
            .then([this, &req](auto&& result) {
                K2LOG_D(log::k2ss, "Collection create received {}", result);
                req.prom.set_value(std::move(result));
            });
    });
}

seastar::future<> PGK2Client::_pollReadQ() {
    return pollQ(readTxQ, [this](auto& req) mutable {
        K2LOG_D(log::k2ss, "Read... {}", req);
        auto fiter = _txns.find(req.mtr);
        if (fiter == _txns.end()) {
            K2LOG_W(log::k2ss, "invalid txn id: {}", req.mtr);
            req.prom.set_value(k2::ReadResult<k2::dto::SKVRecord>(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id"), k2::dto::SKVRecord()));
            return seastar::make_ready_future();
        }

        // Must wait for any active writes before servicing a read
        seastar::future<>& active_writes = _activeWrites.find(req.mtr)->second;
        return active_writes
        .then([this, &req] () {
            auto it = _txns.find(req.mtr);

            if (!req.key.partitionKey.empty()) {
                // Parameters will be copied into a payload by transport so will be RDMA safe without extra copy
                return it->second.read(std::move(req.key), std::move(req.collectionName))
                .then([this, &req](auto&& readResult) {
                    K2LOG_D(log::k2ss, "Key Read received: {}", readResult);
                    req.prom.set_value(std::move(readResult));
                });
            }

            // Copy SKVRecrod to make RDMA safe
            return it->second.read(req.record.deepCopy())
            .then([this, &req](auto&& readResult) {
                K2LOG_D(log::k2ss, "Read received: {}", readResult);
                req.prom.set_value(std::move(readResult));
            });
        });
    });
}

seastar::future<> PGK2Client::_pollCreateScanReadQ() {
    return pollQ(scanReadCreateTxQ, [this](auto& req) {
        K2LOG_D(log::k2ss, "Create scan... {}", req);
        // Parameters will be copied into a payload by transport so will be RDMA safe without extra copy
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
        auto fiter = _txns.find(req.mtr);
        if (fiter == _txns.end()) {
            K2LOG_W(log::k2ss, "invalid txn id: {}", req.mtr);
            req.prom.set_value(k2::QueryResult(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id")));
            return seastar::make_ready_future();
        }

        // Must wait for any active writes before servicing a scan
        seastar::future<>& active_writes = _activeWrites.find(req.mtr)->second;
        return active_writes
        .then([this, &req] () {
            req.query->copyPayloads();
            auto it = _txns.find(req.mtr);
            return it->second.query(*req.query)
            .then([this, &req](auto&& queryResult) {
                K2LOG_D(log::k2ss, "Scanned... {}", queryResult);
                req.prom.set_value(std::move(queryResult));
            });
        });
    });
}

seastar::future<> PGK2Client::_pollWriteQ() {
    return pollQ(writeTxQ, [this](auto& req) mutable {
        K2LOG_D(log::k2ss, "Write... {}", req);
        auto fiter = _txns.find(req.mtr);
        if (fiter == _txns.end()) {
            K2LOG_W(log::k2ss, "invalid txn id: {}", req.mtr);
            req.prom.set_value(k2::WriteResult(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id"), k2::dto::K23SIWriteResponse{}));
            return seastar::make_ready_future();
        }
        // Copy SKVRecord to make RDMA safe
        k2::dto::SKVRecord copy = req.record.deepCopy();
        // TODO RDMA initDB may require rejectIfExits=false
        seastar::future<>& active_writes = _activeWrites.find(req.mtr)->second;
        active_writes = active_writes
        .then([this, copy=std::move(copy), erase=req.erase, rejectFlag=req.rejectIfExists,
                                                            mtr=req.mtr, &req] () mutable {
            auto it = _txns.find(mtr);
            return it->second.write(copy, erase, rejectFlag)
            .then([this, &req](auto&& writeResult) {
                K2LOG_D(log::k2ss, "Written... {}", writeResult);
                if (!_concurrentWrites) {
                    req.prom.set_value(std::move(writeResult));
                }
            });
        });

        if (_concurrentWrites) {
            k2::WriteResult result(k2::dto::K23SIStatus::OK("Concurrent write enqueued"),
                                   k2::dto::K23SIWriteResponse());
            req.prom.set_value(std::move(result));
            return seastar::make_ready_future();
        } else {
            seastar::future<> f = std::move(active_writes);
            _activeWrites.insert_or_assign(req.mtr, seastar::make_ready_future());
            return f;
        }
    });
}

seastar::future<> PGK2Client::_pollUpdateQ() {
    return pollQ(updateTxQ, [this](auto& req) mutable {
        K2LOG_D(log::k2ss, "Update... {}", req);
        auto fiter = _txns.find(req.mtr);
        if (fiter == _txns.end()) {
            K2LOG_W(log::k2ss, "invalid txn id: {}", req.mtr);
            req.prom.set_value(k2::PartialUpdateResult(k2::dto::K23SIStatus::OperationNotAllowed("invalid txn id")));
            return seastar::make_ready_future();
        }
        // Copy SKVRecord to make RDMA safe
        k2::dto::SKVRecord copy = req.record.deepCopy();

        seastar::future<>& active_writes = _activeWrites.find(req.mtr)->second;
        active_writes = active_writes
        .then([this, copy=std::move(copy), fields=std::move(req.fieldsForUpdate),
                                           key=std::move(req.key), mtr=req.mtr, &req] () mutable {
            auto it = _txns.find(mtr);
            return it->second.partialUpdate(copy, std::move(fields), std::move(key))
            .then([this, &req](auto&& updateResult) {
                K2LOG_D(log::k2ss, "Updated... {}", updateResult);
                if (!_concurrentWrites) {
                    req.prom.set_value(std::move(updateResult));
                }
            });
        });

        if (_concurrentWrites) {
            k2::PartialUpdateResult result(k2::dto::K23SIStatus::OK("Concurrent update enqueued"));
            req.prom.set_value(std::move(result));
            return seastar::make_ready_future();
        } else {
            seastar::future<> f = std::move(active_writes);
            _activeWrites.insert_or_assign(req.mtr, seastar::make_ready_future());
            return f;
        }
    });
}

seastar::future<> PGK2Client::_pollForWork() {
    return seastar::when_all_succeed(
        _pollBeginQ(), _pollEndQ(), _pollSchemaGetQ(), _pollSchemaCreateQ(), _pollScanReadQ(), _pollReadQ(), _pollWriteQ(), _pollCreateScanReadQ(), _pollUpdateQ(),
        _pollCreateCollectionQ()).discard_result();  // TODO: collection creation is rare, maybe consider some optimization later on to pull on demand only.
}

}  // namespace gate
}  // namespace k2pg
