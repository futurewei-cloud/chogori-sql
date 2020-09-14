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
    return seastar::do_until(
        [] {
            // keep looping here (ss style) until pg is ready
            return pg_ready.load();
        },
        [] {
            return seastar::sleep(0ms).discard_result();
        }
    ).then([this]{
        // start polling the request queues only on core 0
        if (seastar::engine().cpu_id() == 0) {
            _poller = _poller.then([this] {
                return seastar::do_until(
                    [] {
                        return false; // TODO break out of poller pool if asked to exit
                    },
                    [this] {
                        return pollForWork();
                    }
                );
            });
        }
    });
}

seastar::future<> PGK2Client::pollForWork() {
    bool haveSome = false;
    seastar::future<> response = seastar::make_ready_future();
    {
        std::lock_guard lock{requestQMutex};
        while (!beginTxQ.empty()) {
            auto& resp = beginTxQ.front();
            resp.prom.set_exception(std::make_exception_ptr(std::runtime_error("not implemented")));
            beginTxQ.pop();
        }
        while (!endTxQ.empty()) {
            auto& resp = endTxQ.front();
            resp.prom.set_exception(std::make_exception_ptr(std::runtime_error("not implemented")));
            endTxQ.pop();
        }
        while (!readTxQ.empty()) {
            auto& resp = readTxQ.front();
            resp.prom.set_exception(std::make_exception_ptr(std::runtime_error("not implemented")));
            readTxQ.pop();
        }
        while (!writeTxQ.empty()) {
            auto& resp = writeTxQ.front();
            resp.prom.set_exception(std::make_exception_ptr(std::runtime_error("not implemented")));
            writeTxQ.pop();
        }
    }

    if (haveSome) {
        std::unique_lock lock{resultQMutex};
        resultReady.notify_one();
    }
    return seastar::make_ready_future();
}

}  // namespace k2gate
