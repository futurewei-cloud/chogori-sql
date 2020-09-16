#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/module/k23si/client/k23si_client.h>

namespace k2gate {

class PGK2Client {
public:
    PGK2Client();
    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();
private:
    k2::K23SIClient _client;
    uint64_t _txnidCounter;
    std::unordered_map<uint64_t, k2::K2TxnHandle> _txns;

    seastar::future<> _poller = seastar::make_ready_future();
    seastar::future<> _pollForWork();

    seastar::future<> _pollBeginQ();
    seastar::future<> _pollEndQ();
    seastar::future<> _pollSchemaQ();
    seastar::future<> _pollReadQ();
    seastar::future<> _pollQueryQ();
    seastar::future<> _pollWriteQ();
};

}// ns k2gate
