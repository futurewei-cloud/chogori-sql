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
    seastar::future<> _poller = seastar::make_ready_future();
    seastar::future<> pollForWork();
    k2::K23SIClient _client;
};

}// ns k2gate
