#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/tso/client/tso_clientlib.h>

#include "postmaster/postmaster_hook.h"
#include "yb/pggate/k23si_seastar_app.h"

extern "C" {

// this function initializes the K2 client library and hooks it up with the k2 pg connector
void startK2App(int argc, char** argv) {
    std::thread k2thread([argc, argv] {
        k2::App app("PG");
        app.addApplet<k2::TSO_ClientLib>(0s);
        app.addApplet<k2gate::PGK2Client>();
        app.addOptions()
        // config for dependencies
        ("partition_request_timeout", bpo::value<k2::ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo_request_timeout", bpo::value<k2::ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<k2::ParseableDuration>(), "CPO request backoff");
        app.start(argc, argv);
    });
	//TODO setup a notification callback back in PG so that we can get called on shutdown in order to stop the thread
}

int PostgresServerProcessMain(int argc, char** argv);
}


int main(int argc, char** argv) {
	// setup the k2 hook in pg backend so that we can initialize k2 when PG forks to handle a new client
	k2_init_func = startK2App;
	return PostgresServerProcessMain(argc, argv);
}
