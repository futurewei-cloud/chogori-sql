#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/tso/client/tso_clientlib.h>

#include "postmaster/postmaster_hook.h"
#include "yb/pggate/k23si_seastar_app.h"

#include <string>
#include <thread>

extern "C" {
std::thread k2thread;

static void
killK2App(int, unsigned long) {
    pthread_kill(k2thread.native_handle(), SIGINT);
    k2thread.join();
}

// this function initializes the K2 client library and hooks it up with the k2 pg connector
void startK2App(int argc, char** argv) {
    K2INFO("Creating PG-K2 thread");

    char** argvN = (char**)malloc(argc*sizeof(char*));
    assert(argvN != NULL);
    for (int i = 0; i < argc; ++i) {
        std::string el(argv[i]);
        argvN[i] = (char*)malloc(sizeof(char)*(el.size() + 1));
        assert(argvN[i] != NULL);

        strncpy(argvN[i], el.c_str(), el.size());
        argvN[i][el.size()] = '\0';
    }
    k2thread = std::thread([argc, argvN]() mutable {
        try {
            K2INFO("Configure app");
            k2::App app("PG");
            app.addApplet<k2::TSO_ClientLib>();
            app.addApplet<k2pg::gate::PGK2Client>();
            app.addOptions()
            // config for dependencies
            ("partition_request_timeout", bpo::value<k2::ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
            ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
            ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
            ("cpo_request_timeout", bpo::value<k2::ParseableDuration>(), "CPO request timeout")
            ("cpo_request_backoff", bpo::value<k2::ParseableDuration>(), "CPO request backoff");
            K2INFO("Starting app");
            auto code = app.start(argc, argvN);
            K2INFO("App ended with code: " << code);
        }
        catch(...) {
            K2ERROR("caught exception in main thread")
            for (int i = 0; i < argc; ++i) {
                free(argvN[i]);
            }
            free(argvN);
        }
    });

    K2INFO("PG-K2 thread created");
}

int PostgresServerProcessMain(int argc, char** argv);
}


int main(int argc, char** argv) {
	// setup the k2 hook in pg backend so that we can initialize k2 when PG forks to handle a new client
	k2_init_func = startK2App;
    k2_kill_func = killK2App;
	auto code= PostgresServerProcessMain(argc, argv);
    K2INFO("PG process exiting...")
    return code;
}
