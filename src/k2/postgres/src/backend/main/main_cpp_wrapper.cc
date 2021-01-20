#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/common/Log.h>

#include "postmaster/postmaster_hook.h"
#include "yb/pggate/k23si_seastar_app.h"
#include "yb/pggate/k2_log_init.h"
#include <k2/tso/client/tso_clientlib.h>

#include <string>
#include <thread>

namespace k2pg::log {
inline thread_local k2::logging::Logger main("k2::pg_main");
}

extern "C" {
std::thread k2thread;

static bool inited = false;

static void
killK2App(int, unsigned long) {
    pthread_kill(k2thread.native_handle(), SIGINT);
    k2thread.join();
}


// this function initializes the K2 client library and hooks it up with the k2 pg connector
void startK2App(int argc, char** argv) {
	const int MAX_K2_ARGS = 128;

    if (inited) {
        K2LOG_I(k2pg::log::main, "Skipping creating PG-K2 thread because it was already created");
        return;
    }

    K2LOG_I(k2pg::log::main, "Creating PG-K2 thread");
    inited = true;

    char** argvN = (char**)malloc(MAX_K2_ARGS*sizeof(char*));
    assert(argvN != NULL);
    bool isLogLevel = false;
    int curarg = 0;
    auto addArg = [argvN, &curarg](const std::string& sarg) {
        argvN[curarg] = (char*)malloc(sizeof(char)*(sarg.size() + 1));
        assert(argvN[curarg] != NULL);
        strncpy(argvN[curarg], sarg.c_str(), sarg.size());
        argvN[curarg][sarg.size()] = '\0';
        curarg++;
    };
    // set loglevel for this thread;
    k2::logging::Logger::threadLocalLogLevel = k2::logging::LogLevel::INFO;

    for (int i = 0; i < argc; ++i) {
        std::string el(argv[i]);
        if (isLogLevel) {
            isLogLevel = false; // done with log level processing
            k2pg::processLogLevelArg(el, addArg);
        }
        else {
            addArg(el);
        }

        if (el == "--log_level") isLogLevel = true; // process log level args next time
    }
    k2thread = std::thread([curarg, argvN]() mutable {
        try {
            K2LOG_I(k2pg::log::main, "Configure app");
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
            K2LOG_I(k2pg::log::main, "Starting app");
            auto code = app.start(curarg, argvN);
            K2LOG_I(k2pg::log::main, "App ended with code: {}", code);
        }
        catch(...) {
            K2LOG_E(k2pg::log::main, "caught exception in main thread")
            for (int i = 0; i < curarg; ++i) {
                free(argvN[i]);
            }
            free(argvN);
        }
    });

    K2LOG_I(k2pg::log::main, "PG-K2 thread created");
}

int PostgresServerProcessMain(int argc, char** argv);
}


int main(int argc, char** argv) {
	// setup the k2 hook in pg backend so that we can initialize k2 when PG forks to handle a new client
	k2_init_func = startK2App;
    k2_kill_func = killK2App;
	auto code= PostgresServerProcessMain(argc, argv);
    K2LOG_I(k2pg::log::main, "PG process exiting...")
    return code;
}
