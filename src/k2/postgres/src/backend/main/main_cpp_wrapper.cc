#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/common/Log.h>
#include <k2/tso/client/tso_clientlib.h>

#include <string>
#include <thread>

#include "postmaster/postmaster_hook.h"
#include "yb/pggate/k23si_seastar_app.h"
#include "yb/pggate/k2_config.h"
#include "yb/pggate/k2_session_metrics.h"
#include "yb/pggate/k2_log_init.h"

namespace k2pg::log {
inline thread_local k2::logging::Logger main("k2::pg_main");
}

extern "C" {
#include <microhttpd.h>
#include <prom.h>
#include <promhttp.h>

struct MHD_Daemon* prom_daemon;
promhttp_push_handle_t* prom_pusher;

std::thread k2thread;

static bool inited = false;

static void
killK2App(int, unsigned long) {
    if (k2thread.joinable()) {
        pthread_kill(k2thread.native_handle(), SIGINT);
        k2thread.join();
    }

    sleep(10); // sleep in order to allow for metrics to be collected by prometheus before we shutdown
    prom_collector_registry_destroy(PROM_COLLECTOR_REGISTRY_DEFAULT);
    if (prom_daemon) MHD_stop_daemon(prom_daemon);
    if (prom_pusher) promhttp_stop_push_metrics(prom_pusher);
    k2pg::session::stop();
}

const std::string& getHostName() {
    static const long max_hostname = sysconf(_SC_HOST_NAME_MAX);
    static const long size = (max_hostname > 255) ? max_hostname + 1 : 256;
    static std::string hostname(size, '\0');
    if (hostname[0] == '\0') {
        ::gethostname(hostname.data(), size - 1);
    }

    return hostname;
}

// this function initializes the K2 client library and hooks it up with the k2 pg connector
void startK2App(int argc, char** argv) {
    const int MAX_K2_ARGS = 128;

    if (inited) {
        K2LOG_I(k2pg::log::main, "Skipping creating PG-K2 thread because it was already created");
        return;
    }
    // set the default loglevel for this thread, just in case one isn't specified by cmd line
    k2::logging::Logger::threadLocalLogLevel = k2::logging::LogLevel::INFO;

    k2pg::gate::Config conf;
    int promport = conf()["prometheus_port"];
    int prometheus_push_interval_ms = conf()["prometheus_push_interval_ms"];
    // make these static so that it sticks around while the thread is working on it
    static std::string prometheus_address = conf()["prometheus_push_address"];
    static std::string prometheus_push_url;

    prom_collector_registry_default_init();
    promhttp_set_active_collector_registry(NULL);
    k2pg::session::start();

    if (promport >=0) {
        K2LOG_I(k2pg::log::main, "Creating prometheus thread on port: {}", promport);
        prom_daemon = promhttp_start_daemon(MHD_USE_INTERNAL_POLLING_THREAD, promport, NULL, NULL);
        K2ASSERT(k2pg::log::main, prom_daemon != 0, "Unable to create prometheus thread");
    }

    if (prometheus_address.size() > 0) {
        prometheus_push_url = "http://" + prometheus_address + "/metrics/job/k2pg_gate/instance/" + getHostName() + ":" + std::to_string(::getpid());
        K2LOG_I(k2pg::log::main, "Creating prometheus push thread to url: {}, pushing every {}ms", prometheus_push_url, prometheus_push_interval_ms);
        prom_pusher = promhttp_start_push_metrics(prometheus_push_url.c_str(), prometheus_push_interval_ms);
        K2ASSERT(k2pg::log::main, prom_pusher != NULL, "Unable to create metrics pusher");
    }
    K2LOG_I(k2pg::log::main, "Creating PG-K2 thread");
    inited = true;

    // in order to pass the args to the seastar thread, we need to copy them into a new array as their storage will
    // disappear.
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

    // process all command line arguments
    for (int i = 0; i < argc; ++i) {
        std::string el(argv[i]);
        if (isLogLevel) {
            // the previous argument was "--log_level". That means that currently we're looking at the
            // log levels string. This would be one string, e.g. "DEBUG k2::pg=INFO k2::transport=INFO"
            isLogLevel = false; // done with log level processing

            // process this single string into individual args by providing a callback to be called on each arg
            k2pg::processLogLevelArg(el, addArg);
        }
        else {
            addArg(el);
        }

        if (el == "--log_level") isLogLevel = true; // process log level args next time
    }

    // setup metrics args
    if (prometheus_address.size() > 0) {
        addArg("--prometheus_push_address");
        addArg(prometheus_address);
        addArg("--prometheus_port");
        addArg("0"); // listen on random port for prometheus to avoid conflicts
        addArg("--prometheus_push_interval");
        addArg(std::to_string(prometheus_push_interval_ms/1000) + "s");
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
