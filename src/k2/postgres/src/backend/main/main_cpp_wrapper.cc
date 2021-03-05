#include <filesystem>
#include <string>
#include <thread>

#include "postmaster/postmaster_hook.h"
#include "pggate/k2_includes.h"
#include "pggate/k2_seastar_app.h"
#include "pggate/k2_config.h"
#include "pggate/k2_log_init.h"
#include "pggate/k2_session_metrics.h"

namespace k2pg::log {
inline thread_local k2::logging::Logger main("k2::pg_main");
}

extern "C" {
#include <microhttpd.h>
#include <prom.h>
#include <promhttp.h>

namespace globals {
struct MHD_Daemon* prom_daemon = 0;
promhttp_push_handle_t* prom_pusher = 0;

std::thread k2thread;
std::unique_ptr<k2pg::gate::Config> config;

bool inited = false;
}

static void
killK2App(int, unsigned long) {
    K2LOG_I(k2pg::log::main, "shutting down K2 app");
    if (!globals::inited) {
        K2LOG_E(k2pg::log::main, "asked to shutdown but was never initialized");
        return;
    }
    if (globals::k2thread.joinable()) {
        pthread_kill(globals::k2thread.native_handle(), SIGINT);
        globals::k2thread.join();
    }

    if (globals::prom_daemon || globals::prom_pusher) {
        sleep(10); // sleep in order to allow for metrics to be collected by prometheus before we shutdown
    }
    if (globals::prom_daemon) {
        K2LOG_I(k2pg::log::main, "shutting down prometheus http server");
        MHD_stop_daemon(globals::prom_daemon);
    }
    if (globals::prom_pusher) {
        K2LOG_I(k2pg::log::main, "shutting down prometheus push thread");
        promhttp_stop_push_metrics(globals::prom_pusher);
    }

    K2LOG_I(k2pg::log::main, "cleaning up metrics");
    auto* registry = PROM_COLLECTOR_REGISTRY_DEFAULT;
    PROM_COLLECTOR_REGISTRY_DEFAULT = NULL;
    K2LOG_I(k2pg::log::main, "replacing collector registry");
    promhttp_set_active_collector_registry(NULL);
    K2LOG_I(k2pg::log::main, "deleting collector registry");
    prom_collector_registry_destroy(registry);
    K2LOG_I(k2pg::log::main, "done cleaning up");
}

const std::string& getHostName() {
    static const long max_hostname = sysconf(_SC_HOST_NAME_MAX);
    static const long size = (max_hostname > 255) ? max_hostname + 1 : 256;
    static std::string hostname(size, '\0');
    if (hostname[0] == '\0') {
        ::gethostname(hostname.data(), size - 1);
        hostname.resize(strlen(hostname.c_str()));
    }

    return hostname;
}

void initMetrics(int promport, const std::string& push_url, int push_interval_ms) {
    prom_collector_registry_default_init();
    promhttp_set_active_collector_registry(NULL);
    k2pg::session::start();

    if (promport >=0) {
        K2LOG_I(k2pg::log::main, "Creating prometheus thread on port: {}", promport);
        globals::prom_daemon = promhttp_start_daemon(MHD_USE_INTERNAL_POLLING_THREAD, promport, NULL, NULL);
        K2ASSERT(k2pg::log::main, globals::prom_daemon != 0, "Unable to create prometheus thread");
    }

    if (push_url.size() > 0) {
        globals::prom_pusher = promhttp_start_push_metrics(push_url.c_str(), push_interval_ms);
        K2ASSERT(k2pg::log::main, globals::prom_pusher != NULL, "Unable to create metrics pusher");
    }
}

// this function initializes the K2 client library and hooks it up with the k2 pg connector
void startK2App() {
    const int MAX_K2_ARGS = 128;

    if (globals::inited) {
        K2LOG_I(k2pg::log::main, "Skipping creating PG-K2 thread because it was already created");
        return;
    }
    // set the default loglevel for this thread, just in case one isn't specified by cmd line
    k2::logging::Logger::threadLocalLogLevel = k2::logging::LogLevel::INFO;
    globals::config = std::make_unique<k2pg::gate::Config>();

    int promport = globals::config->get("prometheus_port", -1);
    int prometheus_push_interval_ms = globals::config->get("prometheus_push_interval_ms", 10000);
    // make these static so that it sticks around while the thread is working on it
    static std::string prometheus_push_addr = globals::config->get("prometheus_push_address", std::string{});
    static std::string prometheus_push_url;
    if (prometheus_push_addr.size() > 0) {
        prometheus_push_url = "http://" + prometheus_push_addr + "/metrics/job/k2pg_gate/instance/" + getHostName() + ":" + std::to_string(::getpid());
    }
    initMetrics(promport, prometheus_push_url, prometheus_push_interval_ms);
    K2LOG_I(k2pg::log::main, "Creating PG-K2 thread");

    // in order to pass the args to the seastar thread, we need to copy them into a new array as their storage will
    // disappear.
    char** argv = (char**)malloc(MAX_K2_ARGS*sizeof(char*));
    assert(argv != NULL);
    int argc = 0;

    // this is an argument adder function
    auto addArg = [argv, &argc](const std::string& sarg) {
        assert(argc < MAX_K2_ARGS);
        argv[argc] = (char*)malloc(sizeof(char)*(sarg.size() + 1));
        assert(argv[argc] != NULL);
        strncpy(argv[argc], sarg.c_str(), sarg.size());
        argv[argc][sarg.size()] = '\0';
        argc++;
    };

    // this helper adds an argument if it is present in the config file. By default, it also adds the
    // arg value from the config file, but caller can override that behavior by specifying a different getter
    // e.g. json= {"cpo_endpoint" : "http://12345:12345"}
    // addNamedArg("cpo_endpoint") would add the cmdline args ["--cpo_endpoint", "http://12345:12345"]
    // addNamedArg("blah") would add nothing since this arg isn't present in the config file
    auto addNamedArg = [addArg](const std::string& argName, std::function<std::string(const std::string&)> getter=[](const std::string& arg) {
                return globals::config->get(arg, std::string{});
            }) {
        auto argv = getter(argName);
        if (!argv.empty()) {
            addArg(std::string("--") + argName);
            addArg(argv);
        }
    };
    // config helper, adds an arg as a switch (with no value) if in json it is present with boolean true
    auto addSwitchArg = [addArg](const std::string& argName) {
        if (globals::config->get(argName, false)) {
            addArg(std::string("--") + argName);
        }
    };

    addArg("k2_pg"); // set the app name
    std::string log_level_args = globals::config->get("log_level", std::string{});
    if (!log_level_args.empty()) {
        addArg("--log_level");
        k2pg::processLogLevelArg(log_level_args, addArg);
    }

    // setup metrics args
    if (prometheus_push_addr.size() > 0) {
        addArg("--prometheus_push_address");
        addArg(prometheus_push_addr);
        addArg("--prometheus_port");
        addArg("0"); // listen on random port for prometheus to avoid conflicts
        addArg("--prometheus_push_interval");
        addArg(std::to_string(prometheus_push_interval_ms/1000) + "s");
    }

    addNamedArg("cpo");
    addNamedArg("tso_endpoint");
    addNamedArg("partition_request_timeout");
    addNamedArg("cpo_request_timeout");
    addNamedArg("cpo_request_backoff");
    addNamedArg("enable_tx_checksum", [](const auto& argName) {
        return globals::config->get(argName, false) ? "true" : "false";
    });
    addNamedArg("smp", [](const auto& argName) {
        return std::to_string(globals::config->get(argName, 1));
    });
    addNamedArg("memory");
    addSwitchArg("hugepages");
    addNamedArg("rdma");
    addNamedArg("reactor-backend");
    addSwitchArg("poll-mode");
    addNamedArg("thread-affinity", [](const auto& argName) {
        return globals::config->get(argName, false) ? "true" : "false";
    });

    globals::k2thread = std::thread([argc, argv]() mutable {
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
            auto code = app.start(argc, argv);
            K2LOG_I(k2pg::log::main, "App ended with code: {}", code);
        }
        catch(...) {
            K2LOG_E(k2pg::log::main, "caught exception in main thread")
            for (int i = 0; i < argc; ++i) {
                free(argv[i]);
            }
            free(argv);
        }
    });

    K2LOG_I(k2pg::log::main, "PG-K2 thread created");
    globals::inited = true;
}

int PostgresServerProcessMain(int argc, char** argv);
}


int main(int argc, char** argv) {
    k2::logging::Logger::procName = std::filesystem::path(argv[0]).filename().c_str();

    // setup the k2 hooks in pg backend so that we can initialize k2 when PG forks to handle a new client
    k2_init_func = startK2App;
    k2_kill_func = killK2App;

    auto code= PostgresServerProcessMain(argc, argv);
    K2LOG_I(k2pg::log::main, "PG process exiting...")
    return code;
}
