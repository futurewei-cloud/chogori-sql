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

#include "sql_executor.h"
#include <algorithm>
#include <list>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "common/status.h"
#include "common/flag_tags.h"
#include "common/env.h"

DEFINE_int32(sql_executor_svc_num_threads, -1,
"Number of threads for the SqlExecutor service. If -1, it is auto configured.");
TAG_FLAG(sql_executor_svc_num_threads, advanced);

namespace k2pg {
namespace sql {
    using yb::Status;

    static yb::Env* default_env;

    SqlExecutor::~SqlExecutor() {
        Shutdown();
    }

    Status SqlExecutor::Init() {
        CHECK(!initted_.load(std::memory_order_acquire));
        log_prefix_ = Format("P $0: ", cluster_uuid());

        // TODO: initialization steps

        initted_.store(true, std::memory_order_release);
        return Status::OK();
    }

    Status SqlExecutor::WaitInited() {
        //TODO: WaitForAllBootstrapsToFinish();
        return Status::OK();
    }

    void SqlExecutor::AutoInitServiceFlags() {
        const int32 num_cores = base::NumCPUs();

        if (FLAGS_sql_executor_svc_num_threads == -1) {
            // Auto select number of threads for the SQL Executor service based on number of cores.
            // But bound it between 64 & 512.
            const int32 num_threads = std::min(512, num_cores * 32);
            FLAGS_sql_executor_svc_num_threads = std::max(64, num_threads);
            LOG(INFO) << "Auto setting FLAGS_sql_executor_svc_num_threads to "
                      << FLAGS_sql_executor_svc_num_threads;
        }
    }

    Status SqlExecutor::RegisterServices() {
        // TODO: wire service components

        return Status::OK();
    }

    Status SqlExecutor::Start() {
        CHECK(initted_.load(std::memory_order_acquire));

        AutoInitServiceFlags();

        RETURN_NOT_OK(RegisterServices());

        // TODO: start up steps

        google::FlushLogFiles(google::INFO); // Flush the startup messages.

        return Status::OK();
    }

    void SqlExecutor::Shutdown() {
        LOG(INFO) << "SqlExecutor shutting down...";

        bool expected = true;
        if (initted_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            // TODO: shut down steps

        }

        LOG(INFO) << "SqlExecutor shut down complete. Bye!";
    }

    void SqlExecutor::set_cluster_uuid(const std::string& cluster_uuid) {
        std::lock_guard<simple_spinlock> l(lock_);
        cluster_uuid_ = cluster_uuid;
    }

    std::string SqlExecutor::cluster_uuid() const {
        std::lock_guard<simple_spinlock> l(lock_);
        return cluster_uuid_;
    }

    Env* SqlExecutor::GetEnv() {
        return default_env;
    }

    void SqlExecutor::SetCatalogVersion(uint64_t new_version) {
        std::lock_guard<simple_spinlock> l(lock_);
        uint64_t ysql_catalog_version_ = catalog_version_.load(std::memory_order_acquire);
        if (new_version > ysql_catalog_version_) {
            catalog_version_.store(new_version, std::memory_order_release);
        } else if (new_version < ysql_catalog_version_) {
            LOG(DFATAL) << "Ignoring ysql catalog version update: new version too old. "
                        << "New: " << new_version << ", Old: " << ysql_catalog_version_;
        }
    }
}
}




