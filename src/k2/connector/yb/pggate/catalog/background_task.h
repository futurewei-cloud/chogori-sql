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

#pragma once

#include <thread>
#include <mutex>
#include <pthread.h>
#include <chrono>

#include "catalog_log.h"

namespace k2pg {
namespace sql {
namespace catalog {

class BackgroundTask {
    public:
    explicit BackgroundTask(std::function<void()> task, const std::string& name,
        std::chrono::milliseconds initial_wait, std::chrono::milliseconds interval)
        : task_(std::move(task)), name_(name), initial_wait_(std::move(initial_wait)), interval_(std::move(interval)) {
    }

    void Start() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (running_) {
            K2LOG_I(log::catalog, "Background task {} has already started", name_);
        } else {
            thread_ = std::make_unique<std::thread>([this](){
                RunTask();
            });
            thread_handler_ = std::make_optional<pthread_t>(thread_->native_handle());
            thread_->detach();
            running_ = true;
            K2LOG_I(log::catalog, "Background task {} started successfully", name_);
       }
    }

    void Shutdown() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (running_) {
            if(thread_handler_ != std::nullopt) {
                pthread_cancel(thread_handler_.value());
                thread_handler_ = std::nullopt;
                running_ = false;
            }
            K2LOG_I(log::catalog, "Background task {} stopped successfully", name_);
        } else {
            K2LOG_I(log::catalog, "Background task {} has already been started yet", name_);
        }
    }

    private:

    void RunTask() {
        std::this_thread::sleep_for(initial_wait_);
        while(true) {
            K2LOG_I(log::catalog, "Running background task {}", name_);
            try {
                task_();
            } catch (const std::exception& e) {
                K2LOG_E(log::catalog, "Failed to run background task {} due to {}", name_, e.what());
            }
            std::this_thread::sleep_for(interval_);
        }
    }

    std::function<void()> task_;
    std::string name_;
    mutable std::mutex mutex_;
    std::unique_ptr<std::thread> thread_;
    std::optional<pthread_t> thread_handler_ = std::nullopt;
    std::chrono::milliseconds initial_wait_;
    std::chrono::milliseconds interval_;
    std::atomic<bool> running_ = false;
};

} // namespace sql
} // namespace sql
} // namespace k2pg