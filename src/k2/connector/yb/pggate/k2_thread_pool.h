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
#include "k2_includes.h"
#include "k2_config.h"

#include <pthread.h>

#include <atomic>
#include <deque>
#include <thread>
#include <vector>

#include <seastar/core/resource.hh>
#include <seastar/core/memory.hh>
#include "k2_log.h"

namespace k2pg {
class ThreadPool {
public:
    // Create a threadpool with the given number of total threads. All threads are created and started here.
    // If the firstCPUPin is >=0, each thread will be pinned in incrementing order, starting with firstCPUPin
    // In synthetic testing, the thread dispatch has overhead of avg~150nsec with CPU pinning and 1 worker thread.
    // Adding more threads reduces the performance to avg~1.5usec. Thread pinning also helps here, especially in the high
    // percentiles where threads may hop to different cores.
    // One possible issue to consider is that if threads are pinned on hyperthreads, the performance may be worse
    // due to cache misses.
    ThreadPool(int threadCount, int firstCPUPin=-1) {
        for (int i = 0; i < threadCount; ++i) {
            _workers.push_back(std::thread([i, firstCPUPin, this,
                     modLogLevels=k2::logging::Logger::moduleLevels,
                     globalLogLevel=k2::logging::Logger::threadLocalLogLevel] {
                // copy over the logging configuration from the main thread
                k2::logging::Logger::threadLocalLogLevel = globalLogLevel;
                k2::logging::Logger::moduleLevels=modLogLevels;
                for (auto& [module, level]: k2::logging::Logger::moduleLevels) {
                    auto it = k2::logging::Logger::moduleLoggers.find(module);
                    if (it != k2::logging::Logger::moduleLoggers.end()) {
                        it->second->moduleLevel = level;
                    }
                }

                if (firstCPUPin >= 0) {
                    pin(i + firstCPUPin);
                }

                while (!_stop) {
                    std::unique_lock lock(_qMutex);

                    if (!_tasks.empty()) {
                        auto task = _tasks[0];
                        _tasks.pop_front();
                        lock.unlock();
                        try {
                            K2LOG_D(log::pg, "Running task");
                            task();
                            K2LOG_D(log::pg, "Task completed");
                        }
                        catch(const std::exception& exc) {
                            K2LOG_W(log::pg, "Task threw exception: {}", exc.what());
                        }
                        catch(...) {
                            K2LOG_E(log::pg, "Task threw unknown exception");
                        }
                    } else {
                        // no tasks left. Notify anyone waiting on threadpool
                        _waitNotifier.notify_all();
                        // wait for new tasks
                        _qNotifier.wait(lock);
                    }
                }
            }));
        }
    }

    // enqueue a new task to be executed. The task can be a lambda or std::function
    template <typename Func>
    void enqueue(Func&& task) {
        {
            std::lock_guard lock{_qMutex};
            _tasks.push_back(std::forward<Func>(task));
        }
        // notify new task
        _qNotifier.notify_one();
    }

    // Wait for all tasks in this pool to complete. Blocks the caller until all tasks have completed
    void wait() {
        while (1) {
            std::unique_lock lock(_qMutex);
            if (_tasks.empty()) break;
            _waitNotifier.wait(lock);
        }
    }

    ~ThreadPool() {
        _stop = true;
        _qNotifier.notify_all(); // notify all threads so that they can check the stop flag
        for (auto& w : _workers) {
            w.join(); // all workers should have exited
        }
        _workers.clear();
    }

    // pin the calling thread to the given CPU
    static void pin(int cpu) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu, &cpuset);
        if (0 != pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset)) {
            throw std::runtime_error("Unable to set affinity");
        }
    }

private:
    std::vector<std::thread> _workers;
    std::deque<std::function<void()>> _tasks;
    std::atomic<bool> _stop{false};
    std::condition_variable _qNotifier;
    std::mutex _qMutex;
    std::condition_variable _waitNotifier;
};
} // ns k2pg
