// Copyright(c) 2021 Futurewei Cloud
//
// Permission is hereby granted,
//        free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

#pragma once

#include <pthread.h>

#include <chrono>
#include <ctime>
#include <iostream>
#include <sstream>
#include <string>

namespace k2 {
namespace logging {
typedef std::chrono::steady_clock Clock;
typedef Clock::duration Duration;
typedef std::chrono::time_point<Clock> TimePoint;
inline std::chrono::microseconds usec(const Duration& dur) {
    return std::chrono::duration_cast<std::chrono::microseconds>(dur);
}

inline const char* printTime(TimePoint tp) {
    // TODO we can use https://en.cppreference.com/w/cpp/chrono/system_clock/to_stream here, but it is a C++20 feature
    static thread_local char buffer[100];
    auto now = usec(tp.time_since_epoch());
    auto microsec = now.count();
    auto millis = microsec / 1000;
    microsec -= millis * 1000;
    auto secs = millis / 1000;
    millis -= secs * 1000;
    auto mins = (secs / 60);
    secs -= (mins * 60);
    auto hours = (mins / 60);
    mins -= (hours * 60);
    auto days = (hours / 24);
    hours -= (days * 24);
    std::snprintf(buffer, sizeof(buffer), "%04ld:%02ld:%02ld:%02ld.%03ld.%03ld", days, hours, mins, secs, millis, microsec);
    return buffer;
}

class LogEntry {
   public:
    static std::string procName;
    std::ostringstream out;
    LogEntry() = default;
    LogEntry(LogEntry&&) = default;
    ~LogEntry() {
        // this line should output just the str from the stream since all chained "<<" may cause a thread switch
        // and thus garbled log output
        std::cerr << out.rdbuf()->str();
    }
    template <typename T>
    std::ostringstream& operator<<(const T& val) {
        out << val;
        return out;
    }

   private:
    LogEntry(const LogEntry&) = delete;
    LogEntry& operator=(const LogEntry&) = delete;
};

inline LogEntry StartLogStream() {
    LogEntry entry;
    auto id = pthread_self();
    entry.out << "[" << printTime(Clock::now()) << "]-" << LogEntry::procName << "-(" << id << ") ";
    return entry;
}

}  // namespace logging

}  // namespace k2
// This file contains some utility macros for logging and tracing,
// TODO hook this up into proper logging

#define K2LOG(level, msg)                                                                                                                         \
    {                                                                                                                                             \
        k2::logging::StartLogStream() << "[" << level << "] [" << __FILE__ << ":" << __LINE__ << " @" << __FUNCTION__ << "]" << msg << std::endl; \
    }

// TODO warnings and errors must also emit metrics
#define K2INFO(msg) K2LOG("INFO", msg)
#define K2WARN(msg) K2LOG("WARN", msg)
#define K2ERROR(msg) K2LOG("ERROR", msg)
