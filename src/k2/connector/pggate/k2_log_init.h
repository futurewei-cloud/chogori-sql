/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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

#include <sstream>

#include "k2_includes.h"

namespace k2pg {
template<typename Visitor>
inline void processLogLevelArg(const std::string& arg, Visitor& argAdder) {
    // logLevel args are coming in as a single arg, which is a space-delimited string,
    // e.g. "DEBUG k2::pg=INFO k2::transport=INFO"
    // We need to turn each of these tokens into a separate argv arg for K2
    auto tokenize = [](const std::string& arg, const char sep) {
        std::stringstream ss(arg); //convert my_string into string stream
        std::vector<std::string> tokens;
        std::string token;
        while(std::getline(ss, token, sep)) tokens.push_back(token);
        return tokens;
    };

    auto levels = tokenize(arg, ' '); // levels are space-separated

    if (levels.size() == 0) {
        // default log level
        k2::logging::Logger::threadLocalLogLevel = k2::logging::LogLevel::INFO;
    }
    else {
        // we have the individual log-level args into the levels vector, e.g. ["DEBUG", "k2::pg=INFO", "k2::x=DEBUG"]
        // set the very first arg as the thread-local log-level
        k2::logging::Logger::threadLocalLogLevel = k2::logging::LogLevelFromStr(levels[0]);
        argAdder(levels[0]); // ... and add it as an arg for the K2 seastar thread

        // now process the rest of the args as we need to split those based on '=' character
        // to determine which modules have overrides
        for (size_t i = 1; i < levels.size(); ++i) {
            argAdder(levels[i]); // add this full arg to the seastar args (e.g. "k2::pg=INFO"). ss will parse it itself

            // however, for this thread, we need to also set the module-level overrides, so do this now
            auto tokens = tokenize(levels[i], '=');
            auto& module = tokens[0];
            auto level = k2::logging::LogLevelFromStr(tokens[1]);

            // set the thread-local module level override
            k2::logging::Logger::moduleLevels[module] = level;

            // ... and if the logger for this module is already created, update the live object with the override level
            auto it = k2::logging::Logger::moduleLoggers.find(module);
            if (it != k2::logging::Logger::moduleLoggers.end()) {
                it->second->moduleLevel = level;
            }
        }
    }
}
}
