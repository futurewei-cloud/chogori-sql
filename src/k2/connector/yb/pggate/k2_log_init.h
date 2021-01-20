#pragma once

#include <k2/common/Log.h>

namespace k2pg {
template<typename Visitor>
inline void processLogLevelArg(const std::string& arg, Visitor& argAdder) {
    // logLevel args are coming in as a single arg, which is a space-delimited string.
    // We need to turn each of these tokens into a separate argv arg for K2
    std::vector<std::string> levels;
    size_t pos = 0;
    while(1) {
        // split on space to determine individual log levels
        auto stpos = pos;
        pos = arg.find(' ', pos+1);
        auto endpos = pos == std::string::npos ? arg.size() : pos;
        levels.push_back(arg.substr(stpos, endpos - stpos));
        if (pos == std::string::npos) break;
        pos ++; // skip the space
    }

    if (levels.size() == 0) {
        // default log level
        k2::logging::Logger::threadLocalLogLevel = k2::logging::LogLevel::INFO;
    }
    else {
        auto split = [](const std::string& token) {
            auto pos = token.find("=");
            if (pos == std::string::npos) {
                throw std::runtime_error("log level entry must be separated by '='");
            }
            if (pos == 0) {
                throw std::runtime_error("no module name specified in log level override");
            }
            if (pos == token.size() - 1) {
                throw std::runtime_error("no log level specified for module log level override");
            }
            std::string first = token.substr(0, pos);
            std::string second = token.substr(pos+1, token.size() - pos + 1);
            return std::make_tuple(std::move(first), std::move(second));
        };

        k2::logging::Logger::threadLocalLogLevel = k2::logging::LogLevelFromStr(levels[0]);
        argAdder(levels[0]);
        for (size_t i = 1; i < levels.size(); ++i) {
            argAdder(levels[i]); // add this arg to the seastar args

            auto [module, levelStr] = split(levels[i]);
            auto level = k2::logging::LogLevelFromStr(levelStr);
            k2::logging::Logger::moduleLevels[module] = level;
            auto it = k2::logging::Logger::moduleLoggers.find(module);
            if (it != k2::logging::Logger::moduleLoggers.end()) {
                it->second->moduleLevel = level;
            }
        }
    }
}
}
