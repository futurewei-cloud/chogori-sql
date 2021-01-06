#pragma once
#ifdef K2_DEBUG_LOGGING
#undef K2_DEBUG_LOGGING
#endif
#define K2_DEBUG_LOGGING 1

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/common/Log.h>
#include <k2/dto/Collection.h>
#include <k2/dto/SKVRecord.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/tso/client/tso_clientlib.h>
#include <sstream>
#include <string>

namespace k2 {
    inline std::string escape(const char* data, size_t size) {
        std::ostringstream os;
        for (int i = 0; i < size; ++i) {
            if (std::isprint(data[i])) {
                if (data[i] == '_') {
                    os << '_';
                }
                os << data[i];
            }
            else {
                os << "_" << std::to_string(data[i]) << "_";
            }
        }
        return os.str();
    }

    inline std::string escape(const std::string& str) {
        return escape(str.data(), str.size());
    }

    inline std::string escape(const String& str) {
        return escape(str.data(), str.size());
    }
}
