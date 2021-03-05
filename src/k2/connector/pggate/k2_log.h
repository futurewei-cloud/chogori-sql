#pragma once
#include <k2/common/Log.h>

namespace k2pg::log {
inline thread_local k2::logging::Logger pg("k2::pggate");
}
