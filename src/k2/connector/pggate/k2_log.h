#pragma once
#include "k2_includes.h"

namespace k2pg::log {
inline thread_local k2::logging::Logger pg("k2::pggate");
inline thread_local k2::logging::Logger k2Adapter("k2::k2Adapter");
inline thread_local k2::logging::Logger k2Client("k2::k2Client");
}
