#pragma once
#include "k2_includes.h"

namespace k2pg::log {
inline thread_local k2::logging::Logger pg("k2::pggate");
}
