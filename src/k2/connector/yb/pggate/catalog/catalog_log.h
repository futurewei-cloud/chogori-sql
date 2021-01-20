#pragma once
#include <k2/common/Log.h>

namespace k2pg::sql::catalog::log {
inline thread_local k2::logging::Logger catalog("k2::pg_catalog");
}
