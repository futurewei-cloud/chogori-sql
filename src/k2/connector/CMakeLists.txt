set(CMAKE_CXX_STANDARD 17)

set(K2_CONNECTOR_SRCS
  src/yb/common/ybc_util.h
  src/yb/yql/pggate/ybc_pg_typedefs.h
  src/yb/yql/pggate/ybc_pggate.h
  src/ybgate/ybgate_api.h
)

add_library(k2_connector SHARED ${K2_CONNECTOR_SRCS})
set_target_properties(k2_connector PROPERTIES LINKER_LANGUAGE C)
