file(GLOB HEADERS "*.h" "*/*.h")
file(GLOB SOURCES "*.cc" "*/*.cc")

add_library(k2pggate STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2pggate PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )
target_link_libraries (k2pggate PRIVATE glog gflags k2pgcommon k2pgentities k2::dto Seastar::seastar prom nlohmann_json::nlohmann_json)

set_target_properties(k2pggate PROPERTIES LINKER_LANGUAGE C)
