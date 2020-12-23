file(GLOB HEADERS "*.h" "*/*.h")
file(GLOB SOURCES "*.cc" "*/*.cc")

add_library(ybpggate STATIC ${HEADERS} ${SOURCES})

set_target_properties(ybpggate PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )
target_link_libraries (ybpggate PRIVATE glog gflags ybcommon k2entities k2::dto Seastar::seastar nlohmann_json::nlohmann_json)

set_target_properties(ybpggate PROPERTIES LINKER_LANGUAGE C)
