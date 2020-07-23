file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cc")

add_library(ybpggate SHARED ${HEADERS} ${SOURCES})

set_target_properties(ybpggate PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )
target_link_libraries (ybpggate PRIVATE glog gflags ybcommon)

set_target_properties(ybpggate PROPERTIES LINKER_LANGUAGE C)
