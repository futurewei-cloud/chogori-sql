file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cc")

add_library(k2sqlexecutor SHARED ${HEADERS} ${SOURCES})

set_target_properties(k2sqlexecutor  PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )
target_link_libraries (k2sqlexecutor PRIVATE glog gflags ybcommon k2entities)
