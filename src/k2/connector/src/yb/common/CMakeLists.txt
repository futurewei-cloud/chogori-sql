file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cc")

add_library(ybcommon SHARED ${HEADERS} ${SOURCES})

set_target_properties(ybcommon PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )
target_link_libraries (ybcommon PRIVATE glog)

set_target_properties(ybcommon PROPERTIES LINKER_LANGUAGE C)
