file(GLOB HEADERS "*.h" "*/*.h")
file(GLOB SOURCES "*.cc" "*.c" "*/*.cc" "*/*.c")

add_library(ybcommon SHARED ${HEADERS} ${SOURCES})

set_target_properties(ybcommon PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )

target_link_libraries (ybcommon PRIVATE glog ybutf backtrace boost_thread)

set_target_properties(ybcommon PROPERTIES LINKER_LANGUAGE C)
