file(GLOB HEADERS "*.h" "type/*.h" "strings/*.h" "utf/*.h" "hash/*.h" "sys/*.h" "util/*.h" "concurrent/*.h" "metrics/*.h" "log/*.h")
file(GLOB SOURCES "*.cc" "*.c" "type/*.cc" "strings/*.cc" "utf/*.c" "hash/*.cc" "sys/*.cc" "util/*.cc" "concurrent/*.cc" "metrics/*.cc" "log/*.cc")

add_library(ybcommon SHARED ${HEADERS} ${SOURCES})

set_target_properties(ybcommon PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )

#target_link_libraries (ybcommon PRIVATE glog gflags boost_thread libbacktrace)
target_link_libraries (ybcommon PRIVATE glog)

set_target_properties(ybcommon PROPERTIES LINKER_LANGUAGE C)
