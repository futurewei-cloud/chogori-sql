file(GLOB HEADERS "*.h" "*/*.h")
file(GLOB SOURCES "*.cc" "*.c" "*/*.cc" "*/*.c")

add_library(k2pgcommon SHARED ${HEADERS} ${SOURCES})

set_target_properties(k2pgcommon PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )

target_link_libraries (k2pgcommon PRIVATE glog gflags ybutf backtrace boost_thread ssl crypto)

set_target_properties(k2pgcommon PROPERTIES LINKER_LANGUAGE C)
