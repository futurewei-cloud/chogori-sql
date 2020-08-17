file(GLOB HEADERS "*.h" "*/*.h")
file(GLOB SOURCES "*.cc" "*.c" "*/*.cc" "*/*.c")

add_compile_options(-O3 -g -fPIC -Werror -Wno-sign-compare -Wno-deprecated -Wno-unused-parameter -Wno-type-limits -Wno-unused-function -Wno-char-subscripts -Wno-ignored-qualifiers -Wno-strict-aliasing -Wall)

add_library(ybcommon SHARED ${HEADERS} ${SOURCES})

set_target_properties(ybcommon PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )

target_link_libraries (ybcommon PRIVATE glog ybutf backtrace boost_thread ssl crypto)

set_target_properties(ybcommon PROPERTIES LINKER_LANGUAGE C)
