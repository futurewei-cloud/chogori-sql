file(GLOB HEADERS "*.h" "strings/*.h" "hash/*.h" "util/*.h" "concurrent/*.h" "metrics/*.h" "log/*.h")
file(GLOB SOURCES "*.cc" "strings/*.cc" "hash/*.cc" "util/*.cc" "concurrent/*.cc" "metrics/*.cc" "log/*.cc")

add_compile_options(-O3 -g -fPIC -Werror -Wno-sign-compare -Wno-deprecated -Wno-unused-parameter -Wno-type-limits -Wno-unused-function -Wno-char-subscripts -Wno-ignored-qualifiers -Wno-redundant-move -Wall -Wextra -march=skylake -mtune=skylake-avx512 -mavx -mavx2 -mxsave -mbmi -mbmi2 -mlzcnt -maes -mpopcnt)

add_library(ybcommon SHARED ${HEADERS} ${SOURCES})

set_target_properties(ybcommon PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )
target_link_libraries (ybcommon PRIVATE glog)

set_target_properties(ybcommon PROPERTIES LINKER_LANGUAGE C)
