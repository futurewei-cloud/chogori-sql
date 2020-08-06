add_compile_options(-O3 -g -fPIC -Werror -Wno-sign-compare -Wno-deprecated -Wno-unused-parameter -Wno-type-limits -Wno-unused-function -Wno-char-subscripts -Wno-ignored-qualifiers -Wno-redundant-move -Wall -Wextra -march=skylake -mtune=skylake-avx512 -mavx -mavx2 -mxsave -mbmi -mbmi2 -mlzcnt -maes -mpopcnt)

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cc")

add_library(k2sqlexecutor SHARED ${HEADERS} ${SOURCES})

set_target_properties(k2sqlexecutor  PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )
target_link_libraries (k2sqlexecutor PRIVATE glog gflags ybcommon k2entities)