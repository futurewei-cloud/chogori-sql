file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cc")

add_compile_options(-O3 -g -Werror -Wno-sign-compare -Wno-deprecated -Wno-unused-parameter -Wno-type-limits -Wno-unused-function -Wno-char-subscripts -Wno-ignored-qualifiers -Wno-strict-aliasing -Wall)

add_library(ybpggate STATIC ${HEADERS} ${SOURCES})

set_target_properties(ybpggate PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )
target_link_libraries (ybpggate PRIVATE glog gflags ybcommon k2entities k2::dto Seastar::seastar)

set_target_properties(ybpggate PROPERTIES LINKER_LANGUAGE C)
