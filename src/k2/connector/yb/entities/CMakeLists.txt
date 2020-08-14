file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cc")

add_compile_options(-O3 -g -fPIC -Werror -Wno-sign-compare -Wno-deprecated -Wno-unused-parameter -Wno-type-limits -Wno-unused-function -Wno-char-subscripts -Wno-ignored-qualifiers -Wno-strict-aliasing -Wall)

add_library(k2entities SHARED ${HEADERS} ${SOURCES})

set_target_properties(k2entities PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )

target_link_libraries (k2entities PRIVATE glog ybcommon)

set_target_properties(k2entities PROPERTIES LINKER_LANGUAGE CXX)
