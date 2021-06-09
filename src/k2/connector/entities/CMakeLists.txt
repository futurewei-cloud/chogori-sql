file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cc")

add_library(k2pgentities SHARED ${HEADERS} ${SOURCES})

set_target_properties(k2pgentities PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )

target_link_libraries (k2pgentities PRIVATE glog k2pgcommon)

set_target_properties(k2pgentities PROPERTIES LINKER_LANGUAGE CXX)
