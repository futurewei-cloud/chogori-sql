file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cc")

add_library(k2entities SHARED ${HEADERS} ${SOURCES})

set_target_properties(k2entities PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )

target_link_libraries (k2entities PRIVATE ybcommon)

set_target_properties(k2entities PROPERTIES LINKER_LANGUAGE CXX)
