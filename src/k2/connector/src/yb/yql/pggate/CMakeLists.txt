file(GLOB HEADERS "*.h")
#file(GLOB SOURCES "*.cc")

#add_library(ybpggate SHARED ${HEADERS} ${SOURCES})
add_library(ybpggate SHARED ${HEADERS})

set_target_properties(ybpggate PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )
target_link_libraries (ybpggate PRIVATE glog)

set_target_properties(ybpggate PROPERTIES LINKER_LANGUAGE C)
