file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cc")

add_library(k2sqlcatalogmanager SHARED ${HEADERS} ${SOURCES})

set_target_properties(k2sqlcatalogmanager  PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )
target_link_libraries (k2sqlcatalogmanager PRIVATE glog gflags ybcommon k2entities)
