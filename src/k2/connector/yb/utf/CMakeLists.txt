file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.c")

add_library(ybutf SHARED ${HEADERS} ${SOURCES})

set_target_properties(ybutf PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        )

set_target_properties(ybutf PROPERTIES LINKER_LANGUAGE C)
