add_compile_options(${CMAKE_CXX_FLAGS} -Wno-strict-aliasing -Wno-stringop-truncation)

add_subdirectory(utf)
add_subdirectory(common)
add_subdirectory(entities)
add_subdirectory(pggate)
add_subdirectory(server)
