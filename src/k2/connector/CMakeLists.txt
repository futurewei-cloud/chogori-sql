add_compile_options(${CMAKE_CXX_FLAGS} -Wno-strict-aliasing -Wno-stringop-truncation)

add_subdirectory(yb/utf)
add_subdirectory(yb/common)
add_subdirectory(yb/entities)
add_subdirectory(yb/pggate)
add_subdirectory(yb/server)
