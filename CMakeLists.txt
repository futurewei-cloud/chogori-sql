cmake_minimum_required(VERSION 3.17)

project (k2_sql VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)

add_compile_options(-O3 -g -Werror -Wno-sign-compare -Wno-deprecated -Wno-unused-parameter -Wno-type-limits -Wno-unused-function -Wall -Wextra -march=skylake -mtune=skylake-avx512 -mavx -mavx2 -mxsave -mbmi -mbmi2 -mlzcnt -maes -mpopcnt)

# K2_DEBUG_LOGGING enables some verbose logging for debug purposes.
if(DEFINED ENV{K2_DEBUG_LOGGING})
	set(K2_DEBUG_LOGGING $ENV{K2_DEBUG_LOGGING})
else()
    set(K2_DEBUG_LOGGING 0)
endif()

add_compile_definitions(K2_DEBUG_LOGGING=${K2_DEBUG_LOGGING})
# link types: a - auto, s - static, d - dynamic
set(LINK_TYPE "a")

find_package(Boost REQUIRED)
find_package(glog REQUIRED)
find_package(gflags REQUIRED)

add_subdirectory(src/k2/connector)

include(ExternalProject)
ExternalProject_Add(
    postgres
    DEPENDS ybcommon ybpggate
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres
    CONFIGURE_COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres/configure --prefix=<INSTALL_DIR>
    PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres
    BUILD_COMMAND ${MAKE})

