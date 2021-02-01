cmake_minimum_required(VERSION 3.17)

project (k2_sql VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)

add_compile_options(-O3 -g -Werror -Wno-sign-compare -Wno-deprecated -Wno-unused-parameter -Wno-type-limits -Wno-unused-function -Wno-char-subscripts -Wno-ignored-qualifiers -Wall -Wextra -march=skylake -mtune=skylake-avx512 -mavx -mavx2 -mxsave -mbmi -mbmi2 -mlzcnt -maes -mpopcnt)

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
find_package(Backtrace REQUIRED)
find_package(k2 REQUIRED)
find_package(Seastar REQUIRED)
find_package(nlohmann_json REQUIRED)

include_directories(src/k2/connector)
add_subdirectory(src/k2/connector)

include(ExternalProject)
ExternalProject_Add(
    postgres
    DEPENDS ybcommon k2entities ybpggate
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres
    STAMP_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres/cmakedir/stamp
    TMP_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres/cmakedir/tmp
    LOG_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres/cmakedir/log
    BINARY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres/cmakedir/build
    CONFIGURE_COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres/configure_helper.sh    ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres    "-lybpggate -lybcommon -lk2entities  -lgflags -lybutf -std=c++17"     "-L/usr/local/lib/k2/ -L${CMAKE_CURRENT_SOURCE_DIR}/build/src/k2/connector/yb/common -L${CMAKE_CURRENT_SOURCE_DIR}/build/src/k2/connector/yb/entities -L${CMAKE_CURRENT_SOURCE_DIR}/build/src/k2/connector/yb/pggate -L${CMAKE_CURRENT_SOURCE_DIR}/build/src/k2/connector/yb/utf"      "-I${CMAKE_CURRENT_SOURCE_DIR}/src/k2/connector"     <INSTALL_DIR>
    PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/src/k2/postgres
    BUILD_ALWAYS 1
    BUILD_COMMAND rm -f /build/src/k2/postgres/bin/postgres /build/src/k2/postgres/cmakedir/build/src/backend/postgres && make -j -f Makefile "PG_CPP_LIBS=-lybpggate -lk2entities -lk23si_client -lcpo_client -ltso_client -ldto -lappbase -ltransport -lcrc32c -lconfig -lcommon -lseastar -lboost_thread -lyaml-cpp -lboost_program_options -lfmt -libverbs -lhwloc -lnuma -lcrypto++ -ldl -lrt -lprotobuf -lgnutls")
