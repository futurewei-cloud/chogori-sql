cmake_minimum_required (VERSION 3.13)

project (k2_sql VERSION 0.0.1 LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)
add_compile_options(-O3 -g -Werror -Wall -Wextra -march=skylake -mtune=skylake-avx512 -mavx -mavx2 -mxsave -mbmi -mbmi2 -mlzcnt -maes -mpopcnt)

# K2_DEBUG_LOGGING enables some verbose logging for debug purposes.
if(DEFINED ENV{K2_DEBUG_LOGGING})
	set(K2_DEBUG_LOGGING $ENV{K2_DEBUG_LOGGING})
else()
    set(K2_DEBUG_LOGGING 0)
endif()

add_compile_definitions(K2_DEBUG_LOGGING=${K2_DEBUG_LOGGING})

add_subdirectory(src/k2/connector)
add_subdirectory(src/k2/postgres)

#find_package (Seastar REQUIRED)
#find_package (yaml-cpp REQUIRED) # required for configuration file parsing

