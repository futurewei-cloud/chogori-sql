set(CMAKE_CXX_STANDARD 17)
add_compile_options(-O3 -g -Werror -Wno-sign-compare -Wno-deprecated -Wno-unused-parameter -Wno-type-limits -Wno-unused-function -Wall -Wextra -march=skylake -mtune=skylake-avx512 -mavx -mavx2 -mxsave -mbmi -mbmi2 -mlzcnt -maes -mpopcnt)

add_subdirectory(src/yb/common)
add_subdirectory(src/yb/yql/pggate)
