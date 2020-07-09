cmake_minimum_required (VERSION 3.13)

project (k2_sql VERSION 0.0.1 LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)

# K2_DEBUG_LOGGING enables some verbose logging for debug purposes.
if(DEFINED ENV{K2_DEBUG_LOGGING})
	set(K2_DEBUG_LOGGING $ENV{K2_DEBUG_LOGGING})
else()
    set(K2_DEBUG_LOGGING 0)
endif()

function(ADD_THIRDPARTY_LIB LIB_NAME)
  set(options)
  set(one_value_args SHARED_LIB STATIC_LIB)
  set(multi_value_args DEPS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(("${LINK_TYPE}" STREQUAL "s" AND ARG_STATIC_LIB) OR (NOT ARG_SHARED_LIB))
    if(NOT ARG_STATIC_LIB)
      message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
    endif()
    add_library(${LIB_NAME} STATIC IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")
  else()
    add_library(${LIB_NAME} SHARED IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
  endif()

  if(ARG_DEPS)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
  endif()

  # Set up an "exported variant" for this thirdparty library (see "Visibility"
  # above). It's the same as the real target, just with an "_exported" suffix.
  # We prefer the static archive if it exists (as it's akin to an "internal"
  # library), but we'll settle for the shared object if we must.
  #
  # A shared object exported variant will force any "leaf" library that
  # transitively depends on it to also depend on it at runtime; this is
  # desirable for some libraries (e.g. cyrus_sasl).
  set(LIB_NAME_EXPORTED ${LIB_NAME}_exported)
  if(ARG_STATIC_LIB)
    add_library(${LIB_NAME_EXPORTED} STATIC IMPORTED)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
  else()
    add_library(${LIB_NAME_EXPORTED} SHARED IMPORTED)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
  endif()
  if(ARG_DEPS)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
  endif()
endfunction()

add_compile_definitions(K2_DEBUG_LOGGING=${K2_DEBUG_LOGGING})
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake_modules")
#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")
set(THIRDPARTY_INSTALLED_DIR "${PROJECT_SOURCE_DIR}/thirdparty/installed")
set(CMAKE_PREFIX_PATH ${THIRDPARTY_INSTALLED_DIR} ${CMAKE_PREFIX_PATH})

# link types: a - auto, s - static, d - dynamic
set(LINK_TYPE "a")

## GLog
find_package(GLog 0.4.0 REQUIRED)
include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
#ADD_THIRDPARTY_LIB(glog
#  STATIC_LIB "${GLOG_STATIC_LIB}"
#  SHARED_LIB "${GLOG_SHARED_LIB}")

add_subdirectory(src/k2/connector)
add_subdirectory(src/k2/postgres)

#find_package (Seastar REQUIRED)
#find_package (yaml-cpp REQUIRED) # required for configuration file parsing

