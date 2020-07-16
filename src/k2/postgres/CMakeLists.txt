set(POSTGRES_EXTRA_C_CXX_FLAGS "")

include(ExternalProject)
ExternalProject_Add(postgres
    DEPENDS ybcommon ybpggate
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
    CONFIGURE_COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/configure --prefix=<INSTALL_DIR>
    BUILD_COMMAND ${MAKE}
    INSTALL_COMMAND ${MAKE} install)

#add_custom_target(configure_postgres ALL
#  COMMAND ${SHELL_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/configure --prefix=<INSTALL_DIR>
#  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#)

#add_custom_target(postgres ALL
#   COMMAND ${MAKE}
#   DEPENDS configure_postgres
#   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#)

# ------------------------------------------------------------------------------------------------
# Dependencies between postgres and other targets
# ------------------------------------------------------------------------------------------------
#add_dependencies(postgres k2_connector)

