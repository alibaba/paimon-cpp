# Copyright 2024-present Alibaba Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");

set(_PAIMON_GLOG_ROOTS ${glog_ROOT} ${GLOG_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_GLOG_ROOTS "")
if(_PAIMON_GLOG_ROOTS)
    set(_PAIMON_GLOG_FIND_ARGS HINTS ${_PAIMON_GLOG_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(glog CONFIG QUIET ${_PAIMON_GLOG_FIND_ARGS})

set(_PAIMON_GLOG_TARGETS glog::glog glog)
foreach(_target IN LISTS _PAIMON_GLOG_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_GLOG_TARGET ${_target})
        break()
    endif()
endforeach()

if(_PAIMON_GLOG_TARGET)
    if(NOT TARGET glog)
        add_library(glog INTERFACE IMPORTED)
        get_target_property(GLOG_INCLUDE_DIR ${_PAIMON_GLOG_TARGET}
                            INTERFACE_INCLUDE_DIRECTORIES)
        if(GLOG_INCLUDE_DIR)
            set_target_properties(glog PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                  "${GLOG_INCLUDE_DIR}")
        endif()
        target_link_libraries(glog INTERFACE ${_PAIMON_GLOG_TARGET})
    endif()
    set(glogAlt_FOUND TRUE)
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PC_glog QUIET libglog)
    endif()

    find_path(GLOG_INCLUDE_DIR NAMES glog/logging.h ${_PAIMON_GLOG_FIND_ARGS}
              HINTS ${PC_glog_INCLUDE_DIRS} PATH_SUFFIXES include)
    find_library(GLOG_LIBRARY NAMES glog ${_PAIMON_GLOG_FIND_ARGS}
                 HINTS ${PC_glog_LIBRARY_DIRS} PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(glogAlt REQUIRED_VARS GLOG_LIBRARY
                                                        GLOG_INCLUDE_DIR)

    if(glogAlt_FOUND AND NOT TARGET glog)
        add_library(glog UNKNOWN IMPORTED)
        set_target_properties(
            glog PROPERTIES IMPORTED_LOCATION "${GLOG_LIBRARY}"
                            INTERFACE_INCLUDE_DIRECTORIES "${GLOG_INCLUDE_DIR}"
                            INTERFACE_COMPILE_DEFINITIONS "GLOG_USE_GLOG_EXPORT")
        find_library(LIBUNWIND_LIBRARY NAMES unwind)
        if(LIBUNWIND_LIBRARY)
            target_link_libraries(glog INTERFACE ${LIBUNWIND_LIBRARY})
        endif()
    endif()
endif()

unset(_PAIMON_GLOG_FIND_ARGS)
unset(_PAIMON_GLOG_ROOTS)
unset(_PAIMON_GLOG_TARGET)
unset(_PAIMON_GLOG_TARGETS)
