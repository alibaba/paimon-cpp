# Copyright 2024-present Alibaba Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");

set(_PAIMON_RE2_ROOTS ${RE2_ROOT} ${re2_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_RE2_ROOTS "")
if(_PAIMON_RE2_ROOTS)
    set(_PAIMON_RE2_FIND_ARGS HINTS ${_PAIMON_RE2_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(re2 CONFIG QUIET ${_PAIMON_RE2_FIND_ARGS})

if(TARGET re2::re2)
    get_target_property(RE2_INCLUDE_DIR re2::re2 INTERFACE_INCLUDE_DIRECTORIES)
    set(RE2_LIBRARIES re2::re2)
    set(RE2Alt_FOUND TRUE)
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PC_RE2 QUIET re2)
    endif()

    find_path(RE2_INCLUDE_DIR
              NAMES re2/re2.h ${_PAIMON_RE2_FIND_ARGS}
              HINTS ${PC_RE2_INCLUDE_DIRS}
              PATH_SUFFIXES include)
    find_library(RE2_LIBRARY
                 NAMES re2 ${_PAIMON_RE2_FIND_ARGS}
                 HINTS ${PC_RE2_LIBRARY_DIRS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(RE2Alt REQUIRED_VARS RE2_LIBRARY RE2_INCLUDE_DIR)

    if(RE2Alt_FOUND)
        add_library(re2::re2 UNKNOWN IMPORTED)
        set_target_properties(re2::re2
                              PROPERTIES IMPORTED_LOCATION "${RE2_LIBRARY}"
                                         INTERFACE_INCLUDE_DIRECTORIES
                                         "${RE2_INCLUDE_DIR}")
        set(RE2_LIBRARIES "${RE2_LIBRARY}")
    endif()
endif()

unset(_PAIMON_RE2_FIND_ARGS)
unset(_PAIMON_RE2_ROOTS)
