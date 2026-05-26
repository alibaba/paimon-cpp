# Copyright 2026-present Alibaba Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(_PAIMON_FMT_ROOTS ${fmt_ROOT} ${FMT_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_FMT_ROOTS "")
if(_PAIMON_FMT_ROOTS)
    set(_PAIMON_FMT_FIND_ARGS HINTS ${_PAIMON_FMT_ROOTS} NO_DEFAULT_PATH)
endif()

include(FindPackageUtils)
find_package(fmt CONFIG QUIET ${_PAIMON_FMT_FIND_ARGS})

set(_PAIMON_FMT_TARGETS fmt::fmt fmt::fmt-header-only)
foreach(_target IN LISTS _PAIMON_FMT_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_FMT_TARGET ${_target})
        break()
    endif()
endforeach()

if(_PAIMON_FMT_TARGET)
    paimon_find_target_headers(FMT_INCLUDE_DIR
                               ${_PAIMON_FMT_TARGET}
                               NAMES
                               fmt/core.h
                               ${_PAIMON_FMT_FIND_ARGS})

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(fmtAlt REQUIRED_VARS FMT_INCLUDE_DIR)

    if(fmtAlt_FOUND AND NOT TARGET fmt)
        add_library(fmt INTERFACE IMPORTED)
        set_target_properties(fmt PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                             "${FMT_INCLUDE_DIR}")
        target_link_libraries(fmt INTERFACE ${_PAIMON_FMT_TARGET})
    endif()
    if(fmtAlt_FOUND)
        set(FMT_LIBRARIES ${_PAIMON_FMT_TARGET})
    endif()
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PC_fmt QUIET fmt)
    endif()

    find_path(FMT_INCLUDE_DIR
              NAMES fmt/core.h ${_PAIMON_FMT_FIND_ARGS}
              HINTS ${PC_fmt_INCLUDE_DIRS}
              PATH_SUFFIXES include)
    find_library(FMT_LIBRARY
                 NAMES fmt ${_PAIMON_FMT_FIND_ARGS}
                 HINTS ${PC_fmt_LIBRARY_DIRS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(fmtAlt REQUIRED_VARS FMT_INCLUDE_DIR FMT_LIBRARY)

    if(fmtAlt_FOUND AND NOT TARGET fmt)
        add_library(fmt UNKNOWN IMPORTED)
        set_target_properties(fmt
                              PROPERTIES IMPORTED_LOCATION "${FMT_LIBRARY}"
                                         INTERFACE_INCLUDE_DIRECTORIES
                                         "${FMT_INCLUDE_DIR}")
        set(FMT_LIBRARIES "${FMT_LIBRARY}")
    endif()
endif()

unset(_PAIMON_FMT_FIND_ARGS)
unset(_PAIMON_FMT_ROOTS)
unset(_PAIMON_FMT_TARGET)
unset(_PAIMON_FMT_TARGETS)
