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

set(_PAIMON_GLOG_ROOTS ${glog_ROOT} ${GLOG_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_GLOG_ROOTS "")
if(_PAIMON_GLOG_ROOTS)
    set(_PAIMON_GLOG_FIND_ARGS HINTS ${_PAIMON_GLOG_ROOTS} NO_DEFAULT_PATH)
endif()

include(FindPackageUtils)
find_package(glog CONFIG QUIET ${_PAIMON_GLOG_FIND_ARGS})

set(_PAIMON_GLOG_TARGETS glog::glog glog)
foreach(_target IN LISTS _PAIMON_GLOG_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_GLOG_TARGET ${_target})
        break()
    endif()
endforeach()

if(_PAIMON_GLOG_TARGET)
    paimon_find_target_headers(GLOG_INCLUDE_DIR
                               ${_PAIMON_GLOG_TARGET}
                               NAMES
                               glog/logging.h
                               ${_PAIMON_GLOG_FIND_ARGS})

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(glogAlt REQUIRED_VARS GLOG_INCLUDE_DIR)

    if(glogAlt_FOUND AND NOT TARGET glog)
        add_library(glog INTERFACE IMPORTED)
        set_target_properties(glog PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                              "${GLOG_INCLUDE_DIR}")
        target_link_libraries(glog INTERFACE ${_PAIMON_GLOG_TARGET})
    endif()
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PC_glog QUIET libglog)
    endif()

    find_path(GLOG_INCLUDE_DIR
              NAMES glog/logging.h ${_PAIMON_GLOG_FIND_ARGS}
              HINTS ${PC_glog_INCLUDE_DIRS}
              PATH_SUFFIXES include)
    find_library(GLOG_LIBRARY
                 NAMES glog ${_PAIMON_GLOG_FIND_ARGS}
                 HINTS ${PC_glog_LIBRARY_DIRS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(glogAlt REQUIRED_VARS GLOG_LIBRARY GLOG_INCLUDE_DIR)

    if(glogAlt_FOUND AND NOT TARGET glog)
        add_library(glog UNKNOWN IMPORTED)
        set_target_properties(glog
                              PROPERTIES IMPORTED_LOCATION "${GLOG_LIBRARY}"
                                         INTERFACE_INCLUDE_DIRECTORIES
                                         "${GLOG_INCLUDE_DIR}"
                                         INTERFACE_COMPILE_DEFINITIONS
                                         "GLOG_USE_GLOG_EXPORT")
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
