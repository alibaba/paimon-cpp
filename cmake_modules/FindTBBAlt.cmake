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

set(_PAIMON_TBB_ROOTS ${TBB_ROOT} ${tbb_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_TBB_ROOTS "")
if(_PAIMON_TBB_ROOTS)
    set(_PAIMON_TBB_FIND_ARGS HINTS ${_PAIMON_TBB_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(TBB CONFIG QUIET ${_PAIMON_TBB_FIND_ARGS})

set(_PAIMON_TBB_TARGETS TBB::tbb tbb)
foreach(_target IN LISTS _PAIMON_TBB_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_TBB_TARGET ${_target})
        break()
    endif()
endforeach()

if(_PAIMON_TBB_TARGET)
    if(NOT TARGET tbb)
        add_library(tbb INTERFACE IMPORTED)
        get_target_property(TBB_INCLUDE_DIR ${_PAIMON_TBB_TARGET}
                            INTERFACE_INCLUDE_DIRECTORIES)
        if(TBB_INCLUDE_DIR)
            set_target_properties(tbb PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                 "${TBB_INCLUDE_DIR}")
        endif()
        target_link_libraries(tbb INTERFACE ${_PAIMON_TBB_TARGET})
    endif()
    set(TBBAlt_FOUND TRUE)
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PC_TBB QUIET tbb)
    endif()

    find_path(TBB_INCLUDE_DIR
              NAMES tbb/tbb.h oneapi/tbb/tbb.h ${_PAIMON_TBB_FIND_ARGS}
              HINTS ${PC_TBB_INCLUDE_DIRS}
              PATH_SUFFIXES include)
    find_library(TBB_LIBRARY
                 NAMES tbb ${_PAIMON_TBB_FIND_ARGS}
                 HINTS ${PC_TBB_LIBRARY_DIRS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(TBBAlt REQUIRED_VARS TBB_LIBRARY TBB_INCLUDE_DIR)

    if(TBBAlt_FOUND AND NOT TARGET tbb)
        add_library(tbb UNKNOWN IMPORTED)
        set_target_properties(tbb
                              PROPERTIES IMPORTED_LOCATION "${TBB_LIBRARY}"
                                         INTERFACE_INCLUDE_DIRECTORIES
                                         "${TBB_INCLUDE_DIR}")
    endif()
endif()

unset(_PAIMON_TBB_FIND_ARGS)
unset(_PAIMON_TBB_ROOTS)
unset(_PAIMON_TBB_TARGET)
unset(_PAIMON_TBB_TARGETS)
