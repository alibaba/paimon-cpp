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

set(_PAIMON_ZSTD_ROOTS ${zstd_ROOT} ${ZSTD_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_ZSTD_ROOTS "")
if(_PAIMON_ZSTD_ROOTS)
    set(_PAIMON_ZSTD_FIND_ARGS HINTS ${_PAIMON_ZSTD_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(zstd CONFIG QUIET ${_PAIMON_ZSTD_FIND_ARGS})

set(_PAIMON_ZSTD_TARGETS)
if(PAIMON_DEPENDENCY_USE_SHARED)
    list(APPEND _PAIMON_ZSTD_TARGETS zstd::libzstd_shared zstd::libzstd)
endif()
list(APPEND
     _PAIMON_ZSTD_TARGETS
     zstd::libzstd_static
     zstd::libzstd
     zstd::zstd)

foreach(_target IN LISTS _PAIMON_ZSTD_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_ZSTD_TARGET ${_target})
        break()
    endif()
endforeach()

if(_PAIMON_ZSTD_TARGET)
    get_target_property(ZSTD_INCLUDE_DIR ${_PAIMON_ZSTD_TARGET}
                        INTERFACE_INCLUDE_DIRECTORIES)
    if(NOT TARGET zstd)
        add_library(zstd INTERFACE IMPORTED)
        if(ZSTD_INCLUDE_DIR)
            set_target_properties(zstd PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                  "${ZSTD_INCLUDE_DIR}")
        endif()
        target_link_libraries(zstd INTERFACE ${_PAIMON_ZSTD_TARGET})
    endif()
    set(ZSTD_LIBRARIES ${_PAIMON_ZSTD_TARGET})
    set(zstdAlt_FOUND TRUE)
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PC_zstd QUIET libzstd)
    endif()

    find_path(ZSTD_INCLUDE_DIR
              NAMES zstd.h ${_PAIMON_ZSTD_FIND_ARGS}
              HINTS ${PC_zstd_INCLUDE_DIRS}
              PATH_SUFFIXES include)
    find_library(ZSTD_LIBRARY
                 NAMES zstd libzstd ${_PAIMON_ZSTD_FIND_ARGS}
                 HINTS ${PC_zstd_LIBRARY_DIRS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(zstdAlt REQUIRED_VARS ZSTD_LIBRARY ZSTD_INCLUDE_DIR)

    if(zstdAlt_FOUND AND NOT TARGET zstd)
        add_library(zstd UNKNOWN IMPORTED)
        set_target_properties(zstd
                              PROPERTIES IMPORTED_LOCATION "${ZSTD_LIBRARY}"
                                         INTERFACE_INCLUDE_DIRECTORIES
                                         "${ZSTD_INCLUDE_DIR}")
        set(ZSTD_LIBRARIES "${ZSTD_LIBRARY}")
    endif()
endif()

unset(_PAIMON_ZSTD_FIND_ARGS)
unset(_PAIMON_ZSTD_ROOTS)
unset(_PAIMON_ZSTD_TARGET)
unset(_PAIMON_ZSTD_TARGETS)
