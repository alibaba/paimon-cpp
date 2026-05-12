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

set(_PAIMON_SNAPPY_ROOTS ${Snappy_ROOT} ${SNAPPY_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_SNAPPY_ROOTS "")
if(_PAIMON_SNAPPY_ROOTS)
    set(_PAIMON_SNAPPY_FIND_ARGS HINTS ${_PAIMON_SNAPPY_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(Snappy CONFIG QUIET ${_PAIMON_SNAPPY_FIND_ARGS})

set(_PAIMON_SNAPPY_TARGETS Snappy::snappy snappy::snappy)
foreach(_target IN LISTS _PAIMON_SNAPPY_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_SNAPPY_TARGET ${_target})
        break()
    endif()
endforeach()

if(_PAIMON_SNAPPY_TARGET)
    get_target_property(SNAPPY_INCLUDE_DIR ${_PAIMON_SNAPPY_TARGET}
                        INTERFACE_INCLUDE_DIRECTORIES)
    if(NOT TARGET snappy)
        add_library(snappy INTERFACE IMPORTED)
        if(SNAPPY_INCLUDE_DIR)
            set_target_properties(snappy PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                    "${SNAPPY_INCLUDE_DIR}")
        endif()
        target_link_libraries(snappy INTERFACE ${_PAIMON_SNAPPY_TARGET})
    endif()
    set(SNAPPY_LIBRARIES ${_PAIMON_SNAPPY_TARGET})
    set(SnappyAlt_FOUND TRUE)
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PC_Snappy QUIET snappy)
    endif()

    find_path(SNAPPY_INCLUDE_DIR
              NAMES snappy.h ${_PAIMON_SNAPPY_FIND_ARGS}
              HINTS ${PC_Snappy_INCLUDE_DIRS}
              PATH_SUFFIXES include)
    find_library(SNAPPY_LIBRARY
                 NAMES snappy ${_PAIMON_SNAPPY_FIND_ARGS}
                 HINTS ${PC_Snappy_LIBRARY_DIRS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(SnappyAlt REQUIRED_VARS SNAPPY_LIBRARY
                                                              SNAPPY_INCLUDE_DIR)

    if(SnappyAlt_FOUND AND NOT TARGET snappy)
        add_library(snappy UNKNOWN IMPORTED)
        set_target_properties(snappy
                              PROPERTIES IMPORTED_LOCATION "${SNAPPY_LIBRARY}"
                                         INTERFACE_INCLUDE_DIRECTORIES
                                         "${SNAPPY_INCLUDE_DIR}")
        set(SNAPPY_LIBRARIES "${SNAPPY_LIBRARY}")
    endif()
endif()

unset(_PAIMON_SNAPPY_FIND_ARGS)
unset(_PAIMON_SNAPPY_ROOTS)
unset(_PAIMON_SNAPPY_TARGET)
unset(_PAIMON_SNAPPY_TARGETS)
