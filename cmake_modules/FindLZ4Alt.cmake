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

set(_PAIMON_LZ4_ROOTS ${LZ4_ROOT} ${lz4_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_LZ4_ROOTS "")
if(_PAIMON_LZ4_ROOTS)
    set(_PAIMON_LZ4_FIND_ARGS HINTS ${_PAIMON_LZ4_ROOTS} NO_DEFAULT_PATH)
endif()

include(FindPackageUtils)
find_package(lz4 CONFIG QUIET ${_PAIMON_LZ4_FIND_ARGS})
find_package(LZ4 CONFIG QUIET ${_PAIMON_LZ4_FIND_ARGS})

set(_PAIMON_LZ4_TARGETS lz4::lz4 lz4::lz4_static LZ4::lz4 LZ4::LZ4)
foreach(_target IN LISTS _PAIMON_LZ4_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_LZ4_TARGET ${_target})
        break()
    endif()
endforeach()

if(_PAIMON_LZ4_TARGET)
    paimon_find_target_headers(LZ4_INCLUDE_DIR
                               ${_PAIMON_LZ4_TARGET}
                               NAMES
                               lz4.h
                               ${_PAIMON_LZ4_FIND_ARGS})

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(LZ4Alt REQUIRED_VARS LZ4_INCLUDE_DIR)

    if(LZ4Alt_FOUND AND NOT TARGET lz4)
        add_library(lz4 INTERFACE IMPORTED)
        set_target_properties(lz4 PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                             "${LZ4_INCLUDE_DIR}")
        target_link_libraries(lz4 INTERFACE ${_PAIMON_LZ4_TARGET})
    endif()
    if(LZ4Alt_FOUND)
        set(LZ4_LIBRARIES ${_PAIMON_LZ4_TARGET})
    endif()
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PC_LZ4 QUIET liblz4)
    endif()

    find_path(LZ4_INCLUDE_DIR
              NAMES lz4.h ${_PAIMON_LZ4_FIND_ARGS}
              HINTS ${PC_LZ4_INCLUDE_DIRS}
              PATH_SUFFIXES include)
    find_library(LZ4_LIBRARY
                 NAMES lz4 liblz4 ${_PAIMON_LZ4_FIND_ARGS}
                 HINTS ${PC_LZ4_LIBRARY_DIRS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(LZ4Alt REQUIRED_VARS LZ4_LIBRARY LZ4_INCLUDE_DIR)

    if(LZ4Alt_FOUND AND NOT TARGET lz4)
        add_library(lz4 UNKNOWN IMPORTED)
        set_target_properties(lz4
                              PROPERTIES IMPORTED_LOCATION "${LZ4_LIBRARY}"
                                         INTERFACE_INCLUDE_DIRECTORIES
                                         "${LZ4_INCLUDE_DIR}")
        set(LZ4_LIBRARIES "${LZ4_LIBRARY}")
    endif()
endif()

unset(_PAIMON_LZ4_FIND_ARGS)
unset(_PAIMON_LZ4_ROOTS)
unset(_PAIMON_LZ4_TARGET)
unset(_PAIMON_LZ4_TARGETS)
