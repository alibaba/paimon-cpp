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

set(_PAIMON_ORC_ROOTS ${ORC_ROOT} ${orc_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_ORC_ROOTS "")
if(_PAIMON_ORC_ROOTS)
    set(_PAIMON_ORC_FIND_ARGS HINTS ${_PAIMON_ORC_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(orc CONFIG QUIET ${_PAIMON_ORC_FIND_ARGS})
find_package(ORC CONFIG QUIET ${_PAIMON_ORC_FIND_ARGS})

set(_PAIMON_ORC_TARGETS orc::orc ORC::orc ORC::ORC orc)
foreach(_target IN LISTS _PAIMON_ORC_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_ORC_TARGET ${_target})
        break()
    endif()
endforeach()

if(_PAIMON_ORC_TARGET)
    if(NOT TARGET orc::orc)
        add_library(orc::orc INTERFACE IMPORTED)
        target_link_libraries(orc::orc INTERFACE ${_PAIMON_ORC_TARGET})
    endif()

    foreach(_dependency
            zstd
            snappy
            lz4
            zlib
            libprotobuf)
        if(TARGET ${_dependency})
            target_link_libraries(orc::orc INTERFACE ${_dependency})
        endif()
    endforeach()

    get_target_property(ORC_INCLUDE_DIR orc::orc INTERFACE_INCLUDE_DIRECTORIES)
    set(ORCAlt_FOUND TRUE)
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PC_ORC QUIET orc)
    endif()

    find_path(ORC_INCLUDE_DIR
              NAMES orc/OrcFile.hh ${_PAIMON_ORC_FIND_ARGS}
              HINTS ${PC_ORC_INCLUDE_DIRS}
              PATH_SUFFIXES include)
    find_library(ORC_LIBRARY
                 NAMES orc ${_PAIMON_ORC_FIND_ARGS}
                 HINTS ${PC_ORC_LIBRARY_DIRS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(ORCAlt REQUIRED_VARS ORC_LIBRARY ORC_INCLUDE_DIR)

    if(ORCAlt_FOUND AND NOT TARGET orc::orc)
        add_library(orc::orc UNKNOWN IMPORTED)
        set_target_properties(orc::orc
                              PROPERTIES IMPORTED_LOCATION "${ORC_LIBRARY}"
                                         INTERFACE_INCLUDE_DIRECTORIES
                                         "${ORC_INCLUDE_DIR}")
        foreach(_dependency
                zstd
                snappy
                lz4
                zlib
                libprotobuf)
            if(TARGET ${_dependency})
                target_link_libraries(orc::orc INTERFACE ${_dependency})
            endif()
        endforeach()
    endif()
endif()

unset(_PAIMON_ORC_FIND_ARGS)
unset(_PAIMON_ORC_ROOTS)
unset(_PAIMON_ORC_TARGET)
unset(_PAIMON_ORC_TARGETS)
