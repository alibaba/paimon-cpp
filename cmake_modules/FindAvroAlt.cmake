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

set(_PAIMON_AVRO_ROOTS ${Avro_ROOT} ${AVRO_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_AVRO_ROOTS "")
if(_PAIMON_AVRO_ROOTS)
    set(_PAIMON_AVRO_FIND_ARGS HINTS ${_PAIMON_AVRO_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(PC_Avro QUIET avro-cpp)
endif()

find_path(AVRO_INCLUDE_DIR
          NAMES avro/Decoder.hh ${_PAIMON_AVRO_FIND_ARGS}
          HINTS ${PC_Avro_INCLUDE_DIRS}
          PATH_SUFFIXES include)

if(PAIMON_DEPENDENCY_USE_SHARED)
    set(_PAIMON_AVRO_LIBRARY_NAMES avrocpp avrocpp_s)
else()
    set(_PAIMON_AVRO_LIBRARY_NAMES avrocpp_s avrocpp)
endif()

find_library(AVRO_LIBRARY
             NAMES ${_PAIMON_AVRO_LIBRARY_NAMES} ${_PAIMON_AVRO_FIND_ARGS}
             HINTS ${PC_Avro_LIBRARY_DIRS}
             PATH_SUFFIXES lib lib64)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(AvroAlt REQUIRED_VARS AVRO_LIBRARY AVRO_INCLUDE_DIR)

if(AvroAlt_FOUND AND NOT TARGET avro)
    add_library(avro UNKNOWN IMPORTED)
    set_target_properties(avro
                          PROPERTIES IMPORTED_LOCATION "${AVRO_LIBRARY}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${AVRO_INCLUDE_DIR}")
    foreach(_dependency zlib zstd snappy)
        if(TARGET ${_dependency})
            target_link_libraries(avro INTERFACE ${_dependency})
        endif()
    endforeach()
    set(AVRO_LIBRARIES "${AVRO_LIBRARY}")
endif()

unset(_PAIMON_AVRO_FIND_ARGS)
unset(_PAIMON_AVRO_LIBRARY_NAMES)
unset(_PAIMON_AVRO_ROOTS)
