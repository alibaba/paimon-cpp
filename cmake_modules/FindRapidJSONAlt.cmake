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

set(_PAIMON_RAPIDJSON_ROOTS ${RapidJSON_ROOT} ${RAPIDJSON_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_RAPIDJSON_ROOTS "")
if(_PAIMON_RAPIDJSON_ROOTS)
    set(_PAIMON_RAPIDJSON_FIND_ARGS HINTS ${_PAIMON_RAPIDJSON_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(RapidJSON CONFIG QUIET ${_PAIMON_RAPIDJSON_FIND_ARGS})

set(_PAIMON_RAPIDJSON_TARGETS RapidJSON RapidJSON::RapidJSON)
foreach(_target IN LISTS _PAIMON_RAPIDJSON_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_RAPIDJSON_TARGET ${_target})
        break()
    endif()
endforeach()

if(_PAIMON_RAPIDJSON_TARGET)
    if(NOT TARGET RapidJSON)
        add_library(RapidJSON INTERFACE IMPORTED)
        target_link_libraries(RapidJSON INTERFACE ${_PAIMON_RAPIDJSON_TARGET})
    endif()
    get_target_property(RAPIDJSON_INCLUDE_DIR RapidJSON INTERFACE_INCLUDE_DIRECTORIES)
    set(RapidJSONAlt_FOUND TRUE)
else()
    find_path(RAPIDJSON_INCLUDE_DIR
              NAMES rapidjson/rapidjson.h ${_PAIMON_RAPIDJSON_FIND_ARGS}
              PATH_SUFFIXES include)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(RapidJSONAlt REQUIRED_VARS RAPIDJSON_INCLUDE_DIR)

    if(RapidJSONAlt_FOUND AND NOT TARGET RapidJSON)
        add_library(RapidJSON INTERFACE IMPORTED)
        target_include_directories(RapidJSON INTERFACE "${RAPIDJSON_INCLUDE_DIR}")
    endif()
endif()

unset(_PAIMON_RAPIDJSON_FIND_ARGS)
unset(_PAIMON_RAPIDJSON_ROOTS)
unset(_PAIMON_RAPIDJSON_TARGET)
unset(_PAIMON_RAPIDJSON_TARGETS)
