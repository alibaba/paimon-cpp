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

function(paimon_find_target_headers OUT_VAR TARGET_NAME)
    set(options NO_DEFAULT_PATH)
    set(one_value_args)
    set(multi_value_args NAMES HINTS PATH_SUFFIXES)
    cmake_parse_arguments(ARG
                          "${options}"
                          "${one_value_args}"
                          "${multi_value_args}"
                          ${ARGN})

    if(NOT TARGET ${TARGET_NAME} OR NOT ARG_NAMES)
        set(${OUT_VAR}
            "${OUT_VAR}-NOTFOUND"
            PARENT_SCOPE)
        return()
    endif()

    set(_target_include_dirs)
    foreach(_property INTERFACE_INCLUDE_DIRECTORIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES
                      INCLUDE_DIRECTORIES)
        get_target_property(_property_value ${TARGET_NAME} ${_property})
        if(_property_value AND NOT _property_value MATCHES "-NOTFOUND$")
            list(APPEND _target_include_dirs ${_property_value})
        endif()
    endforeach()

    set(_search_dirs)
    foreach(_dir IN LISTS _target_include_dirs)
        if(_dir MATCHES "^\\$<BUILD_INTERFACE:(.*)>$")
            list(APPEND _search_dirs "${CMAKE_MATCH_1}")
        elseif(_dir MATCHES "^\\$<INSTALL_INTERFACE:(.*)>$")
            if(IS_ABSOLUTE "${CMAKE_MATCH_1}")
                list(APPEND _search_dirs "${CMAKE_MATCH_1}")
            endif()
        elseif(NOT _dir MATCHES "^\\$<")
            list(APPEND _search_dirs "${_dir}")
        endif()
    endforeach()

    list(APPEND _search_dirs ${ARG_HINTS})
    if(_search_dirs)
        list(REMOVE_DUPLICATES _search_dirs)
    endif()

    string(MAKE_C_IDENTIFIER "${TARGET_NAME}_${ARG_NAMES}" _header_var_suffix)
    set(_header_dir_var "PAIMON_${_header_var_suffix}_HEADER_DIR")
    set(_find_args NAMES ${ARG_NAMES})
    if(_search_dirs)
        list(APPEND _find_args HINTS ${_search_dirs})
    endif()
    if(ARG_PATH_SUFFIXES)
        list(APPEND _find_args PATH_SUFFIXES ${ARG_PATH_SUFFIXES})
    endif()
    list(APPEND _find_args NO_DEFAULT_PATH)

    unset(${_header_dir_var} CACHE)
    find_path(${_header_dir_var} ${_find_args})

    if(NOT ${_header_dir_var})
        get_property(_partial_targets GLOBAL PROPERTY PAIMON_PARTIAL_SYSTEM_TARGETS)
        list(APPEND _partial_targets "${TARGET_NAME}: ${ARG_NAMES}")
        set_property(GLOBAL PROPERTY PAIMON_PARTIAL_SYSTEM_TARGETS "${_partial_targets}")
    endif()

    set(${OUT_VAR}
        "${${_header_dir_var}}"
        PARENT_SCOPE)

    unset(${_header_dir_var} CACHE)
    unset(_find_args)
    unset(_header_dir_var)
    unset(_header_var_suffix)
    unset(_partial_targets)
    unset(_property_value)
    unset(_search_dirs)
    unset(_target_include_dirs)
endfunction()
