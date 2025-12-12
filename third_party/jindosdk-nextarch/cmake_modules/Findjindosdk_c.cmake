# Copyright 2024-present Alibaba Inc.
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

# JINDOSDK_ROOT environmental variable is used to check for JINDOSDK headers and dynamic library

# JINDOSDK_INCLUDE_DIR: directory containing headers
# JINDOSDK_LIBRARY: path to libjindosdk_c.so
# JINDOSDK_FOUND: whether JINDOSDK has been found

if(NOT JINDOSDK_ROOT)
    if(DEFINED ENV{JINDOSDK_ROOT})
        set(JINDOSDK_ROOT "$ENV{JINDOSDK_ROOT}")
    endif()
endif()

if(NOT "${JINDOSDK_ROOT}" STREQUAL "")
    file(TO_CMAKE_PATH "${JINDOSDK_ROOT}" _jindosdk_path)
endif()

message(STATUS "JINDOSDK_ROOT: ${JINDOSDK_ROOT}")

find_path(JINDOSDK_INCLUDE_DIR jdo_api.h
          HINTS ${_jindosdk_path}
          NO_DEFAULT_PATH
          PATH_SUFFIXES "include")

find_library(JINDOSDK_LIBRARY
             NAMES jindosdk_c
             HINTS ${_jindosdk_path}
             PATH_SUFFIXES "lib/native")

if(JINDOSDK_INCLUDE_DIR AND JINDOSDK_LIBRARY)
    set(JINDOSDK_FOUND TRUE)
    set(JINDOSDK_HEADER_NAME jdo_api.h)
    set(JINDOSDK_HEADER ${JINDOSDK_INCLUDE_DIR}/${JINDOSDK_HEADER_NAME})
else()
    set(JINDOSDK_FOUND FALSE)
endif()

if(JINDOSDK_FOUND)
    message(STATUS "Found the JINDOSDK header: ${JINDOSDK_HEADER}")
    message(STATUS "Found the JINDOSDK library: ${JINDOSDK_LIBRARY}")
else()
    if(_jindosdk_path)
        set(JINDOSDK_ERR_MSG "Could not find JINDOSDK. Looked in ${_jindosdk_path}.")
    else()
        set(JINDOSDK_ERR_MSG "Could not find JINDOSDK in system search paths.")
    endif()

    if(JINDOSDK_FIND_REQUIRED)
        message(FATAL_ERROR "${JINDOSDK_ERR_MSG}")
    else()
        message(STATUS "${JINDOSDK_ERR_MSG}")
    endif()
endif()

mark_as_advanced(JINDOSDK_INCLUDE_DIR JINDOSDK_LIBRARY)

if(JINDOSDK_FOUND AND NOT TARGET JINDOSDK::JINDOSDK)
    add_library(JINDOSDK::JINDOSDK UNKNOWN IMPORTED)
    set_target_properties(JINDOSDK::JINDOSDK
                          PROPERTIES IMPORTED_LOCATION "${JINDOSDK_LIBRARY}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${JINDOSDK_INCLUDE_DIR}")
endif()
