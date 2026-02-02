# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if(glogAlt_FOUND)
    return()
endif()

set(find_package_args)
if(glogAlt_FIND_VERSION)
    list(APPEND find_package_args ${glogAlt_FIND_VERSION})
endif()
if(glogAlt_FIND_QUIETLY)
    list(APPEND find_package_args QUIET)
endif()

# Try to find glog using standard find_package first
find_package(glog ${find_package_args})
if(glog_FOUND)
    set(glogAlt_FOUND TRUE)
    if(TARGET glog::glog)
        return()
    endif()
endif()

# Manual search
set(PAIMON_LIBRARY_PATH_SUFFIXES
    lib
    lib64
    lib/${CMAKE_LIBRARY_ARCHITECTURE})
set(PAIMON_INCLUDE_PATH_SUFFIXES include)

if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
    set(GLOG_LIB_SUFFIX "d")
else()
    set(GLOG_LIB_SUFFIX "")
endif()

if(PAIMON_GLOG_USE_SHARED)
    set(GLOG_LIB_NAMES)
    if(CMAKE_IMPORT_LIBRARY_SUFFIX)
        list(APPEND GLOG_LIB_NAMES
             "${CMAKE_IMPORT_LIBRARY_PREFIX}glog${GLOG_LIB_SUFFIX}${CMAKE_IMPORT_LIBRARY_SUFFIX}")
    endif()
    list(APPEND GLOG_LIB_NAMES
         "${CMAKE_SHARED_LIBRARY_PREFIX}glog${GLOG_LIB_SUFFIX}${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
    set(GLOG_LIB_NAMES
        "${CMAKE_STATIC_LIBRARY_PREFIX}glog${GLOG_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")
endif()

if(glog_ROOT)
    find_library(glog_LIB
                 NAMES ${GLOG_LIB_NAMES}
                 PATHS ${glog_ROOT}
                 PATH_SUFFIXES ${PAIMON_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_path(glog_INCLUDE_DIR
              NAMES glog/logging.h
              PATHS ${glog_ROOT}
              NO_DEFAULT_PATH
              PATH_SUFFIXES ${PAIMON_INCLUDE_PATH_SUFFIXES})
else()
    find_library(glog_LIB NAMES ${GLOG_LIB_NAMES}
                 PATH_SUFFIXES ${PAIMON_LIBRARY_PATH_SUFFIXES})
    find_path(glog_INCLUDE_DIR
              NAMES glog/logging.h
              PATH_SUFFIXES ${PAIMON_INCLUDE_PATH_SUFFIXES})
endif()

find_package_handle_standard_args(glogAlt REQUIRED_VARS glog_LIB glog_INCLUDE_DIR)

if(glogAlt_FOUND)
    if(NOT TARGET glog::glog)
        if(PAIMON_GLOG_USE_SHARED)
            set(glog_TARGET_TYPE SHARED)
        else()
            set(glog_TARGET_TYPE STATIC)
        endif()
        add_library(glog::glog ${glog_TARGET_TYPE} IMPORTED)
        set_target_properties(glog::glog
                              PROPERTIES IMPORTED_LOCATION "${glog_LIB}"
                                         INTERFACE_INCLUDE_DIRECTORIES "${glog_INCLUDE_DIR}"
                                         INTERFACE_COMPILE_DEFINITIONS "GLOG_USE_GLOG_EXPORT")
    endif()
endif()
