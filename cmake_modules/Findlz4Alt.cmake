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

if(lz4Alt_FOUND)
    return()
endif()

set(find_package_args)
if(lz4Alt_FIND_VERSION)
    list(APPEND find_package_args ${lz4Alt_FIND_VERSION})
endif()
if(lz4Alt_FIND_QUIETLY)
    list(APPEND find_package_args QUIET)
endif()

# Try to find lz4 using standard find_package first
find_package(lz4 ${find_package_args})
if(lz4_FOUND)
    set(lz4Alt_FOUND TRUE)
    if(TARGET LZ4::lz4)
        return()
    elseif(TARGET lz4::lz4)
        add_library(LZ4::lz4 ALIAS lz4::lz4)
        return()
    elseif(PAIMON_LZ4_USE_SHARED AND TARGET LZ4::lz4_shared)
        add_library(LZ4::lz4 ALIAS LZ4::lz4_shared)
        return()
    elseif(TARGET LZ4::lz4_static)
        add_library(LZ4::lz4 ALIAS LZ4::lz4_static)
        return()
    endif()
endif()

# Manual search
set(PAIMON_LIBRARY_PATH_SUFFIXES
    lib
    lib64
    lib/${CMAKE_LIBRARY_ARCHITECTURE})
set(PAIMON_INCLUDE_PATH_SUFFIXES include)

if(MSVC_TOOLCHAIN AND NOT DEFINED LZ4_MSVC_LIB_PREFIX)
    set(LZ4_MSVC_LIB_PREFIX "lib")
endif()
set(LZ4_LIB_NAME_BASE "${LZ4_MSVC_LIB_PREFIX}lz4")

if(PAIMON_LZ4_USE_SHARED)
    set(LZ4_LIB_NAMES)
    if(CMAKE_IMPORT_LIBRARY_SUFFIX)
        list(APPEND LZ4_LIB_NAMES
             "${CMAKE_IMPORT_LIBRARY_PREFIX}${LZ4_LIB_NAME_BASE}${CMAKE_IMPORT_LIBRARY_SUFFIX}")
    endif()
    list(APPEND LZ4_LIB_NAMES
         "${CMAKE_SHARED_LIBRARY_PREFIX}${LZ4_LIB_NAME_BASE}${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
    if(MSVC AND NOT DEFINED LZ4_MSVC_STATIC_LIB_SUFFIX)
        set(LZ4_MSVC_STATIC_LIB_SUFFIX "_static")
    endif()
    set(LZ4_STATIC_LIB_SUFFIX "${LZ4_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(LZ4_LIB_NAMES
        "${CMAKE_STATIC_LIBRARY_PREFIX}${LZ4_LIB_NAME_BASE}${LZ4_STATIC_LIB_SUFFIX}")
endif()

if(lz4_ROOT)
    find_library(LZ4_LIB
                 NAMES ${LZ4_LIB_NAMES}
                 PATHS ${lz4_ROOT}
                 PATH_SUFFIXES ${PAIMON_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_path(LZ4_INCLUDE_DIR
              NAMES lz4.h
              PATHS ${lz4_ROOT}
              NO_DEFAULT_PATH
              PATH_SUFFIXES ${PAIMON_INCLUDE_PATH_SUFFIXES})
else()
    # Try pkg-config first
    find_package(PkgConfig QUIET)
    pkg_check_modules(LZ4_PC liblz4 QUIET)
    if(LZ4_PC_FOUND)
        set(LZ4_INCLUDE_DIR "${LZ4_PC_INCLUDEDIR}")
        list(APPEND LZ4_PC_LIBRARY_DIRS "${LZ4_PC_LIBDIR}")
        find_library(LZ4_LIB
                     NAMES ${LZ4_LIB_NAMES}
                     PATHS ${LZ4_PC_LIBRARY_DIRS}
                     NO_DEFAULT_PATH
                     PATH_SUFFIXES ${PAIMON_LIBRARY_PATH_SUFFIXES})
    else()
        find_library(LZ4_LIB NAMES ${LZ4_LIB_NAMES}
                     PATH_SUFFIXES ${PAIMON_LIBRARY_PATH_SUFFIXES})
        find_path(LZ4_INCLUDE_DIR
                  NAMES lz4.h
                  PATH_SUFFIXES ${PAIMON_INCLUDE_PATH_SUFFIXES})
    endif()
endif()

find_package_handle_standard_args(lz4Alt REQUIRED_VARS LZ4_LIB LZ4_INCLUDE_DIR)

if(lz4Alt_FOUND)
    if(NOT TARGET LZ4::lz4)
        add_library(LZ4::lz4 UNKNOWN IMPORTED)
        set_target_properties(LZ4::lz4
                              PROPERTIES IMPORTED_LOCATION "${LZ4_LIB}"
                                         INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}")
    endif()
endif()
