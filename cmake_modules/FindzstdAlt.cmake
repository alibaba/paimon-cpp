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

if(zstdAlt_FOUND)
    return()
endif()

set(find_package_args)
if(zstdAlt_FIND_VERSION)
    list(APPEND find_package_args ${zstdAlt_FIND_VERSION})
endif()
if(zstdAlt_FIND_QUIETLY)
    list(APPEND find_package_args QUIET)
endif()

# Try to find zstd using standard find_package first
find_package(zstd ${find_package_args})
if(zstd_FOUND)
    set(zstdAlt_FOUND TRUE)
    if(PAIMON_ZSTD_USE_SHARED)
        if(TARGET zstd::libzstd_shared)
            set(zstd_TARGET zstd::libzstd_shared)
        elseif(TARGET zstd::libzstd)
            set(zstd_TARGET zstd::libzstd)
        endif()
    else()
        if(TARGET zstd::libzstd_static)
            set(zstd_TARGET zstd::libzstd_static)
        elseif(TARGET zstd::libzstd)
            set(zstd_TARGET zstd::libzstd)
        endif()
    endif()
    if(zstd_TARGET)
        return()
    endif()
endif()

# Manual search
set(PAIMON_LIBRARY_PATH_SUFFIXES
    lib
    lib64
    lib/${CMAKE_LIBRARY_ARCHITECTURE})
set(PAIMON_INCLUDE_PATH_SUFFIXES include)

if(PAIMON_ZSTD_USE_SHARED)
    set(ZSTD_LIB_NAMES)
    if(CMAKE_IMPORT_LIBRARY_SUFFIX)
        list(APPEND ZSTD_LIB_NAMES
             "${CMAKE_IMPORT_LIBRARY_PREFIX}zstd${CMAKE_IMPORT_LIBRARY_SUFFIX}")
    endif()
    list(APPEND ZSTD_LIB_NAMES
         "${CMAKE_SHARED_LIBRARY_PREFIX}zstd${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
    set(ZSTD_LIB_NAMES
        "${CMAKE_STATIC_LIBRARY_PREFIX}zstd${CMAKE_STATIC_LIBRARY_SUFFIX}")
endif()

if(zstd_ROOT)
    find_library(zstd_LIB
                 NAMES ${ZSTD_LIB_NAMES}
                 PATHS ${zstd_ROOT}
                 PATH_SUFFIXES ${PAIMON_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_path(zstd_INCLUDE_DIR
              NAMES zstd.h
              PATHS ${zstd_ROOT}
              NO_DEFAULT_PATH
              PATH_SUFFIXES ${PAIMON_INCLUDE_PATH_SUFFIXES})
else()
    # Try pkg-config first
    find_package(PkgConfig QUIET)
    pkg_check_modules(zstd_PC libzstd QUIET)
    if(zstd_PC_FOUND)
        set(zstd_INCLUDE_DIR "${zstd_PC_INCLUDEDIR}")
        list(APPEND zstd_PC_LIBRARY_DIRS "${zstd_PC_LIBDIR}")
        find_library(zstd_LIB
                     NAMES ${ZSTD_LIB_NAMES}
                     PATHS ${zstd_PC_LIBRARY_DIRS}
                     NO_DEFAULT_PATH
                     PATH_SUFFIXES ${PAIMON_LIBRARY_PATH_SUFFIXES})
    else()
        find_library(zstd_LIB NAMES ${ZSTD_LIB_NAMES}
                     PATH_SUFFIXES ${PAIMON_LIBRARY_PATH_SUFFIXES})
        find_path(zstd_INCLUDE_DIR
                  NAMES zstd.h
                  PATH_SUFFIXES ${PAIMON_INCLUDE_PATH_SUFFIXES})
    endif()
endif()

find_package_handle_standard_args(zstdAlt REQUIRED_VARS zstd_LIB zstd_INCLUDE_DIR)

if(zstdAlt_FOUND)
    if(PAIMON_ZSTD_USE_SHARED)
        set(zstd_TARGET zstd::libzstd_shared)
        set(zstd_TARGET_TYPE SHARED)
    else()
        set(zstd_TARGET zstd::libzstd_static)
        set(zstd_TARGET_TYPE STATIC)
    endif()

    if(NOT TARGET ${zstd_TARGET})
        add_library(${zstd_TARGET} ${zstd_TARGET_TYPE} IMPORTED)
        set_target_properties(${zstd_TARGET}
                              PROPERTIES IMPORTED_LOCATION "${zstd_LIB}"
                                         INTERFACE_INCLUDE_DIRECTORIES "${zstd_INCLUDE_DIR}")
    endif()
endif()
