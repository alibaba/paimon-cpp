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

set(_PAIMON_ZLIB_ROOTS ${ZLIB_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_ZLIB_ROOTS "")
if(_PAIMON_ZLIB_ROOTS)
    set(_PAIMON_ZLIB_FIND_ARGS HINTS ${_PAIMON_ZLIB_ROOTS} NO_DEFAULT_PATH)
endif()

include(FindPackageUtils)
if(_PAIMON_ZLIB_ROOTS)
    find_package(ZLIB CONFIG QUIET ${_PAIMON_ZLIB_FIND_ARGS})
else()
    find_package(ZLIB QUIET)
endif()

if(TARGET ZLIB::ZLIB)
    paimon_find_target_headers(ZLIB_INCLUDE_DIR
                               ZLIB::ZLIB
                               NAMES
                               zlib.h
                               HINTS
                               ${ZLIB_INCLUDE_DIRS}
                               ${_PAIMON_ZLIB_FIND_ARGS})

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(ZLIBAlt REQUIRED_VARS ZLIB_INCLUDE_DIR)

    if(ZLIBAlt_FOUND AND NOT TARGET zlib)
        add_library(zlib INTERFACE IMPORTED)
        set_target_properties(zlib PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                              "${ZLIB_INCLUDE_DIR}")
        target_link_libraries(zlib INTERFACE ZLIB::ZLIB)
    endif()
    if(ZLIBAlt_FOUND)
        set(ZLIB_LIBRARIES ZLIB::ZLIB)
    endif()
else()
    find_path(ZLIB_INCLUDE_DIR
              NAMES zlib.h ${_PAIMON_ZLIB_FIND_ARGS}
              PATH_SUFFIXES include)
    find_library(ZLIB_LIBRARY
                 NAMES z zlib ${_PAIMON_ZLIB_FIND_ARGS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(ZLIBAlt REQUIRED_VARS ZLIB_LIBRARY ZLIB_INCLUDE_DIR)

    if(ZLIBAlt_FOUND AND NOT TARGET zlib)
        add_library(zlib UNKNOWN IMPORTED)
        set_target_properties(zlib
                              PROPERTIES IMPORTED_LOCATION "${ZLIB_LIBRARY}"
                                         INTERFACE_INCLUDE_DIRECTORIES
                                         "${ZLIB_INCLUDE_DIR}")
        set(ZLIB_LIBRARIES "${ZLIB_LIBRARY}")
    endif()
endif()

unset(_PAIMON_ZLIB_FIND_ARGS)
unset(_PAIMON_ZLIB_ROOTS)
