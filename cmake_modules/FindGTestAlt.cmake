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

set(_PAIMON_GTEST_ROOTS ${GTest_ROOT} ${GTEST_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_GTEST_ROOTS "")
if(_PAIMON_GTEST_ROOTS)
    set(_PAIMON_GTEST_FIND_ARGS HINTS ${_PAIMON_GTEST_ROOTS} NO_DEFAULT_PATH)
endif()

include(FindPackageUtils)
find_package(GTest CONFIG QUIET ${_PAIMON_GTEST_FIND_ARGS})

if(NOT TARGET GTest::gtest
   OR NOT TARGET GTest::gtest_main
   OR NOT TARGET GTest::gmock)
    find_path(GTEST_INCLUDE_DIR
              NAMES gtest/gtest.h ${_PAIMON_GTEST_FIND_ARGS}
              PATH_SUFFIXES include)
    find_library(GTEST_LIBRARY
                 NAMES gtest ${_PAIMON_GTEST_FIND_ARGS}
                 PATH_SUFFIXES lib lib64)
    find_library(GTEST_MAIN_LIBRARY
                 NAMES gtest_main ${_PAIMON_GTEST_FIND_ARGS}
                 PATH_SUFFIXES lib lib64)
    find_library(GMOCK_LIBRARY
                 NAMES gmock ${_PAIMON_GTEST_FIND_ARGS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(
        GTestAlt REQUIRED_VARS GTEST_INCLUDE_DIR GTEST_LIBRARY GTEST_MAIN_LIBRARY
                               GMOCK_LIBRARY)

    if(GTestAlt_FOUND)
        if(NOT TARGET GTest::gtest)
            add_library(GTest::gtest UNKNOWN IMPORTED)
            set_target_properties(GTest::gtest
                                  PROPERTIES IMPORTED_LOCATION "${GTEST_LIBRARY}"
                                             INTERFACE_INCLUDE_DIRECTORIES
                                             "${GTEST_INCLUDE_DIR}")
        endif()
        if(NOT TARGET GTest::gtest_main)
            add_library(GTest::gtest_main UNKNOWN IMPORTED)
            set_target_properties(GTest::gtest_main
                                  PROPERTIES IMPORTED_LOCATION "${GTEST_MAIN_LIBRARY}"
                                             INTERFACE_INCLUDE_DIRECTORIES
                                             "${GTEST_INCLUDE_DIR}")
        endif()
        if(NOT TARGET GTest::gmock)
            add_library(GTest::gmock UNKNOWN IMPORTED)
            set_target_properties(GTest::gmock
                                  PROPERTIES IMPORTED_LOCATION "${GMOCK_LIBRARY}"
                                             INTERFACE_INCLUDE_DIRECTORIES
                                             "${GTEST_INCLUDE_DIR}")
        endif()
    endif()
else()
    paimon_find_target_headers(GTEST_INCLUDE_DIR
                               GTest::gtest
                               NAMES
                               gtest/gtest.h
                               ${_PAIMON_GTEST_FIND_ARGS})
    paimon_find_target_headers(GMOCK_INCLUDE_DIR
                               GTest::gmock
                               NAMES
                               gmock/gmock.h
                               ${_PAIMON_GTEST_FIND_ARGS})

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(GTestAlt REQUIRED_VARS GTEST_INCLUDE_DIR
                                                             GMOCK_INCLUDE_DIR)
endif()

if(GTestAlt_FOUND)
    find_package(Threads REQUIRED)
    set(GTEST_LINK_TOOLCHAIN GTest::gtest_main GTest::gtest GTest::gmock Threads::Threads)
endif()

unset(_PAIMON_GTEST_FIND_ARGS)
unset(_PAIMON_GTEST_ROOTS)
