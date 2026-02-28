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

cmake_minimum_required(VERSION 3.16)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

# https://cmake.org/cmake/help/latest/policy/CMP0135.html
#
# CMP0135 is for solving re-building and re-downloading.
# We don't have a real problem with the OLD behavior for now
# but we use the NEW behavior explicitly to suppress CMP0135
# warnings.
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE
        Release
        CACHE STRING "Choose the type of build.")
endif()

project(paimon
        VERSION 0.1.0
        DESCRIPTION "Paimon C++ Project")

string(TOUPPER "${CMAKE_BUILD_TYPE}" UPPERCASE_BUILD_TYPE)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(PAIMON_BUILD_STATIC "Build static library" ON)
option(PAIMON_BUILD_SHARED "Build shared library" ON)
option(PAIMON_BUILD_TESTS "Build tests" OFF)
option(PAIMON_USE_ASAN "Use Address Sanitizer" OFF)
option(PAIMON_USE_UBSAN "Use Undefined Behavior Sanitizer" OFF)
option(PAIMON_USE_CXX11_ABI "Use C++11 ABI" ON)
option(PAIMON_ENABLE_AVRO "Whether to enable avro file format" ON)
option(PAIMON_ENABLE_ORC "Whether to enable orc file format" ON)
option(PAIMON_ENABLE_LANCE "Whether to enable lance file format" OFF)
option(PAIMON_ENABLE_JINDO "Whether to enable jindo file system" OFF)
option(PAIMON_ENABLE_LUMINA "Whether to enable lumina vector index" ON)
option(PAIMON_ENABLE_LUCENE "Whether to enable lucene index" ON)

if(PAIMON_ENABLE_ORC)
    add_definitions(-DPAIMON_ENABLE_ORC)
endif()
if(PAIMON_ENABLE_AVRO)
    add_definitions(-DPAIMON_ENABLE_AVRO)
endif()
if(PAIMON_ENABLE_JINDO)
    add_definitions(-DPAIMON_ENABLE_JINDO)
endif()
if(PAIMON_USE_CXX11_ABI)
    add_definitions(-D_GLIBCXX_USE_CXX11_ABI=1)
else()
    # lance and lumina are provided in so file, cannot be recompiled with C++11 ABI off.
    if(PAIMON_ENABLE_LANCE)
        message(FATAL_ERROR "Lance cannot be enabled with C++11 ABI off")
    endif()
    if(PAIMON_ENABLE_LUMINA)
        message(FATAL_ERROR "Lumina cannot be enabled with C++11 ABI off")
    endif()
    add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)
endif()
if(PAIMON_ENABLE_LANCE)
    add_definitions(-DPAIMON_ENABLE_LANCE)
endif()
if(PAIMON_ENABLE_LUMINA)
    add_definitions(-DPAIMON_ENABLE_LUMINA)
endif()

if(PAIMON_ENABLE_LUCENE)
    add_definitions(-DPAIMON_ENABLE_LUCENE)
endif()

add_definitions(-DSNAPPY_CODEC_AVAILABLE)
add_definitions(-DZSTD_CODEC_AVAILABLE)
add_definitions(-DRAPIDJSON_HAS_STDSTRING)

set(CLANG_TIDY_BIN "clang-tidy")

set(PAIMON_SOURCE_DIR ${PROJECT_SOURCE_DIR})
set(PAIMON_BINARY_DIR ${PROJECT_BINARY_DIR})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules")
set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build_support")
set(PAIMON_CMAKE_INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")

include(GNUInstallDirs)
include(DefineOptions)

if(PAIMON_OPTIONAL_INSTALL)
    # Don't make the "install" target depend on the "all" target
    set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)
    set(INSTALL_IS_OPTIONAL OPTIONAL)
endif()

if(PAIMON_EXTRA_ERROR_CONTEXT)
    add_definitions(-DPAIMON_EXTRA_ERROR_CONTEXT)
endif()

if(NOT PAIMON_VERBOSE_LINT)
    set(PAIMON_LINT_QUIET "--quiet")
endif()

set(LINT_EXCLUSIONS_FILE ${BUILD_SUPPORT_DIR}/lint_exclusions.txt)

set(LINT_SOURCE_DIRS ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src
                     ${CMAKE_SOURCE_DIR}/test ${CMAKE_SOURCE_DIR}/examples)

set(CLANG_TIDY_OPTIONS
    ${PAIMON_LINT_QUIET}
    --compile_commands
    ${CMAKE_BINARY_DIR}/compile_commands.json
    --exclude_globs
    ${LINT_EXCLUSIONS_FILE})

if(PAIMON_LINT_GIT_DIFF_MODE)
    list(JOIN LINT_SOURCE_DIRS "|" _lint_dir_union)
    set(LINT_DIR_REGEX "^(${_lint_dir_union}\/?)")

    set(GIT_DIFF_FILE_PATH ${CMAKE_BINARY_DIR}/git_diff_files.txt)
    add_custom_target(update_diff_files
                      # get git-diff file list
                      COMMAND git diff-index --name-only --diff-filter=d --cached
                              ${PAIMON_LINT_GIT_TARGET_COMMIT} > ${GIT_DIFF_FILE_PATH}
                      # convert to absolute path
                      COMMAND sed -i \"s|^|${CMAKE_SOURCE_DIR}/|\" ${GIT_DIFF_FILE_PATH}
                      # filter with ${LINT_SOURCE_DIRS} dirs
                      COMMAND sed -nE -i '\\@${LINT_DIR_REGEX}@p' ${GIT_DIFF_FILE_PATH}
                      COMMENT "Generate git_diff_files.txt for git-diff lint"
                      BYPRODUCTS ${GIT_DIFF_FILE_PATH})

    list(APPEND CLANG_TIDY_OPTIONS --source_file @${GIT_DIFF_FILE_PATH})
else()
    list(APPEND CLANG_TIDY_OPTIONS --source_dir ${LINT_SOURCE_DIRS})
endif()

# runs clang-tidy and attempts to fix any warning automatically
add_custom_target(clang-tidy
                  ${PYTHON_EXECUTABLE}
                  ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
                  --clang_tidy_binary
                  ${CLANG_TIDY_BIN}
                  ${CLANG_TIDY_OPTIONS}
                  --fix)

# runs clang-tidy and exits with a non-zero exit code if any errors are found.
add_custom_target(check-clang-tidy
                  ${PYTHON_EXECUTABLE}
                  ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
                  --clang_tidy_binary
                  ${CLANG_TIDY_BIN}
                  ${CLANG_TIDY_OPTIONS})

if(PAIMON_LINT_GIT_DIFF_MODE)
    add_dependencies(clang-tidy update_diff_files)
    add_dependencies(check-clang-tidy update_diff_files)
endif()

if(UNIX)
    add_custom_target(iwyu
                      ${CMAKE_COMMAND}
                      -E
                      env
                      "PYTHON=${PYTHON_EXECUTABLE}"
                      ${BUILD_SUPPORT_DIR}/iwyu/iwyu.sh)
    add_custom_target(iwyu-all
                      ${CMAKE_COMMAND}
                      -E
                      env
                      "PYTHON=${PYTHON_EXECUTABLE}"
                      ${BUILD_SUPPORT_DIR}/iwyu/iwyu.sh
                      all)
endif(UNIX)

include(SetupCxxFlags)

# set compile output directory
string(TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)

# If build in-source, create the latest symlink. If build out-of-source, which
# is preferred, simply output the binaries in the build folder
if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
    set(BUILD_OUTPUT_ROOT_DIRECTORY
        "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}/")
    # Link build/latest to the current build directory, to avoid developers
    # accidentally running the latest debug build when in fact they're building
    # release builds.
    file(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
    if(NOT APPLE)
        set(MORE_ARGS "-T")
    endif()
    execute_process(COMMAND ln ${MORE_ARGS} -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
                            ${CMAKE_CURRENT_BINARY_DIR}/build/latest)
else()
    set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}/")
endif()

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(RUNTIME_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

include(BuildUtils)
enable_testing()

# Add common flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_COMMON_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PAIMON_CXXFLAGS}")

# For any C code, use the same flags. These flags don't contain C++ specific
# flags.
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CXX_COMMON_FLAGS} ${PAIMON_CXXFLAGS}")

# Remove --std=c++17 to avoid errors from C compilers
string(REPLACE "-std=c++17" "" CMAKE_C_FLAGS ${CMAKE_C_FLAGS})

# Add C++-only flags, like -std=c++11
set(CMAKE_CXX_FLAGS "${CXX_ONLY_FLAGS} ${CMAKE_CXX_FLAGS}")

include(san-config)

# Code coverage
if("${PAIMON_GENERATE_COVERAGE}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -DCOVERAGE_BUILD")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -DCOVERAGE_BUILD")
endif()

# CMAKE_CXX_FLAGS now fully assembled
message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(src)

#
# Visibility
#
# For generate_export_header() and add_compiler_export_flags().
include(GenerateExportHeader)
include(ExternalProject)
include(ThirdpartyToolchain)

add_custom_target(paimon_dependencies)

if(PAIMON_STATIC_LINK_LIBS)
    add_dependencies(paimon_dependencies ${PAIMON_STATIC_LINK_LIBS})
endif()

set(PAIMON_SHARED_PRIVATE_LINK_LIBS ${PAIMON_STATIC_LINK_LIBS})

add_subdirectory(third_party/roaring_bitmap EXCLUDE_FROM_ALL)
add_subdirectory(third_party/xxhash EXCLUDE_FROM_ALL)

if(PAIMON_ENABLE_LANCE)
    add_subdirectory(third_party/lance EXCLUDE_FROM_ALL)
    link_directories(third_party/lance/lance_lib/target/release)
    install(FILES ${PROJECT_SOURCE_DIR}/third_party/lance/lance_lib/target/release/liblance_lib_rc.so
            DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

list(APPEND PAIMON_LINK_LIBS ${CMAKE_DL_LIBS})
list(APPEND PAIMON_SHARED_INSTALL_INTERFACE_LIBS ${CMAKE_DL_LIBS})
if(PAIMON_ENABLE_LUMINA)
    add_subdirectory(third_party/lumina EXCLUDE_FROM_ALL)
    link_directories(third_party/lumina/lib)
    install(FILES ${PROJECT_SOURCE_DIR}/third_party/lumina/lib/liblumina.so
            DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

if(PAIMON_ENABLE_LUCENE)
    set(PAIMON_DICT_DEST "share/paimon/dict")

    install(DIRECTORY ${JIEBA_DICT_DIR}/
            DESTINATION ${PAIMON_DICT_DEST}
            FILES_MATCHING
            PATTERN "jieba.dict.utf8"
            PATTERN "hmm_model.utf8"
            PATTERN "idf.utf8"
            PATTERN "stop_words.utf8"
            PATTERN "user.dict.utf8"
            PATTERN "pos_dict"
            PATTERN ".git*" EXCLUDE
            PATTERN "*.md" EXCLUDE)
endif()

install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
        DESTINATION "include"
        FILES_MATCHING
        PATTERN "*.h")

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories("${CMAKE_SOURCE_DIR}/third_party/roaring_bitmap")
include_directories("${CMAKE_SOURCE_DIR}/third_party/xxhash")

if(PAIMON_ENABLE_LANCE)
    include_directories("${CMAKE_SOURCE_DIR}/third_party/lance")
endif()

if(PAIMON_ENABLE_LUMINA)
    include_directories("${CMAKE_SOURCE_DIR}/third_party/lumina/include")
endif()

include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
include_directories(SYSTEM ${TBB_INCLUDE_DIR})

include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
add_compile_definitions("GLOG_USE_GLOG_EXPORT")

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
set(PAIMON_VERSION_SCRIPT_FLAGS
    "-Wl,--version-script=${CMAKE_SOURCE_DIR}/src/paimon/symbols.map")

set(ENV{PAIMON_TEST_DATA} "${CMAKE_SOURCE_DIR}/test/test_data")

if(PAIMON_BUILD_TESTS)
    if(NOT PAIMON_ENABLE_ORC)
        message(FATAL_ERROR "PAIMON_ENABLE_ORC must be enabled if PAIMON_BUILD_TESTS is enable"
        )
    endif()
    if(NOT PAIMON_ENABLE_AVRO)
        message(FATAL_ERROR "PAIMON_ENABLE_AVRO must be enabled if PAIMON_BUILD_TESTS is enable"
        )
    endif()
    # Adding unit tests part of the "paimon" portion of the test suite
    add_custom_target(paimon-tests)
    build_gtest()

    add_custom_target(unittest
                      ctest
                      -j4
                      -L
                      unittest
                      --output-on-failure)
    add_dependencies(unittest paimon-tests)

    include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
    include_directories("${CMAKE_SOURCE_DIR}/test/")

    set(TEST_STATIC_LINK_LIBS
        "-Wl,--whole-archive"
        paimon_file_index_static
        paimon_global_index_static
        paimon_local_file_system_static
        paimon_mock_file_format_static
        "-Wl,--no-whole-archive"
        "-Wl,--no-as-needed"
        paimon_parquet_file_format_shared
        paimon_blob_file_format_shared
        "-Wl,--as-needed")

    if(PAIMON_ENABLE_LANCE)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--no-as-needed")
        list(APPEND TEST_STATIC_LINK_LIBS paimon_lance_file_format_shared)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--as-needed")
    endif()
    if(PAIMON_ENABLE_ORC)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--no-as-needed")
        list(APPEND TEST_STATIC_LINK_LIBS paimon_orc_file_format_shared)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--as-needed")
    endif()
    if(PAIMON_ENABLE_AVRO)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--no-as-needed")
        list(APPEND TEST_STATIC_LINK_LIBS paimon_avro_file_format_shared)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--as-needed")
    endif()
    if(PAIMON_ENABLE_JINDO)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--no-as-needed")
        list(APPEND TEST_STATIC_LINK_LIBS paimon_jindo_file_system_shared)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--as-needed")
    endif()
    if(PAIMON_ENABLE_LUMINA)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--no-as-needed")
        list(APPEND TEST_STATIC_LINK_LIBS paimon_lumina_index_shared)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--as-needed")
    endif()
    if(PAIMON_ENABLE_LUCENE)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--no-as-needed")
        list(APPEND TEST_STATIC_LINK_LIBS paimon_lucene_index_shared)
        list(APPEND TEST_STATIC_LINK_LIBS "-Wl,--as-needed")
    endif()
endif()

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/PaimonConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion)

configure_package_config_file("${CMAKE_CURRENT_SOURCE_DIR}/PaimonConfig.cmake.in"
                              "${CMAKE_CURRENT_BINARY_DIR}/PaimonConfig.cmake"
                              INSTALL_DESTINATION lib/cmake/Paimon)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/PaimonConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/PaimonConfigVersion.cmake"
        DESTINATION lib/cmake/Paimon)

config_summary_message()

add_subdirectory(src/paimon)
add_subdirectory(src/paimon/fs/local)
add_subdirectory(src/paimon/fs/jindo)
add_subdirectory(src/paimon/format/blob)
add_subdirectory(src/paimon/format/orc)
add_subdirectory(src/paimon/format/parquet)
add_subdirectory(src/paimon/format/avro)
add_subdirectory(src/paimon/format/lance)
add_subdirectory(src/paimon/global_index/lumina)
add_subdirectory(src/paimon/global_index/lucene)
add_subdirectory(src/paimon/testing/mock)
add_subdirectory(src/paimon/testing/utils)
add_subdirectory(test/inte)
