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

set(THIRDPARTY_LOG_OPTIONS
    LOG_CONFIGURE
    1
    LOG_BUILD
    1
    LOG_INSTALL
    1
    LOG_DOWNLOAD
    1)
set(THIRDPARTY_CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}")
if(CMAKE_GENERATOR_TOOLSET)
    list(APPEND THIRDPARTY_CONFIGURE_COMMAND -T "${CMAKE_GENERATOR_TOOLSET}")
endif()

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

set(EP_COMMON_TOOLCHAIN "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                        "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}")

macro(set_urls URLS)
    set(${URLS} ${ARGN})
endmacro()

set(THIRDPARTY_DIR "${CMAKE_SOURCE_DIR}/third_party")
# Read toolchain versions from third_party/versions.txt
file(STRINGS "${THIRDPARTY_DIR}/versions.txt" TOOLCHAIN_VERSIONS_TXT)
foreach(_VERSION_ENTRY ${TOOLCHAIN_VERSIONS_TXT})
    # Exclude comments
    if(NOT
       ((_VERSION_ENTRY MATCHES "^[^#][A-Za-z0-9-_]+_VERSION=")
        OR (_VERSION_ENTRY MATCHES "^[^#][A-Za-z0-9-_]+_CHECKSUM=")
        OR (_VERSION_ENTRY MATCHES "^[^#][A-Za-z0-9-_]+_PKG_NAME=")))
        continue()
    endif()

    string(REGEX MATCH "^[^=]*" _VARIABLE_NAME ${_VERSION_ENTRY})
    string(REPLACE "${_VARIABLE_NAME}=" "" _VARIABLE_VALUE ${_VERSION_ENTRY})

    # Skip blank or malformed lines
    if(_VARIABLE_VALUE STREQUAL "")
        continue()
    endif()

    set(${_VARIABLE_NAME} ${_VARIABLE_VALUE})

    if(_VARIABLE_NAME MATCHES "_PKG_NAME$")
        # Expand version to package name
        string(CONFIGURE "${${_VARIABLE_NAME}}" _EXPANDED ESCAPE_QUOTES)
        set(${_VARIABLE_NAME} "${_EXPANDED}")
    endif()

    # For debugging
    message(STATUS "${_VARIABLE_NAME}: ${${_VARIABLE_NAME}}")
endforeach()

if(DEFINED ENV{PAIMON_THIRDPARTY_MIRROR_URL})
    set(THIRDPARTY_MIRROR_URL "$ENV{PAIMON_THIRDPARTY_MIRROR_URL}")
else()
    set(THIRDPARTY_MIRROR_URL "")
endif()

if(DEFINED ENV{PAIMON_ARROW_URL})
    set(ARROW_SOURCE_URL "$ENV{PAIMON_ARROW_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_ARROW_PKG_NAME}")
        set_urls(ARROW_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_ARROW_PKG_NAME}")
    else()
        set_urls(ARROW_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/apache/arrow/releases/download/apache-arrow-${PAIMON_ARROW_BUILD_VERSION}/apache-arrow-${PAIMON_ARROW_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_RAPIDJSON_URL})
    set(RAPIDJSON_SOURCE_URL "$ENV{PAIMON_RAPIDJSON_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_RAPIDJSON_PKG_NAME}")
        set_urls(RAPIDJSON_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_RAPIDJSON_PKG_NAME}")
    else()
        set_urls(RAPIDJSON_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/miloyip/rapidjson/archive/${PAIMON_RAPIDJSON_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_FMT_URL})
    set(FMT_SOURCE_URL "$ENV{PAIMON_FMT_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_FMT_PKG_NAME}")
        set_urls(FMT_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_FMT_PKG_NAME}")
    else()
        set_urls(FMT_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/fmtlib/fmt/archive/refs/tags/${PAIMON_FMT_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_LUCENE_URL})
    set(LUCENE_SOURCE_URL "$ENV{PAIMON_LUCENE_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_LUCENE_PKG_NAME}")
        set_urls(LUCENE_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_LUCENE_PKG_NAME}")
    else()
        set_urls(LUCENE_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/luceneplusplus/LucenePlusPlus/archive/refs/tags/${PAIMON_LUCENE_PKG_NAME}"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_GLOG_URL})
    set(GLOG_SOURCE_URL "$ENV{PAIMON_GLOG_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_GLOG_PKG_NAME}")
        set_urls(GLOG_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_GLOG_PKG_NAME}")
    else()
        set_urls(GLOG_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/google/glog/archive/${PAIMON_GLOG_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_ZLIB_URL})
    set(ZLIB_SOURCE_URL "$ENV{PAIMON_ZLIB_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_ZLIB_PKG_NAME}")
        set_urls(ZLIB_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_ZLIB_PKG_NAME}")
    else()
        set_urls(ZLIB_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/madler/zlib/releases/download/v${PAIMON_ZLIB_BUILD_VERSION}/zlib-${PAIMON_ZLIB_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_ZSTD_URL})
    set(ZSTD_SOURCE_URL "$ENV{PAIMON_ZSTD_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_ZSTD_PKG_NAME}")
        set_urls(ZSTD_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_ZSTD_PKG_NAME}")
    else()
        set_urls(ZSTD_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/facebook/zstd/releases/download/v${PAIMON_ZSTD_BUILD_VERSION}/zstd-${PAIMON_ZSTD_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_LZ4_URL})
    set(LZ4_SOURCE_URL "$ENV{PAIMON_LZ4_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_LZ4_PKG_NAME}")
        set_urls(LZ4_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_LZ4_PKG_NAME}")
    else()
        set_urls(LZ4_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/lz4/lz4/archive/${PAIMON_LZ4_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_SNAPPY_URL})
    set(SNAPPY_SOURCE_URL "$ENV{PAIMON_SNAPPY_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_SNAPPY_PKG_NAME}")
        set_urls(SNAPPY_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_SNAPPY_PKG_NAME}")
    else()
        set_urls(SNAPPY_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/google/snappy/archive/${PAIMON_SNAPPY_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_PROTOBUF_URL})
    set(PROTOBUF_SOURCE_URL "$ENV{PAIMON_PROTOBUF_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_PROTOBUF_PKG_NAME}")
        set_urls(PROTOBUF_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_PROTOBUF_PKG_NAME}")
    else()
        set_urls(PROTOBUF_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/protocolbuffers/protobuf/releases/download/v${PAIMON_PROTOBUF_BUILD_VERSION}/protobuf-all-${PAIMON_PROTOBUF_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_GTEST_URL})
    set(GTEST_SOURCE_URL "$ENV{PAIMON_GTEST_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_GTEST_PKG_NAME}")
        set_urls(GTEST_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_GTEST_PKG_NAME}")
    else()
        set_urls(GTEST_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/google/googletest/archive/release-${PAIMON_GTEST_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_TBB_URL})
    set(TBB_SOURCE_URL "$ENV{PAIMON_TBB_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_TBB_PKG_NAME}")
        set_urls(TBB_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_TBB_PKG_NAME}")
    else()
        set_urls(TBB_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/uxlfoundation/oneTBB/archive/refs/tags/${PAIMON_TBB_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_ORC_URL})
    set(ORC_SOURCE_URL "$ENV{PAIMON_ORC_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_ORC_PKG_NAME}")
        set_urls(ORC_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_ORC_PKG_NAME}")
    else()
        set_urls(ORC_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/apache/orc/archive/refs/tags/${PAIMON_ORC_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_AVRO_URL})
    set(AVRO_SOURCE_URL "$ENV{PAIMON_AVRO_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_AVRO_PKG_NAME}")
        set_urls(AVRO_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_AVRO_PKG_NAME}")
    else()
        set_urls(AVRO_SOURCE_URL
                 "${THIRDPARTY_MIRROR_URL}https://github.com/apache/avro/archive/${PAIMON_AVRO_BUILD_VERSION}.tar.gz"
        )
    endif()
endif()

if(DEFINED ENV{PAIMON_JINDOSDK_C_URL})
    set(JINDOSDK_C_SOURCE_URL "$ENV{PAIMON_JINDOSDK_C_URL}")
else()
    if(EXISTS "${THIRDPARTY_DIR}/${PAIMON_JINDOSDK_C_PKG_NAME}")
        set_urls(JINDOSDK_C_SOURCE_URL "${THIRDPARTY_DIR}/${PAIMON_JINDOSDK_C_PKG_NAME}")
    else()
        set_urls(JINDOSDK_C_SOURCE_URL
                 "https://jindodata-binary.oss-cn-shanghai.aliyuncs.com/release/${PAIMON_JINDOSDK_C_BUILD_VERSION}/jindosdk-${PAIMON_JINDOSDK_C_BUILD_VERSION}-linux.tar.gz"
        )
    endif()
endif()

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS}")
string(REPLACE "-Wglobal-constructors" "" EP_CXX_FLAGS ${EP_CXX_FLAGS})
string(REPLACE "-Wglobal-constructors" "" EP_C_FLAGS ${EP_C_FLAGS})
if(NOT MSVC_TOOLCHAIN)
    # Set -fPIC on all external projects
    string(APPEND EP_CXX_FLAGS
           " -fPIC -Wno-error -Wno-sign-compare -Wno-ignored-attributes")
    string(APPEND EP_C_FLAGS " -fPIC")
endif()

if(PAIMON_USE_CXX11_ABI)
    string(APPEND EP_CXX_FLAGS " -D_GLIBCXX_USE_CXX11_ABI=1")
else()
    string(APPEND EP_CXX_FLAGS " -D_GLIBCXX_USE_CXX11_ABI=0")
endif()

# External projects are still able to override the following declarations.
# cmake command line will favor the last defined variable when a duplicate is
# encountered. This requires that `EP_COMMON_CMAKE_ARGS` is always the first
# argument.
set(EP_COMMON_CMAKE_ARGS
    ${EP_COMMON_TOOLCHAIN}
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_STATIC_LIBS=ON
    -DBUILD_TESTING=OFF
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
    -DCMAKE_C_FLAGS=${EP_C_FLAGS}
    -DCMAKE_INSTALL_LIBDIR=lib)

macro(build_lucene)
    message(STATUS "Building lucene from source")
    set(LUCENE_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/lucene_ep-install")
    set(LUCENE_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        "-DENABLE_TEST=OFF"
        "-DCMAKE_C_FLAGS=-pthread"
        "-DCMAKE_CXX_FLAGS=-pthread"
        "-DCMAKE_EXE_LINKER_FLAGS=-pthread"
        "-DBoost_INCLUDE_DIR=${BOOST_INCLUDE_DIR}"
        "-DBoost_LIBRARY_DIR=${BOOST_LIBRARY_DIR}"
        "-DBOOST_ROOT=${BOOST_INSTALL}"
        "-DBoost_CHRONO_FOUND=TRUE"
        "-DBoost_THREAD_FOUND=TRUE"
        "-DCMAKE_INSTALL_PREFIX=${LUCENE_PREFIX}")

    set(LUCENE_LIB "${LUCENE_PREFIX}/lib/liblucene++.so.0")
    externalproject_add(lucene_ep
                        ${EP_COMMON_OPTIONS}
                        URL ${LUCENE_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_LUCENE_BUILD_SHA256_CHECKSUM}"
                        CMAKE_ARGS ${LUCENE_CMAKE_ARGS}
                        BUILD_BYPRODUCTS ${LUCENE_LIB}
                        DEPENDS boost_date_time
                                boost_filesystem
                                boost_regex
                                boost_thread
                                boost_iostreams
                                boost_system
                                boost_chrono
                                boost_atomic)

    set(LUCENE_INCLUDE_DIR "${LUCENE_PREFIX}/include")
    # The include directory must exist before it is referenced by a target.
    file(MAKE_DIRECTORY "${LUCENE_INCLUDE_DIR}")
    include_directories(SYSTEM ${LUCENE_INCLUDE_DIR} ${BOOST_INCLUDE_DIR})
    add_library(lucene INTERFACE IMPORTED)
    target_include_directories(lucene SYSTEM INTERFACE "${LUCENE_INCLUDE_DIR}")
    target_compile_options(lucene INTERFACE -pthread)

    target_link_libraries(lucene
                          INTERFACE "${LUCENE_LIB}"
                                    boost_date_time
                                    boost_filesystem
                                    boost_regex
                                    boost_thread
                                    boost_iostreams
                                    boost_system
                                    boost_chrono
                                    boost_atomic
                                    pthread
                                    dl)
    add_dependencies(lucene lucene_ep)
endmacro()

macro(build_jieba)
    message(STATUS "Building jieba from source")
    set(JIEBA_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/jieba_ep-prefix")
    set(JIEBA_INSTALL "${CMAKE_CURRENT_BINARY_DIR}/jieba_ep-install")
    set(JIEBA_INCLUDE_DIR "${JIEBA_INSTALL}/include")
    set(JIEBA_DICT_DIR "${JIEBA_INSTALL}/dict")
    file(MAKE_DIRECTORY ${JIEBA_INCLUDE_DIR})
    file(MAKE_DIRECTORY ${JIEBA_DICT_DIR})

    set(JIEBA_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS} "-DENABLE_TEST=OFF" "-DCPPJIEBA_TOP_LEVEL_PROJECT=OFF"
        "-DCMAKE_INSTALL_PREFIX=${JIEBA_INSTALL}")

    set(PATCH_FILE "${CMAKE_CURRENT_LIST_DIR}/jieba.diff")
    externalproject_add(jieba_ep
                        ${EP_COMMON_OPTIONS}
                        GIT_REPOSITORY https://github.com/yanyiwu/cppjieba.git
                        GIT_TAG ${PAIMON_JIEBA_BUILD_VERSION}
                        GIT_SHALLOW FALSE
                        GIT_PROGRESS TRUE
                        GIT_SUBMODULES_RECURSE TRUE
                        CMAKE_ARGS ${JIEBA_CMAKE_ARGS}
                        LOG_PATCH ON
                        PATCH_COMMAND ${CMAKE_COMMAND} -E chdir <SOURCE_DIR> bash -c
                                      "[ -f .patched ] && echo '<SOURCE_DIR> patch already applied, ignore...' || patch -s -N -p1 -i '${PATCH_FILE}' && touch .patched"
                        INSTALL_COMMAND bash -c
                                        "cp -r ${JIEBA_PREFIX}/src/jieba_ep/include/* ${JIEBA_INSTALL}/include/ && cp -r ${JIEBA_PREFIX}/src/jieba_ep/dict/* ${JIEBA_INSTALL}/dict/ && cp -r ${JIEBA_PREFIX}/src/jieba_ep/deps/limonp/include/* ${JIEBA_INSTALL}/include/"
    )

    # The include directory must exist before it is referenced by a target.
    include_directories(SYSTEM ${JIEBA_INCLUDE_DIR} ${JIEBA_DICT_DIR})
    add_library(jieba INTERFACE IMPORTED)
    target_include_directories(jieba SYSTEM
                               INTERFACE "${JIEBA_INCLUDE_DIR} ${JIEBA_DICT_DIR}")
    add_dependencies(jieba jieba_ep)
endmacro()

macro(build_rapidjson)
    message(STATUS "Building RapidJSON from source")
    set(RAPIDJSON_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/rapidjson_ep-install")
    set(RAPIDJSON_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        -DRAPIDJSON_BUILD_DOC=OFF
        -DRAPIDJSON_BUILD_EXAMPLES=OFF
        -DRAPIDJSON_BUILD_TESTS=OFF
        "-DCMAKE_INSTALL_PREFIX=${RAPIDJSON_PREFIX}")

    externalproject_add(rapidjson_ep
                        ${EP_COMMON_OPTIONS}
                        URL ${RAPIDJSON_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_RAPIDJSON_BUILD_SHA256_CHECKSUM}"
                        CMAKE_ARGS ${RAPIDJSON_CMAKE_ARGS})

    set(RAPIDJSON_INCLUDE_DIR "${RAPIDJSON_PREFIX}/include")
    # The include directory must exist before it is referenced by a target.
    file(MAKE_DIRECTORY "${RAPIDJSON_INCLUDE_DIR}")

    include_directories(SYSTEM ${RAPIDJSON_INCLUDE_DIR})
    add_library(RapidJSON INTERFACE IMPORTED)
    target_include_directories(RapidJSON INTERFACE "${RAPIDJSON_INCLUDE_DIR}")
    add_dependencies(RapidJSON rapidjson_ep)
endmacro()

macro(build_fmt)
    message(STATUS "Building fmt from source")
    set(FMT_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/fmt_ep-install")
    set(FMT_INCLUDE_DIR "${FMT_PREFIX}/include")
    if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
        set(FMT_LIB_SUFFIX "d")
    else()
        set(FMT_LIB_SUFFIX "")
    endif()
    set(FMT_STATIC_LIB_NAME fmt)
    set(FMT_STATIC_LIB
        "${FMT_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${FMT_STATIC_LIB_NAME}${FMT_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(FMT_LIBRARIES ${FMT_STATIC_LIB})
    set(FMT_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -Wno-error")
    set(FMT_CMAKE_C_FLAGS "${EP_C_FLAGS} -Wno-error")
    string(REPLACE "-Werror" "" FMT_CMAKE_CXX_FLAGS ${FMT_CMAKE_CXX_FLAGS})

    set(FMT_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        -DCMAKE_INSTALL_PREFIX=${FMT_PREFIX}
        "-DCMAKE_CXX_FLAGS=${FMT_CMAKE_CXX_FLAGS}"
        "-DCMAKE_C_FLAGS=${FMT_CMAKE_C_FLAGS}"
        -DFMT_TEST=OFF
        -DFMT_DOC=OFF)
    set(FMT_CONFIGURE CMAKE_ARGS ${FMT_CMAKE_ARGS})
    externalproject_add(fmt_ep
                        URL ${FMT_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_FMT_BUILD_SHA256_CHECKSUM}"
                        ${FMT_CONFIGURE} ${THIRDPARTY_LOG_OPTIONS}
                        BUILD_BYPRODUCTS ${FMT_STATIC_LIB})

    file(MAKE_DIRECTORY "${FMT_INCLUDE_DIR}")

    include_directories(SYSTEM ${FMT_INCLUDE_DIR})
    add_library(fmt STATIC IMPORTED)
    set_target_properties(fmt PROPERTIES IMPORTED_LOCATION ${FMT_STATIC_LIB})
    target_include_directories(fmt INTERFACE ${FMT_INCLUDE_DIR})
    add_dependencies(fmt fmt_ep)
endmacro(build_fmt)

macro(build_boost)
    message(STATUS "Building boost from source")
    set(BOOST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/boost_ep-prefix")
    set(BOOST_INSTALL "${CMAKE_CURRENT_BINARY_DIR}/boost_ep-install")
    set(BOOST_INCLUDE_DIR "${BOOST_INSTALL}/include")
    set(BOOST_LIBRARY_DIR ${BOOST_INSTALL}/lib)
    file(MAKE_DIRECTORY ${BOOST_INCLUDE_DIR})
    file(MAKE_DIRECTORY ${BOOST_LIBRARY_DIR})

    set(BOOST_BYPRODUCTS
        ${BOOST_LIBRARY_DIR}/libboost_date_time.a
        ${BOOST_LIBRARY_DIR}/libboost_filesystem.a
        ${BOOST_LIBRARY_DIR}/libboost_system.a
        ${BOOST_LIBRARY_DIR}/libboost_regex.a
        ${BOOST_LIBRARY_DIR}/libboost_thread.a
        ${BOOST_LIBRARY_DIR}/libboost_atomic.a
        ${BOOST_LIBRARY_DIR}/libboost_chrono.a
        ${BOOST_LIBRARY_DIR}/libboost_iostreams.a)

    externalproject_add(boost_ep
                        GIT_REPOSITORY https://github.com/boostorg/boost.git
                        GIT_TAG boost-${PAIMON_BOOST_BUILD_VERSION}
                        GIT_SHALLOW FALSE
                        GIT_PROGRESS TRUE
                        GIT_SUBMODULES_RECURSE TRUE
                        CONFIGURE_COMMAND ${BOOST_PREFIX}/src/boost_ep/bootstrap.sh
                                          --with-libraries=date_time,filesystem,iostreams,regex,system,thread,chrono,atomic
                        BUILD_IN_SOURCE TRUE
                        BUILD_COMMAND ${BOOST_PREFIX}/src/boost_ep/b2
                                      --prefix=${BOOST_INSTALL}
                                      --libdir=${BOOST_LIBRARY_DIR} link=static
                                      runtime-link=shared threading=multi variant=release
                                      cxxflags=-fPIC install
                        INSTALL_COMMAND bash -c
                                        "mkdir -p ${BOOST_INSTALL}/include/boost && cp -r ${BOOST_PREFIX}/src/boost_ep/libs/*/include/boost/* ${BOOST_INSTALL}/include/boost && cp -r ${BOOST_PREFIX}/src/boost_ep/libs/*/*/include/boost/* ${BOOST_INSTALL}/include/boost"
                        BUILD_BYPRODUCTS ${BOOST_BYPRODUCTS}
                        LOG_DOWNLOAD ON
                        LOG_CONFIGURE ON
                        LOG_BUILD ON)

    include_directories(SYSTEM ${BOOST_INCLUDE_DIR})

    add_library(boost_atomic STATIC IMPORTED)
    set_target_properties(boost_atomic
                          PROPERTIES IMPORTED_LOCATION
                                     ${BOOST_LIBRARY_DIR}/libboost_atomic.a
                                     INTERFACE_INCLUDE_DIRECTORIES ${BOOST_INCLUDE_DIR})
    add_library(boost_chrono STATIC IMPORTED)
    set_target_properties(boost_chrono
                          PROPERTIES IMPORTED_LOCATION
                                     ${BOOST_LIBRARY_DIR}/libboost_chrono.a
                                     INTERFACE_INCLUDE_DIRECTORIES ${BOOST_INCLUDE_DIR})
    add_library(boost_date_time STATIC IMPORTED)
    set_target_properties(boost_date_time
                          PROPERTIES IMPORTED_LOCATION
                                     ${BOOST_LIBRARY_DIR}/libboost_date_time.a
                                     INTERFACE_INCLUDE_DIRECTORIES ${BOOST_INCLUDE_DIR})
    add_library(boost_filesystem STATIC IMPORTED)
    set_target_properties(boost_filesystem
                          PROPERTIES IMPORTED_LOCATION
                                     ${BOOST_LIBRARY_DIR}/libboost_filesystem.a
                                     INTERFACE_INCLUDE_DIRECTORIES ${BOOST_INCLUDE_DIR})
    add_library(boost_regex STATIC IMPORTED)
    set_target_properties(boost_regex
                          PROPERTIES IMPORTED_LOCATION
                                     ${BOOST_LIBRARY_DIR}/libboost_regex.a
                                     INTERFACE_INCLUDE_DIRECTORIES ${BOOST_INCLUDE_DIR})
    add_library(boost_thread STATIC IMPORTED)
    set_target_properties(boost_thread
                          PROPERTIES IMPORTED_LOCATION
                                     ${BOOST_LIBRARY_DIR}/libboost_thread.a
                                     INTERFACE_INCLUDE_DIRECTORIES ${BOOST_INCLUDE_DIR})
    add_library(boost_iostreams STATIC IMPORTED)
    set_target_properties(boost_iostreams
                          PROPERTIES IMPORTED_LOCATION
                                     ${BOOST_LIBRARY_DIR}/libboost_iostreams.a
                                     INTERFACE_INCLUDE_DIRECTORIES ${BOOST_INCLUDE_DIR})
    add_library(boost_system STATIC IMPORTED)
    set_target_properties(boost_system
                          PROPERTIES IMPORTED_LOCATION
                                     ${BOOST_LIBRARY_DIR}/libboost_system.a
                                     INTERFACE_INCLUDE_DIRECTORIES ${BOOST_INCLUDE_DIR})

    add_dependencies(boost_atomic boost_ep)
    add_dependencies(boost_chrono boost_ep)
    add_dependencies(boost_date_time boost_ep)
    add_dependencies(boost_filesystem boost_ep)
    add_dependencies(boost_regex boost_ep)
    add_dependencies(boost_thread boost_ep)
    add_dependencies(boost_iostreams boost_ep)
    add_dependencies(boost_system boost_ep)
endmacro(build_boost)

macro(build_snappy)
    message(STATUS "Building snappy from source")
    set(SNAPPY_HOME "${CMAKE_CURRENT_BINARY_DIR}/snappy_ep-install")
    set(SNAPPY_INCLUDE_DIR "${SNAPPY_HOME}/include")
    set(SNAPPY_STATIC_LIB
        "${SNAPPY_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}snappy${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(SNAPPY_LIBRARIES ${SNAPPY_STATIC_LIB})
    set(SNAPPY_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} -DCMAKE_INSTALL_PREFIX=${SNAPPY_HOME}
                          -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF)

    externalproject_add(snappy_ep
                        URL ${SNAPPY_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_SNAPPY_BUILD_SHA256_CHECKSUM}"
                        CMAKE_ARGS ${SNAPPY_CMAKE_ARGS} ${THIRDPARTY_LOG_OPTIONS}
                        BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}")

    file(MAKE_DIRECTORY "${SNAPPY_INCLUDE_DIR}")

    include_directories(SYSTEM ${SNAPPY_INCLUDE_DIR})
    add_library(snappy STATIC IMPORTED)
    set_target_properties(snappy PROPERTIES IMPORTED_LOCATION ${SNAPPY_STATIC_LIB})
    target_include_directories(snappy INTERFACE ${SNAPPY_INCLUDE_DIR})
    add_dependencies(snappy snappy_ep)
endmacro()

macro(build_zlib)
    message(STATUS "Building zlib from source")
    set(ZLIB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zlib_ep-install")
    set(ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
    set(ZLIB_STATIC_LIB_NAME z)
    set(ZLIB_STATIC_LIB
        "${ZLIB_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ZLIB_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(ZLIB_LIBRARIES ${ZLIB_STATIC_LIB})
    set(ZLIB_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} -DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX})

    externalproject_add(zlib_ep
                        URL ${ZLIB_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_ZLIB_BUILD_SHA256_CHECKSUM}"
                        CMAKE_ARGS ${ZLIB_CMAKE_ARGS} ${THIRDPARTY_LOG_OPTIONS}
                        BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}")

    file(MAKE_DIRECTORY "${ZLIB_INCLUDE_DIR}")

    include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
    add_library(zlib STATIC IMPORTED)
    set_target_properties(zlib PROPERTIES IMPORTED_LOCATION ${ZLIB_STATIC_LIB})
    target_include_directories(zlib INTERFACE ${ZLIB_INCLUDE_DIR})
    add_dependencies(zlib zlib_ep)
endmacro()

macro(build_zstd)
    message(STATUS "Building zstd from source")
    set(ZSTD_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zstd_ep-install")
    set(ZSTD_INCLUDE_DIR "${ZSTD_PREFIX}/include")
    set(ZSTD_STATIC_LIB_NAME zstd)
    set(ZSTD_STATIC_LIB
        "${ZSTD_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ZSTD_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(ZSTD_LIBRARIES ${ZSTD_STATIC_LIB})
    set(ZSTD_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -Wno-error")
    set(ZSTD_CMAKE_C_FLAGS "${EP_C_FLAGS} -Wno-error")
    string(REPLACE "-Werror" "" ZSTD_CMAKE_CXX_FLAGS ${ZSTD_CMAKE_CXX_FLAGS})

    set(ZSTD_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        -DCMAKE_INSTALL_PREFIX=${ZSTD_PREFIX}
        "-DCMAKE_CXX_FLAGS=${ZSTD_CMAKE_CXX_FLAGS}"
        "-DCMAKE_C_FLAGS=${ZSTD_CMAKE_C_FLAGS}"
        -DZSTD_BUILD_SHARED=OFF
        -DZSTD_BUILD_PROGRAMS=OFF)

    set(ZSTD_CONFIGURE SOURCE_SUBDIR "build/cmake" CMAKE_ARGS ${ZSTD_CMAKE_ARGS})
    externalproject_add(zstd_ep
                        URL ${ZSTD_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_ZSTD_BUILD_SHA256_CHECKSUM}"
                        ${ZSTD_CONFIGURE} ${THIRDPARTY_LOG_OPTIONS}
                        BUILD_BYPRODUCTS ${ZSTD_STATIC_LIB})

    file(MAKE_DIRECTORY "${ZSTD_INCLUDE_DIR}")

    include_directories(SYSTEM ${ZSTD_INCLUDE_DIR})
    add_library(zstd STATIC IMPORTED)
    set_target_properties(zstd PROPERTIES IMPORTED_LOCATION ${ZSTD_STATIC_LIB})
    target_include_directories(zstd INTERFACE ${ZSTD_INCLUDE_DIR})
    add_dependencies(zstd zstd_ep)
endmacro(build_zstd)

macro(build_lz4)
    message(STATUS "Building lz4 from source")
    set(LZ4_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-install")
    set(LZ4_INCLUDE_DIR "${LZ4_PREFIX}/include")
    set(LZ4_STATIC_LIB
        "${LZ4_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}lz4${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(LZ4_LIBRARIES ${LZ4_STATIC_LIB})
    set(LZ4_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} -DCMAKE_INSTALL_PREFIX=${LZ4_PREFIX}
                       -DLZ4_BUILD_CLI=OFF -DLZ4_BUILD_LEGACY_LZ4C=OFF)

    set(LZ4_CONFIGURE SOURCE_SUBDIR "build/cmake" CMAKE_ARGS ${LZ4_CMAKE_ARGS})
    externalproject_add(lz4_ep
                        URL ${LZ4_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_LZ4_BUILD_SHA256_CHECKSUM}"
                        ${LZ4_CONFIGURE} ${THIRDPARTY_LOG_OPTIONS}
                        BUILD_BYPRODUCTS ${LZ4_STATIC_LIB})

    file(MAKE_DIRECTORY "${LZ4_INCLUDE_DIR}")

    include_directories(SYSTEM ${LZ4_INCLUDE_DIR})
    add_library(lz4 STATIC IMPORTED)
    set_target_properties(lz4 PROPERTIES IMPORTED_LOCATION ${LZ4_STATIC_LIB})
    target_include_directories(lz4 INTERFACE ${LZ4_INCLUDE_DIR})
    add_dependencies(lz4 lz4_ep)
endmacro()

macro(build_jindosdk_c)
    message(STATUS "Building jindosdk-c from precompiled package")

    set(JINDOSDK_C_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/jindosdk_ep-install")
    set(JINDOSDK_C_HOME "${JINDOSDK_C_PREFIX}")
    set(JINDOSDK_C_INCLUDE_DIR "${JINDOSDK_C_PREFIX}/include")
    set(JINDOSDK_C_LIB_DIR "${JINDOSDK_C_PREFIX}/lib/native")
    set(JINDOSDK_C_DYNAMIC_LIB "${JINDOSDK_C_LIB_DIR}/libjindosdk_c.so")

    # Extract and install jindosdk from precompiled package
    externalproject_add(jindosdk_ep
                        URL ${JINDOSDK_C_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_JINDOSDK_C_BUILD_SHA256_CHECKSUM}"
                        ${THIRDPARTY_LOG_OPTIONS}
                        CONFIGURE_COMMAND ""
                        BUILD_COMMAND ""
                        INSTALL_COMMAND bash -c
                                        "cp -r <SOURCE_DIR>/include/* ${JINDOSDK_C_INCLUDE_DIR}"
                        COMMAND bash -c
                                "cp -r <SOURCE_DIR>/lib/native/libjindosdk_c.so* ${JINDOSDK_C_LIB_DIR}"
                        BUILD_BYPRODUCTS "${JINDOSDK_C_DYNAMIC_LIB}")

    # The include directory must exist before it is referenced by a target.
    file(MAKE_DIRECTORY "${JINDOSDK_C_INCLUDE_DIR}")
    file(MAKE_DIRECTORY "${JINDOSDK_C_LIB_DIR}")

    add_library(jindosdk::c_sdk SHARED IMPORTED)
    set_target_properties(jindosdk::c_sdk
                          PROPERTIES IMPORTED_LOCATION "${JINDOSDK_C_DYNAMIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${JINDOSDK_C_INCLUDE_DIR}")
    list(APPEND JINDOSDK_INCLUDE_DIR ${JINDOSDK_C_INCLUDE_DIR})

    add_dependencies(jindosdk::c_sdk jindosdk_ep)
    install(DIRECTORY "${JINDOSDK_C_LIB_DIR}/"
            DESTINATION ${CMAKE_INSTALL_LIBDIR}
            FILES_MATCHING
            PATTERN "libjindosdk_c.so*")

endmacro()

macro(build_jindosdk_nextarch)
    message(STATUS "Building jindosdk-nextarch from local source")

    set(JINDOSDK_NEXTARCH_PREFIX
        "${CMAKE_CURRENT_BINARY_DIR}/jindosdk-nextarch_ep-install")
    set(JINDOSDK_NEXTARCH_HOME "${JINDOSDK_NEXTARCH_PREFIX}")
    set(JINDOSDK_NEXTARCH_INCLUDE_DIR "${JINDOSDK_NEXTARCH_PREFIX}/include")
    set(JINDOSDK_NEXTARCH_LIB_DIR "${JINDOSDK_NEXTARCH_PREFIX}/lib")
    set(JINDOSDK_NEXTARCH_SOURCE_DIR "${CMAKE_SOURCE_DIR}/third_party/jindosdk-nextarch")
    set(JINDOSDK_NEXTARCH_STATIC_LIB
        "${JINDOSDK_NEXTARCH_LIB_DIR}/libjindosdk-nextarch.a")

    # Get jindosdk dependencies (headers and dynamic library)
    get_target_property(JINDOSDK_C_INCLUDE_DIR jindosdk::c_sdk
                        INTERFACE_INCLUDE_DIRECTORIES)
    get_target_property(JINDOSDK_C_LIBRARY_LOCATION jindosdk::c_sdk IMPORTED_LOCATION)
    get_filename_component(JINDOSDK_C_DIR_ROOT "${JINDOSDK_C_INCLUDE_DIR}" DIRECTORY)

    # Compile flags for jindosdk-nextarch
    set(JINDOSDK_NEXTARCH_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS}")
    set(JINDOSDK_NEXTARCH_CMAKE_C_FLAGS "${EP_C_FLAGS}")
    set(JINDOSDK_NEXTARCH_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        "-DCMAKE_INSTALL_PREFIX=${JINDOSDK_NEXTARCH_PREFIX}"
        "-DCMAKE_CXX_FLAGS=${JINDOSDK_NEXTARCH_CMAKE_CXX_FLAGS}"
        "-DCMAKE_C_FLAGS=${JINDOSDK_NEXTARCH_CMAKE_C_FLAGS}"
        -DJINDOSDK_ROOT=${JINDOSDK_C_DIR_ROOT})

    externalproject_add(jindosdk-nextarch_ep
                        SOURCE_DIR ${JINDOSDK_NEXTARCH_SOURCE_DIR}
                        CMAKE_ARGS ${JINDOSDK_NEXTARCH_CMAKE_ARGS}
                        BUILD_BYPRODUCTS "${JINDOSDK_NEXTARCH_STATIC_LIB}"
                        DEPENDS jindosdk::c_sdk ${THIRDPARTY_LOG_OPTIONS})

    # The include directory must exist before it is referenced by a target.
    file(MAKE_DIRECTORY "${JINDOSDK_NEXTARCH_INCLUDE_DIR}")
    file(MAKE_DIRECTORY "${JINDOSDK_NEXTARCH_LIB_DIR}")

    add_library(jindosdk::nextarch STATIC IMPORTED)
    set_target_properties(jindosdk::nextarch
                          PROPERTIES IMPORTED_LOCATION "${JINDOSDK_NEXTARCH_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${JINDOSDK_NEXTARCH_INCLUDE_DIR}")
    target_link_libraries(jindosdk::nextarch INTERFACE jindosdk::c_sdk pthread dl)
    list(APPEND JINDOSDK_INCLUDE_DIR ${JINDOSDK_NEXTARCH_INCLUDE_DIR})

    add_dependencies(jindosdk::nextarch jindosdk-nextarch_ep)
endmacro()

macro(build_protobuf)
    message(STATUS "Building protobuf from source")
    set(PROTOBUF_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep-install")
    set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_PREFIX}/include")
    set(PROTOBUF_STATIC_LIB
        "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(PROTOC_STATIC_LIB
        "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protoc${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(PROTOBUF_LIBRARIES ${PROTOBUF_STATIC_LIB})
    set(PROTOBUF_COMPILER "${PROTOBUF_PREFIX}/bin/protoc")

    get_target_property(THIRDPARTY_ZLIB_INCLUDE_DIR zlib INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(THIRDPARTY_ZLIB_ROOT "${THIRDPARTY_ZLIB_INCLUDE_DIR}"
                           DIRECTORY)

    # Strip lto flags (which may be added by dh_auto_configure)
    # See https://github.com/protocolbuffers/protobuf/issues/7092
    set(PROTOBUF_C_FLAGS ${EP_C_FLAGS})
    set(PROTOBUF_CXX_FLAGS ${EP_CXX_FLAGS})
    string(REPLACE "-flto=auto" "" PROTOBUF_C_FLAGS "${PROTOBUF_C_FLAGS}")
    string(REPLACE "-ffat-lto-objects" "" PROTOBUF_C_FLAGS "${PROTOBUF_C_FLAGS}")
    string(REPLACE "-flto=auto" "" PROTOBUF_CXX_FLAGS "${PROTOBUF_CXX_FLAGS}")
    string(REPLACE "-ffat-lto-objects" "" PROTOBUF_CXX_FLAGS "${PROTOBUF_CXX_FLAGS}")
    string(APPEND PROTOBUF_CXX_FLAGS
           " -Wno-inconsistent-missing-override -Wno-unneeded-internal-declaration")
    set(PROTOBUF_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        -DCMAKE_INSTALL_PREFIX=${PROTOBUF_PREFIX}
        "-DCMAKE_CXX_FLAGS=${PROTOBUF_CXX_FLAGS}"
        "-DCMAKE_C_FLAGS=${PROTOBUF_C_FLAGS}"
        "-DZLIB_ROOT=${THIRDPARTY_ZLIB_ROOT}"
        -Dprotobuf_BUILD_TESTS=OFF
        -Dprotobuf_DEBUG_POSTFIX=)
    set(PROTOBUF_CONFIGURE SOURCE_SUBDIR "cmake" CMAKE_ARGS ${PROTOBUF_CMAKE_ARGS})

    externalproject_add(protobuf_ep
                        URL ${PROTOBUF_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_PROTOBUF_BUILD_SHA256_CHECKSUM}"
                        ${PROTOBUF_CONFIGURE} ${THIRDPARTY_LOG_OPTIONS}
                        # BUILD_IN_SOURCE 1
                        BUILD_BYPRODUCTS "${PROTOBUF_STATIC_LIB}" "${PROTOBUF_COMPILER}"
                        DEPENDS zlib)

    file(MAKE_DIRECTORY "${PROTOBUF_INCLUDE_DIR}")

    include_directories(SYSTEM ${PROTOBUF_INCLUDE_DIR})
    add_library(libprotobuf STATIC IMPORTED)
    set_target_properties(libprotobuf PROPERTIES IMPORTED_LOCATION ${PROTOBUF_STATIC_LIB})
    target_include_directories(libprotobuf INTERFACE ${PROTOBUF_INCLUDE_DIR})
    add_library(libprotoc STATIC IMPORTED)
    set_target_properties(libprotoc PROPERTIES IMPORTED_LOCATION ${PROTOC_STATIC_LIB})
    target_include_directories(libprotoc INTERFACE ${PROTOBUF_INCLUDE_DIR})

    add_executable(protoc IMPORTED)
    set_target_properties(protoc PROPERTIES IMPORTED_LOCATION ${PROTOBUF_COMPILER})

    add_dependencies(libprotobuf protobuf_ep)
    add_dependencies(protoc protobuf_ep)
endmacro()

macro(build_avro)
    message(STATUS "Building avro from source")
    set(AVRO_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/avro_ep-install")
    set(AVRO_INCLUDE_DIR "${AVRO_PREFIX}/include")
    set(AVRO_STATIC_LIB_NAME avrocpp_s)
    set(AVRO_STATIC_LIB
        "${AVRO_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${AVRO_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(AVRO_LIBRARIES ${AVRO_STATIC_LIB})

    get_target_property(AVRO_SNAPPY_INCLUDE_DIR snappy INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(AVRO_SNAPPY_ROOT "${AVRO_SNAPPY_INCLUDE_DIR}" DIRECTORY)

    get_target_property(AVRO_ZSTD_INCLUDE_DIR zstd INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(AVRO_ZSTD_ROOT "${AVRO_ZSTD_INCLUDE_DIR}" DIRECTORY)

    get_target_property(AVRO_ZLIB_INCLUDE_DIR zlib INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(AVRO_ZLIB_ROOT "${AVRO_ZLIB_INCLUDE_DIR}" DIRECTORY)

    get_target_property(AVRO_FMT_INCLUDE_DIR fmt INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(AVRO_FMT_ROOT "${AVRO_FMT_INCLUDE_DIR}" DIRECTORY)

    set(AVRO_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -Wno-error")
    set(AVRO_CMAKE_C_FLAGS "${EP_C_FLAGS} -Wno-error")

    set(AVRO_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        "-DCMAKE_INSTALL_PREFIX=${AVRO_PREFIX}"
        "-DCMAKE_CXX_FLAGS=${AVRO_CMAKE_CXX_FLAGS}"
        "-DCMAKE_C_FLAGS=${AVRO_CMAKE_C_FLAGS}"
        "-DAVRO_BUILD_TESTS=OFF"
        "-DAVRO_BUILD_EXECUTABLES=OFF"
        "-DZLIB_ROOT=${AVRO_ZLIB_ROOT}"
        "-Dfmt_ROOT=${AVRO_FMT_ROOT}"
        "-Dzstd_ROOT=${AVRO_ZSTD_ROOT}"
        "-DSnappy_ROOT=${AVRO_SNAPPY_ROOT}")
    externalproject_add(avro_ep
                        URL ${AVRO_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_AVRO_BUILD_SHA256_CHECKSUM}"
                        SOURCE_SUBDIR "lang/c++"
                        CMAKE_ARGS ${AVRO_CMAKE_ARGS}
                        BUILD_BYPRODUCTS "${AVRO_STATIC_LIB}"
                        DEPENDS fmt zlib zstd snappy)

    file(MAKE_DIRECTORY "${AVRO_INCLUDE_DIR}")

    include_directories(SYSTEM ${AVRO_INCLUDE_DIR})
    add_library(avro STATIC IMPORTED)
    set_target_properties(avro PROPERTIES IMPORTED_LOCATION ${AVRO_STATIC_LIB})
    target_include_directories(avro INTERFACE ${AVRO_INCLUDE_DIR})
    target_link_libraries(avro INTERFACE zlib zstd snappy)
    add_dependencies(avro avro_ep)
endmacro()

macro(build_orc)
    message(STATUS "Building orc from source")

    get_target_property(ORC_SNAPPY_INCLUDE_DIR snappy INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_SNAPPY_ROOT "${ORC_SNAPPY_INCLUDE_DIR}" DIRECTORY)

    get_target_property(ORC_LZ4_INCLUDE_DIR lz4 INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_LZ4_ROOT "${ORC_LZ4_INCLUDE_DIR}" DIRECTORY)

    get_target_property(ORC_ZSTD_INCLUDE_DIR zstd INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_ZSTD_ROOT "${ORC_ZSTD_INCLUDE_DIR}" DIRECTORY)

    get_target_property(ORC_ZLIB_INCLUDE_DIR zlib INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_ZLIB_ROOT "${ORC_ZLIB_INCLUDE_DIR}" DIRECTORY)

    get_target_property(ORC_PROTOBUF_INCLUDE_DIR libprotobuf
                        INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_PROTOBUF_ROOT "${ORC_PROTOBUF_INCLUDE_DIR}" DIRECTORY)

    get_property(PAIMON_RPATH GLOBAL PROPERTY PAIMON_RPATH)
    message(STATUS "PAIMON_RPATH value: ${PAIMON_RPATH}")
    set(ORC_RPATH ${PAIMON_RPATH})
    message(STATUS "ORC_RPATH value: ${ORC_RPATH}")

    string(REPLACE "-Werror" "" EP_CXX_FLAGS ${EP_CXX_FLAGS})

    set(ORC_CMAKE_CXX_FLAGS
        "${EP_CXX_FLAGS} -fPIC -Wno-error ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
    set(ORC_CMAKE_C_FLAGS
        "${EP_C_FLAGS} -fPIC -Wno-error ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")

    set(ORC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/orc_ep-prefix")
    set(ORC_INCLUDE_DIR "${ORC_PREFIX}/include")
    set(ORC_SOURCE_DIR "${ORC_PREFIX}/cpp")
    set(ORC_BUILD_DIR "${CMAKE_BINARY_DIR}/build/orc")

    set(ORC_STATIC_LIB "${ORC_PREFIX}/lib/liborc.a")

    message("ORC_STATIC_LIB IS ${ORC_STATIC_LIB}")
    message("ORC_CMAKE_CXX_FLAGS ${ORC_CMAKE_CXX_FLAGS}")
    message("ORC_CMAKE_C_FLAGS ${ORC_CMAKE_C_FLAGS}")

    set(ORC_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        "-DCMAKE_INSTALL_PREFIX=${ORC_PREFIX}"
        "-DCMAKE_CXX_FLAGS=${ORC_CMAKE_CXX_FLAGS}"
        "-DCMAKE_C_FLAGS=${ORC_CMAKE_C_FLAGS}"
        "-DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${ORC_CMAKE_CXX_FLAGS}"
        "-DCMAKE_EXE_LINKER_FLAGS=-Wl,-rpath=${ORC_RPATH}"
        "-DCMAKE_SHARED_LINKER_FLAGS=-Wl,-rpath=${ORC_RPATH}"
        "-DCMAKE_MODULE_LINKER_FLAGS=-Wl,-rpath=${ORC_RPATH}"
        "-DSNAPPY_HOME=${ORC_SNAPPY_ROOT}"
        "-DLZ4_HOME=${ORC_LZ4_ROOT}"
        "-DZSTD_HOME=${ORC_ZSTD_ROOT}"
        "-DZLIB_HOME=${ORC_ZLIB_ROOT}"
        "-DPROTOBUF_HOME=${ORC_PROTOBUF_ROOT}"
        "-DProtobuf_ROOT=${ORC_PROTOBUF_ROOT}"
        -DBUILD_JAVA=OFF
        -DBUILD_CPP_TESTS=OFF
        -DBUILD_TOOLS=OFF
        -DBUILD_CPP_ENABLE_METRICS=ON)

    set(PATCH_FILE "${CMAKE_CURRENT_LIST_DIR}/orc.diff")
    externalproject_add(orc_ep
                        URL ${ORC_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_ORC_BUILD_SHA256_CHECKSUM}"
                        SOURCE_DIR ${ORC_SOURCE_DIR}
                        BINARY_DIR ${ORC_BUILD_DIR}
                        CMAKE_ARGS ${ORC_CMAKE_ARGS}
                        LOG_PATCH ON
                        PATCH_COMMAND ${CMAKE_COMMAND} -E chdir <SOURCE_DIR> bash -c
                                      "[ -f .patched ] && echo '<SOURCE_DIR> patch already applied, ignore...' || patch -s -N -p1 -i '${PATCH_FILE}' && touch .patched"
                        UPDATE_DISCONNECTED 1
                        BUILD_BYPRODUCTS ${ORC_STATIC_LIB}
                        DEPENDS zstd
                                snappy
                                lz4
                                zlib
                                libprotobuf)

    # The include directory must exist before it is referenced by a target.
    file(MAKE_DIRECTORY "${ORC_INCLUDE_DIR}")

    add_library(orc::orc STATIC IMPORTED)
    set_target_properties(orc::orc
                          PROPERTIES IMPORTED_LOCATION "${ORC_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ORC_INCLUDE_DIR}")
    target_link_libraries(orc::orc
                          INTERFACE zstd
                                    snappy
                                    lz4
                                    zlib
                                    libprotobuf)

    add_dependencies(orc::orc orc_ep)
endmacro()

macro(build_arrow)
    message(STATUS "Building Arrow from source")

    get_target_property(ARROW_SNAPPY_INCLUDE_DIR snappy INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ARROW_SNAPPY_ROOT "${ARROW_SNAPPY_INCLUDE_DIR}" DIRECTORY)

    get_target_property(ARROW_LZ4_INCLUDE_DIR lz4 INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ARROW_LZ4_ROOT "${ARROW_LZ4_INCLUDE_DIR}" DIRECTORY)

    get_target_property(ARROW_ZSTD_INCLUDE_DIR zstd INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ARROW_ZSTD_ROOT "${ARROW_ZSTD_INCLUDE_DIR}" DIRECTORY)

    get_target_property(ARROW_ZLIB_INCLUDE_DIR zlib INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ARROW_ZLIB_ROOT "${ARROW_ZLIB_INCLUDE_DIR}" DIRECTORY)

    set(ARROW_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -Wno-error")
    set(ARROW_CMAKE_C_FLAGS "${EP_C_FLAGS} -Wno-error")
    string(REPLACE "-Werror" "" ARROW_CMAKE_CXX_FLAGS ${ARROW_CMAKE_CXX_FLAGS})
    # Fix for thrift Mutex.h missing #include <cstdint> (GCC 15 strictness)
    # Use -include to force include cstdint for all C++ files
    string(APPEND ARROW_CMAKE_CXX_FLAGS " -include cstdint")

    set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-install")
    set(ARROW_HOME "${ARROW_PREFIX}")
    set(ARROW_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep")

    set(_ARROW_LIBRARY_SUFFIX "${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")

    file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")

    set(ARROW_BUILD_DIR "${CMAKE_BINARY_DIR}/arrow")
    string(TOLOWER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_LOWER)
    set(ARROW_STATIC_LIB
        "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}arrow${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(ARROW_DATASET_STATIC_LIB
        "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_dataset${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(ARROW_ACERO_STATIC_LIB
        "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_acero${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(ARROW_BUNDLED_DEP_STATIC_LIB
        "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(PARQUET_STATIC_LIB
        "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}parquet${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )

    set(ARROW_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        "-DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}"
        "-DCMAKE_CXX_FLAGS=${ARROW_CMAKE_CXX_FLAGS}"
        "-DCMAKE_C_FLAGS=${ARROW_CMAKE_C_FLAGS}"
        "-DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${ARROW_CMAKE_CXX_FLAGS}"
        -DARROW_DEPENDENCY_USE_SHARED=OFF
        -DARROW_BUILD_SHARED=OFF
        -DARROW_BUILD_STATIC=ON
        -DARROW_BUILD_TESTS=OFF
        -DARROW_BUILD_BENCHMARKS=OFF
        -DARROW_BUILD_EXAMPLES=OFF
        -DARROW_JEMALLOC=OFF
        -DARROW_WITH_RE2=OFF
        -DARROW_WITH_UTF8PROC=OFF
        -DARROW_ORC=OFF
        -DARROW_SIMD_LEVEL=NONE
        -DARROW_RUNTIME_SIMD_LEVEL=NONE
        -DARROW_PARQUET=ON
        -DARROW_IPC=ON
        -DARROW_DATASET=ON
        -DARROW_JSON=ON
        -DARROW_COMPUTE=ON
        -DARROW_WITH_SNAPPY=ON
        -DARROW_WITH_ZLIB=ON
        -DARROW_WITH_LZ4=ON
        -DARROW_WITH_ZSTD=ON
        -DARROW_WITH_BZ2=OFF
        -DARROW_WITH_BROTLI=ON
        -DZSTD_ROOT=${ARROW_ZSTD_ROOT}
        -DZLIB_ROOT=${ARROW_ZLIB_ROOT}
        -DSnappy_ROOT=${ARROW_SNAPPY_ROOT}
        -DLZ4_ROOT=${ARROW_LZ4_ROOT})

    set(ARROW_CONFIGURE SOURCE_SUBDIR "cpp" CMAKE_ARGS ${ARROW_CMAKE_ARGS})
    set(PATCH_FILE "${CMAKE_CURRENT_LIST_DIR}/arrow.diff")
    externalproject_add(arrow_ep
                        URL ${ARROW_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_ARROW_BUILD_SHA256_CHECKSUM}"
                        LOG_PATCH ON
                        PATCH_COMMAND ${CMAKE_COMMAND} -E chdir <SOURCE_DIR> bash -c
                                      "[ -f .patched ] && echo '<SOURCE_DIR> patch already applied, ignore...' || patch -s -N -p1 -i '${PATCH_FILE}' && touch .patched"
                        GIT_SUBMODULES "" GIT_SUBMODULES_RECURSE FALSE ${ARROW_CONFIGURE}
                        UPDATE_DISCONNECTED 1
                        BUILD_BYPRODUCTS "${ARROW_STATIC_LIB}"
                                         "${ARROW_BUNDLED_DEP_STATIC_LIB}"
                                         "${PARQUET_STATIC_LIB}"
                                         "${ARROW_DATASET_STATIC_LIB}"
                                         "${ARROW_ACERO_STATIC_LIB}"
                        DEPENDS zstd snappy lz4 zlib)

    add_library(arrow STATIC IMPORTED)
    set_target_properties(arrow
                          PROPERTIES IMPORTED_LOCATION "${ARROW_PREFIX}/lib/libarrow.a"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}"
                                     INTERFACE_LINK_DIRECTORIES
                                     "${ARROW_BUILD_DIR}/${CMAKE_BUILD_TYPE_LOWER}")

    add_library(arrow_dataset STATIC IMPORTED)
    set_target_properties(arrow_dataset
                          PROPERTIES IMPORTED_LOCATION
                                     "${ARROW_PREFIX}/lib/libarrow_dataset.a"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}"
                                     INTERFACE_LINK_DIRECTORIES
                                     "${ARROW_BUILD_DIR}/${CMAKE_BUILD_TYPE_LOWER}")

    add_library(arrow_acero STATIC IMPORTED)
    set_target_properties(arrow_acero
                          PROPERTIES IMPORTED_LOCATION
                                     "${ARROW_PREFIX}/lib/libarrow_acero.a"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}"
                                     INTERFACE_LINK_DIRECTORIES
                                     "${ARROW_BUILD_DIR}/${CMAKE_BUILD_TYPE_LOWER}")

    add_library(parquet STATIC IMPORTED)
    set_target_properties(parquet
                          PROPERTIES IMPORTED_LOCATION "${ARROW_PREFIX}/lib/libparquet.a"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}"
                                     INTERFACE_LINK_DIRECTORIES
                                     "${ARROW_BUILD_DIR}/${CMAKE_BUILD_TYPE_LOWER}")

    add_library(arrow_bundled_dependencies STATIC IMPORTED)
    set_target_properties(arrow_bundled_dependencies
                          PROPERTIES IMPORTED_LOCATION
                                     "${ARROW_PREFIX}/lib/libarrow_bundled_dependencies.a"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}"
                                     INTERFACE_LINK_DIRECTORIES
                                     "${ARROW_BUILD_DIR}/${CMAKE_BUILD_TYPE_LOWER}")

    add_dependencies(arrow arrow_ep)
    add_dependencies(parquet arrow_ep)
    add_dependencies(arrow_bundled_dependencies arrow_ep)
    add_dependencies(arrow_dataset arrow_ep)
    add_dependencies(arrow_acero arrow_ep)

    target_link_libraries(arrow_acero INTERFACE arrow)

    target_link_libraries(arrow_dataset INTERFACE arrow_acero)

    target_link_libraries(arrow
                          INTERFACE zstd
                                    snappy
                                    lz4
                                    zlib
                                    arrow_bundled_dependencies)

    target_link_libraries(parquet
                          INTERFACE zstd
                                    snappy
                                    lz4
                                    zlib
                                    arrow_bundled_dependencies
                                    arrow_dataset)

endmacro(build_arrow)

macro(build_gtest)
    message(STATUS "Building gtest from source")

    set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -Wno-error")
    string(REPLACE "-Werror" "" GTEST_CMAKE_CXX_FLAGS ${GTEST_CMAKE_CXX_FLAGS})

    set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-install")
    set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")

    set(_GTEST_RUNTIME_DIR ${BUILD_OUTPUT_ROOT_DIRECTORY})

    # Library and runtime same on non-Windows
    set(_GTEST_LIBRARY_DIR "${_GTEST_RUNTIME_DIR}")

    if(CMAKE_BUILD_TYPE_LOWER STREQUAL "debug")
        set(GTEST_STATIC_LIB "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtestd.a")
        set(GMOCK_STATIC_LIB "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gmockd.a")
        set(GTEST_MAIN_STATIC_LIB
            "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_maind.a")
    else()
        set(GTEST_STATIC_LIB "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest.a")
        set(GMOCK_STATIC_LIB "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gmock.a")
        set(GTEST_MAIN_STATIC_LIB
            "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main.a")
    endif()
    set(GTEST_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        "-DCMAKE_INSTALL_PREFIX=${GTEST_PREFIX}"
        "-DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS}"
        "-DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${GTEST_CMAKE_CXX_FLAGS}"
        "-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY=${_GTEST_RUNTIME_DIR}"
        "-DCMAKE_RUNTIME_OUTPUT_DIRECTORY_${CMAKE_BUILD_TYPE}=${_GTEST_RUNTIME_DIR}")

    externalproject_add(googletest_ep
                        URL ${GTEST_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_GTEST_BUILD_SHA256_CHECKSUM}"
                        CMAKE_ARGS ${GTEST_CMAKE_ARGS}
                        BUILD_BYPRODUCTS "${GTEST_STATIC_LIB}" "${GTEST_MAIN_STATIC_LIB}"
                                         "${GMOCK_STATIC_LIB}")

    # The include directory must exist before it is referenced by a target.
    file(MAKE_DIRECTORY "${GTEST_INCLUDE_DIR}")

    add_library(GTest::gtest STATIC IMPORTED)
    set_target_properties(GTest::gtest
                          PROPERTIES IMPORTED_LOCATION "${GTEST_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIR}")

    add_library(GTest::gtest_main STATIC IMPORTED)
    set_target_properties(GTest::gtest_main
                          PROPERTIES IMPORTED_LOCATION "${GTEST_MAIN_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIR}")

    add_library(GTest::gmock STATIC IMPORTED)
    set_target_properties(GTest::gmock
                          PROPERTIES IMPORTED_LOCATION "${GMOCK_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIR}")
    add_dependencies(GTest::gtest googletest_ep)
    add_dependencies(GTest::gtest_main googletest_ep)
    add_dependencies(GTest::gmock googletest_ep)

    find_package(Threads REQUIRED)
    set(GTEST_LINK_TOOLCHAIN GTest::gtest_main GTest::gtest GTest::gmock Threads::Threads)
endmacro()

macro(build_tbb)
    message(STATUS "Building Tbb from source")

    set(TBB_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -Wno-error")
    set(TBB_CMAKE_C_FLAGS "${EP_C_FLAGS} -Wno-error")
    string(REPLACE "-Werror" "" TBB_CMAKE_CXX_FLAGS ${TBB_CMAKE_CXX_FLAGS})

    string(REPLACE "-Wdocumentation" "" TBB_CMAKE_CXX_FLAGS ${TBB_CMAKE_CXX_FLAGS})
    string(REPLACE "-Wdocumentation" "" TBB_CMAKE_C_FLAGS ${TBB_CMAKE_C_FLAGS})

    set(TBB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/tbb_ep-install")

    if(CMAKE_BUILD_TYPE_LOWER STREQUAL "debug")
        set(TBB_STATIC_LIB "${TBB_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}tbb_debug.a")
    else()
        set(TBB_STATIC_LIB "${TBB_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}tbb.a")
    endif()
    set(TBB_INCLUDE_DIR "${TBB_PREFIX}/include")

    file(MAKE_DIRECTORY "${TBB_INCLUDE_DIR}")

    set(TBB_BUILD_DIR "${CMAKE_BINARY_DIR}/tbb")

    string(TOLOWER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_LOWER)

    set(TBB_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        "-DCMAKE_INSTALL_PREFIX=${TBB_PREFIX}"
        "-DCMAKE_CXX_FLAGS=${TBB_CMAKE_CXX_FLAGS}"
        "-DCMAKE_C_FLAGS=${TBB_CMAKE_C_FLAGS}"
        "-DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${TBB_CMAKE_CXX_FLAGS}"
        -DTBB_TEST=OFF)

    externalproject_add(tbb_ep
                        URL ${TBB_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_TBB_BUILD_SHA256_CHECKSUM}"
                        CMAKE_ARGS ${TBB_CMAKE_ARGS}
                        BUILD_BYPRODUCTS "${TBB_STATIC_LIB}")

    add_library(tbb STATIC IMPORTED)
    set_target_properties(tbb
                          PROPERTIES IMPORTED_LOCATION "${TBB_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${TBB_INCLUDE_DIR}"
                                     INTERFACE_LINK_DIRECTORIES
                                     "${TBB_BUILD_DIR}/${CMAKE_BUILD_TYPE_LOWER}")
    add_dependencies(tbb tbb_ep)

endmacro(build_tbb)

macro(build_glog)
    message(STATUS "Building glog from source")
    set(GLOG_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/glog_ep-install")
    set(GLOG_INCLUDE_DIR "${GLOG_PREFIX}/include")
    if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
        set(GLOG_LIB_SUFFIX "d")
    else()
        set(GLOG_LIB_SUFFIX "")
    endif()
    set(GLOG_STATIC_LIB "${GLOG_PREFIX}/lib/libglog${GLOG_LIB_SUFFIX}.a")
    set(GLOG_CMAKE_CXX_FLAGS " -Wno-error ${EP_CXX_FLAGS}")
    set(GLOG_CMAKE_C_FLAGS " -Wno-error ${EP_C_FLAGS}")
    if(CMAKE_THREAD_LIBS_INIT)
        string(APPEND GLOG_CMAKE_CXX_FLAGS " ${CMAKE_THREAD_LIBS_INIT}")
        string(APPEND GLOG_CMAKE_C_FLAGS " ${CMAKE_THREAD_LIBS_INIT}")
    endif()

    set(GLOG_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        -DCMAKE_INSTALL_PREFIX=${GLOG_PREFIX}
        -DWITH_GFLAGS=OFF
        -DWITH_GTEST=OFF
        -DCMAKE_CXX_FLAGS=${GLOG_CMAKE_CXX_FLAGS}
        -DCMAKE_C_FLAGS=${GLOG_CMAKE_C_FLAGS})

    externalproject_add(glog_ep
                        URL ${GLOG_SOURCE_URL}
                        URL_HASH "SHA256=${PAIMON_GLOG_BUILD_SHA256_CHECKSUM}"
                        CMAKE_ARGS ${GLOG_CMAKE_ARGS}
                        BUILD_BYPRODUCTS "${GLOG_STATIC_LIB}")

    file(MAKE_DIRECTORY "${GLOG_INCLUDE_DIR}")
    add_library(glog STATIC IMPORTED)
    set_target_properties(glog
                          PROPERTIES IMPORTED_LOCATION "${GLOG_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${GLOG_INCLUDE_DIR}"
                                     INTERFACE_LINK_DIRECTORIES "${GLOG_BUILD_DIR}/lib"
                                     INTERFACE_COMPILE_DEFINITIONS "GLOG_USE_GLOG_EXPORT")

    add_dependencies(glog glog_ep)

    find_library(LIBUNWIND_LIBRARY NAMES unwind)
    if(LIBUNWIND_LIBRARY)
        target_link_libraries(glog INTERFACE ${LIBUNWIND_LIBRARY})
    endif()
endmacro()

build_fmt()
build_rapidjson()
build_snappy()
build_zstd()
build_zlib()
build_lz4()
build_arrow()
build_tbb()
build_glog()

if(PAIMON_ENABLE_AVRO)
    build_avro()
endif()
if(PAIMON_ENABLE_ORC)
    build_protobuf()
    build_orc()
endif()
if(PAIMON_ENABLE_JINDO)
    build_jindosdk_c()
    build_jindosdk_nextarch()
endif()
if(PAIMON_ENABLE_LUCENE)
    build_boost()
    build_lucene()
    build_jieba()
endif()
