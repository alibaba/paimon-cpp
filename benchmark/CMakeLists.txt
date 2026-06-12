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

if(NOT PAIMON_BUILD_BENCHMARKS AND NOT PAIMON_BUILD_TESTS)
    return()
endif()

find_package(Threads REQUIRED)

set(PAIMON_BENCHMARK_STATIC_LINK_LIBS
    paimon_shared ${PAIMON_LOCAL_FILE_SYSTEM_SHARED_LINK_LIBS}
    ${PAIMON_PARQUET_FILE_FORMAT_STATIC_LINK_LIBS}
    ${PAIMON_BLOB_FILE_FORMAT_STATIC_LINK_LIBS})

if(PAIMON_ENABLE_ORC)
    list(APPEND PAIMON_BENCHMARK_STATIC_LINK_LIBS
         ${PAIMON_ORC_FILE_FORMAT_STATIC_LINK_LIBS})
endif()

if(PAIMON_ENABLE_AVRO)
    list(APPEND PAIMON_BENCHMARK_STATIC_LINK_LIBS
         ${PAIMON_AVRO_FILE_FORMAT_STATIC_LINK_LIBS})
endif()

set(PAIMON_BENCHMARK_PLATFORM_LINK_LIBS)
if(UNIX AND NOT APPLE)
    find_library(PAIMON_BENCHMARK_RT_LIBRARY rt)
    if(PAIMON_BENCHMARK_RT_LIBRARY)
        list(APPEND PAIMON_BENCHMARK_PLATFORM_LINK_LIBS ${PAIMON_BENCHMARK_RT_LIBRARY})
    endif()
endif()

if(PAIMON_BUILD_BENCHMARKS)
    add_paimon_benchmark(read_write_benchmark
                         SOURCES
                         benchmark_helpers.cpp
                         benchmark_suite.cpp
                         benchmark_case_write.cpp
                         benchmark_case_read.cpp
                         benchmark_case_pk_write.cpp
                         benchmark_case_mor_read.cpp
                         read_write_benchmark.cpp
                         STATIC_LINK_LIBS
                         arrow
                         parquet
                         ${PAIMON_BENCHMARK_STATIC_LINK_LIBS}
                         test_utils_static
                         Threads::Threads
                         ${CMAKE_DL_LIBS}
                         ${PAIMON_BENCHMARK_PLATFORM_LINK_LIBS}
                         ${PAIMON_BENCHMARK_LINK_TOOLCHAIN}
                         EXTRA_INCLUDES
                         ${CMAKE_SOURCE_DIR})
endif()

if(PAIMON_BUILD_TESTS)
    add_paimon_test(cli_option_parsing_test
                    SOURCES
                    cli_option_parsing_test.cpp
                    EXTRA_INCLUDES
                    ${CMAKE_SOURCE_DIR}
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${GTEST_LINK_TOOLCHAIN})
endif()
