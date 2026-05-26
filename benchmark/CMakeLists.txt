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
    paimon_shared
    "-Wl,--whole-archive"
    paimon_local_file_system_shared
    "-Wl,--no-whole-archive"
    "-Wl,--no-as-needed"
    paimon_parquet_file_format_shared
    paimon_blob_file_format_shared
    "-Wl,--as-needed")

if(PAIMON_ENABLE_ORC)
    list(APPEND PAIMON_BENCHMARK_STATIC_LINK_LIBS "-Wl,--no-as-needed")
    list(APPEND PAIMON_BENCHMARK_STATIC_LINK_LIBS paimon_orc_file_format_shared)
    list(APPEND PAIMON_BENCHMARK_STATIC_LINK_LIBS "-Wl,--as-needed")
endif()

if(PAIMON_ENABLE_AVRO)
    list(APPEND PAIMON_BENCHMARK_STATIC_LINK_LIBS "-Wl,--no-as-needed")
    list(APPEND PAIMON_BENCHMARK_STATIC_LINK_LIBS paimon_avro_file_format_shared)
    list(APPEND PAIMON_BENCHMARK_STATIC_LINK_LIBS "-Wl,--as-needed")
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
                         Threads::Threads
                         ${CMAKE_DL_LIBS}
                         rt
                         ${PAIMON_BENCHMARK_LINK_TOOLCHAIN})
endif()

if(PAIMON_BUILD_TESTS)
    add_paimon_test(cli_option_parsing_test
                    SOURCES
                    cli_option_parsing_test.cpp
                    STATIC_LINK_LIBS
                    ${GTEST_LINK_TOOLCHAIN})
endif()
