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

cmake_minimum_required(VERSION 3.22)

project(example)

set(CMAKE_CXX_STANDARD 17)

# Arrow's static export refers to LZ4::lz4, while some lz4 packages export a
# differently named target. Define a compatibility alias before loading Arrow.
find_package(lz4 CONFIG QUIET)
if(TARGET LZ4::lz4_static AND NOT TARGET LZ4::lz4)
    add_library(LZ4::lz4 ALIAS LZ4::lz4_static)
elseif(TARGET lz4::lz4 AND NOT TARGET LZ4::lz4)
    add_library(LZ4::lz4 ALIAS lz4::lz4)
endif()

find_package(Arrow CONFIG REQUIRED)
find_package(Paimon CONFIG REQUIRED)

if(TARGET Arrow::arrow_shared)
    set(PAIMON_EXAMPLE_ARROW_TARGET Arrow::arrow_shared)
elseif(TARGET Arrow::arrow_static)
    set(PAIMON_EXAMPLE_ARROW_TARGET Arrow::arrow_static)
else()
    message(FATAL_ERROR "Neither Arrow::arrow_shared nor Arrow::arrow_static is available"
    )
endif()

add_executable(read_write_demo read_write_demo.cpp)
add_executable(clean_demo clean_demo.cpp)

set(CMAKE_EXE_LINKER_FLAGS "-Wl,--no-as-needed ${CMAKE_EXE_LINKER_FLAGS}")

target_link_libraries(read_write_demo
                      PRIVATE ${PAIMON_EXAMPLE_ARROW_TARGET}
                              Paimon::paimon_shared
                              Paimon::paimon_parquet_file_format_shared
                              Paimon::paimon_orc_file_format_shared
                              Paimon::paimon_local_file_system_shared)

target_link_libraries(clean_demo
                      PRIVATE ${PAIMON_EXAMPLE_ARROW_TARGET}
                              Paimon::paimon_shared
                              Paimon::paimon_parquet_file_format_shared
                              Paimon::paimon_orc_file_format_shared
                              Paimon::paimon_local_file_system_shared)
