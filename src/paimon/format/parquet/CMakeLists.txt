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

set(PAIMON_PARQUET_FILE_FORMAT
    parquet_field_id_converter.cpp
    predicate_converter.cpp
    file_reader_wrapper.cpp
    parquet_timestamp_converter.cpp
    parquet_file_batch_reader.cpp
    parquet_file_format_factory.cpp
    parquet_format_writer.cpp
    parquet_schema_util.cpp
    parquet_stats_extractor.cpp
    parquet_writer_builder.cpp)

add_paimon_lib(paimon_parquet_file_format
               SOURCES
               ${PAIMON_PARQUET_FILE_FORMAT}
               DEPENDENCIES
               paimon_shared
               parquet
               STATIC_LINK_LIBS
               parquet
               arrow
               glog
               fmt
               dl
               Threads::Threads
               SHARED_LINK_LIBS
               paimon_shared
               SHARED_LINK_FLAGS
               ${PAIMON_VERSION_SCRIPT_FLAGS})

if(PAIMON_BUILD_TESTS)
    add_paimon_test(parquet_format_test
                    SOURCES
                    file_reader_wrapper_test.cpp
                    parquet_timestamp_converter_test.cpp
                    parquet_field_id_converter_test.cpp
                    parquet_file_batch_reader_test.cpp
                    parquet_format_writer_test.cpp
                    parquet_stats_extractor_test.cpp
                    parquet_writer_builder_test.cpp
                    predicate_converter_test.cpp
                    predicate_pushdown_test.cpp
                    STATIC_LINK_LIBS
                    paimon_shared
                    test_utils_static
                    "-Wl,--whole-archive"
                    paimon_local_file_system_static
                    paimon_parquet_file_format_static
                    "-Wl,--no-whole-archive"
                    parquet
                    ${GTEST_LINK_TOOLCHAIN})
endif()
