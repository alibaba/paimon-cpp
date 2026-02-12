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

if(PAIMON_ENABLE_AVRO)

    set(PAIMON_AVRO_FILE_FORMAT
        avro_direct_decoder.cpp
        avro_direct_encoder.cpp
        avro_file_batch_reader.cpp
        avro_file_format.cpp
        avro_file_format_factory.cpp
        avro_format_writer.cpp
        avro_input_stream_impl.cpp
        avro_output_stream_impl.cpp
        avro_schema_converter.cpp
        avro_stats_extractor.cpp)

    add_paimon_lib(paimon_avro_file_format
                   SOURCES
                   ${PAIMON_AVRO_FILE_FORMAT}
                   EXTRA_INCLUDES
                   ${AVRO_INCLUDE_DIR}
                   DEPENDENCIES
                   paimon_shared
                   avro
                   STATIC_LINK_LIBS
                   arrow
                   glog
                   fmt
                   avro
                   tbb
                   dl
                   Threads::Threads
                   SHARED_LINK_LIBS
                   paimon_shared
                   SHARED_LINK_FLAGS
                   ${PAIMON_VERSION_SCRIPT_FLAGS})

    if(PAIMON_BUILD_TESTS)
        add_paimon_test(avro_format_test
                        SOURCES
                        avro_direct_encoder_decoder_test.cpp
                        avro_file_batch_reader_test.cpp
                        avro_file_format_test.cpp
                        avro_format_writer_test.cpp
                        avro_input_stream_impl_test.cpp
                        avro_schema_converter_test.cpp
                        avro_stats_extractor_test.cpp
                        avro_writer_builder_test.cpp
                        EXTRA_INCLUDES
                        ${AVRO_INCLUDE_DIR}
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        "-Wl,--whole-archive"
                        paimon_local_file_system_static
                        paimon_avro_file_format_static
                        "-Wl,--no-whole-archive"
                        ${GTEST_LINK_TOOLCHAIN})

    endif()

endif()
