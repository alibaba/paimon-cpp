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

if(PAIMON_ENABLE_ORC)

    set(PAIMON_ORC_FILE_FORMAT
        predicate_converter.cpp
        orc_reader_wrapper.cpp
        read_range_generator.cpp
        orc_file_format_factory.cpp
        orc_file_batch_reader.cpp
        orc_input_stream_impl.cpp
        orc_output_stream_impl.cpp
        orc_adapter.cpp
        orc_stats_extractor.cpp
        orc_format_writer.cpp)

    add_paimon_lib(paimon_orc_file_format
                   SOURCES
                   ${PAIMON_ORC_FILE_FORMAT}
                   EXTRA_INCLUDES
                   ${ORC_INCLUDE_DIR}
                   DEPENDENCIES
                   paimon_shared
                   orc::orc
                   STATIC_LINK_LIBS
                   arrow
                   glog
                   fmt
                   orc::orc
                   tbb
                   dl
                   Threads::Threads
                   SHARED_LINK_LIBS
                   paimon_shared
                   SHARED_LINK_FLAGS
                   ${PAIMON_VERSION_SCRIPT_FLAGS})

    if(PAIMON_BUILD_TESTS)
        add_paimon_test(orc_format_test
                        SOURCES
                        complex_predicate_test.cpp
                        predicate_converter_test.cpp
                        predicate_pushdown_test.cpp
                        read_range_generator_test.cpp
                        orc_reader_wrapper_test.cpp
                        orc_input_output_stream_test.cpp
                        orc_adapter_test.cpp
                        orc_stats_extractor_test.cpp
                        orc_format_writer_test.cpp
                        orc_file_batch_reader_test.cpp
                        EXTRA_INCLUDES
                        ${ORC_INCLUDE_DIR}
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        "-Wl,--whole-archive"
                        paimon_local_file_system_static
                        paimon_orc_file_format_static
                        "-Wl,--no-whole-archive"
                        ${GTEST_LINK_TOOLCHAIN})
    endif()
endif()
