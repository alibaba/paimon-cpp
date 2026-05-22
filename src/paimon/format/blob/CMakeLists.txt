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

set(PAIMON_BLOB_FILE_FORMAT blob_file_batch_reader.cpp blob_file_format_factory.cpp
                            blob_format_writer.cpp blob_stats_extractor.cpp)

add_paimon_lib(paimon_blob_file_format
               SOURCES
               ${PAIMON_BLOB_FILE_FORMAT}
               EXTRA_INCLUDES
               ${BLOB_INCLUDE_DIR}
               DEPENDENCIES
               paimon_shared
               STATIC_LINK_LIBS
               arrow
               fmt
               ${CMAKE_DL_LIBS}
               Threads::Threads
               SHARED_LINK_LIBS
               paimon_shared
               SHARED_LINK_FLAGS
               ${PAIMON_VERSION_SCRIPT_FLAGS})

if(PAIMON_BUILD_TESTS)
    add_paimon_test(blob_format_test
                    SOURCES
                    blob_format_writer_test.cpp
                    blob_file_batch_reader_test.cpp
                    blob_stats_extractor_test.cpp
                    blob_writer_builder_test.cpp
                    blob_file_format_factory_test.cpp
                    EXTRA_INCLUDES
                    ${BLOB_INCLUDE_DIR}
                    STATIC_LINK_LIBS
                    paimon_shared
                    test_utils_static
                    ${PAIMON_BLOB_FILE_FORMAT_STATIC_LINK_LIBS}
                    ${PAIMON_LOCAL_FILE_SYSTEM_STATIC_LINK_LIBS}
                    ${GTEST_LINK_TOOLCHAIN})
endif()
