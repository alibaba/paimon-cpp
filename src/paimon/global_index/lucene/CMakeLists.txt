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

if(PAIMON_ENABLE_LUCENE)
    set(PAIMON_LUCENE
        lucene_global_index.cpp
        lucene_directory.cpp
        lucene_utils.cpp
        jieba_analyzer.cpp
        lucene_global_index_writer.cpp
        lucene_global_index_reader.cpp
        lucene_global_index_factory.cpp)

    add_paimon_lib(paimon_lucene_index
                   SOURCES
                   ${PAIMON_LUCENE}
                   EXTRA_INCLUDES
                   ${LUCENE_INCLUDE_DIR}
                   ${JIEBA_INCLUDE_DIR}
                   DEPENDENCIES
                   paimon_shared
                   lucene
                   jieba
                   STATIC_LINK_LIBS
                   lucene
                   arrow
                   fmt
                   dl
                   Threads::Threads
                   SHARED_LINK_LIBS
                   paimon_shared
                   SHARED_LINK_FLAGS
                   ${PAIMON_VERSION_SCRIPT_FLAGS})
    if(PAIMON_BUILD_TESTS)
        target_compile_definitions(paimon_lucene_index_objlib
                                   PRIVATE JIEBA_TEST_DICT_DIR="${JIEBA_DICT_DIR}")

    endif()

    if(PAIMON_BUILD_TESTS)
        add_paimon_test(lucene_index_test
                        SOURCES
                        jieba_analyzer_test.cpp
                        lucene_api_test.cpp
                        jieba_api_test.cpp
                        lucene_directory_test.cpp
                        lucene_global_index_test.cpp
                        lucene_filter_test.cpp
                        EXTRA_INCLUDES
                        ${LUCENE_INCLUDE_DIR}
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        "-Wl,--whole-archive"
                        paimon_local_file_system_static
                        paimon_lucene_index_static
                        "-Wl,--no-whole-archive"
                        ${GTEST_LINK_TOOLCHAIN})

    endif()
endif()
