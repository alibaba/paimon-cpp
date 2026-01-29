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
    set(PAIMON_LUCENE lucene_global_index.cpp lucene_directory.cpp
                      lucene_global_index_factory.cpp)

    add_paimon_lib(paimon_lucene_index
                   SOURCES
                   ${PAIMON_LUCENE}
                   EXTRA_INCLUDES
                   ${LUCENE_INCLUDE_DIR}
                   DEPENDENCIES
                   paimon_shared
                   lucene
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
        add_paimon_test(lucene_index_test
                        SOURCES
                        lucene_api_test.cpp
                        lucene_directory_test.cpp
                        lucene_global_index_test.cpp
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
