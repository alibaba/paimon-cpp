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

if(PAIMON_ENABLE_LANCE)
    set(PAIMON_LANCE_FILE_FORMAT lance_file_batch_reader.cpp lance_format_writer.cpp
                                 lance_stats_extractor.cpp lance_file_format_factory.cpp)

    add_paimon_lib(paimon_lance_file_format
                   SOURCES
                   ${PAIMON_LANCE_FILE_FORMAT}
                   DEPENDENCIES
                   paimon_shared
                   STATIC_LINK_LIBS
                   arrow
                   glog
                   fmt
                   ${CMAKE_DL_LIBS}
                   Threads::Threads
                   SHARED_LINK_LIBS
                   paimon_shared
                   lance_lib_rc
                   SHARED_LINK_FLAGS
                   ${PAIMON_VERSION_SCRIPT_FLAGS})

    if(PAIMON_BUILD_TESTS)
        add_paimon_test(lance_format_test
                        SOURCES
                        lance_file_format_test.cpp
                        lance_api_test.cpp
                        lance_format_reader_writer_test.cpp
                        lance_stats_extractor_test.cpp
                        EXTRA_INCLUDES
                        ${LANCE_INCLUDE_DIR}
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        ${PAIMON_LANCE_FILE_FORMAT_STATIC_LINK_LIBS}
                        ${PAIMON_LOCAL_FILE_SYSTEM_STATIC_LINK_LIBS}
                        lance_lib_rc
                        ${GTEST_LINK_TOOLCHAIN})
    endif()

endif()
