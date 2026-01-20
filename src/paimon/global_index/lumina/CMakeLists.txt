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

if(PAIMON_ENABLE_LUMINA)
    set(PAIMON_LUMINA_INDEX lumina_global_index.cpp lumina_global_index_factory.cpp)

    add_paimon_lib(paimon_lumina_index
                   SOURCES
                   ${PAIMON_LUMINA_INDEX}
                   DEPENDENCIES
                   paimon_shared
                   STATIC_LINK_LIBS
                   arrow
                   glog
                   fmt
                   dl
                   Threads::Threads
                   SHARED_LINK_LIBS
                   lumina::interface
                   paimon_shared
                   SHARED_LINK_FLAGS
                   ${PAIMON_VERSION_SCRIPT_FLAGS})

    if(PAIMON_BUILD_TESTS)
        add_paimon_test(lumina_index_test
                        SOURCES
                        lumina_utils_test.cpp
                        lumina_api_test.cpp
                        lumina_file_io_test.cpp
                        lumina_global_index_test.cpp
                        EXTRA_INCLUDES
                        ${LUMINA_INCLUDE_DIR}
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        "-Wl,--whole-archive"
                        paimon_local_file_system_static
                        paimon_lumina_index_static
                        "-Wl,--no-whole-archive"
                        lumina::interface
                        ${GTEST_LINK_TOOLCHAIN})
    endif()

endif()
