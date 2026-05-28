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

if(PAIMON_BUILD_TESTS)

    set(PAIMON_TEST_UTILS testharness.cpp data_generator.cpp)

    add_paimon_lib(test_utils
                   SOURCES
                   ${PAIMON_TEST_UTILS}
                   DEPENDENCIES
                   paimon_static
                   arrow
                   STATIC_LINK_LIBS
                   paimon_static
                   ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(test_utils_test
                    SOURCES
                    data_generator_test.cpp
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${PAIMON_LOCAL_FILE_SYSTEM_SHARED_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})

endif()
