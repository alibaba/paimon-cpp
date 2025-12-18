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
    add_paimon_test(blob_table_inte_test
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${TEST_STATIC_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(data_evolution_table_test
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${TEST_STATIC_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(global_index_test
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${TEST_STATIC_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(write_and_read_inte_test
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${TEST_STATIC_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(clean_inte_test
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${TEST_STATIC_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(read_inte_test
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${TEST_STATIC_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(scan_and_read_inte_test
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${TEST_STATIC_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(scan_inte_test
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${TEST_STATIC_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(write_inte_test
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${TEST_STATIC_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(read_inte_with_index_test
                    STATIC_LINK_LIBS
                    paimon_shared
                    ${TEST_STATIC_LINK_LIBS}
                    test_utils_static
                    ${GTEST_LINK_TOOLCHAIN})
endif()
