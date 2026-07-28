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

if(PAIMON_ENABLE_S3)
    add_paimon_lib(paimon_s3_file_system
                   SOURCES
                   s3_file_system.cpp
                   s3_file_system_factory.cpp
                   EXTRA_INCLUDES
                   ${AWS_AUTH_INCLUDE_DIR}
                   DEPENDENCIES
                   paimon_shared
                   CURL::libcurl
                   STATIC_LINK_LIBS
                   arrow
                   CURL::libcurl
                   aws_auth_minimal
                   fmt
                   SHARED_LINK_LIBS
                   paimon_shared
                   SHARED_LINK_FLAGS
                   ${PAIMON_VERSION_SCRIPT_FLAGS})

    add_dependencies(paimon_s3_file_system_objlib aws-c-auth_ep)

    if(PAIMON_BUILD_TESTS)
        add_paimon_test(s3_file_system_test
                        SOURCES
                        s3_file_system_test.cpp
                        EXTRA_INCLUDES
                        ${AWS_AUTH_INCLUDE_DIR}
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        ${PAIMON_LOCAL_FILE_SYSTEM_STATIC_LINK_LIBS}
                        ${PAIMON_S3_FILE_SYSTEM_STATIC_LINK_LIBS}
                        ${GTEST_LINK_TOOLCHAIN})
    endif()
endif()
