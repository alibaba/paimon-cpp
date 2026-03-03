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

if(PAIMON_ENABLE_JINDO)
    set(PAIMON_JINDO_FILE_SYSTEM jindo_file_system.cpp jindo_file_system_factory.cpp)

    add_paimon_lib(paimon_jindo_file_system
                   SOURCES
                   ${PAIMON_JINDO_FILE_SYSTEM}
                   EXTRA_INCLUDES
                   ${JINDOSDK_INCLUDE_DIR}
                   DEPENDENCIES
                   paimon_shared
                   jindosdk::c_sdk
                   jindosdk::nextarch
                   STATIC_LINK_LIBS
                   jindosdk::c_sdk
                   jindosdk::nextarch
                   fmt
                   SHARED_LINK_LIBS
                   paimon_shared
                   SHARED_LINK_FLAGS
                   ${PAIMON_VERSION_SCRIPT_FLAGS})
endif()
