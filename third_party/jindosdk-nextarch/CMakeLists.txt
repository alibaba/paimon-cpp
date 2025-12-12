# Copyright 2024-present Alibaba Cloud.
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

cmake_minimum_required(VERSION 3.17)

project(jindosdk-nextarch)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules")

include_directories(${CMAKE_CURRENT_LIST_DIR}/include)

find_package(jindosdk_c REQUIRED)

file(GLOB_RECURSE JINDOSDK_SOURCES src/*.cpp)

add_library(jindosdk-nextarch STATIC ${JINDOSDK_SOURCES})
target_link_libraries(jindosdk-nextarch PUBLIC pthread dl JINDOSDK::JINDOSDK)
target_include_directories(jindosdk-nextarch
                           PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                  $<INSTALL_INTERFACE:include>)

install(TARGETS jindosdk-nextarch
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
        DESTINATION "include"
        FILES_MATCHING
        PATTERN "*.hpp")
