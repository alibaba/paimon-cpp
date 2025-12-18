#!/usr/bin/env bash
#
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

set -eux

source_dir=${1}
enable_sanitizer=${2:-false}
build_dir=${1}/build

mkdir ${build_dir}
pushd ${build_dir}

CMAKE_ARGS=(
    "-G Ninja"
    "-DCMAKE_BUILD_TYPE=Debug"
    "-DPAIMON_BUILD_TESTS=ON"
    "-DPAIMON_ENABLE_LANCE=ON"
    "-DPAIMON_ENABLE_JINDO=ON"
)

if [[ "${enable_sanitizer}" == "true" ]]; then
    CMAKE_ARGS+=(
        "-DPAIMON_USE_ASAN=ON"
        "-DPAIMON_USE_UBSAN=ON"
    )
fi

cmake "${CMAKE_ARGS[@]}" ${source_dir}
cmake --build . -- -j$(nproc)
ctest --output-on-failure -j $(nproc)

popd

rm -rf ${build_dir}
