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
check_clang_tidy=${3:-false}
build_type=${4:-Debug}
build_dir=${1}/build

# Display ccache status if available
if command -v ccache &> /dev/null; then
    echo "=== ccache found: $(ccache --version | head -1) ==="
    ccache -p | grep -E "cache_dir|max_size|compression" || true
    ccache -z  # Reset statistics for this build
else
    echo "=== ccache not found, compiling without cache acceleration ==="
fi

mkdir ${build_dir}
pushd ${build_dir}

ENABLE_LUMINA="ON"
ENABLE_LANCE="ON"
if [[ "${CC:-}" == *"gcc-8"* ]] || [[ "${CXX:-}" == *"g++-8"* ]]; then
    ENABLE_LUMINA="OFF" # Lumina is only supported on GCC 9 or higher.
    ENABLE_LANCE="OFF"
    # Lance's prebuilt binaries can only be compiled on Ubuntu 22.04 and above
    # which requires a higher version of glibc,
    # but Ubuntu 22.04 and above no longer ships with gcc-8 by default.
    # Consider supporting Lance from source compilation in the future
fi

CMAKE_ARGS=(
    "-G Ninja"
    "-DCMAKE_BUILD_TYPE=${build_type}"
    "-DPAIMON_BUILD_TESTS=ON"
    "-DPAIMON_ENABLE_LANCE=${ENABLE_LANCE}"
    "-DPAIMON_ENABLE_JINDO=ON"
    "-DPAIMON_ENABLE_LUMINA=${ENABLE_LUMINA}"
    "-DPAIMON_ENABLE_LUCENE=ON"
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

if [[ "${check_clang_tidy}" == "true" ]]; then
    cmake --build . --target check-clang-tidy
fi

# Print ccache statistics after build
if command -v ccache &> /dev/null; then
    echo "=== ccache statistics after build ==="
    ccache -s
fi

popd

rm -rf ${build_dir}
