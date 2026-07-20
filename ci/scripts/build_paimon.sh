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

usage() {
    echo "Usage: $0 --source_dir <path> [--enable_asan] [--enable_ubsan] [--enable_tsan] [--check_clang_tidy] [--build_type <type>] [--lint_git_target_commit <commit-or-branch>]"
}

source_dir=""
enable_asan="false"
enable_ubsan="false"
enable_tsan="false"
check_clang_tidy="false"
build_type="Debug"
lint_git_target_commit="origin/main"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source_dir)
            if [[ $# -lt 2 ]]; then
                echo "Missing value for --source_dir" >&2
                usage >&2
                exit 1
            fi
            source_dir=$2
            shift 2
            ;;
        --enable_asan)
            enable_asan="true"
            shift
            ;;
        --enable_ubsan)
            enable_ubsan="true"
            shift
            ;;
        --enable_tsan)
            enable_tsan="true"
            shift
            ;;
        --check_clang_tidy)
            check_clang_tidy="true"
            shift
            ;;
        --build_type)
            if [[ $# -lt 2 ]]; then
                echo "Missing value for --build_type" >&2
                usage >&2
                exit 1
            fi
            build_type=$2
            shift 2
            ;;
        --lint_git_target_commit)
            if [[ $# -lt 2 ]]; then
                echo "Missing value for --lint_git_target_commit" >&2
                usage >&2
                exit 1
            fi
            lint_git_target_commit=$2
            shift 2
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [[ -z "${source_dir}" ]]; then
    echo "--source_dir is required" >&2
    usage >&2
    exit 1
fi

if [[ "${enable_asan}" == "true" && "${enable_tsan}" == "true" ]]; then
    echo "ASAN and TSAN cannot be enabled together" >&2
    usage >&2
    exit 1
fi

build_dir="${source_dir}/build"

# Display ccache status if available
if command -v ccache &> /dev/null; then
    echo "=== ccache found: $(ccache --version | head -1) ==="
    ccache -p | grep -E "cache_dir|max_size|compression" || true
    ccache -z  # Reset statistics for this build
else
    echo "=== ccache not found, compiling without cache acceleration ==="
fi

mkdir -p "${build_dir}"
pushd "${build_dir}"

ENABLE_LUMINA="ON"
ENABLE_LANCE="ON"
ENABLE_TANTIVY="ON"
if [[ "${CC:-}" == *"gcc-8"* ]] || [[ "${CXX:-}" == *"g++-8"* ]]; then
    ENABLE_LUMINA="OFF" # Lumina is only supported on GCC 9 or higher.
    ENABLE_LANCE="OFF"
    # Lance's prebuilt binaries can only be compiled on Ubuntu 22.04 and above
    # which requires a higher version of glibc,
    # but Ubuntu 22.04 and above no longer ships with gcc-8 by default.
    # Consider supporting Lance from source compilation in the future
    ENABLE_TANTIVY="OFF" # tantivy-fts (Rust FFI) is not built on the gcc-8 image.
fi
if [[ "${enable_tsan}" == "true" ]]; then
    ENABLE_TANTIVY="OFF" # Tantivy's Rust library is not TSAN-instrumented.
fi

CMAKE_ARGS=(
    "-G Ninja"
    "-DCMAKE_BUILD_TYPE=${build_type}"
    "-DPAIMON_BUILD_TESTS=ON"
    "-DPAIMON_ENABLE_LANCE=${ENABLE_LANCE}"
    "-DPAIMON_ENABLE_JINDO=ON"
    "-DPAIMON_ENABLE_LUMINA=${ENABLE_LUMINA}"
    "-DPAIMON_ENABLE_LUCENE=ON"
    "-DPAIMON_ENABLE_TANTIVY=${ENABLE_TANTIVY}"
    "-DPAIMON_LINT_GIT_TARGET_COMMIT=${lint_git_target_commit}"
)

if [[ "${enable_asan}" == "true" ]]; then
    CMAKE_ARGS+=("-DPAIMON_USE_ASAN=ON")
fi
if [[ "${enable_ubsan}" == "true" ]]; then
    CMAKE_ARGS+=("-DPAIMON_USE_UBSAN=ON")
fi
if [[ "${enable_tsan}" == "true" ]]; then
    CMAKE_ARGS+=("-DPAIMON_USE_TSAN=ON")
fi

cmake "${CMAKE_ARGS[@]}" "${source_dir}"
cmake --build . -- -j "$(nproc)"
ctest --output-on-failure -j "$(nproc)"

if [[ "${check_clang_tidy}" == "true" ]]; then
    cmake --build . --target check-clang-tidy
fi

# Print ccache statistics after build
if command -v ccache &> /dev/null; then
    echo "=== ccache statistics after build ==="
    ccache -s
fi

popd

rm -rf "${build_dir}"
