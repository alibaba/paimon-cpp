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

if [[ -n "${PAIMON_BUILD_JOBS:-}" ]]; then
    build_jobs="${PAIMON_BUILD_JOBS}"
elif command -v nproc >/dev/null 2>&1; then
    build_jobs=$(nproc)
elif command -v sysctl >/dev/null 2>&1; then
    build_jobs=$(sysctl -n hw.ncpu)
else
    build_jobs=4
fi

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
ENABLE_TANTIVY="ON"
if [[ "${CC:-}" == *"gcc-8"* ]] || [[ "${CXX:-}" == *"g++-8"* ]]; then
    ENABLE_LUMINA="OFF" # Lumina is only supported on GCC 9 or higher.
    ENABLE_TANTIVY="OFF" # tantivy-fts (Rust FFI) is not built on the gcc-8 image.
fi
if [[ "${enable_tsan}" == "true" ]]; then
    ENABLE_TANTIVY="OFF" # Tantivy's Rust library is not TSAN-instrumented.
fi

CMAKE_ARGS=(
    "-G Ninja"
    "-DCMAKE_BUILD_TYPE=${build_type}"
    "-DPAIMON_BUILD_TESTS=ON"
    "-DPAIMON_ENABLE_JINDO=ON"
    "-DPAIMON_ENABLE_S3=ON"
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
cmake --build . -- -j "${build_jobs}"

# Let the OS write a core file so build_support/run-test.sh can print a backtrace
# for a crashing test. The limit only applies to descendants of this shell, so it
# has to be raised here rather than in a separate CI step.
if [[ -n "${PAIMON_ENABLE_CORE_DUMPS:-}" ]]; then
    ulimit -c unlimited || echo "warning: failed to raise the core dump size limit"
fi

# TEMPORARY (flake hunt): when PAIMON_FLAKE_HUNT_TEST is set, rerun only the
# ctest tests it matches until one fails, instead of running the whole suite
# once. Each iteration is a fresh process going through build_support/run-test.sh,
# so a crash still gets its backtrace printed. Widen the regex to also reproduce
# the concurrency of a normal CI run. Remove together with the env vars set in
# .github/workflows/build_and_test.yaml.
if [[ -n "${PAIMON_FLAKE_HUNT_TEST:-}" ]]; then
    # --no-tests=error: a regex that matches nothing must fail instead of
    # reporting success without having run anything.
    ctest --output-on-failure -j "${build_jobs}" -R "${PAIMON_FLAKE_HUNT_TEST}" \
        --repeat "until-fail:${PAIMON_FLAKE_HUNT_RUNS:-100}" --no-tests=error
else
    ctest --output-on-failure -j "${build_jobs}"
fi

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
