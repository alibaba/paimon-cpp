#!/usr/bin/env bash
# Smoke-test script for the tantivy-fts migration.
#
# Purpose: one-shot regression of the lucene-fts + tantivy-fts tests inside the
# Dev Container.
# Rationale: the command line gets long and error-prone, so wrap it in a script
# maintained per stage.
#
# Usage:
#   ./scripts/tantivy_smoke.sh                # default: release, no sanitizer
#   ./scripts/tantivy_smoke.sh --asan         # ASAN build
#   ./scripts/tantivy_smoke.sh --tsan         # TSAN build
#   ./scripts/tantivy_smoke.sh --configure    # cmake configure only
#   ./scripts/tantivy_smoke.sh --build        # cmake build only (skip configure)
#   ./scripts/tantivy_smoke.sh --tests-only   # ctest only (assumes already built)
#
# Maintenance notes:
#   - From Stage 1 on, update TEST_REGEX below whenever a new ctest target is added
#   - Stage 11 adds the full --with-asan / --with-tsan path

set -e

CMAKE_BUILD_TYPE="Release"
USE_ASAN="OFF"
USE_TSAN="OFF"
BUILD_DIR_SUFFIX=""
DO_CONFIGURE=1
DO_BUILD=1
DO_TEST=1

# ctest regex: during per-stage acceptance, run only this subset rather than the
# full ctest (~531s, too slow). Contents = the lucene-fts baseline + the
# tantivy-fts targets added in the current and previous stages. Append a target
# here as each stage completes. Only Stage 11 should run the full ctest.
TEST_REGEX='paimon-lucene-index-test|paimon-global-index-test|paimon-tantivy-smoke-test|paimon-tantivy-ffi-test|paimon-tantivy-tokenizer-test|paimon-tantivy-writer-test|paimon-tantivy-reader-test|paimon-tantivy-filter-limit-test|paimon-tantivy-index-test|paimon-tantivy-lucene-coexist-test|paimon-tantivy-equivalence-test|paimon-tantivy-streaming-test|paimon-tantivy-java-compat-test'

while [ $# -gt 0 ]; do
    case "$1" in
        --asan)        USE_ASAN="ON";  CMAKE_BUILD_TYPE="Debug"; BUILD_DIR_SUFFIX="-asan" ;;
        --tsan)        USE_TSAN="ON";  CMAKE_BUILD_TYPE="Debug"; BUILD_DIR_SUFFIX="-tsan" ;;
        --configure)   DO_BUILD=0; DO_TEST=0 ;;
        --build)       DO_CONFIGURE=0; DO_TEST=0 ;;
        --tests-only)  DO_CONFIGURE=0; DO_BUILD=0 ;;
        -h|--help)     sed -n '2,20p' "$0"; exit 0 ;;
        *)             echo "Unknown option: $1"; exit 2 ;;
    esac
    shift
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build${BUILD_DIR_SUFFIX}"

cd "${REPO_ROOT}"

if [ "${DO_CONFIGURE}" = "1" ]; then
    echo "==> cmake configure (${BUILD_DIR})"
    cmake -S . -B "${BUILD_DIR}" \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DPAIMON_BUILD_TESTS=ON \
        -DPAIMON_USE_ASAN="${USE_ASAN}" \
        -DPAIMON_USE_TSAN="${USE_TSAN}" \
        -DPAIMON_ENABLE_FSLIB=OFF \
        -DPAIMON_ENABLE_LUMINA=OFF \
        -DPAIMON_ENABLE_JINDO=OFF \
        -DPAIMON_ENABLE_LUCENE=ON \
        -DPAIMON_ENABLE_ORC=ON \
        -DPAIMON_ENABLE_ALIORC=ON \
        -DPAIMON_ENABLE_AVRO=ON \
        -G Ninja
fi

if [ "${DO_BUILD}" = "1" ]; then
    echo "==> cmake build"
    cmake --build "${BUILD_DIR}" -j
fi

if [ "${DO_TEST}" = "1" ]; then
    echo "==> ctest (${TEST_REGEX})"
    ctest --test-dir "${BUILD_DIR}" -R "${TEST_REGEX}" --output-on-failure
fi

echo "==> tantivy_smoke.sh DONE"
