#!/usr/bin/env bash
# tantivy-fts 迁移期 smoke 测试脚本。
#
# 用途: 在 Dev Container 内一键回归 lucene-fts + tantivy-fts 相关测试。
# 设计哲学: 命令行越拼越长容易出错,封装成一个脚本各 Stage 持续维护。
#
# 用法:
#   ./scripts/tantivy_smoke.sh                # default: release, no sanitizer
#   ./scripts/tantivy_smoke.sh --asan         # ASAN 构建
#   ./scripts/tantivy_smoke.sh --tsan         # TSAN 构建
#   ./scripts/tantivy_smoke.sh --configure    # 仅 cmake configure
#   ./scripts/tantivy_smoke.sh --build        # 仅 cmake build (跳过 configure)
#   ./scripts/tantivy_smoke.sh --tests-only   # 仅 ctest (假定已 build 过)
#
# 维护约定:
#   - Stage 1+ 每加一个新 ctest target 就更新下面 TEST_REGEX
#   - Stage 11 加 --with-asan / --with-tsan 完整路径

set -e

CMAKE_BUILD_TYPE="Release"
USE_ASAN="OFF"
USE_TSAN="OFF"
BUILD_DIR_SUFFIX=""
DO_CONFIGURE=1
DO_BUILD=1
DO_TEST=1

# ctest 正则: 各 Stage 验收时只跑这批测试,不跑全量 ctest (~531s 太慢)。
# 内容 = lucene-fts 对照基线 + 当前 Stage 及之前 Stage 新增的 tantivy-fts target。
# 每个 Stage 完成时往这里追加 target。只有 Stage 11 才应跑全量 ctest。
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
        -DPAIMON_ENABLE_LANCE=OFF \
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
