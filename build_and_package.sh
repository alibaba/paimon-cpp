#!/usr/bin/env bash
#
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

set -euo pipefail

SOURCE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUTPUT_DIR="$SOURCE_ROOT/output"
BUILD_TYPE="release"
BUILD_NAME="paimon-cpp"
MAKE_CLEAN=false
PACKAGE=false
CMAKE_OPTIONS=""

show_help() {
    cat << EOF
Usage: $0 [options] [cmake_options...]

Options:
  -r, --release     Build release version (default)
  -d, --debug       Build debug version
  -c, --clean       Clean build directory before building
  -p, --package     Package creation
  -h, --help        Show this help message

CMake Options:
  Any unrecognized options will be passed directly to CMake.
  You can specify multiple CMake options.

Examples:
  $0 -r -p -DPAIMON_BUILD_SHARED=ON -DPAIMON_BUILD_STATIC=OFF
  $0 --debug --clean --package

EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -r|--release)
            BUILD_TYPE="Release"
            shift
            ;;
        -d|--debug)
            BUILD_TYPE="Debug"
            BUILD_NAME=$BUILD_NAME"-debug"
            shift
            ;;
        -c|--clean)
            MAKE_CLEAN=true
            shift
            ;;
        -p|--package)
            PACKAGE=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            # All remaining parameters are CMake options
            CMAKE_OPTIONS="$CMAKE_OPTIONS $1"
            shift
            ;;
    esac
done

CMAKE_OPTIONS=$(echo "$CMAKE_OPTIONS" | xargs)

echo "========== Build Configuration =========="
echo "Build Type: $BUILD_TYPE"
echo "Package Name: $BUILD_NAME"
echo "Clean Build: $MAKE_CLEAN"
echo "Package: $PACKAGE"
if [ -n "$CMAKE_OPTIONS" ]; then
    echo "CMake Options: $CMAKE_OPTIONS"
else
    echo "CMake Options: None"
fi
echo "========================================="

echo "Step 1: Downloading dependencies..."
"$SOURCE_ROOT"/third_party/download_dependencies.sh

echo "Step 2: Building Paimon..."
BUILD_DIR="$SOURCE_ROOT/build-$BUILD_TYPE"
PACKAGE_DIR="$OUTPUT_DIR/$BUILD_NAME"

if [ "$MAKE_CLEAN" = true ]; then
    rm -rf "$BUILD_DIR"
fi
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
    -DCMAKE_INSTALL_PREFIX="$PACKAGE_DIR"
)

if [ -n "$CMAKE_OPTIONS" ]; then
    CMAKE_ARGS+=("$CMAKE_OPTIONS")
fi

cmake "${CMAKE_ARGS[@]}" ..

JOBS=$(nproc 2>/dev/null || echo 4)
make -j"$JOBS"

if [ "$PACKAGE" = true ]; then
    echo "Step 3: Packaging..."
    mkdir -p "$OUTPUT_DIR"
    cd "$BUILD_DIR"
    make install
    tar -czvf "$OUTPUT_DIR/$BUILD_NAME.tar.gz" -C "$OUTPUT_DIR" "$BUILD_NAME"
    echo "Package created: $OUTPUT_DIR/$BUILD_NAME.tar.gz"
else
    echo "Step 3: Packaging skipped."
fi
