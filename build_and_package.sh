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
BUILD_TYPE="Release"
BUILD_DIR="$SOURCE_ROOT/build-release"
BUILD_NAME="paimon-cpp"
MAKE_CLEAN=false
PACKAGE=false
CMAKE_OPTIONS=()
JOBS=""

show_help() {
    cat << EOF
Usage: $0 [options] [cmake_options...]

Options:
  -r, --release     Build release version (default)
  -d, --debug       Build debug version
  -c, --clean       Clean build directory before building
  -p, --package     Package creation
  -j, --jobs <num>  Number of parallel jobs for building (default: auto-detect)
  -h, --help        Show this help message

CMake Options:
  Any unrecognized options will be passed directly to CMake.
  You can specify multiple CMake options.

Examples:
  $0 -r -p -j 8 -DPAIMON_BUILD_SHARED=ON -DPAIMON_BUILD_STATIC=OFF
  $0 --debug --clean --package --jobs 4

EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -r|--release)
            BUILD_TYPE="Release"
            BUILD_DIR="$SOURCE_ROOT/build-release"
            BUILD_NAME="paimon-cpp"
            shift
            ;;
        -d|--debug)
            BUILD_TYPE="Debug"
            BUILD_DIR="$SOURCE_ROOT/build-debug"
            BUILD_NAME="paimon-cpp-debug"
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
        -j|--jobs)
            shift
            if [[ $# -gt 0 && $1 =~ ^[0-9]+$ ]]; then
                JOBS="$1"
                shift
            else
                echo "Error: -j/--jobs requires a numeric argument" >&2
                exit 1
            fi
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            # All remaining parameters are CMake options
            CMAKE_OPTIONS+=("$1")
            shift
            ;;
    esac
done

echo "========== Build Configuration =========="
echo "Build Type: $BUILD_TYPE"
echo "Package Name: $BUILD_NAME"
echo "Clean Build: $MAKE_CLEAN"
echo "Package: $PACKAGE"
if [ -n "$JOBS" ]; then
    echo "Parallel Jobs: $JOBS"
else
    echo "Parallel Jobs: auto-detect"
fi
if [ ${#CMAKE_OPTIONS[@]} -gt 0 ]; then
    echo "CMake Options: ${CMAKE_OPTIONS[*]}"
else
    echo "CMake Options: None"
fi
echo "========================================="

echo "Step 1: Downloading dependencies..."
"$SOURCE_ROOT"/third_party/download_dependencies.sh

echo "Step 2: Building Paimon..."
PACKAGE_DIR="$OUTPUT_DIR/$BUILD_NAME"

if [ "$MAKE_CLEAN" = true ]; then
    echo "Cleaning build directory: $BUILD_DIR"
    rm -rf "$BUILD_DIR"
fi
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

CMAKE_ARGS=(
    -G "Ninja"
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
    -DCMAKE_INSTALL_PREFIX="$PACKAGE_DIR"
)

if [ ${#CMAKE_OPTIONS[@]} -gt 0 ]; then
    CMAKE_ARGS+=("${CMAKE_OPTIONS[@]}")
fi

cmake "${CMAKE_ARGS[@]}" ..

# Set default JOBS if not specified
if [ -z "$JOBS" ]; then
    JOBS=$(nproc 2>/dev/null || echo 4)
fi

ninja -j"$JOBS"

if [ "$PACKAGE" = true ]; then
    echo "Step 3: Packaging..."
    mkdir -p "$OUTPUT_DIR"
    cd "$BUILD_DIR"
    ninja install
    tar -czvf "$OUTPUT_DIR/$BUILD_NAME.tar.gz" -C "$OUTPUT_DIR" "$BUILD_NAME"
    echo "Package created: $OUTPUT_DIR/$BUILD_NAME.tar.gz"
else
    echo "Step 3: Packaging skipped."
fi
