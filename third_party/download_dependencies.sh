#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Adapted from Apache Arrow
# https://github.com/apache/arrow/blob/main/cpp/thirdparty/download_dependencies.sh

# This script downloads all the thirdparty dependencies as a series of tarballs
# that can be used for offline builds, etc.

set -eu

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 1 ]; then
  orig_destdir=${SOURCE_DIR}
else
  orig_destdir=$1
fi

# Try to canonicalize. Not all platforms support `readlink -f` or `realpath`.
# This only matters if there are symlinks you need to resolve before downloading
DESTDIR=$(readlink -f "${orig_destdir}" 2> /dev/null) || DESTDIR="${orig_destdir}"

download_dependency() {
  local url=$1
  local out=$2
  local expected_checksum=$3

  # Determine which checksum command is available
  local checksum_cmd=""
  if command -v sha256sum >/dev/null 2>&1; then
    checksum_cmd=(sha256sum)
    checksum_field=1
  elif command -v shasum >/dev/null 2>&1; then
    checksum_cmd=(shasum -a 256)
    checksum_field=1
  elif command -v openssl >/dev/null 2>&1; then
    checksum_cmd=(openssl dgst -sha256)
    checksum_field=2
  else
    echo "Error: No checksum command available (sha256sum, shasum, or openssl)" 1>&2
    exit 1
  fi

  # Function to calculate checksum
  calculate_checksum() {
    local file=$1
    "${checksum_cmd[@]}" "${file}" | cut -d' ' -f"${checksum_field}"
  }

  # Check if the file already exists
  if [ -f "${out}" ]; then
    echo "File ${out} already exists, verifying checksum..."
    # Calculate checksum of existing file
    local actual_checksum
    actual_checksum=$(calculate_checksum "${out}")
    
    # Compare checksums
    if [ "${actual_checksum}" = "${expected_checksum}" ]; then
      echo "Checksum matches, skipping download ${out}"
      return 0
    else
      echo "Checksum mismatch (expected: ${expected_checksum}, actual: ${actual_checksum}), re-downloading..."
      rm -f "${out}"
    fi
  fi

  echo "Downloading ${url} to ${out}..."
  wget --continue --output-document="${out}" "${url}" || \
    (echo "Failed downloading ${url}" 1>&2; exit 1)
    
  # Verify checksum after download
  echo "Verifying checksum of downloaded file..."
  local actual_checksum
  actual_checksum=$(calculate_checksum "${out}")
  if [ "${actual_checksum}" != "${expected_checksum}" ]; then
    echo "Error: Checksum mismatch (expected: ${expected_checksum}, actual: ${actual_checksum})" 1>&2
    rm -f "${out}"
    exit 1
  fi
  echo "Checksum verification passed"
}

main() {
  mkdir -p "${DESTDIR}"

  # Load `DEPENDENCIES` variable.
  source "${SOURCE_DIR}"/versions.txt

  echo "# Environment variables for offline Paimon build"
  for ((i = 0; i < ${#DEPENDENCIES[@]}; i++)); do
    local dep_packed=${DEPENDENCIES[$i]}

    # Unpack each entry of the form "$home_var $tar_out $dep_url"
    IFS=" " read -r dep_url_var dep_tar_name dep_url <<< "${dep_packed}"

    # Get dependency name for finding checksum
    local dep_name=${dep_url_var%_URL}
    local checksum_var="${dep_name}_BUILD_SHA256_CHECKSUM"
    local expected_checksum=${!checksum_var}

    local out=${DESTDIR}/${dep_tar_name}
    download_dependency "${dep_url}" "${out}" "${expected_checksum}"

    echo "export ${dep_url_var}=${out}"
  done
}

main
