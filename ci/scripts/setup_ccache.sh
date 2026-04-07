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

# Centralized ccache configuration for CI environments.
# All ccache parameters are defined here as environment variables so that
# they can be tuned in a single place.

set -euo pipefail

# ---- ccache parameters (edit here to change defaults) ----
export CCACHE_MAXSIZE="${CCACHE_MAXSIZE:-5G}"
export CCACHE_COMPRESS="${CCACHE_COMPRESS:-true}"
export CCACHE_COMPRESSLEVEL="${CCACHE_COMPRESSLEVEL:-6}"

# Apply configuration via ccache CLI (persisted to ~/.ccache/ccache.conf)
ccache --set-config=max_size="${CCACHE_MAXSIZE}"
ccache --set-config=compression="${CCACHE_COMPRESS}"
ccache --set-config=compression_level="${CCACHE_COMPRESSLEVEL}"

# Reset statistics so post-build stats reflect only this build
ccache -z

echo "=== ccache configured ==="
ccache -p | grep -E "max_size|compression" || true
