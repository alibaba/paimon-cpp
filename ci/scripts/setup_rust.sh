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
# Install the Rust toolchain + cbindgen required to build the
# tantivy-fts FFI crate (third_party/tantivy_ffi) from CI.
#
# The dev container (see .devcontainer/) already has these preinstalled;
# this script is for the GitHub Actions runners. Called by
# .github/workflows/gcc_test.yaml and test_with_sanitizer.yaml before
# ci/scripts/build_paimon.sh.
#
# Idempotent: a second invocation is a no-op when the tools already exist.

set -eux

RUSTUP_VERSION=${RUSTUP_VERSION:-1.29.0}
RUST_VERSION=${RUST_VERSION:-1.85.0}
CBINDGEN_VERSION=${CBINDGEN_VERSION:-0.29.2}

# Install rustup + default toolchain if cargo isn't on PATH yet.
if ! command -v cargo >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain "${RUST_VERSION}" --profile minimal --no-modify-path
fi

# Export for the remainder of the CI job.
export PATH="${HOME}/.cargo/bin:${PATH}"
echo "${HOME}/.cargo/bin" >> "${GITHUB_PATH:-/dev/null}" || true

rustup toolchain install "${RUST_VERSION}" --profile minimal
rustup default "${RUST_VERSION}"
rustup component add rustfmt clippy

# cbindgen is used by the crate's build.rs to emit the C header that the
# C++ side includes. Corrosion will also run cbindgen at CMake configure
# time; both paths need it available.
if ! command -v cbindgen >/dev/null 2>&1; then
    cargo install cbindgen --version "${CBINDGEN_VERSION}" --locked
fi

rustc --version
cargo --version
cbindgen --version
