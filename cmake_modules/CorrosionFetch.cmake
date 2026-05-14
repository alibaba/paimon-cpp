# Copyright 2026-present Alibaba Inc.
#
# Licensed under the Apache License, Version 2.0.
#
# Pull Corrosion-rs via FetchContent so we can import Cargo crates as CMake
# targets. Used to bring in third_party/tantivy_ffi for the tantivy-fulltext
# global index (see docs/dev/tantivy_fts_migration_plan.md).
#
# Pinned to v0.5.0 (stable release). Requires CMake >= 3.22.

include(FetchContent)

# Corrosion does heavy cargo/rustc work at configure+build time; pin tag for
# reproducibility and allow override via env var for offline builds.
set(PAIMON_CORROSION_TAG "v0.5.2" CACHE STRING
    "Git tag of corrosion-rs to fetch; change only when upgrading. v0.5.1+
    is required for rustup >= 1.28 whose `rustup toolchain list --verbose`
    output format broke v0.5.0's FindRust.cmake regex.")

set(PAIMON_CORROSION_REPO "https://github.com/corrosion-rs/corrosion.git"
    CACHE STRING "Override to a private mirror for offline / firewalled builds.")

# Help Corrosion find rustc/cargo when CMake is invoked without a login shell
# or when rustup is installed to a non-default location. We try, in order:
#   1. Existing Rust_COMPILER cache variable (user override)
#   2. $CARGO_HOME/bin/rustc (when env var set)
#   3. $HOME/.cargo/bin/rustc (rustup's default install)
#   4. Fallback: let Corrosion's FindRust.cmake try its own detection
function(_paimon_find_rustup_bin _var _name)
    if(DEFINED ENV{CARGO_HOME} AND EXISTS "$ENV{CARGO_HOME}/bin/${_name}")
        set(${_var} "$ENV{CARGO_HOME}/bin/${_name}" PARENT_SCOPE)
    elseif(DEFINED ENV{HOME} AND EXISTS "$ENV{HOME}/.cargo/bin/${_name}")
        set(${_var} "$ENV{HOME}/.cargo/bin/${_name}" PARENT_SCOPE)
    endif()
endfunction()

if(NOT DEFINED Rust_COMPILER OR Rust_COMPILER STREQUAL "")
    _paimon_find_rustup_bin(_rustc_path rustc)
    if(_rustc_path)
        set(Rust_COMPILER "${_rustc_path}" CACHE FILEPATH "rustc")
    endif()
endif()
if(NOT DEFINED Rust_CARGO OR Rust_CARGO STREQUAL "")
    _paimon_find_rustup_bin(_cargo_path cargo)
    if(_cargo_path)
        set(Rust_CARGO "${_cargo_path}" CACHE FILEPATH "cargo")
    endif()
endif()
# Corrosion reads `rustup which rustc` to resolve the real toolchain binary.
# If CMake is invoked from a non-login shell, $PATH may miss ~/.cargo/bin and
# `rustup` can't be found. Prepend rustup's bin dir so child processes see it.
if(DEFINED Rust_COMPILER)
    get_filename_component(_rustup_bin_dir "${Rust_COMPILER}" DIRECTORY)
    if(_rustup_bin_dir AND NOT "$ENV{PATH}" MATCHES "${_rustup_bin_dir}")
        set(ENV{PATH} "${_rustup_bin_dir}:$ENV{PATH}")
    endif()
endif()
message(STATUS "Corrosion: Rust_COMPILER=${Rust_COMPILER}")
message(STATUS "Corrosion: Rust_CARGO=${Rust_CARGO}")

FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY "${PAIMON_CORROSION_REPO}"
    GIT_TAG "${PAIMON_CORROSION_TAG}"
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(Corrosion)
