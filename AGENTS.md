<!---
  Copyright 2026-present Alibaba Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# AGENTS.md

This file provides repository-specific instructions for coding agents working on Paimon C++.
For contributor-facing setup and the complete coding conventions, also read
[`CONTRIBUTING.md`](CONTRIBUTING.md) and [`docs/code-style.md`](docs/code-style.md).

## Scope

These instructions apply to the entire repository. More deeply nested `AGENTS.md` files, if
added later, may provide additional or more specific instructions for their directory trees.

Keep the requested scope exact. Do not include unrelated refactors, formatting changes, API
redesigns, dependency updates, or generated files in a focused change.

## Repository Layout

- `include/paimon/`: public C++ API headers.
- `src/paimon/`: core implementation and most unit tests.
- `test/inte/`: end-to-end integration tests.
- `benchmark/`: benchmarks and benchmark-specific tests.
- `examples/`: example programs.
- `docs/` and `apidoc/`: user documentation and API documentation.
- `cmake_modules/` and `build_support/`: CMake helpers and build infrastructure.
- `ci/`: scripts used by continuous integration.
- `test/test_data/`: checked-in test fixtures.
- `third_party/`: third-party sources and patches.
- `build/`, `build-release/`, and `output/`: generated or local build output.

Do not edit `third_party/`, checked-in fixtures, generated output, or Git LFS objects unless the
task explicitly requires it. Never add files from local build directories to a change.

## Working Rules

1. Inspect the current implementation, nearby tests, and relevant CMake target before editing.
2. Search for an existing helper or established pattern before adding a new abstraction.
3. Preserve user changes and untracked local files. Do not discard, overwrite, or reformat
   unrelated work.
4. Prefer the smallest change that fully implements or fixes the requested behavior.
5. Add or update a focused regression test for behavior changes when practical.
6. Do not claim a build or test passed unless the corresponding command completed successfully.
7. Do not commit, push, amend commits, or create a pull request unless explicitly requested.

When changing a public API, check the declaration under `include/paimon/`, its implementation,
symbol visibility, documentation, callers, and tests together.

## C++ Requirements

The full rules are in [`docs/code-style.md`](docs/code-style.md). In particular:

- Use C++17; do not introduce C++20 or later features.
- Use `Status` and `Result<T>` for fallible operations. Do not use exceptions for production
  error propagation.
- Propagate errors with the project macros, including `PAIMON_RETURN_NOT_OK` and
  `PAIMON_ASSIGN_OR_RAISE`.
- Use an explicit type, not `auto`, as the declaration in `PAIMON_ASSIGN_OR_RAISE` and
  `PAIMON_ASSIGN_OR_RAISE_FROM_ARROW`.
- Prefer `std::unique_ptr` for sole ownership and use `std::shared_ptr` only for genuine shared
  ownership.
- Use `static Create()` plus a private constructor when object initialization can fail.
- Mark new public API symbols with `PAIMON_EXPORT`.
- Reuse helpers under `src/paimon/common/utils/` instead of duplicating utility code.
- Follow `.clang-format`; do not manually restyle unrelated code.
- Add the repository's Apache 2.0 license header to every new source or documentation file.

## Build and Test

Use an existing configured build directory when it is compatible with the change. To configure a
new debug build with tests:

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Debug \
  -DPAIMON_BUILD_TESTS=ON
```

Start with the narrowest relevant validation:

```bash
cmake --build build --target <test-target> -j "$(nproc)"
./build/debug/<test-binary> --gtest_filter='<Suite.Test>'
```

Then broaden validation in proportion to the change:

```bash
# All unit tests
cmake --build build --target unittest -j "$(nproc)"

# Formatting, lint, and repository checks for changed files
pre-commit run --files <changed-files>
git diff --check
```

For changes to build configuration, public APIs, shared infrastructure, or cross-module behavior,
run the relevant wider test suite. The CI-equivalent build entry point is
`ci/scripts/build_paimon.sh`; it may rebuild all dependencies and take substantially longer than a
focused local target.

If a required check cannot be run because of missing dependencies, unsupported hardware, or time
constraints, report exactly what was and was not run.

## Testing Conventions

- Use GoogleTest and name test files `*_test.cpp`.
- Place unit tests next to the corresponding implementation unless an existing target establishes
  another location.
- Extend an existing test target when appropriate instead of creating a new executable for one
  small test.
- Test externally observable behavior and failure cases; avoid coupling tests to incidental
  implementation details.
- Use existing test utilities and temporary-directory helpers. Do not write tests that depend on
  developer-specific absolute paths.
- Keep fixtures deterministic and small. Do not rewrite existing fixture data unless the task
  explicitly calls for it.
- Use `ASSERT_*` when later assertions depend on the condition succeeding.

## Delivery

Before handing off a change:

1. Review `git diff` for accidental or unrelated edits.
2. Run `git diff --check`.
3. Run the narrowest relevant build and test, plus any wider checks justified by the risk.
4. Summarize the changed behavior and list the exact validation commands that ran.
5. Call out skipped validation, remaining risks, or follow-up work explicitly.
6. When explicitly asked to commit, follow the Conventional Commits requirements in
   [`CONTRIBUTING.md`](CONTRIBUTING.md#commit-messages-and-pull-request-titles).
7. When explicitly asked to open a pull request, follow
   [`CONTRIBUTING.md`](CONTRIBUTING.md#commit-messages-and-pull-request-titles), use a
   Conventional Commits title, and complete every applicable section of
   [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md).
