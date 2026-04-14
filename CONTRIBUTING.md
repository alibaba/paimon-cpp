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

# Contributing to Paimon C++

Thank you for your interest in contributing to paimon-cpp! This document explains how to get started.

---

## Reporting Issues

If you find a bug or want to request a feature, please open an [issue](https://github.com/alibaba/paimon-cpp/issues/new). Include as much detail as possible — steps to reproduce, expected vs. actual behavior, environment info, etc.

---

## Submitting Pull Requests

1. **Fork** the repository and create a feature branch from `main`.
2. Make your changes following the [Code Style Guide](docs/code-style.md).
3. Add or update tests for the functionality you changed.
4. Ensure all checks pass.
5. Open a pull request against `main`. Fill in the [PR template](.github/PULL_REQUEST_TEMPLATE.md).

### PR Checklist

Before submitting, please verify:

- [ ] Code compiles without warnings under `-Wall` (enabled by default in the build system).
- [ ] `pre-commit run --all-files` passes.
- [ ] New / modified public APIs are marked with `PAIMON_EXPORT`.
- [ ] Every new file has the Apache 2.0 license header.
- [ ] Error handling uses `Status` / `Result<T>` — no exceptions.
- [ ] New utility code checks for existing helpers before reinventing (see the [utility reference](docs/code-style.md#reuse-existing-utilities)).
- [ ] Tests are added or updated for the changed functionality.
- [ ] PR description follows the [template](.github/PULL_REQUEST_TEMPLATE.md).

---

## Development Setup

### Prerequisites

- **C++17** compatible compiler (GCC recommended)
- **CMake** ≥ 3.16
- **Python 3** (for linting scripts and pre-commit)
- **git-lfs** (the repository uses Git LFS for large files)

---

## Code Style

Please read the full [Code Style Guide](docs/code-style.md) before writing code. Key highlights:

- **Formatting**: Google C++ Style base, 100-column limit, 4-space indent. Let clang-format handle it.
- **Error handling**: Use `Status` / `Result<T>` and project macros (`PAIMON_RETURN_NOT_OK`, `PAIMON_ASSIGN_OR_RAISE`). No exceptions.
- **Naming**: PascalCase for classes and methods, snake_case for variables, trailing `_` for members.
- **Headers**: `#pragma once`, includes ordered by category, Apache 2.0 license header on every file.
- **Factory pattern**: Use `static Create()` + private constructor when construction can fail.
- **Testing**: Google Test, `ASSERT_*` preferred over `EXPECT_*`, test files named `*_test.cpp` next to source.

---

## Dev Containers

We provide Dev Container configuration file templates for VS Code:

```bash
cd .devcontainer
cp Dockerfile.template Dockerfile
cp devcontainer.json.template devcontainer.json
```

Then select **Dev Containers: Reopen in Container** from VS Code's Command Palette.

If you make improvements that could benefit all developers, please update the template files and submit a pull request.

---

## License

By contributing to paimon-cpp, you agree that your contributions will be licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
