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

# Paimon C++ Code Style Guide

This document defines the coding conventions for the paimon-cpp project. All pull requests are expected to follow these rules. Automated tooling (clang-format, clang-tidy, cpplint, pre-commit) enforces many of them.

---

## Language Standard

- **C++17** is the target standard.
- Do **not** use C++20 or later features.
- Only the **x86_64** architecture is currently supported.

---

## Formatting

Formatting is based on **Google C++ Style** with the following overrides (defined in `.clang-format`):

| Setting | Value |
|---------|-------|
| `ColumnLimit` | 100 |
| `IndentWidth` | 4 |
| `AccessModifierOffset` | -3 |
| `AllowShortFunctionsOnASingleLine` | Empty |

---

## Naming Conventions

| Category | Style | Example |
|----------|-------|---------|
| Class / Struct | PascalCase | `TableScan`, `FileUtils` |
| Method | PascalCase | `CreatePlan()`, `ReadFile()` |
| Member variable | snake_case with trailing `_` | `schema_id_`, `commit_user_` |
| Local variable / parameter | snake_case | `table_path`, `json_str` |
| Constant (new code) | `k` prefix + PascalCase | `kFieldVersion`, `kMaxRetryCount` |
| Constant (existing code) | Match surrounding style | `FIELD_VERSION` if that is the local convention |
| Namespace | lowercase | `paimon`, `paimon::test` |
| File name | snake_case | `table_scan.cpp`, `file_utils.h` |
| Test file | `*_test.cpp` next to source | `table_scan_test.cpp` |

---

## File Layout

### License Header

Every file **must** start with the Apache 2.0 license header. Use the current copyright year:

```cpp
/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

### Header Guard

Use `#pragma once`. Do **not** use `#ifndef` / `#define` guards.

### Include Order

Group includes in the following order, separated by blank lines:

1. **Corresponding header** — e.g. `snapshot.cpp` includes `paimon/core/snapshot.h` first.
2. **C standard headers** — `<cstdint>`, `<cstring>`, etc.
3. **C++ standard headers** — `<string>`, `<vector>`, `<memory>`, etc.
4. **Third-party headers** — `fmt/format.h`, `rapidjson/document.h`, etc.
5. **Project headers** — `paimon/status.h`, `paimon/result.h`, etc.

### Namespace

All code lives inside `namespace paimon { }`. Close with a comment:

```cpp
namespace paimon {
// ...
}  // namespace paimon
```

Test code uses `namespace paimon::test`.

---

## Error Handling

### `Status` / `Result<T>` — No Exceptions

All fallible functions return `Status` (no value) or `Result<T>` (with value). **Exceptions are not used** for error propagation in production code.

### Error Propagation Macros

| Macro | Purpose |
|-------|---------|
| `PAIMON_RETURN_NOT_OK(status)` | Propagate a `Status` error |
| `PAIMON_ASSIGN_OR_RAISE(lhs, rexpr)` | Extract value from `Result<T>`, return on error |
| `PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow_status)` | Bridge Apache Arrow `Status` |
| `PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(lhs, rexpr)` | Bridge Apache Arrow `Result` |

### `PAIMON_ASSIGN_OR_RAISE` and `PAIMON_ASSIGN_OR_RAISE_FROM_ARROW` — Always Use Explicit Types

```cpp
// ✅ Good — explicit type
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan,
                       TableScan::Create(std::move(ctx)));

// ❌ Bad — auto
PAIMON_ASSIGN_OR_RAISE(auto scan, TableScan::Create(std::move(ctx)));
```

If you need a `shared_ptr` from a function returning `unique_ptr`, write the target type directly — the macro already moves:

```cpp
PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<T> ret, FuncReturningUniquePtr());
```

---

## Class Design

### Factory Pattern — `static Create()` + Private Constructor

When construction can fail, use a factory method:

```cpp
class TableScan {
 public:
    static Result<std::unique_ptr<TableScan>> Create(std::unique_ptr<ScanContext> context);
    ~TableScan() = default;

 private:
    explicit TableScan(std::unique_ptr<ScanContext> context);
};
```

- `Create()` returns `Result<std::unique_ptr<T>>`.
- The private constructor does only trivial member assignment.
- All fallible initialization goes in `Create()`, **after** construction.
- If construction cannot fail (POD, data-only classes), a public constructor is fine.

### Utility Classes

Static-only utility classes delete their constructors and deconstructors:

```cpp
class StringUtils {
 public:
    StringUtils() = delete;
    ~StringUtils() = delete;

    static std::vector<std::string> Split(const std::string& s, char delim);
    // ...
};
```

### Interface Classes

Pure-virtual base classes use a virtual default destructor:

```cpp
virtual ~ClassName() = default;
```

---

## Smart Pointers & Memory

- Prefer `std::unique_ptr` for sole ownership.
- Use `std::shared_ptr` only when shared ownership is genuinely needed.
- **No raw `new` / `delete`** except inside factory `Create()` methods that call a private constructor.
- Use `std::optional<T>` for optional values.
- Use `ScopeGuard` (in `src/paimon/common/utils/`) for RAII cleanup.

---

## Comments & Documentation

- Use `///` (Doxygen) for classes and public methods.
- Use `@param` and `@return` for parameters and return values.
- Do **not** comment obvious code.
- Inline comments go **above** the line, not at the end.

```cpp
/// Create a scan plan from the current snapshot.
///
/// @param snapshot_id The snapshot to scan.
/// @return A Plan on success, or an error Status.
Result<std::shared_ptr<Plan>> CreatePlan(int64_t snapshot_id);
```

### TODO Comments

TODOs are allowed but **must** include an author tag:

```cpp
// TODO(alice): Support changelog manifest list size validation
```

---

## String Formatting

Use `fmt::format()` for string formatting. Do **not** concatenate with `std::to_string()` + `+`.

```cpp
// ✅ Good
auto msg = fmt::format("Snapshot {} not found in {}", snapshot_id, path);

// ❌ Bad
auto msg = "Snapshot " + std::to_string(snapshot_id) + " not found in " + path;
```

---

## Public API Visibility

Mark all public API symbols with `PAIMON_EXPORT`:

```cpp
class PAIMON_EXPORT Timestamp { ... };
```

---

## Reuse Existing Utilities

Before writing new helper code, check whether the project already provides what you need. The tables below list the most commonly used utilities.

### General Utilities (`src/paimon/common/utils/`)

| Class | Purpose |
|-------|---------|
| `StringUtils` | Split, Replace, StartsWith, EndsWith, Trim, etc. |
| `ObjectUtils` | Collection helpers (ContainsAll, etc.) |
| `DateTimeUtils` | Date / time conversions |
| `RapidJsonUtil` | JSON serialization / deserialization |
| `StreamUtils` | Stream read / write helpers |
| `OptionsUtils` | Configuration parsing |
| `PathUtil` | Path joining and manipulation |
| `Preconditions` | Precondition checks |
| `ScopeGuard` | RAII cleanup guard |
| `BloomFilter` / `BloomFilter64` | Bloom filters |
| `MurmurHashUtils` | Hashing |
| `CRC32C` | Checksums |
| `BitSet` | Bit set |
| `RoaringBitmap32` / `RoaringBitmap64` | Roaring bitmaps |
| `SerializationUtils` | Serialization helpers |
| `DataConverterUtils` | Data conversion |
| `DecimalUtils` | Decimal arithmetic |
| `FieldTypeUtils` | Field type helpers |
| `BinPacking` | Bin-packing algorithm |
| `ConcurrentHashMap` | Thread-safe hash map |
| `ThreadsafeQueue` | Thread-safe queue |
| `LinkedHashMap` | Insertion-ordered hash map |
| `LongCounter` | Atomic counter |
| `UUID` | UUID generation |

### Arrow Utilities (`src/paimon/common/utils/arrow/`)

| File | Purpose |
|------|---------|
| `ArrowUtils` | Arrow format helpers |
| `MemUtils` | Arrow memory helpers |
| `status_utils.h` | Arrow ↔ Paimon status bridge macros |

### Core Utilities (`src/paimon/core/utils/`)

| Class | Purpose |
|-------|---------|
| `FileUtils` | File listing, version file operations |
| `FileStorePathFactory` | Path factory for data / manifest files |
| `SnapshotManager` | Snapshot management |
| `TagManager` | Tag management |
| `BranchManager` | Branch management |
| `PartitionPathUtils` | Partition path helpers |
| `FieldMapping` | Field mapping |
| `FieldsComparator` | Field comparison |
| `PrimaryKeyTableUtils` | Primary-key table helpers |

### Concurrency (`src/paimon/common/executor/`)

| Function | Purpose |
|----------|---------|
| `CollectAll()` | Collect results from multiple futures |
| `Wait()` | Wait for multiple void futures |

### Testing Utilities (`src/paimon/testing/utils/`)

| Class | Purpose |
|-------|---------|
| `TestHelper` | End-to-end helper: create tables, write & commit data, scan & read results |
| `DataGenerator` | Generate `RecordBatch` data, split by partition/bucket, extract partial rows |
| `BinaryRowGenerator` | Generate `BinaryRow`, `InternalRow`, `SimpleStats`, and `BinaryArray` for tests |
| `KeyValueChecker` | Assert expected vs. actual `KeyValue` results in primary-key table tests |
| `ReadResultCollector` | Collect `KeyValue` or Arrow `ChunkedArray` results from a `BatchReader` |
| `DictArrayConverter` | Convert Arrow dictionary-encoded arrays to plain arrays |
| `UniqueTestDirectory` | RAII helper that creates a unique temp directory and cleans it up on destruction |
| `TimezoneGuard` | RAII guard that sets `TZ` environment variable and restores it on destruction |
| `IOExceptionHelper` | Inject I/O errors via `IOHook` for fault-injection testing |
| `testharness.h` | Test macros: `ASSERT_OK`, `ASSERT_NOK`, `ASSERT_NOK_WITH_MSG`, `ASSERT_RAISES` |

### Mock Objects (`src/paimon/testing/mock/`)

| Class | Purpose |
|-------|---------|
| `MockFileSystem` / `MockFileSystemFactory` | Mock file system and its factory for unit tests |
| `MockFileFormat` / `MockFileFormatFactory` | Mock file format and its factory |
| `MockFormatReaderBuilder` / `MockFormatWriterBuilder` | Mock reader/writer builders |
| `MockFormatWriter` / `MockFileBatchReader` | Mock format writer and batch reader |
| `MockIndexPathFactory` | Mock index path factory |
| `MockKeyValueDataFileRecordReader` | Mock key-value data file reader holding in-memory data |
| `MockStatsExtractor` | Mock statistics extractor |

---

## Testing

- Test files live **next to** the source file they test, named `*_test.cpp`.
- Use **Google Test** (`gtest`).
- Test classes go in `namespace paimon::test`.
- Use project test macros: `ASSERT_OK`, `ASSERT_NOK`, `ASSERT_NOK_WITH_MSG`.
- **Prefer `ASSERT_*` over `EXPECT_*`** — `ASSERT_*` stops the test immediately on failure, preventing cascading errors.
  - **Exception**: In non-void helper functions, use `EXPECT_*` because `ASSERT_*` expands to `return;` which is incompatible with non-void return types.

---

## Tooling

### Pre-commit Hooks (Recommended)

The easiest way to stay compliant is to install [pre-commit](https://pre-commit.com/):

```bash
pip install pre-commit
pre-commit install
```

This installs Git hooks that automatically run on every commit:

| Hook | What it checks |
|------|----------------|
| `clang-format` | C++ formatting (`.clang-format`) |
| `cpplint` | Google C++ lint rules |
| `cmake-format` | CMake file formatting (`.cmake-format.py`) |
| `codespell` | Spelling errors |
| `trailing-whitespace` | Trailing whitespace |
| `end-of-file-fixer` | Missing newline at end of file |
| `check-added-large-files` | Files > 5 MB |

To run all hooks on all files manually:

```bash
pre-commit run --all-files
```

### clang-tidy

Static analysis is configured in `.clang-tidy`. Key enabled check groups:

- `bugprone-*` — Common bug patterns
- `google-*` — Google style checks
- `modernize-*` — C++ modernization suggestions
- `clang-analyzer-*` — Clang static analyzer

### Building & Testing

```bash
# Configure (from project root)
mkdir -p build && cd build
cmake ../ \
    -DCMAKE_BUILD_TYPE=debug \
    -DPAIMON_BUILD_TESTS=ON

# Build
make -j$(nproc)

# Run a specific test suite
./debug/paimon-core-test --gtest_filter="CoreOptionsTest*"
```

---

## Pull Request Checklist

Before submitting a PR, please verify:

- [ ] Code compiles without warnings under `-Wall` (enabled by default in the build system).
- [ ] `pre-commit run --all-files` passes (or individual tools: clang-format, cpplint, codespell).
- [ ] New / modified public APIs are marked with `PAIMON_EXPORT`.
- [ ] Every new file has the Apache 2.0 license header.
- [ ] Error handling uses `Status` / `Result<T>` — no exceptions.
- [ ] New utility code checks for existing helpers before reinventing.
- [ ] Tests are added or updated for the changed functionality.
- [ ] `ASSERT_*` is preferred over `EXPECT_*` in tests.
- [ ] No raw `new` / `delete` outside factory methods.
- [ ] PR description follows the [template](.github/PULL_REQUEST_TEMPLATE.md).
