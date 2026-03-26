# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Paimon C++ is a high-performance C++ implementation of Apache Paimon that provides native access to the Paimon datalake format. It supports write, commit, scan, and read operations for both append-only and primary key tables, using Arrow Columnar format for data interchange.

## Build Commands

### Basic Build
```bash
# Using the build script (recommended)
./build_and_package.sh --release --jobs 8

# Manual CMake build
mkdir build && cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
ninja -j8
```

### Debug Build
```bash
./build_and_package.sh --debug --jobs 8
```

### Build with Tests
```bash
./build_and_package.sh --debug --jobs 8 -DPAIMON_BUILD_TESTS=ON
```

### Key Build Options
- `-DPAIMON_BUILD_TESTS=ON/OFF` - Build tests (default: OFF)
- `-DPAIMON_BUILD_STATIC=ON/OFF` - Build static library (default: ON)
- `-DPAIMON_BUILD_SHARED=ON/OFF` - Build shared library (default: ON)
- `-DPAIMON_ENABLE_AVRO=ON/OFF` - Enable Avro format (default: ON)
- `-DPAIMON_ENABLE_ORC=ON/OFF` - Enable ORC format (default: ON)
- `-DPAIMON_ENABLE_LANCE=ON/OFF` - Enable Lance format (default: OFF)
- `-DPAIMON_ENABLE_JINDO=ON/OFF` - Enable Jindo filesystem (default: OFF)
- `-DPAIMON_ENABLE_LUMINA=ON/OFF` - Enable Lumina vector index (default: ON)
- `-DPAIMON_ENABLE_LUCENE=ON/OFF` - Enable Lucene index (default: ON)

## Testing

### Run All Tests
```bash
cd build
ctest -j8 --output-on-failure
```

### Run Specific Test
```bash
cd build
./test/inte/paimon_inte_test --gtest_filter="TestName.Pattern"
```

### Test Organization
- **Integration tests**: `test/inte/` - Main integration tests for all features
- **Unit tests**: Co-located with source files as `*_test.cpp`
- **Test data**: `test/test_data/` - Sample Paimon tables for testing

## Code Quality and Linting

### Install Pre-commit Hooks
```bash
pip install pre-commit
pre-commit install
```

### Run Linting on All Files
```bash
pre-commit run -a
```

### Linting Tools Used
- **clang-format**: C++ code formatting
- **cmake-format**: CMake file formatting
- **codespell**: Spell checking
- **cpplint**: C++ style checker
- **sphinx-lint**: Documentation linting

## Architecture Overview

### Directory Structure
```
src/paimon/
├── common/           # Common utilities and interfaces
│   ├── data/        # Data types and Arrow integration
│   ├── executor/    # Thread pool implementations
│   ├── file_index/  # File index implementations
│   ├── format/      # File format interfaces
│   ├── fs/          # Filesystem abstractions
│   ├── global_index/# Global index interfaces
│   ├── io/          # IO utilities
│   ├── memory/      # Memory pool implementations
│   ├── predicate/   # Predicate pushdown
│   └── types/       # Type system
├── core/            # Core Paimon logic
│   ├── append/      # Append-only table logic
│   ├── catalog/     # Catalog implementations
│   ├── deletionvectors/ # Deletion vector handling
│   ├── manifest/    # Manifest file handling
│   ├── mergetree/   # LSM merge tree logic
│   ├── operation/   # Table operations
│   ├── schema/      # Schema management
│   └── table/       # Table implementations
├── format/          # File format implementations
│   ├── avro/        # Avro format support
│   ├── orc/         # ORC format support
│   ├── parquet/     # Parquet format support
│   └── lance/       # Lance format support (optional)
└── fs/              # Filesystem implementations
    ├── jindo/       # Jindo filesystem (optional)
    └── local/       # Local filesystem
```

### Key Design Patterns

1. **Result Types**: Uses `PAIMON_ASSIGN_OR_RAISE` and `PAIMON_RETURN_NOT_OK` macros for error handling. Always check return statuses.

2. **Builder Pattern**: Common pattern for constructing contexts
   - `WriteContextBuilder` → `WriteContext`
   - `ScanContextBuilder` → `ScanContext`
   - `ReadContextBuilder` → `ReadContext`

3. **Factory Pattern**: Creation through static factory methods
   - `FileStoreWrite::Create()`
   - `TableScan::Create()`
   - `TableRead::Create()`

4. **Two-Phase Operations**:
   - **Write**: Prepare data → Generate commit messages → Commit
   - **Read**: Scan table → Create plan → Read batches

5. **Arrow Integration**: All data interchange uses Arrow Columnar format. Batches are represented as `RecordBatch` objects wrapping Arrow arrays.

6. **Plugin Architecture**: File formats and filesystems are pluggable through interfaces in `common/`.

### Core Abstractions

- **FileStoreWrite**: Writes data to Paimon tables
- **TableScan**: Scans table metadata and creates read plans
- **TableRead**: Reads data files according to plan
- **FileStoreCommit**: Commits write operations
- **Catalog**: Manages databases and tables
- **FileFormat**: Abstracts different file formats (ORC, Parquet, Avro, Lance)
- **FileSystem**: Abstracts storage backends (Local, Jindo)

## Development Workflow

1. **Before making changes**: Run `pre-commit run -a` to ensure code passes linting
2. **For new features**: Add unit tests alongside implementation (`*_test.cpp`)
3. **For bug fixes**: Add regression tests
4. **Before committing**: Ensure all tests pass with `ctest -j8`
5. **Architecture decisions**: Follow existing patterns for consistency

## Important Notes

- The codebase only supports x86_64 architecture
- Java Paimon format compatibility is maintained for commit messages, data splits, and manifests
- C++11 ABI is enabled by default (can be disabled, but some features like Lance and Lumina require it)
- Use the provided macros (`PAIMON_ASSIGN_OR_RAISE`, `PAIMON_RETURN_NOT_OK`) for error handling
- Pre-commit hooks must pass before commits can be made