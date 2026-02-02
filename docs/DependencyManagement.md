# Dependency Management

Paimon C++ provides flexible dependency management to support different build environments and workflows.

## Quick Start

### Use System Libraries (default: AUTO)

By default, Paimon will try to find system libraries first, and fall back to building from source if not found:

```bash
cmake -B build
```

### Force Building All Dependencies from Source

```bash
cmake -B build -DPAIMON_DEPENDENCY_SOURCE=BUNDLED
```

### Use Only System Libraries (fail if not found)

```bash
cmake -B build -DPAIMON_DEPENDENCY_SOURCE=SYSTEM
```

## Configuration Options

### Global Dependency Source Control

The `PAIMON_DEPENDENCY_SOURCE` option controls how all dependencies are acquired:

- **AUTO** (default): Try system libraries first, fall back to bundled build
- **BUNDLED**: Always download and build from source
- **SYSTEM**: Use only system libraries (CMake will fail if not found)
- **CONDA**: Use libraries from `$CONDA_PREFIX` environment
- **VCPKG**: Use vcpkg package manager (experimental)
- **BREW**: Use Homebrew on macOS (experimental)

Example:
```bash
cmake -B build -DPAIMON_DEPENDENCY_SOURCE=AUTO
```

### Per-Dependency Source Control

You can override individual dependencies using `<PACKAGE>_SOURCE`:

```bash
cmake -B build \
  -DPAIMON_DEPENDENCY_SOURCE=AUTO \
  -DSnappy_SOURCE=SYSTEM \
  -Dzstd_SOURCE=BUNDLED \
  -Dlz4_SOURCE=AUTO
```

Supported packages: `Snappy`, `zstd`, `lz4`, `zlib`, `glog`, `fmt`, `RapidJSON`, `Arrow`, `tbb`, `Protobuf`, `avro`, `orc`

### Specifying Library Locations

Use `<PACKAGE>_ROOT` to specify the installation directory of a dependency:

```bash
cmake -B build \
  -DPAIMON_DEPENDENCY_SOURCE=SYSTEM \
  -DSnappy_ROOT=/usr/local \
  -Dzstd_ROOT=/opt/zstd \
  -Dlz4_ROOT=/custom/lz4
```

The build system will search for:
- Headers in `${<PACKAGE>_ROOT}/include`
- Libraries in `${<PACKAGE>_ROOT}/lib`, `${<PACKAGE>_ROOT}/lib64`

### Unified Path Prefix

Set all unspecified `_ROOT` variables at once using `PAIMON_PACKAGE_PREFIX`:

```bash
cmake -B build \
  -DPAIMON_DEPENDENCY_SOURCE=SYSTEM \
  -DPAIMON_PACKAGE_PREFIX=/usr/local
```

This automatically sets:
- `Snappy_ROOT=/usr/local`
- `zstd_ROOT=/usr/local`
- `lz4_ROOT=/usr/local`
- etc.

### Shared vs Static Libraries

Control whether to link against shared or static versions of dependencies:

```bash
# Global control
cmake -B build -DPAIMON_DEPENDENCY_USE_SHARED=OFF

# Per-dependency control
cmake -B build \
  -DPAIMON_DEPENDENCY_USE_SHARED=OFF \
  -DPAIMON_SNAPPY_USE_SHARED=ON \
  -DPAIMON_ZSTD_USE_SHARED=OFF
```

Supported options:
- `PAIMON_DEPENDENCY_USE_SHARED` (default: ON)
- `PAIMON_SNAPPY_USE_SHARED`
- `PAIMON_ZSTD_USE_SHARED`
- `PAIMON_LZ4_USE_SHARED`
- `PAIMON_ZLIB_USE_SHARED`
- `PAIMON_GLOG_USE_SHARED`

## Common Scenarios

### Development with System Libraries

```bash
# Install dependencies via package manager
apt-get install libsnappy-dev libzstd-dev liblz4-dev libgoogle-glog-dev

# Build with system libraries
cmake -B build -DPAIMON_DEPENDENCY_SOURCE=AUTO
make -C build -j$(nproc)
```

### Conda Environment

```bash
# Activate conda environment with dependencies
conda activate myenv
conda install -c conda-forge snappy zstd lz4-c glog arrow-cpp

# Build using conda libraries
cmake -B build -DPAIMON_DEPENDENCY_SOURCE=CONDA
make -C build -j$(nproc)
```

### Custom Library Paths

```bash
cmake -B build \
  -DPAIMON_DEPENDENCY_SOURCE=SYSTEM \
  -DSnappy_ROOT=/opt/custom/snappy \
  -Dzstd_ROOT=/opt/custom/zstd \
  -DArrow_ROOT=/opt/custom/arrow
make -C build -j$(nproc)
```

### Mixed Approach

```bash
# Use system Arrow but build compression libraries
cmake -B build \
  -DPAIMON_DEPENDENCY_SOURCE=AUTO \
  -DArrow_SOURCE=SYSTEM \
  -DArrow_ROOT=/usr/local \
  -DSnappy_SOURCE=BUNDLED \
  -Dzstd_SOURCE=BUNDLED
```

### CI/CD with Cached Dependencies

```bash
# First build: cache dependencies in /opt/deps
cmake -B build \
  -DPAIMON_DEPENDENCY_SOURCE=BUNDLED \
  -DCMAKE_INSTALL_PREFIX=/opt/deps
make -C build -j$(nproc)
make -C build install

# Subsequent builds: reuse cached dependencies
cmake -B build2 \
  -DPAIMON_DEPENDENCY_SOURCE=SYSTEM \
  -DPAIMON_PACKAGE_PREFIX=/opt/deps
make -C build2 -j$(nproc)
```

## Troubleshooting

### "Could not find <package>"

If you get an error like "Could not find Snappy":

1. Check that the package is installed:
   ```bash
   # Ubuntu/Debian
   dpkg -l | grep libsnappy

   # macOS
   brew list | grep snappy
   ```

2. Specify the installation path:
   ```bash
   cmake -B build -DSnappy_ROOT=/path/to/snappy
   ```

3. Or fall back to bundled build:
   ```bash
   cmake -B build -DSnappy_SOURCE=BUNDLED
   ```

### Version Mismatch

If the system library version is too old, you can:

1. Force bundled build for that specific dependency:
   ```bash
   cmake -B build -Dzstd_SOURCE=BUNDLED
   ```

2. Or install a newer version and specify its path:
   ```bash
   cmake -B build -Dzstd_ROOT=/usr/local/zstd-1.5.7
   ```

### Verbose Build Output

To see detailed third-party build logs:

```bash
cmake -B build -DPAIMON_VERBOSE_THIRDPARTY_BUILD=ON
```

## Implementation Status

Currently supported dependencies with flexible resolution:
- ✅ Snappy (compression)
- ✅ zstd (compression)
- ✅ lz4 (compression)
- ✅ glog (logging)
- 🔄 zlib, fmt, RapidJSON, Arrow, tbb (basic support)
- 🔄 Protobuf, avro, orc (basic support)

Dependencies still using BUNDLED approach:
- Jindo SDK (precompiled package)

Legend:
- ✅ Full support with Find modules
- 🔄 Basic support (will use system if found by CMake)
- ⏳ Planned

## See Also

- [Issue #103](https://github.com/alibaba/paimon-cpp/issues/103) - Original feature request
- [CMake find_package documentation](https://cmake.org/cmake/help/latest/command/find_package.html)
- [Apache Arrow dependency management](https://github.com/apache/arrow/blob/main/cpp/cmake_modules/ThirdpartyToolchain.cmake)
