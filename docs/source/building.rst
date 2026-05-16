.. Copyright 2024-present Alibaba Inc.

.. Licensed under the Apache License, Version 2.0 (the "License");
.. you may not use this file except in compliance with the License.
.. You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: console

.. _building-paimon-cpp:

===================
Building Paimon C++
===================

System setup
============

Paimon uses CMake as a build configuration system. We recommend building
out-of-source. For example, you could create ``paimon-cpp/build`` and invoke
``cmake $CMAKE_ARGS ..`` from this directory.

Building requires:

* A C++17-enabled compiler. On Linux, gcc 8 and higher should be
  sufficient. On macOS, use AppleClang from Xcode Command Line Tools or
  LLVM clang from Homebrew. Windows is not supported for now.
* At least 2GB of RAM for a minimal build, 8GB for a minimal
  debug build with tests and 16GB for a full build.

On Ubuntu/Debian you can install the requirements with:

.. code-block:: shell

   sudo apt-get install \
        build-essential \
        cmake

On macOS you can install the requirements with:

.. code-block:: shell

   xcode-select --install
   brew install cmake

The same CMake build options apply on Linux and macOS. If you prefer upstream
LLVM clang instead of AppleClang on macOS, install LLVM and pass the Homebrew
compiler paths when configuring:

.. code-block:: shell

   brew install llvm
   cmake -B build \
       -DCMAKE_C_COMPILER="$(brew --prefix llvm)/bin/clang" \
       -DCMAKE_CXX_COMPILER="$(brew --prefix llvm)/bin/clang++"

When building with upstream Clang on macOS, Paimon uses Apple SDK libc++
headers by default to avoid incompatibilities in bundled third-party
dependencies. Pass ``-DPAIMON_USE_APPLE_LIBCXX_WITH_CLANG=OFF`` to disable
this behavior.

We also provide a docker template to help you get started quickly. See in
``.devcontainer`` folder for more details.

.. _cpp-building-building:

Building
========

All the instructions below assume that you have cloned the paimon-cpp git
repository:

.. code-block::

   $ git clone https://github.com/alibaba/paimon-cpp.git
   $ cd paimon-cpp
   $ git lfs pull

Manual configuration
--------------------

The build system uses ``CMAKE_BUILD_TYPE=Release`` by default, so if this
argument is omitted then a release build will be produced.

Two build types are possible:

* ``Debug``: doesn't apply any compiler optimizations and adds debugging
  information in the binary.
* ``Release``: applies compiler optimizations and removes debug information
  from the binary.

.. note::

  You can also run default build with flag ``-DPAIMON_EXTRA_ERROR_CONTEXT=ON``
  for more error msg context.

Minimal release build (2GB of RAM for building or more recommended):

.. code-block::

   $ mkdir build-release
   $ cd build-release
   $ cmake ..
   $ make -j8       # if you have 8 CPU cores, otherwise adjust
   $ make install

Minimal debug build with unit tests (4GB of RAM for building or more recommended):

.. code-block::

   $ mkdir build-debug
   $ cd build-debug
   $ cmake -DCMAKE_BUILD_TYPE=Debug -DPAIMON_BUILD_TESTS=ON ..
   $ make -j8       # if you have 8 CPU cores, otherwise adjust
   $ make unittest  # to run the tests
   $ make install

The unit tests are not built by default. After building, one can also invoke
the unit tests using the ``ctest`` tool provided by CMake.

Faster builds with Ninja
~~~~~~~~~~~~~~~~~~~~~~~~

Many contributors use the `Ninja build system <https://ninja-build.org/>`_ to
get faster builds. It especially speeds up incremental builds. To use
``ninja``, pass ``-GNinja`` when calling ``cmake`` and then use the ``ninja``
command instead of ``make``.

.. _cpp_build_optional_components:

Optional Components
~~~~~~~~~~~~~~~~~~~

By default, the C++ build system creates a fairly minimal build. We have
several optional system components which you can opt into building by passing
boolean flags to ``cmake``.

* ``-DPAIMON_ENABLE_ORC=ON``: Paimon integration with Apache ORC
* ``-DPAIMON_ENABLE_LANCE=ON``: Paimon integration with Lance
* ``-DPAIMON_ENABLE_AVRO=ON``: Apache Avro libraries and Paimon integration
* ``-DPAIMON_ENABLE_JINDO=ON``: Support for Alibaba Jindo filesystems
* ``-DPAIMON_ENABLE_LUMINA=ON``: Support for Lumina vector index, lumina is only supported on gcc9 or higher.

Third-party dependency source
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Paimon C++ can either build selected third-party dependencies from bundled
sources or use libraries already installed on the system. The default mode is
``AUTO``, which tries system packages first and falls back to bundled sources
when they are not found.

.. code-block:: shell

   cmake -B build -DPAIMON_DEPENDENCY_SOURCE=AUTO

The supported dependency source values are:

* ``AUTO``: use a system package when available, otherwise build bundled sources.
* ``BUNDLED``: always build bundled sources.
* ``SYSTEM``: require system packages and fail if they are not found.

You can override individual dependencies with ``<Dependency>_SOURCE``. The
supported dependency set includes Arrow/Parquet, ORC, Protobuf, Avro, RE2, fmt,
RapidJSON, TBB, glog, GoogleTest, and compression libraries. Arrow and ORC
require project-specific patches, so their supported source values are
``AUTO`` and ``BUNDLED``; ``AUTO`` resolves to bundled sources for them.

.. code-block:: shell

   cmake -B build \
     -DPAIMON_DEPENDENCY_SOURCE=AUTO \
     -Dfmt_SOURCE=SYSTEM \
     -Dfmt_ROOT=/opt/fmt \
     -Dzstd_SOURCE=BUNDLED

Use ``PAIMON_PACKAGE_PREFIX`` to provide one common prefix for dependencies
whose own ``<Package>_ROOT`` variable is not set.

.. code-block:: shell

   cmake -B build \
     -DPAIMON_DEPENDENCY_SOURCE=SYSTEM \
     -DPAIMON_PACKAGE_PREFIX=/opt/paimon-deps

Package-manager-specific modes are intentionally out of scope for this first
dependency source interface. They can still be used through standard CMake
mechanisms such as ``CMAKE_PREFIX_PATH`` or ``CMAKE_TOOLCHAIN_FILE``, while
Paimon keeps the dependency source values limited to ``AUTO``, ``BUNDLED``, and
``SYSTEM``.

When ``Arrow_SOURCE`` is explicitly set to ``BUNDLED`` or left as ``AUTO``, the
compression dependencies default to bundled sources unless individually
overridden. When ``ORC_SOURCE`` is explicitly set to ``BUNDLED`` or left as
``AUTO``, ``Protobuf_SOURCE`` defaults to bundled sources unless individually
overridden.

During configuration, CMake prints a dependency resolution summary showing the
requested source, actual source, compatibility target, and search root for each
resolved dependency.

Optional Targets
~~~~~~~~~~~~~~~~

For development builds, you will often want to enable additional targets in
enable to exercise your changes, using the following ``cmake`` options.

* ``-DPAIMON_BUILD_TESTS=ON``: Build executable unit tests.

Optional Checks
~~~~~~~~~~~~~~~

The following special checks are available as well.  They instrument the
generated code in various ways so as to detect select classes of problems
at runtime (for example when executing unit tests).

* ``-DPAIMON_USE_ASAN=ON``: Enable Address Sanitizer to check for memory leaks,
  buffer overflows or other kinds of memory management issues.
* ``-DPAIMON_USE_UBSAN=ON``: Enable Undefined Behavior Sanitizer to check for
  situations which trigger C++ undefined behavior.

Some of those options are mutually incompatible, so you may have to build
several times with different options if you want to exercise all of them.

CMake version requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~

We support CMake 3.16 and higher.

LLVM and Clang Tools
~~~~~~~~~~~~~~~~~~~~

We currently use LLVM for library builds and for developer tools such as code
formatting with clang-format. LLVM can be installed via most modern package
managers (apt, yum, etc.).

Environment variables
~~~~~~~~~~~~~~~~~~~~~

The build system and helper scripts accept several environment variables that
can alter fetch and build behaviour without changing CMake flags. These are
especially useful when you want to use a local or corporate mirror for
third-party archives, or to override a specific dependency's download URL.

Common environment variables
----------------------------

* ``PAIMON_THIRDPARTY_MIRROR_URL``

  When set, this string is used as a prefix for the default third-party
  download URLs. For example, if a dependency would normally be downloaded
  from

  ``https://github.com/fmtlib/fmt/archive/refs/tags/${PAIMON_FMT_BUILD_VERSION}.tar.gz``

  and ``PAIMON_THIRDPARTY_MIRROR_URL`` is set to

  ``https://mirror.example.com/paimon/thirdparty/``, the build system will
  attempt to download from

  ``https://mirror.example.com/paimon/thirdparty/https://github.com/fmtlib/fmt/archive/refs/tags/${PAIMON_FMT_BUILD_VERSION}.tar.gz``

  (the exact concatenation semantics follow the third-party fetch helpers
  defined in ``cmake_modules/ThirdpartyToolchain.cmake``). If you set a
  mirror URL, prefer including a trailing slash to avoid accidental URL
  concatenation issues.

* Per-dependency override variables (examples)

  Many dependencies support overriding their download URL via a dedicated
  environment variable. Examples implemented in the CMake helper include:

  - ``PAIMON_FMT_URL`` to override the fmt archive URL
  - ``PAIMON_RAPIDJSON_URL`` to override RapidJSON download URL
  - ``PAIMON_ZLIB_URL``, ``PAIMON_ZSTD_URL``, ``PAIMON_LZ4_URL`` etc.

  If one of these per-dependency environment variables is defined, it will
  take precedence over the mirror prefix. Use these variables to precisely
  control where a given dependency is fetched from.

Usage examples
--------------

Use a mirror for all third-party downloads:

.. code-block:: shell

   export PAIMON_THIRDPARTY_MIRROR_URL="https://mirror.example.com/paimon/thirdparty/"
   mkdir build
   cd build
   cmake -DPAIMON_BUILD_TESTS=ON ..

Override only a single dependency (fmt):

.. code-block:: shell

   export PAIMON_FMT_URL="https://internal.example.com/archives/fmt-8.1.1.tar.gz"
   mkdir build
   cd build
   cmake ..

.. note::

  The exact fetch behaviour (how the mirror prefix is concatenated, or whether the helper expects a full URL vs. a prefix)
  is implemented in ``cmake_modules/ThirdpartyToolchain.cmake``. Consult that file when you need a custom setup.
  Unset an environment variable to revert to the default upstream download locations: ``unset PAIMON_THIRDPARTY_MIRROR_URL``
