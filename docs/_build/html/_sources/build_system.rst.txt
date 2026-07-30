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

.. default-domain:: cpp
.. highlight:: cpp

======================
Integrating Paimon C++
======================

This section assumes that you have already built and installed the Paimon C++
libraries on your system after :ref:`building them yourself <building-paimon-cpp>`.
Additionally, you will need `Apache Arrow for C++ <https://arrow.apache.org/docs/cpp/getting_started.html>`_
as in-memory data format interface. Please ensure that Arrow C++ is installed
and available to your build system

The recommended way to integrate the Paimon C++ libraries into your C++ project
is to use CMake's `find_package <https://cmake.org/cmake/help/latest/command/find_package.html>`_
function to locate and integrate dependencies.

CMake
=====

Quick Start
-----------

This ``CMakeLists.txt`` compiles the ``my_example.cc`` source file into
an executable and links it with the Paimon C++ shared library and its plugins
for data format and file system.

.. code-block:: cmake

   cmake_minimum_required(VERSION 3.16)

   project(MyExample)

   # Arrow's static package may export LZ4 under a different target name.
   find_package(lz4 CONFIG QUIET)
   if(TARGET LZ4::lz4_static AND NOT TARGET LZ4::lz4)
     add_library(LZ4::lz4 ALIAS LZ4::lz4_static)
   elseif(TARGET lz4::lz4 AND NOT TARGET LZ4::lz4)
     add_library(LZ4::lz4 ALIAS lz4::lz4)
   endif()

   find_package(Arrow CONFIG REQUIRED)
   find_package(Paimon CONFIG REQUIRED)

   if(TARGET Arrow::arrow_shared)
     set(PAIMON_ARROW_TARGET Arrow::arrow_shared)
   elseif(TARGET Arrow::arrow_static)
     set(PAIMON_ARROW_TARGET Arrow::arrow_static)
   else()
     message(FATAL_ERROR "No supported Arrow CMake target is available")
   endif()

   add_executable(my_example my_example.cc)
   target_link_libraries(my_example PRIVATE ${PAIMON_ARROW_TARGET}
                                            Paimon::paimon_shared
                                            Paimon::paimon_parquet_file_format_shared
                                            Paimon::paimon_local_file_system_shared)

Available variables and targets
-------------------------------

The directive ``find_package(Paimon REQUIRED)`` instructs CMake to locate a
Paimon C++ installation on your system. If successful, it sets ``Paimon_FOUND``
to true if the Paimon C++ libraries were found.

It defines the following supported imported target:

* ``Paimon::paimon_shared`` links to the Paimon shared libraries

Static targets such as ``Paimon::paimon_static`` may also be present when
Paimon is built with ``PAIMON_BUILD_STATIC=ON``. They are not currently
supported as standalone installed targets because their exported link
interfaces do not include every third-party dependency. Installed consumers
should use the shared targets.

Optional plugins (built-in file formats, file systems, and index)
-----------------------------------------------------------------

Paimon provides a set of built-in optional plugins that you can link to as needed:

- File format plugins:

  - ``Paimon::paimon_parquet_file_format_shared``
  - ``Paimon::paimon_orc_file_format_shared``
  - ``Paimon::paimon_avro_file_format_shared``
  - ``Paimon::paimon_blob_file_format_shared``

- File system plugins:

  - ``Paimon::paimon_local_file_system_shared``
  - ``Paimon::paimon_jindo_file_system_shared``

- Index plugins:

  - ``Paimon::paimon_file_index_shared``
  - ``Paimon::paimon_lumina_index_shared``

.. note::

  Static plugin targets have the same installed-consumer limitation as
  ``Paimon::paimon_static`` and should not be used as standalone imported
  targets.
