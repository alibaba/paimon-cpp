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
is to use CMake’s `find_package <https://cmake.org/cmake/help/latest/command/find_package.html>`_
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

   find_package(Arrow REQUIRED)
   find_package(Paimon REQUIRED)

   add_executable(my_example my_example.cc)
   target_link_libraries(my_example PRIVATE arrow_shared
                                            paimon_shared
                                            paimon_parquet_file_format_shared
                                            paimon_local_file_system_shared)

Available variables and targets
-------------------------------

The directive ``find_package(Paimon REQUIRED)`` instructs CMake to locate a
Paimon C++ installation on your system. If successful, it sets ``Paimon_FOUND``
to true if the Paimon C++ libraries were found.

It also defines the following linkable targets (plain strings, not variables):

* ``paimon_shared`` links to the Paimon shared libraries
* ``paimon_static`` links to the Paimon static libraries

In most cases, it is recommended to use the Paimon shared libraries.

Optional plugins (built-in file formats, file systems, and index)
-----------------------------------------------------------------

Paimon provides a set of built-in optional plugins that you can link to as needed:

- File format plugins:

  - ``paimon_parquet_file_format_shared`` / ``paimon_parquet_file_format_static``
  - ``paimon_orc_file_format_shared`` / ``paimon_orc_file_format_static``
  - ``paimon_avro_file_format_shared`` / ``paimon_avro_file_format_static``
  - ``paimon_blob_file_format_shared`` / ``paimon_blob_file_format_static``
  - ``paimon_lance_file_format_shared`` / ``paimon_lance_file_format_static``

- File system plugins:

  - ``paimon_local_file_system_shared`` / ``paimon_local_file_system_static``
  - ``paimon_jindo_file_system_shared`` / ``paimon_jindo_file_system_static``

- Index plugins:

  - ``paimon_file_index_shared`` / ``paimon_file_index_static``
  - ``paimon_lumina_index_shared`` / ``paimon_lumina_index_static``

.. note::

  In most cases, it is recommended to use the shared variants of these plugins.
