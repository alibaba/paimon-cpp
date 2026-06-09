.. Copyright 2026-present Alibaba Inc.

.. Licensed under the Apache License, Version 2.0 (the "License");
.. you may not use this file except in compliance with the License.
.. You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

================
Benchmark Usage
================

Paimon C++ provides Google Benchmark based cases for append-table write/read and
primary-key table write/MOR read paths. Benchmarks are disabled by default.

Build
=====

Enable benchmarks when configuring CMake::

   cmake -S . -B build -DPAIMON_BUILD_BENCHMARKS=ON
   cmake --build build --target paimon-read-write-benchmark

Run all benchmark cases through CTest::

   cmake --build build --target benchmark

Custom Options
==============

``paimon-read-write-benchmark`` accepts Google Benchmark options plus the Paimon
specific options below:

``--paimon_source_data_file=<path>``
   Source data file used to build benchmark data. Currently Parquet source files
   are supported.

``--paimon_source_table_path=<path>``
   Read directly from an existing table path for ``BM_Read`` and ``BM_MOR_Read``.
   When set, the source loading and pre-write stage are skipped.

``--paimon_pk_columns=<col1,col2,...>``
   Primary key columns for ``BM_PK_Write`` and ``BM_MOR_Read``. These cases
   explicitly use ``bucket=1`` because benchmark batches are written to bucket 0.

``--paimon_option=<key1>:<value1>;<key2>:<value2>``
   Repeatable table options passed through to Paimon. The default table file
   format is ``parquet``; use ``--paimon_option file.format:<format>`` to
   override it. For ``BM_PK_Write`` and ``BM_MOR_Read``, ``bucket`` is forced to
   ``1``.

Examples
========

Append table write::

   paimon-read-write-benchmark \
       --paimon_source_data_file /path/data.parquet \
       --benchmark_filter=BM_Write

Append table read with four prefetch workers::

   paimon-read-write-benchmark \
       --paimon_source_data_file /path/data.parquet \
       --benchmark_filter=BM_Read/4

Primary-key table write::

   paimon-read-write-benchmark \
       --paimon_source_data_file /path/data.parquet \
       --paimon_pk_columns=id \
       --benchmark_filter=BM_PK_Write

MOR read from an existing table::

   paimon-read-write-benchmark \
       --paimon_source_table_path /path/table \
       --paimon_pk_columns=id \
       --benchmark_filter=BM_MOR_Read/4
