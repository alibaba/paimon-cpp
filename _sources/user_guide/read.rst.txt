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

Read and Data Evolution
===============================================
Paimon by functionality can be divided into two layers:

- Control Plane: Responsible for accessing and managing Meta (snapshot, manifest, etc.), including:
  - Catalog / Database access
  - Table retrieval
  - Collection and resolution of data files

- Data Plane: Responsible for accessing actual data files, including:
  - Readers for various file formats
  - Coordinated reading of file collections

The control plane and data plane interact primarily via DataSplit (the query plan). Java currently supports a standard
DataSplit protocol which includes the necessary meta information to access data files. With DataSplit, a high-performance
data access path can be integrated.

At compute time, the execution engine (reader) does not need to be aware of the concrete table type or its metadata details.
It only needs to follow the instructions within the DataSplit (query plan) to perform data reading operations.

With the layered abstraction of the control plane and data plane, and the use of DataSplit as a stable protocol interface,
the two layers can evolve their functionality and optimize code relatively independently. This design also enables
cross-language task scheduling and interaction (e.g., Java and C++), substantially reducing engineering maintenance costs
across the two language ecosystems.


Schema Evolution
================

Scope and Compatibility
-----------------------

C++ Paimon supports all evolution kinds available in Java Paimon for non-nested types:

- Add column
- Drop column
- Reorder columns
- Rename column
- Change column type

.. note::

  - Only non-nested type evolution is supported. Nested columns (struct, array, map) are not supported.
  - Partition keys: Only column reordering is supported; other operations are not supported (consistent with Java Paimon).
  - Primary key:

    - Adding or dropping columns is not supported.
    - Other operations are supported (consistent with Java Paimon).

Per-File Schema via Field IDs
-----------------------------

In DataSplit, each file may have a completely different data schema. Paimon uses field IDs to uniquely identify fields.

Overflow Behavior Disclaimer
----------------------------

Overflow behavior is undefined for C++ and Java Paimon. Results in overflow scenarios may:

- Be incorrect values,
- Return an error status,
- Or be null.

C++ Paimon does not guarantee identical results to Java Paimon in overflow scenarios. Users should not rely on identical
return values between implementations.

Type Change Support Matrix
--------------------------

The table below indicates support for changing a column type from ``source`` to ``target``. Refer to the numbered notes below the table
for caveats.

.. list-table::
   :header-rows: 1
   :widths: 12 10 10 10 10 10 10 8 12 10 8 18 10

   * - src \\ target
     - tinyint
     - smallint
     - int
     - bigint
     - float
     - double
     - bool
     - string
     - binary
     - date
     - timestamp (without tz)
     - decimal
   * - tinyint
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ❌
     - ❌
     - ❌
     - ✅
   * - smallint
     - ✅ 1️⃣
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ❌
     - ❌
     - ❌
     - ✅
   * - int
     - ✅ 1️⃣
     - ✅ 1️⃣
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ❌
     - ❌
     - ✅ 1️⃣
     - ✅
   * - bigint
     - ✅ 1️⃣
     - ✅ 1️⃣
     - ✅ 1️⃣
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ❌
     - ❌
     - ✅ 6️⃣
     - ✅
   * - float
     - ✅ 2️⃣
     - ✅ 2️⃣
     - ✅ 2️⃣
     - ✅ 2️⃣
     - ✅
     - ✅
     - ✅
     - ✅ 3️⃣ 4️⃣
     - ❌
     - ❌
     - ❌
     - ✅
   * - double
     - ✅ 2️⃣
     - ✅ 2️⃣
     - ✅ 2️⃣
     - ✅ 2️⃣
     - ✅ 2️⃣
     - ✅
     - ✅
     - ✅ 3️⃣ 4️⃣
     - ❌
     - ❌
     - ❌
     - ✅
   * - bool
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅
     - ❌
     - ❌
     - ❌
     - ✅
   * - string
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅ 3️⃣
     - ✅ 3️⃣
     - ✅
     - ✅
     - ✅
     - ✅
     - ✅ 5️⃣
     - ✅ 7️⃣
   * - binary
     - ❌
     - ❌
     - ❌
     - ❌
     - ❌
     - ❌
     - ❌
     - ✅
     - ✅
     - ❌
     - ❌
     - ❌
   * - date
     - ❌
     - ❌
     - ❌
     - ❌
     - ❌
     - ❌
     - ❌
     - ✅
     - ❌
     - ✅
     - ✅ 5️⃣
     - ❌
   * - timestamp (without tz)
     - ❌
     - ❌
     - ✅ 1️⃣
     - ✅
     - ❌
     - ❌
     - ❌
     - ✅
     - ❌
     - ✅
     - ✅
     - ❌
   * - decimal
     - ✅ 1️⃣
     - ✅ 1️⃣
     - ✅ 1️⃣
     - ✅ 1️⃣
     - ✅
     - ✅
     - ❌
     - ✅
     - ❌
     - ❌
     - ❌
     - ✅

.. admonition:: Overflow Behavior Notes
  :class: note

  1️⃣ Integer downcast overflow behavior matches Java in specific cases.
    Example: smallint -> tinyint, 32767 becomes -1; int -> smallint, -2147483648 becomes 0.

  2️⃣ Floating-point overflow behavior is partially consistent with Java and partially different.
    Example: float -> tinyint
      - Java: MAX_FLOAT -> -1, INFINITY -> -1
      - C++:  MAX_FLOAT -> 0, INFINITY -> 0

  3️⃣ Keyword differences for special float/double values:
    - Java: Infinity, -Infinity, NaN
    - C++:  inf, -inf, nan

  4️⃣ Printing difference:
    - C++ Paimon prints 1.0 as ``1``
    - Java Paimon prints 1.0 as ``1.0``

  5️⃣ Timestamp precision and range differences:
    - Java Paimon: 0000-01-01 00:00:00.000000000 to 9999-12-31 23:59:59.999999999
    - C++ Paimon:  1677-09-21 00:12:43.145224192 to 2262-04-11 23:47:16.854775807
    - C++ only supports nanosecond precision; range is smaller.

  6️⃣ bigint -> timestamp range differences:
    - Java Paimon (ms):   ``[MIN_INT64/1000, MAX_INT64/1000]`` seconds
    - C++ Paimon (ns):    ``[MIN_INT64/1e9,  MAX_INT64/1e9]`` seconds

  7️⃣ string -> decimal with precision > 38:
    - C++ returns ``null`` if parsing would overflow 128-bit arithmetic.
    - Java may rescale and return a value based on the rescaled precision.
    - Example input: ``1111111111111111111111111111111111111.15``, Java returns: ``1111111111111111111111111111111111111.2``, C++ returns: ``null``

Implementation Guidance
-----------------------

- Use DataSplit as the sole interface between control and data planes. Treat it as the canonical query plan contract.
- Resolve field types and IDs per file; prefer inline data file metadata, fallback to table schema files when necessary.
- Expect per-file schema variability; design readers to align by field IDs rather than positional indices.
- Do not assume identical overflow semantics across C++ and Java; tests should validate acceptable ranges and nullability.
- For timestamp handling, consider precision/range constraints in C++ when interoperating with Java-produced data splits.
