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

.. _memory-format:

Memory Format
=============

`Paimon Java <https://paimon.apache.org/docs/master/>`_ uses a row-level
abstraction, ``InternalRow``, as the default data format interface.
Row-level interfaces are generally more intuitive for programming and can be
easily integrated into data processing pipelines such as filtering and merge sorting.
However, this approach introduces conversion and access overhead,
making it difficult to fully leverage the additional performance benefits provided
by modern CPU SIMD vectorization.

Considering that the C++ implementation focuses more on end-to-end performance,
and that underlying data file formats (e.g., ORC, Parquet) are predominantly
columnar, providing a columnar-centric data abstraction can minimize overhead
during data flow, integrate more naturally with vectorized execution engines,
and ultimately deliver superior overall performance.

Why Apache Arrow
----------------

Apache Arrow is currently the most widely adopted in-memory columnar format and
has strong native support for Parquet and ORC. It is well-integrated across
open-source engines including Spark, Pandas, Drill, Impala, and Velox.

Overall, using Apache Arrow as the in-memory format for Paimon C++ allows us to:

- Maximize columnar performance and efficient data access patterns.
- Seamlessly integrate with the Apache Arrow ecosystem and tooling.
- Benefit from mature interoperability with popular data systems and formats.

Versioning and Dependency Concerns
----------------------------------

One important consideration is that Arrow is an active open-source project with
a broad and evolving C++ surface area that spans data formats, compute kernels,
and file I/O. Due to frequent releases and a large API surface, different Arrow
C++ SDK versions can introduce API incompatibilities.

If Paimon C++ directly depends on the full Arrow C++ SDK, it may conflict with
existing Arrow C++ dependencies in other compute engines, raising integration
costs and increasing long-term maintenance complexity.

Adopting the Arrow C Data Interface
-----------------------------------

To leverage Arrow’s performance and ecosystem benefits while avoiding tight
coupling to specific Arrow C++ SDK versions, we use the Arrow C Data
Interface as the default in-memory format for Paimon C++.

Key advantages:

- Version-neutral: The C Data Interface is designed to be stable and forward-compatible
  across Arrow versions.
- Compiler-neutral: C language interfaces avoid ABI friction commonly seen with
  C++ compilers and standard libraries.
- Broad interoperability: The C Data Interface is supported by Arrow-based
  systems and enables zero-copy or minimal-copy interchange of columnar data.

Design Principles
-----------------

- Columnar-first abstraction:
  Paimon C++ will represent in-memory data using columnar buffers and schemas
  compatible with the Arrow C Data Interface, minimizing transformation overhead.

- Minimal dependency footprint:
  Prefer stable C interfaces and lightweight utility layers; avoid linking
  against the full Arrow C++ SDK unless strictly necessary and well-isolated.

- Vectorization-aware execution:
  Structure data layouts to align with SIMD processing (e.g., contiguous
  buffers, clear null bitmaps and type-specific arrays), enabling efficient
  filtering, projection, and aggregation.

- Interoperability:
  Ensure that data produced and consumed by Paimon C++ can be handed off to
  Arrow-compatible engines and libraries without expensive conversions.

- Compatibility with columnar storage:
  Maintain efficient paths from columnar file formats (Parquet, ORC) to in-memory
  columnar representations, minimizing decoding and marshaling overhead.

Implementation Outline
----------------------

Schema and buffers
~~~~~~~~~~~~~~~~~~~
- Represent schemas and arrays using Arrow C Data Interface types (e.g., ``ArrowSchema``, ``ArrowArray``) with clear ownership and lifecycle.
- Support nested types (structs, lists, maps) and common primitives (integers, floats, decimals, timestamps).

Memory management
~~~~~~~~~~~~~~~~~~~~
- Define consistent ownership semantics for buffers and child arrays.
- Employ reference counting or explicit release callbacks aligned with the Arrow C conventions.

Nullability and validity
~~~~~~~~~~~~~~~~~~~~~~~~
- Use standard validity bitmaps for nullability and adhere to Arrow’s canonical buffer organization (validity, offsets, data, etc.).

Conversion boundaries
~~~~~~~~~~~~~~~~~~~~~~
- Provide adapters to
  * Read from columnar file formats (Parquet/ORC) into Arrow-compatible ``ArrowArray`` structures.
  * Export/import data to other Arrow-compatible engines with zero or minimal copies.

- Keep these adapters independent from heavy Arrow C++ SDK dependencies.
