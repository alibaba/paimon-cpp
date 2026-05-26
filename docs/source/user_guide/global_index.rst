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

Global Index
============

Global Index is a powerful indexing mechanism for append-only tables.
It enables efficient row-level lookups and filtering without full-table scans.
Paimon supports multiple global index types:

- **Bitmap Index**: A bitmap-based index. Each distinct value is mapped to a compressed bitmap (RoaringBitmap) that records which rows contain that value, enabling extremely fast set membership tests.
- **BTree Index**: An efficient index based on multi-level SST files for scalar column lookups.
- **Range Bitmap Index**: A range bitmap index optimized for range predicates on ordered scalar columns. Extends the bitmap approach by encoding value ordering, enabling efficient less-than, greater-than, and range conditions.
- **Lucene Index**: A full-text search index powered by Lucene++. Supports tokenized text search with multiple modes including match-all, match-any, phrase, prefix, and wildcard queries.
- **Vector Index (Lumina)**: An approximate nearest neighbor (ANN) index powered by Lumina for vector similarity search with configurable distance metrics.

Global indexes work on top of Data Evolution tables. To use global indexes, your table must have:

- ``'bucket' = '-1'`` (unaware-bucket mode)
- ``'row-tracking.enabled' = 'true'``
- ``'data-evolution.enabled' = 'true'``

Bitmap Index
------------

A bitmap-based index for Equal and In predicates. Each distinct value in the indexed column
is mapped to a compressed bitmap (RoaringBitmap) that records which rows contain that value.
This allows extremely fast set membership tests.

BTree Index
-----------

BTree is an efficient index based on multi-level SST files, supporting rich predicate pushdown, block cache, file-level min/max key pruning, lazy loading, and block compression.

**Special Configuration:**

- **Option**: ``btree-index.read-buffer-size``

  - **Description**: Optional. Specifies the read buffer size for the B-tree index. This setting can be tuned based on query patterns:

    - For **range queries** (e.g., ``VisitLessThan``, ``VisitGreaterOrEqual``), increasing the buffer size (e.g., to 1MB) may improve I/O bandwidth and sequential read performance.
    - For **point queries** (e.g., ``VisitEqual``), buffering can introduce negative effects due to read amplification; it is recommended to leave this option unset.

Range Bitmap Index
------------------

A range bitmap index optimized for range predicates on ordered scalar columns. It extends the
bitmap approach by encoding value ordering information, enabling efficient evaluation of
less-than, greater-than, and range conditions without scanning all bitmaps.


Lucene Index
------------

A full-text search index powered by Lucene++. It supports tokenized text search with multiple
search modes including match-all, match-any, phrase, prefix, and wildcard queries.

**Supported search types:**

- ``MATCH_ALL``: All terms in the query must be present (AND semantics).
- ``MATCH_ANY``: Any term in the query can match (OR semantics).
- ``PHRASE``: Matches the exact sequence of words (with proximity).
- ``PREFIX``: Matches terms starting with the given string (e.g., "run*" → running, runner).
- ``WILDCARD``: Supports wildcards ``*`` and ``?`` (e.g., "ap*e", "app?e" → "apple").

**Special Configuration:**

- **Option**: ``lucene-fts.write.tmp.directory``

  - **Description**: Specifies the temporary directory used during Lucene index writing. No default value; must be explicitly set.

- **Environment Variable**: ``PAIMON_JIEBA_DICT_DIR``

  - **Description**: Specifies the directory containing Jieba dictionary files for Chinese text tokenization. At runtime, the system first checks this environment variable; if not set, it falls back to the compile-time ``JIEBA_TEST_DICT_DIR`` macro (only available in test builds). If neither is available, will fail with an error.

Vector Index (Lumina)
---------------------

An approximate nearest neighbor (ANN) index powered by Lumina for vector similarity search.
Supports high-dimensional vector search with configurable distance metrics and encoding strategies.
For more configurations, please refer to the third_party/lumina/reference directory.
