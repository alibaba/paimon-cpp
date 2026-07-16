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

System Tables
=============

Paimon C++ supports reading system tables by appending a system table suffix to
the data table path. For example, ``/warehouse/db.db/orders$snapshots`` reads
the snapshots system table for ``orders``.

Branch-qualified paths are also supported. For example,
``/warehouse/db.db/orders$branch_audit$files`` reads the files system table from
the ``audit`` branch.

Read-Optimized System Table
---------------------------

The read-optimized system table is addressed by the ``$ro`` suffix:

.. code-block:: text

   /warehouse/db.db/orders$ro

For primary-key tables, ``$ro`` only plans data files from the highest LSM
level, which is the level produced by full compaction. This avoids merging data
from multiple LSM levels during query planning and reading. The tradeoff is that
the result may lag behind the latest committed data until a full compaction
publishes the newest records into the highest level.

This stale view is not guaranteed to correspond to any single historical table
snapshot. Full compaction may finish independently for different buckets. When
``$ro`` scans the latest snapshot, the selected highest-level files can
therefore combine bucket states produced by full-compaction commits at different
snapshot IDs.

For primary-key tables, ``$ro`` also enables value-stats filtering, so file-level
pruning of the reader predicate can be more aggressive than the base table.

For append-only tables, ``$ro`` has the same read behavior as the base table,
including streaming reads.

Limitations
~~~~~~~~~~~

- Primary-key ``$ro`` scans are batch-only. Streaming scans are not supported.
- Freshness depends on full compaction frequency, and the result may not match
  any single historical snapshot across buckets.
- Primary-key tables in bucket-unaware mode are not supported by the current C++
  scan path.

Typical Usage
~~~~~~~~~~~~~

Use ``$ro`` for OLAP or batch workloads that can tolerate stale results and
prefer reading compacted files directly. Use the base table path when the query
must see the latest committed data.
