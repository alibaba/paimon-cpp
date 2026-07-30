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

Compaction
==========
Compaction is the process of merging multiple small data files into fewer, larger
files. It is a resource intensive procedure which consumes CPU time and disk IO,
so too frequent compaction may result in slower writes. However, without
compaction, the accumulation of small files degrades query performance. Tuning
compaction is therefore a trade-off between write throughput and read efficiency.

.. note::
   - There can only be one job working on the same partition's compaction,
     otherwise it will cause conflicts.
   - C++ Paimon does not support producing changelog for now.
   - Compaction is disabled when ``write-only`` is set to ``true``, or when the
     table uses dynamic bucketing (``bucket = -1``) for append-only tables.
   - For a complete list of compaction-related configurations, see the
     :ref:`Options API Reference <cpp-api-options>`.

Append-Only Table Compaction
----------------------------
In append-only table, data files are simply appended in sequence order.
Over time, many small files accumulate, which degrades read performance due to the
overhead of opening and scanning numerous files.

Append-only table compaction merges multiple small files into fewer, larger files
to improve read efficiency. The compaction is performed asynchronously and does
not block writes.

.. note::
   Append-only table compaction is only available for fixed-bucket mode
   (``bucket > 0``). Dynamic bucketing (``bucket = -1``) does not support
   compaction. Tables with blob columns also skip compaction.

Auto Compaction
~~~~~~~~~~~~~~~
During each flush, the writer triggers a best-effort auto compaction. The
compaction picker scans the file queue ordered by sequence number and selects a
contiguous window of files for merging when the number of candidate files reaches
the ``compaction.min.file-num`` threshold.

Full Compaction
~~~~~~~~~~~~~~~
Full compaction rewrites all eligible files in the bucket. During full
compaction:

- Files whose size is already at or above ``compaction.file-size`` (and have no
  associated deletion vectors) are skipped to avoid unnecessary rewrites.
- When deletion vectors are enabled, all files are always eligible for
  compaction regardless of size, because deletion vectors must be applied.
- When ``compaction.force-rewrite-all-files`` is ``true``, all files are
  rewritten unconditionally.
- Without deletion vectors, full compaction only proceeds when the number of
  small files exceeds the number of large files and the total file count is at
  least 3.

After compaction, if the last output file is still smaller than
``compaction.file-size``, it is placed back into the compaction queue for future
merging.

Append-Only Table Compaction Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 10 10 10 40

   * - Option
     - Required
     - Default
     - Type
     - Description
   * - ``compaction.min.file-num``
     - No
     - 5
     - Integer
     - The minimum number of files to trigger an auto compaction for
       append-only tables.


Primary Key Table Compaction
----------------------------
Primary key tables use an LSM tree (log-structured merge-tree) for file storage.
When more and more records are written, the number of sorted runs increases.
Because querying an LSM tree requires all sorted runs to be combined, too many
sorted runs will result in poor query performance, or even out of memory.

To limit the number of sorted runs, several sorted runs are merged into one big
sorted run once in a while. Paimon currently adopts a compaction strategy similar
to RocksDB's `universal compaction
<https://github.com/facebook/rocksdb/wiki/Universal-Compaction>`_.

Primary key table compaction solves:

- Reduce Level 0 files to avoid poor query performance.
- Produce deletion vectors for MOW mode.

Full Compaction
~~~~~~~~~~~~~~~
Paimon uses Universal Compaction. By default, when there is too much incremental
data, Full Compaction will be automatically performed. You don't usually have to
worry about it.

Paimon also provides configurations that allow for regular execution of Full
Compaction:

- ``compaction.optimization-interval``: Implying how often to perform an
  optimization full compaction. This configuration is used to ensure the query
  timeliness of the read-optimized system table.
- ``compaction.total-size-threshold``: Full compaction will be constantly triggered
  when total size is smaller than this threshold.
- ``compaction.incremental-size-threshold``: Full compaction will be constantly
  triggered when incremental size is bigger than this threshold.

Lookup Compaction
~~~~~~~~~~~~~~~~~
When a primary key table is configured with ``lookup`` changelog producer or
``first-row`` merge engine or has enabled deletion vectors for MOW mode, Paimon
will use a radical compaction strategy to force compacting level 0 files to
higher levels for every compaction trigger.

Paimon also provides configurations to optimize the frequency of this
compaction:

- ``lookup-compact``: compact mode used for lookup compaction. Possible values:

  * ``radical``: will use ``ForceUpLevel0Compaction`` strategy to radically
    compact new files.
  * ``gentle``: will use ``UniversalCompaction`` strategy to gently compact new
    files.

- ``lookup-compact.max-interval``: The max interval for a forced L0 lookup
  compaction to be triggered in ``gentle`` mode. This option is only valid when
  ``lookup-compact`` mode is ``gentle``.

By configuring ``lookup-compact`` as ``gentle``, new files in L0 will not be
compacted immediately. This may greatly reduce the overall resource usage at the
expense of worse data freshness in certain cases.

Primary Key Table Compaction Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Number of Sorted Runs to Pause Writing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
When the number of sorted runs is small, Paimon writers will perform compaction
asynchronously in separated threads, so records can be continuously written into
the table. However, to avoid unbounded growth of sorted runs, writers will pause
writing when the number of sorted runs hits the threshold.

.. list-table::
   :header-rows: 1
   :widths: 30 10 10 10 40

   * - Option
     - Required
     - Default
     - Type
     - Description
   * - ``num-sorted-run.stop-trigger``
     - No
     - (none)
     - Integer
     - The number of sorted runs that trigger the stopping of writes. The
       default value is ``num-sorted-run.compaction-trigger + 3``.

Write stalls will become less frequent when ``num-sorted-run.stop-trigger``
becomes larger, thus improving writing performance. However, if this value
becomes too large, more memory and CPU time will be needed when querying the
table.

Number of Sorted Runs to Trigger Compaction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Paimon uses LSM tree which supports a large number of updates. LSM organizes
files in several sorted runs. When querying records from an LSM tree, all sorted
runs must be combined to produce a complete view of all records.

One can easily see that too many sorted runs will result in poor query
performance. To keep the number of sorted runs in a reasonable range, Paimon
writers will automatically perform compactions. The following table property
determines the minimum number of sorted runs to trigger a compaction.

.. list-table::
   :header-rows: 1
   :widths: 30 10 10 10 40

   * - Option
     - Required
     - Default
     - Type
     - Description
   * - ``num-sorted-run.compaction-trigger``
     - No
     - 5
     - Integer
     - The sorted run number to trigger compaction. Includes level 0 files (one
       file one sorted run) and high-level runs (one level one sorted run).

Compaction will become less frequent when ``num-sorted-run.compaction-trigger``
becomes larger, thus improving writing performance. However, if this value
becomes too large, more memory and CPU time will be needed when querying the
table. This is a trade-off between writing and query performance.
