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

Data Cleanup
===================================
.. note::

  The entire data cleanup feature supports only append tables, and cleanup of index manifests, changelog, statistics are not supported. Do not use this feature with primary key tables.

This document describes three cleanup capabilities:

- :ref:`orphan-file-cleanup`
- :ref:`expiring-partitions`
- :ref:`expiring-snapshots`

.. _orphan-file-cleanup:

Orphan File Cleanup
-------------------

Description
~~~~~~~~~~~~~~~~~~~

- Orphan file cleanup currently supports only append tables.
- It runs as an independent task: construct a ``CleanContext`` and
  launch an ``OrphanFilesCleaner``.

Detailed Steps
~~~~~~~~~~~~~~

1. List all Paimon-specific subdirectories under the table directory
   (e.g., ``manifest/``, ``snapshot/``, ``f1=10/bucket-0``, ...).
2. Based on the subdirectories from step 1, enumerate all Paimon files
   in the table directory.
3. Using snapshot information, determine all in-use ``manifest`` files
   and data files.
4. Compute the set of files that appear in step 2 but not in step 3
   (i.e., orphan files). Among those, delete files whose modification
   time is earlier than ``older_than_ms``.

Performance Considerations
~~~~~~~~~~~~~~~~~~~~~~~~~~

- Steps 1, 2, and 4 use an executor to parallelize I/O operations.
- Orphan file cleanup may take a long time; you can pass an executor
  with more threads to accelerate the process.

.. admonition:: TODO
  :class: tip

  - Support cleanup of index manifests, changelog, statistics, etc.

.. _expiring-partitions:

Expiring Partitions
-------------------

Description
~~~~~~~~~~~~~~~~~~~

- Executed within the Commit task. First construct a ``FileStoreCommit``.
- ``DropPartition`` uses a mark-delete strategy. Calling ``DropPartition``
  will send an ``Overwrite`` message, marking all data files in the
  specified partition as ``DELETE``.
- A new snapshot is committed afterward. Actual deletion of data files
  occurs as snapshots expire.

Detailed Steps
~~~~~~~~~~~~~~

1. Build a ``ScanFilter`` for the partition and use the latest snapshot
   to scan the partition.
2. Iterate over the scanned data file list (``ManifestEntries``) and
   rewrite each entry’s type to ``DELETE``.
3. Commit using the rewritten ``ManifestEntries``. If the commit fails,
   retry a limited number of times.

.. _expiring-snapshots:

Expiring Snapshots
------------------

Description
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Executed within the Commit task. First construct a ``FileStoreCommit``.
- The following optional configuration parameters control snapshot
  expiration:

  - ``snapshot.num-retained.min``: The minimum number of completed
    snapshots to retain (>= 1). Default: 10.
  - ``snapshot.num-retained.max``: The maximum number of completed
    snapshots to retain (>= ``snapshot.num-retained.min``). Default:
    ``int32`` max value.
  - ``snapshot.time-retained``: The maximum age of completed snapshots
    to retain. Default: 1 hour.
  - ``snapshot.expire.limit``: The maximum number of snapshots allowed
    to expire at a time. Default: 10.

- The snapshot expiration interface deletes data files according to the
  expiration policy and returns the number of snapshots deleted.

Detailed Steps
~~~~~~~~~~~~~~

1. Use ``snapshot_manager`` to find ``earliest_snapshot_id`` and
   ``latest_snapshot_id``.
2. Based on ``earliest/latest_snapshot_id`` and the expire config,
   determine the range of snapshots to clean.

.. note::

     Consumer subscription management (consumer manager) is not
     currently supported. Users must ensure that snapshots to be expired
     are not in use.

3. Verify that the snapshot range is continuous. Normally, it is
   continuous. If a snapshot is missing, assume earlier snapshots were
   already cleaned and the missing files are orphaned remnants due to
   I/O exceptions; they are out of scope for this cleanup.
4. Clean data files for the updated expiration range.

   - To decide whether a file from a snapshot should be deleted, check if it was marked ``DELETE`` in the delta of the subsequent snapshot.
   - For an expiration range ``[begin, end)``, iterate over ``(begin,end]`` and delete data files whose type is ``DELETE`` in each ``snapshot.DeltaManifestList()``.
   - If a file underwent multiple ``ADD`` and ``DELETE`` operations, deletion follows the operation order:

     * ``ADD`` then ``DELETE`` → the file is deleted.
     * ``DELETE`` then ``ADD`` → the initial ``DELETE`` does not apply (file did not exist yet); the subsequent ``ADD`` ensures the file remains.

5. Clean meta files:
   - Preserve manifests used by the last snapshot in the cleanup range (``end_exclusive_id``).
   - Delete manifest files used by snapshots from ``begin_inclusive_id`` to ``end_exclusive_id`` (exclusive) and delete the snapshot files themselves.
6. Rewrite ``EarliestHint`` to ``end_exclusive_id``.
7. Return the number of snapshots deleted, i.e., ``end_exclusive_id - begin_inclusive_id``.

Performance Considerations
~~~~~~~~~~~~~~~~~~~~~~~~~~

- Step 4 uses an executor to parallelize file deletions.
- If deletion is slow, pass an executor with more threads to accelerate
  the process.

.. admonition:: TODO
  :class: tip

  - Preserve tag (savepoint) data via ``tagManager``.
  - Delete changelog files.
  - Remove empty directories.
