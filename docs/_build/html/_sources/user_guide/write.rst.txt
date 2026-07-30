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

Write
=====
Batch writing requires the compute engine to specify the target ``partition``.
For fixed-bucket tables, the engine must also assign a valid bucket to every
``RecordBatch``. In unaware-bucket and postpone-bucket modes, the writer can
resolve the bucket automatically when it is omitted.

Paimon C++ uses Apache Arrow as the :ref:`in-memory columnar format<memory-format>`
to more efficiently support writing to disk columnar formats such as ORC,
Parquet, and Avro, thereby improving write throughput.

.. note::
  Currently supported table types:
    - Append table
    - Primary Key table

  Not supported in the current scope:
    - Changelog

Bucketing Modes
---------------

- Append tables:

  * Support ``bucket = -1`` (unaware-bucket mode)
  * Support ``bucket > 0`` (fixed bucket mode)

- PK tables:

  * Support ``bucket = -2`` (postpone bucket mode)
  * Support ``bucket > 0`` (fixed bucket mode)

.. note::
   PK tables do not support dynamic bucketing (``bucket = -1``).

RecordBatch Construction
------------------------

- The compute engine must:

  - Assign the correct ``partition`` for each row.
  - In fixed-bucket mode, apply the Paimon-consistent bucketing function, set a
    bucket in ``[0, bucket)``, and group rows into Arrow ``RecordBatch`` objects
    per partition-bucket combination.

- In unaware-bucket mode (append table with ``bucket = -1``), an omitted bucket
  is resolved to ``0``.
- In postpone-bucket mode (primary-key table with ``bucket = -2``), an omitted
  bucket is resolved to ``-2``.

- Recommended practices:

  - Use schema-aligned Arrow arrays with explicit validity bitmaps and offsets.
  - Prefer batch sizes tuned for I/O throughput (e.g., tens to hundreds of MB per flush, depending on filesystem and cluster configuration).
  - Maintain stable sort orders within a batch only if required by downstream merge or compaction logic; otherwise avoid unnecessary ordering costs.

Prepare Commit
----------------

The compute engine is responsible for triggering the writer nodes' ``PrepareCommit``.
Triggering conditions depend on the engine's business needs and can follow either:

- Streaming mode: time-based or periodic triggers (e.g., every N seconds).
- Batch mode: trigger after all data in the batch has been written.

Once the compute engine collects ``CommitMessages`` from all writer nodes, it
can issue a ``Commit`` request to the control plane (management path) to create
a new ``Snapshot``.

Compatibility Goals
~~~~~~~~~~~~~~~~~~~

To ensure interoperability, the ``PrepareCommit`` result produced by Paimon C++
must be consumable by Paimon Java. Therefore:

- The structure and semantics of ``CommitMessage`` must remain consistent with
  Java Paimon.
- Any evolution of the Java-side ``CommitMessage`` schema must be tracked and
  validated on the C++ side to maintain cross-language compatibility.

Interface Design in Paimon C++
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unlike Java Paimon, Paimon C++ does not expose ``BinaryRow``-like types in its
public interfaces. To preserve compatibility without leaking internal row
representations, Paimon C++ provides ``CommitMessage`` only through:

- Serialization: convert the internal commit state into a well-defined binary
  representation that matches Java Paimon's expectations.
- Deserialization: parse the Java-compatible binary representation back into
  C++ commit structures for validation, replay, or tooling needs.

This design ensures that:

- Public APIs are independent of Java-specific row abstractions.
- Cross-language commit payloads remain stable and versionable.
- Internal data layouts can evolve without breaking external consumers.

CommitMessage Contract
~~~~~~~~~~~~~~~~~~~~~~

The ``CommitMessage`` must encode all information required by the coordinator to
produce a correct ``Snapshot``, which commonly includes (but is not limited to):

- Partition and bucket identifiers associated with written data.
- New data files, delete files (as applicable to the table type).
- File-level metadata required for manifest and index updates (e.g., row counts, min/max statistics where applicable).
- Transactional markers and sequence numbers as required by table semantics.
- Any per-writer state necessary for deduplication or idempotent commits.

.. note::

   The C++ writer supports Append and PK tables and can produce
   ``CommitMessage`` objects for both. ``FileStoreCommit`` supports direct
   file-system commits for both table types on non-object-store paths.
   Object-store paths require REST catalog commit mode. Changelog is out of
   scope and should not be emitted in ``CommitMessage`` until explicitly
   supported.

Serialization and Deserialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Binary format:** The binary payload must strictly conform to Java Paimon's
  ``CommitMessage`` encoding. It does not contain a version tag, so callers
  must transport ``CommitMessage::CurrentVersion()`` separately and supply it
  when deserializing.
- **Serialization API:** Use ``CommitMessage::Serialize`` for one message or
  ``CommitMessage::SerializeList`` for a list.
- **Deserialization API:** Use ``CommitMessage::Deserialize`` or
  ``CommitMessage::DeserializeList`` with the separately supplied
  serialization version.
- **Validation:** Conformance and round-trip tests must verify compatibility
  with Java Paimon for supported message versions.

Operational Flow
~~~~~~~~~~~~~~~~~~~~~~~

1. Writer nodes perform data ingestion and produce Arrow ``RecordBatch``
   organized by partition and bucket.

2. Writers flush batches into ORC, Parquet, or Avro files via registered
   ``file.format`` and ``file-system`` backends, producing file-level metadata
   and per-batch commit state.

3. Each writer invokes ``PrepareCommit``, which:
   - Aggregates per-writer state into ``CommitMessage`` objects.
   - Returns ``CommitMessage`` objects; it does not serialize them.

4. The compute engine gathers ``CommitMessage`` objects from all writers. For
   cross-process transport, it explicitly calls ``Serialize`` or
   ``SerializeList`` and carries ``CurrentVersion()`` alongside the payload.

5. For a direct file-system commit on a non-object-store path, the engine
   passes the objects to ``FileStoreCommit`` for either an Append or PK table.
   For an object-store path, it enables REST catalog commit mode, calls
   ``Commit``, obtains the JSON request from ``GetLastCommitTableRequest``, and
   sends that request to the REST catalog.

6. The local committer or REST catalog validates the messages, updates
   manifests/metadata, and finalizes the snapshot atomically.
