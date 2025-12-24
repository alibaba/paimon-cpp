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

Write And Prepare Commit
========================
Batch writing requires the compute engine to pre-bucket data (bucket), using the
same bucketing strategy as Paimon to ensure correct ``Scan`` behavior, and to
specify the target ``partition``. Data should be accumulated into ``RecordBatch``
and written to Paimon.

Paimon C++ uses Apache Arrow as the :ref:`in-memory columnar format<memory-format>`
to more efficiently support writing to disk columnar formats such as ORC and
Parquet, thereby improving write throughput.

.. note::
  Currently supported table types:
    - Append table
    - Primary Key table

  Not supported in the current scope:
    - Changelog
    - Indexes

Bucketing Modes
---------------

- Append tables:

  * Support ``bucket = -1`` (dynamic bucket mode)
  * Support ``bucket > 0`` (fixed bucket mode)

- PK tables:

  * Support ``bucket > 0`` (fixed bucket mode)

.. note::
   PK tables do not support dynamic bucketing (``bucket = -1``).

RecordBatch Construction
------------------------

- The compute engine must:

  - Apply the Paimon-consistent bucketing function to each row prior to batching.
  - Assign the correct ``partition`` for each row.
  - Group rows into Arrow ``RecordBatch`` per partition-bucket combination to minimize writer state changes and I/O overhead.

- Recommended practices:

  - Use schema-aligned Arrow arrays with explicit validity bitmaps and offsets.
  - Prefer batch sizes tuned for I/O throughput (e.g., tens to hundreds of MB per flush, depending on filesystem and cluster configuration).
  - Maintain stable sort orders within a batch only if required by downstream merge or compaction logic; otherwise avoid unnecessary ordering costs.

Prepare Commit
----------------

The compute engine is responsible for triggering the writer nodes' ``PrepareCommit``.
Triggering conditions depend on the engine’s business needs and can follow either:

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
  representation that matches Java Paimon’s expectations.
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
- New data files, delete files, or changelog artifacts (as applicable to the table type).
- File-level metadata required for manifest and index updates (e.g., row counts, min/max statistics where applicable).
- Transactional markers and sequence numbers as required by table semantics.
- Any per-writer state necessary for deduplication or idempotent commits.

.. note::

   Current C++ scope supports Append and PK tables. Changelog and index
   artifacts are out of scope and should not be emitted in ``CommitMessage`` until
   explicitly supported.

Serialization and Deserialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Binary Format:
  - The binary payload must strictly conform to Java Paimon’s ``CommitMessage`` encoding.
  - Version tags or schema identifiers should be included to enable forwards/backwards compatibility and safe upgrades.

- Serialization API:
  - Provide a function to serialize the writer’s commit state into a byte buffer (or stream) consumable by Java Paimon.

- Deserialization API:
  - Provide a function to parse a Java-produced ``CommitMessage`` binary payload back into C++ commit structures for verification, replay, and testing.

- Validation:
  - Include conformance tests to assert that C++ serialized payloads are accepted by Java Paimon.
  - Include round-trip tests to ensure C++ can parse Java-produced payloads and vice versa for supported message versions.

Operational Flow
~~~~~~~~~~~~~~~~~~~~~~~

1. Writer nodes perform data ingestion and produce Arrow ``RecordBatch``
   organized by partition and bucket.

2. Writers flush batches into ORC/Parquet files via registered ``file.format``
   and ``file-system`` backends, producing file-level metadata and per-batch
   commit state.

3. Each writer invokes ``PrepareCommit``, which:
   - Aggregates per-writer state into a ``CommitMessage``.
   - Serializes the message into a Java-compatible binary payload.

4. The compute engine gathers ``CommitMessages`` from all writers.

5. The compute engine issues a ``Commit`` request to the control plane with the
   collected messages, resulting in a new ``Snapshot``.

6. The coordinator validates the messages, updates manifests/metadata, and
   finalizes the snapshot atomically.
