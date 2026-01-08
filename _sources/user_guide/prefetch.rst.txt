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

Prefetch
========

.. image:: ../_static/prefetch.svg
   :alt: File Layout
   :align: center
   :width: 100%

In C++ Paimon, we use a multi-producer, single-consumer model to optimize file reading. The core idea is to split a file into line-based ReadRanges and assign them to multiple reader threads (producers). Each reader thread owns an independent result queue that holds its processed RecordBatches. In the main reader thread (the consumer), we sort the heads of all queues by the ReadRange start offset in ascending order and select the RecordBatch with the smallest start offset to ensure globally ordered results.

Read Range Splitting Strategy
=============================

Designing an efficient ReadRange splitting strategy requires balancing two key objectives:

- Minimize read amplification: Ensure the data fetched from storage is used effectively, avoiding unnecessary I/O overhead.
- Reduce ReadRange span: Ideally, the size of a ReadRange should match a single read batch size to enable fine-grained parallel control.

Below we detail how these strategies are applied to formats Parquet.

Parquet
=======

Parquet files are organized into RowGroups and Pages. Since C++ Parquet does not support row-level seeking, prefetching can only be done at the RowGroup level. This naturally avoids read amplification, but introduces a new challenge: if a file contains only a small number of RowGroups, parallelism is severely limited. Therefore, we recommend users reduce RowGroup size when writing Parquet files to increase opportunities for parallel processing.

Another critical difference is the read behavior compared to Orc. Orc strictly returns RecordBatches aligned to Stripe boundaries, whereas C++ Parquet may return a RecordBatch containing data from multiple RowGroups. This can lead to output order confusion during parallel reads. We modified C++ Parquet internals to return results strictly aligned to RowGroup boundaries, matching Orc’s behavior. With this change, parallel reading no longer requires complex seek operations, improving overall read efficiency.

.. admonition:: TODO
  :class: tip

  Support prefetch for Orc.
