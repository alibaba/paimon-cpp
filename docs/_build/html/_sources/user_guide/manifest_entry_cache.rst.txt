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

Manifest Entry Cache
====================

Overview
--------

Large tables may contain many manifest entries, while a scan may only need a
small subset after bucket, partition, and statistics pruning. The snapshot live
manifest entry cache reduces repeated manifest decoding cost for successive full
scans that target the same bucket.

The cache stores decoded and merged live manifest entries by table path, branch,
and bucket for ``ScanMode::ALL``. Each cache value can retain several snapshot
results for that bucket. Exact snapshot hits are served from the cache; cache
misses rebuild the target snapshot bucket from the target snapshot's data
manifests and store the rebuilt live entries.

Request-specific filters are not stored in the cache. Partition, level, and
predicate filters are still evaluated for each scan, so cached entries can be
reused safely across different scan predicates for the same bucket.

Configuration
-------------

Manifest entry caching reuses the cache instance provided by
``ScanContextBuilder::WithCache()`` and stores bucket-scoped snapshot entries
under
``CacheKind::SNAPSHOT_LIVE_MANIFEST``:

.. code-block:: cpp

   auto cache = std::make_shared<LruCache>(128 * 1024 * 1024);
   ScanContextBuilder context_builder(table_path);
   PAIMON_ASSIGN_OR_RAISE(
       std::unique_ptr<ScanContext> scan_context,
       context_builder
           .WithCache(cache)
           .AddOption(Options::SCAN_MANIFEST_ENTRY_CACHE_MAX_SNAPSHOTS, "3")
           .Finish());

Cache entries are scoped by table path, branch, and bucket, so they can be
reused across newly created ``TableScan`` and ``FileStoreScan`` instances as
long as they share the same cache object and scan the same bucket.

``Options::SCAN_MANIFEST_ENTRY_CACHE_MAX_SNAPSHOTS`` controls how many snapshot
results are retained in each table/branch/bucket cache value. Older snapshots in
the same bucket are evicted first. The default value is ``0``, which disables
the cache path. Set it to a positive value to enable the cache when
``ScanContextBuilder::WithCache()`` is also configured. Physical cache eviction
is still controlled by the configured ``Cache`` implementation, for example the
capacity of ``LruCache``.

If no cache is provided through ``ScanContextBuilder::WithCache()``, this
optimization is skipped. The snapshot manifest entry cache shares the same
``Cache`` interface with raw manifest and data-file footer caches, but it uses a
dedicated ``CacheKind`` and a table/branch/bucket key instead of file byte
ranges.

Limitations
-----------

The cache is currently used only for ``ScanMode::ALL`` scans that can determine
a single target bucket. It is skipped for scans without a bucket filter because
reading or deserializing all buckets would be too expensive for selective
queries. It is also skipped for row-range scans because row-range pruning is
applied at manifest-meta level.

Metrics
-------

The scan metrics expose existing counters for the last scan:

- ``lastScannedManifests``: how many manifest files were loaded during this
  scan before manifest entry decoding.
