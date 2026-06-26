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

Parquet Metadata Cache
======================

Overview
--------

paimon-cpp can cache serialized Parquet metadata footer bytes for Parquet data files.
The cache is used by ``ParquetReaderBuilder`` before opening the Arrow Parquet
reader. On a cache miss, paimon-cpp loads the Parquet file metadata, serializes
it as a complete metadata footer, and stores those bytes in the public
``Cache`` abstraction. On a cache hit, paimon-cpp parses the cached footer bytes into
``parquet::FileMetaData`` and passes the metadata to the Parquet reader.

The cache stores serialized metadata footer bytes instead of caching a
``parquet::FileMetaData`` instance. This keeps the cache value compact and
similar to manifest cache values: the cache weight follows the actual cached
bytes, while the Parquet library still owns metadata parsing and validation.

This optimization is useful when the same Parquet files are opened repeatedly
in the same process, for example repeated ``get`` or ``scan`` requests over the
same snapshot. On a cache hit, the read path avoids reading the Parquet footer
bytes from the filesystem again. paimon-cpp still parses the cached footer bytes
into ``parquet::FileMetaData`` for each reader open. Data pages, page indexes,
and column chunks are still read from the file as usual.

Configuration
-------------

Parquet metadata caching is disabled by default. Embedding applications that
need it can provide a custom ``Cache`` implementation and inject it through
``ScanContextBuilder`` or ``ReadContextBuilder``. Parquet reader builders
receive the cache from the read context and create cache keys with
``CacheKind::DATA_FILE_FOOTER`` internally.

The cache key represents the file footer and is created from the file URI with
position ``-1`` and length ``-1``. Callers do not need to construct this key
directly; they only need to route ``CacheKind::DATA_FILE_FOOTER`` entries to an
appropriate cache backend.

Example:

.. code-block:: cpp

   class RoutingCache : public paimon::Cache {
    public:
     RoutingCache(std::shared_ptr<paimon::Cache> default_cache,
                  std::shared_ptr<paimon::Cache> parquet_metadata_cache)
         : default_cache_(std::move(default_cache)),
           parquet_metadata_cache_(std::move(parquet_metadata_cache)) {}

     paimon::Result<std::shared_ptr<paimon::CacheValue>> Get(
         const std::shared_ptr<paimon::CacheKey>& key,
         std::function<paimon::Result<std::shared_ptr<paimon::CacheValue>>(
             const std::shared_ptr<paimon::CacheKey>&)> supplier) override {
       return Select(key)->Get(key, std::move(supplier));
     }

     // Put(), Invalidate(), InvalidateAll(), and Size() route in the same way.

    private:
     std::shared_ptr<paimon::Cache> Select(
         const std::shared_ptr<paimon::CacheKey>& key) const {
       return key && key->GetKind() == paimon::CacheKind::DATA_FILE_FOOTER
                  ? parquet_metadata_cache_
                  : default_cache_;
     }

     std::shared_ptr<paimon::Cache> default_cache_;
     std::shared_ptr<paimon::Cache> parquet_metadata_cache_;
   };

   auto cache = std::make_shared<RoutingCache>(
       std::make_shared<MyDefaultCache>(),
       std::make_shared<MyParquetMetadataCache>());

   paimon::ScanContextBuilder scan_builder(table_path);
   scan_builder.WithCache(cache);

   paimon::ReadContextBuilder read_builder(table_path);
   read_builder.WithCache(cache);

Passing ``nullptr`` or omitting ``WithCache()`` leaves Parquet metadata caching
disabled. If a file URI cannot be obtained, paimon-cpp also bypasses the cache
and opens the Parquet file normally.

Future Optimizations
--------------------

- Add hit, miss, bypass, and eviction metrics for Parquet metadata cache.
- Add single-flight loading for high-concurrency misses on the same Parquet
  file.
- Evaluate sharing cached metadata footer bytes with page-index prefetch logic when
  those read paths can use the same cache abstraction.
