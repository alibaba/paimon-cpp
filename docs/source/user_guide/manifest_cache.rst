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

Manifest Cache
==============

Overview
--------

paimon-cpp caches raw manifest file bytes at the ``ObjectsFile<T>::Read()``
layer. The cache uses the public ``Cache`` abstraction and is injected through
``ScanContextBuilder`` or ``ReadContextBuilder``. The cache covers data
manifests, manifest lists, and index manifests because they all read through
``ObjectsFile<T>``.

For repeated ``get``, ``scan``, or batch ``get/scan -f`` requests in the same
process, the same snapshot often reads the same manifest files repeatedly. On a
cache hit, the read path skips remote filesystem ``open/read``, builds an
in-memory input stream from cached bytes, and still runs the format reader,
Arrow decoding, and object deserialization. This design primarily reduces
remote IO latency and bandwidth while keeping cache weight aligned with the
actual cached bytes.

Configuration
-------------

Manifest caching is disabled by default. Embedding applications that need it can
provide a custom ``Cache`` implementation and inject it through ``WithCache``.
Manifest reads create cache keys with ``CacheKind::MANIFEST`` internally, so
callers do not need to pass the cache kind through scan or read contexts. The
same cache instance can be reused across multiple scan or read contexts when
process-local sharing is desired.

Example:

.. code-block:: cpp

   class RoutingCache : public paimon::Cache {
    public:
     RoutingCache(std::shared_ptr<paimon::Cache> default_cache,
                  std::shared_ptr<paimon::Cache> manifest_cache)
         : default_cache_(std::move(default_cache)),
           manifest_cache_(std::move(manifest_cache)) {}

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
       return key && key->GetKind() == paimon::CacheKind::MANIFEST
                  ? manifest_cache_
                  : default_cache_;
     }

     std::shared_ptr<paimon::Cache> default_cache_;
     std::shared_ptr<paimon::Cache> manifest_cache_;
   };

   auto cache = std::make_shared<RoutingCache>(
       std::make_shared<MyDefaultCache>(),
       std::make_shared<MyManifestCache>());

   paimon::ScanContextBuilder scan_builder(table_path);
   scan_builder.WithCache(cache);

   paimon::ReadContextBuilder read_builder(table_path);
   read_builder.WithCache(cache);

Passing ``nullptr`` or omitting ``WithCache()`` leaves manifest caching disabled.

Future Optimizations
--------------------

- Add hit, miss, bypass, and eviction metrics to read trace or metrics.
- Add single-flight loading for high-concurrency misses on the same manifest
  path.
- Evaluate a decoded-records second-level cache, configurable as a
  CPU-vs-memory tradeoff.
