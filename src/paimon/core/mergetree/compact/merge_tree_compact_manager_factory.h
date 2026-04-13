/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "paimon/common/io/cache/cache_manager.h"
#include "paimon/core/compact/cancellation_controller.h"
#include "paimon/core/compact/compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/compact_rewriter.h"
#include "paimon/core/mergetree/compact/compact_strategy.h"
#include "paimon/core/mergetree/lookup/remote_lookup_file_manager.h"
#include "paimon/core/operation/metrics/compaction_metrics.h"
#include "paimon/result.h"
namespace arrow {
class Schema;
}

namespace paimon {

class BinaryRow;
class BucketedDvMaintainer;
class Executor;
class FieldsComparator;
class FileStorePathFactory;
class FileStorePathFactoryCache;
class IOManager;
class Levels;
class SchemaManager;
class TableSchema;
class MemoryPool;
class LookupStoreFactory;
template <typename T>
class LookupLevels;
template <typename T>
class PersistProcessor;
struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

/// Factory for creating merge-tree compact strategy and compact manager.
class MergeTreeCompactManagerFactory {
 public:
    MergeTreeCompactManagerFactory(
        const CoreOptions& options, const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const std::shared_ptr<CompactionMetrics>& compaction_metrics,
        const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<SchemaManager>& schema_manager,
        const std::shared_ptr<IOManager>& io_manager,
        const std::shared_ptr<CacheManager>& cache_manager,
        const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
        const std::string& root_path, const std::shared_ptr<MemoryPool>& pool)
        : options_(options),
          pool_(pool),
          key_comparator_(key_comparator),
          user_defined_seq_comparator_(user_defined_seq_comparator),
          compaction_metrics_(compaction_metrics),
          table_schema_(table_schema),
          schema_(schema),
          schema_manager_(schema_manager),
          io_manager_(io_manager),
          cache_manager_(cache_manager),
          file_store_path_factory_(file_store_path_factory),
          root_path_(root_path) {}

    std::shared_ptr<CompactStrategy> CreateCompactStrategy() const;

    Result<std::shared_ptr<CompactManager>> CreateCompactManager(
        const BinaryRow& partition, int32_t bucket,
        const std::shared_ptr<CompactStrategy>& compact_strategy,
        const std::shared_ptr<Executor>& compact_executor, const std::shared_ptr<Levels>& levels,
        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer) const;

    void Close() {}

 private:
    std::shared_ptr<CompactionMetrics::Reporter> CreateCompactionMetricsReporter(
        const BinaryRow& partition, int32_t bucket) const;

    Result<std::shared_ptr<CompactRewriter>> CreateRewriter(
        const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
        const std::shared_ptr<CancellationController>& cancellation_controller) const;

    Result<std::shared_ptr<CompactRewriter>> CreateLookupRewriter(
        const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
        const LookupStrategy& lookup_strategy,
        const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
        const std::shared_ptr<CancellationController>& cancellation_controller) const;

    Result<std::shared_ptr<CompactRewriter>> CreateLookupRewriterWithDeletionVector(
        const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
        const LookupStrategy& lookup_strategy,
        const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
        const std::shared_ptr<CancellationController>& cancellation_controller,
        const std::shared_ptr<RemoteLookupFileManager>& remote_lookup_file_manager) const;

    Result<std::shared_ptr<CompactRewriter>> CreateLookupRewriterWithoutDeletionVector(
        const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
        const LookupStrategy& lookup_strategy,
        const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
        const std::shared_ptr<CancellationController>& cancellation_controller,
        const std::shared_ptr<RemoteLookupFileManager>& remote_lookup_file_manager) const;

    Result<std::shared_ptr<RemoteLookupFileManager>> CreateRemoteLookupFileManager(
        const BinaryRow& partition, int32_t bucket) const;

    CoreOptions options_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<FieldsComparator> user_defined_seq_comparator_;
    std::shared_ptr<CompactionMetrics> compaction_metrics_;
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::shared_ptr<IOManager> io_manager_;
    std::shared_ptr<CacheManager> cache_manager_;
    std::shared_ptr<FileStorePathFactory> file_store_path_factory_;
    std::string root_path_;
};

}  // namespace paimon
