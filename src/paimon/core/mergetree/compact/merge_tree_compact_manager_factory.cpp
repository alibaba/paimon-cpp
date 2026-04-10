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

#include "paimon/core/mergetree/compact/merge_tree_compact_manager_factory.h"

#include <vector>

#include "paimon/common/data/serializer/row_compacted_serializer.h"
#include "paimon/core/compact/noop_compact_manager.h"
#include "paimon/core/mergetree/compact/aggregate/aggregate_merge_function.h"
#include "paimon/core/mergetree/compact/early_full_compaction.h"
#include "paimon/core/mergetree/compact/force_up_level0_compaction.h"
#include "paimon/core/mergetree/compact/lookup_merge_tree_compact_rewriter.h"
#include "paimon/core/mergetree/compact/merge_tree_compact_manager.h"
#include "paimon/core/mergetree/compact/merge_tree_compact_rewriter.h"
#include "paimon/core/mergetree/compact/off_peak_hours.h"
#include "paimon/core/mergetree/compact/universal_compaction.h"
#include "paimon/core/mergetree/lookup/default_lookup_serializer_factory.h"
#include "paimon/core/mergetree/lookup/persist_empty_processor.h"
#include "paimon/core/mergetree/lookup/persist_position_processor.h"
#include "paimon/core/mergetree/lookup/persist_value_and_pos_processor.h"
#include "paimon/core/mergetree/lookup/persist_value_processor.h"
#include "paimon/core/mergetree/lookup/positioned_key_value.h"
#include "paimon/core/mergetree/lookup_levels.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/file_store_path_factory_cache.h"
#include "paimon/core/utils/primary_key_table_utils.h"

namespace arrow {
class Schema;
}

namespace paimon {

namespace {

template <typename T>
Result<std::unique_ptr<LookupLevels<T>>> CreateLookupLevelsInternal(
    const CoreOptions& options, const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<IOManager>& io_manager,
    const std::shared_ptr<CacheManager>& cache_manager,
    const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
    const std::shared_ptr<TableSchema>& table_schema, const BinaryRow& partition, int32_t bucket,
    const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<typename PersistProcessor<T>::Factory>& processor_factory,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
    const std::shared_ptr<RemoteLookupFileManager>& remote_lookup_file_manager,
    const std::shared_ptr<MemoryPool>& pool) {
    if (io_manager == nullptr) {
        return Status::Invalid("Can not use lookup, there is no temp disk directory to use.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> key_schema,
                           table_schema->TrimmedPrimaryKeySchema());
    PAIMON_ASSIGN_OR_RAISE(MemorySlice::SliceComparator lookup_key_comparator,
                           RowCompactedSerializer::CreateSliceComparator(key_schema, pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<LookupStoreFactory> lookup_store_factory,
        LookupStoreFactory::Create(lookup_key_comparator, cache_manager, options));
    auto dv_factory = DeletionVector::CreateFactory(dv_maintainer);
    auto serializer_factory = std::make_shared<DefaultLookupSerializerFactory>();
    return LookupLevels<T>::Create(
        options.GetFileSystem(), partition, bucket, options, schema_manager, io_manager,
        file_store_path_factory, table_schema, levels, dv_factory, processor_factory,
        serializer_factory, lookup_store_factory, remote_lookup_file_manager, pool);
}

}  // namespace

std::shared_ptr<CompactStrategy> MergeTreeCompactManagerFactory::CreateCompactStrategy() const {
    auto universal = std::make_shared<UniversalCompaction>(
        options_.GetCompactionMaxSizeAmplificationPercent(), options_.GetCompactionSizeRatio(),
        options_.GetNumSortedRunsCompactionTrigger(), EarlyFullCompaction::Create(options_),
        OffPeakHours::Create(options_));

    if (options_.NeedLookup()) {
        std::optional<int32_t> compact_max_interval = std::nullopt;
        switch (options_.GetLookupCompactMode()) {
            case LookupCompactMode::GENTLE:
                compact_max_interval = options_.GetLookupCompactMaxInterval();
                break;
            case LookupCompactMode::RADICAL:
                break;
        }
        return std::make_shared<ForceUpLevel0Compaction>(universal, compact_max_interval);
    }

    if (options_.CompactionForceUpLevel0()) {
        return std::make_shared<ForceUpLevel0Compaction>(universal,
                                                         /*max_compact_interval=*/std::nullopt);
    }
    return universal;
}

Result<std::shared_ptr<CompactManager>> MergeTreeCompactManagerFactory::CreateCompactManager(
    const BinaryRow& partition, int32_t bucket,
    const std::shared_ptr<CompactStrategy>& compact_strategy,
    const std::shared_ptr<Executor>& compact_executor, const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer) const {
    if (options_.WriteOnly()) {
        return std::make_shared<NoopCompactManager>();
    }

    auto cancellation_controller = std::make_shared<CancellationController>();
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<CompactRewriter> rewriter,
        CreateRewriter(partition, bucket, levels, dv_maintainer, cancellation_controller));
    auto metrics_reporter = CreateCompactionMetricsReporter(partition, bucket);

    return std::make_shared<MergeTreeCompactManager>(
        levels, compact_strategy, key_comparator_,
        options_.GetCompactionFileSize(/*has_primary_key=*/true),
        options_.GetNumSortedRunsStopTrigger(), rewriter, metrics_reporter, dv_maintainer,
        options_.PrepareCommitWaitCompaction(), options_.NeedLookup(),
        options_.CompactionForceRewriteAllFiles(),
        /*force_keep_delete=*/false, cancellation_controller, compact_executor);
}

std::shared_ptr<CompactionMetrics::Reporter>
MergeTreeCompactManagerFactory::CreateCompactionMetricsReporter(const BinaryRow& partition,
                                                                int32_t bucket) const {
    return compaction_metrics_ ? compaction_metrics_->CreateReporter(partition, bucket) : nullptr;
}

Result<std::shared_ptr<CompactRewriter>> MergeTreeCompactManagerFactory::CreateRewriter(
    const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
    const std::shared_ptr<CancellationController>& cancellation_controller) const {
    auto path_factory_cache =
        std::make_shared<FileStorePathFactoryCache>(root_path_, table_schema_, options_, pool_);
    if (options_.GetChangelogProducer() == ChangelogProducer::FULL_COMPACTION) {
        return Status::NotImplemented("not support full changelog merge tree compact rewriter");
    }
    if (options_.NeedLookup()) {
        int32_t max_level = options_.GetNumLevels() - 1;
        return CreateLookupRewriter(partition, bucket, levels, dv_maintainer, max_level,
                                    options_.GetLookupStrategy(), path_factory_cache,
                                    cancellation_controller);
    }

    auto dv_factory = DeletionVector::CreateFactory(dv_maintainer);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MergeTreeCompactRewriter> rewriter,
                           MergeTreeCompactRewriter::Create(
                               bucket, partition, table_schema_, dv_factory, path_factory_cache,
                               options_, cancellation_controller, pool_));
    return std::shared_ptr<CompactRewriter>(std::move(rewriter));
}

Result<std::shared_ptr<CompactRewriter>> MergeTreeCompactManagerFactory::CreateLookupRewriter(
    const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
    const LookupStrategy& lookup_strategy,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    const std::shared_ptr<CancellationController>& cancellation_controller) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RemoteLookupFileManager> remote_lookup_file_manager,
                           CreateRemoteLookupFileManager(partition, bucket));
    if (lookup_strategy.is_first_row) {
        if (options_.DeletionVectorsEnabled()) {
            return Status::Invalid(
                "First row merge engine does not need deletion vectors because there is no "
                "deletion of old data in this merge engine.");
        }

        auto processor_factory = std::make_shared<PersistEmptyProcessor::Factory>();
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<LookupLevels<bool>> lookup_levels,
            CreateLookupLevelsInternal<bool>(options_, schema_manager_, io_manager_, cache_manager_,
                                             file_store_path_factory_, table_schema_, partition,
                                             bucket, levels, processor_factory, dv_maintainer,
                                             remote_lookup_file_manager, pool_));
        auto merge_function_wrapper_factory =
            [lookup_levels_ptr = lookup_levels.get(), ignore_delete = options_.IgnoreDelete()](
                int32_t output_level) -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
            std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper =
                LookupMergeTreeCompactRewriter<bool>::CreateFirstRowMergeFunctionWrapper(
                    std::make_unique<FirstRowMergeFunction>(ignore_delete), output_level,
                    lookup_levels_ptr);
            return merge_function_wrapper;
        };
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LookupMergeTreeCompactRewriter<bool>> rewriter,
                               LookupMergeTreeCompactRewriter<bool>::Create(
                                   max_level, std::move(lookup_levels), dv_maintainer,
                                   std::move(merge_function_wrapper_factory), bucket, partition,
                                   table_schema_, path_factory_cache, options_,
                                   cancellation_controller, remote_lookup_file_manager, pool_));
        return std::shared_ptr<CompactRewriter>(std::move(rewriter));
    }

    if (lookup_strategy.deletion_vector) {
        return CreateLookupRewriterWithDeletionVector(
            partition, bucket, levels, dv_maintainer, max_level, lookup_strategy,
            path_factory_cache, cancellation_controller, remote_lookup_file_manager);
    }

    return CreateLookupRewriterWithoutDeletionVector(
        partition, bucket, levels, dv_maintainer, max_level, lookup_strategy, path_factory_cache,
        cancellation_controller, remote_lookup_file_manager);
}

Result<std::shared_ptr<CompactRewriter>>
MergeTreeCompactManagerFactory::CreateLookupRewriterWithDeletionVector(
    const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
    const LookupStrategy& lookup_strategy,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    const std::shared_ptr<CancellationController>& cancellation_controller,
    const std::shared_ptr<RemoteLookupFileManager>& remote_lookup_file_manager) const {
    auto merge_engine = options_.GetMergeEngine();
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema_->TrimmedPrimaryKeys());
    if (lookup_strategy.produce_changelog || merge_engine != MergeEngine::DEDUPLICATE ||
        !options_.GetSequenceField().empty()) {
        auto processor_factory = std::make_shared<PersistValueAndPosProcessor::Factory>(schema_);
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<LookupLevels<PositionedKeyValue>> lookup_levels,
            CreateLookupLevelsInternal<PositionedKeyValue>(
                options_, schema_manager_, io_manager_, cache_manager_, file_store_path_factory_,
                table_schema_, partition, bucket, levels, processor_factory, dv_maintainer,
                remote_lookup_file_manager, pool_));
        auto merge_function_wrapper_factory =
            [data_schema = schema_, options = options_, trimmed_primary_keys,
             lookup_levels_ptr = lookup_levels.get(), lookup_strategy,
             dv_maintainer_ptr = dv_maintainer,
             user_defined_seq_comparator = user_defined_seq_comparator_](
                int32_t output_level) -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MergeFunction> merge_func,
                                   PrimaryKeyTableUtils::CreateMergeFunction(
                                       data_schema, trimmed_primary_keys, options));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper,
                LookupMergeTreeCompactRewriter<PositionedKeyValue>::
                    CreateLookupMergeFunctionWrapper(
                        std::make_unique<LookupMergeFunction>(std::move(merge_func)), output_level,
                        dv_maintainer_ptr, lookup_strategy, user_defined_seq_comparator,
                        lookup_levels_ptr));
            return merge_function_wrapper;
        };
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<LookupMergeTreeCompactRewriter<PositionedKeyValue>> rewriter,
            LookupMergeTreeCompactRewriter<PositionedKeyValue>::Create(
                max_level, std::move(lookup_levels), dv_maintainer,
                std::move(merge_function_wrapper_factory), bucket, partition, table_schema_,
                path_factory_cache, options_, cancellation_controller, remote_lookup_file_manager,
                pool_));
        return std::shared_ptr<CompactRewriter>(std::move(rewriter));
    }
    auto processor_factory = std::make_shared<PersistPositionProcessor::Factory>();
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<LookupLevels<FilePosition>> lookup_levels,
        CreateLookupLevelsInternal<FilePosition>(
            options_, schema_manager_, io_manager_, cache_manager_, file_store_path_factory_,
            table_schema_, partition, bucket, levels, processor_factory, dv_maintainer,
            remote_lookup_file_manager, pool_));
    auto merge_function_wrapper_factory =
        [data_schema = schema_, options = options_, trimmed_primary_keys,
         lookup_levels_ptr = lookup_levels.get(), lookup_strategy,
         dv_maintainer_ptr = dv_maintainer,
         user_defined_seq_comparator = user_defined_seq_comparator_](
            int32_t output_level) -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<MergeFunction> merge_func,
            PrimaryKeyTableUtils::CreateMergeFunction(data_schema, trimmed_primary_keys, options));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper,
            LookupMergeTreeCompactRewriter<FilePosition>::CreateLookupMergeFunctionWrapper(
                std::make_unique<LookupMergeFunction>(std::move(merge_func)), output_level,
                dv_maintainer_ptr, lookup_strategy, user_defined_seq_comparator,
                lookup_levels_ptr));
        return merge_function_wrapper;
    };
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LookupMergeTreeCompactRewriter<FilePosition>> rewriter,
                           LookupMergeTreeCompactRewriter<FilePosition>::Create(
                               max_level, std::move(lookup_levels), dv_maintainer,
                               std::move(merge_function_wrapper_factory), bucket, partition,
                               table_schema_, path_factory_cache, options_, cancellation_controller,
                               remote_lookup_file_manager, pool_));
    return std::shared_ptr<CompactRewriter>(std::move(rewriter));
}

Result<std::shared_ptr<CompactRewriter>>
MergeTreeCompactManagerFactory::CreateLookupRewriterWithoutDeletionVector(
    const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
    const LookupStrategy& lookup_strategy,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    const std::shared_ptr<CancellationController>& cancellation_controller,
    const std::shared_ptr<RemoteLookupFileManager>& remote_lookup_file_manager) const {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema_->TrimmedPrimaryKeys());
    auto processor_factory = std::make_shared<PersistValueProcessor::Factory>(schema_);
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<LookupLevels<KeyValue>> lookup_levels,
        CreateLookupLevelsInternal<KeyValue>(options_, schema_manager_, io_manager_, cache_manager_,
                                             file_store_path_factory_, table_schema_, partition,
                                             bucket, levels, processor_factory, dv_maintainer,
                                             remote_lookup_file_manager, pool_));
    auto merge_function_wrapper_factory =
        [data_schema = schema_, options = options_, trimmed_primary_keys,
         lookup_levels_ptr = lookup_levels.get(), lookup_strategy,
         dv_maintainer_ptr = dv_maintainer,
         user_defined_seq_comparator = user_defined_seq_comparator_](
            int32_t output_level) -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<MergeFunction> merge_func,
            PrimaryKeyTableUtils::CreateMergeFunction(data_schema, trimmed_primary_keys, options));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper,
            LookupMergeTreeCompactRewriter<KeyValue>::CreateLookupMergeFunctionWrapper(
                std::make_unique<LookupMergeFunction>(std::move(merge_func)), output_level,
                dv_maintainer_ptr, lookup_strategy, user_defined_seq_comparator,
                lookup_levels_ptr));
        return merge_function_wrapper;
    };

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LookupMergeTreeCompactRewriter<KeyValue>> rewriter,
                           LookupMergeTreeCompactRewriter<KeyValue>::Create(
                               max_level, std::move(lookup_levels), dv_maintainer,
                               std::move(merge_function_wrapper_factory), bucket, partition,
                               table_schema_, path_factory_cache, options_, cancellation_controller,
                               remote_lookup_file_manager, pool_));
    return std::shared_ptr<CompactRewriter>(std::move(rewriter));
}

Result<std::shared_ptr<RemoteLookupFileManager>>
MergeTreeCompactManagerFactory::CreateRemoteLookupFileManager(const BinaryRow& partition,
                                                              int32_t bucket) const {
    if (options_.LookupRemoteFileEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFilePathFactory> data_path_factory,
            file_store_path_factory_->CreateDataFilePathFactory(partition, bucket));
        return std::make_shared<RemoteLookupFileManager>(options_.GetLookupRemoteLevelThreshold(),
                                                         data_path_factory,
                                                         options_.GetFileSystem(), pool_);
    }
    return std::shared_ptr<RemoteLookupFileManager>();
}

}  // namespace paimon
