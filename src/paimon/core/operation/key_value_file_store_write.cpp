/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/core/operation/key_value_file_store_write.h"

#include <limits>
#include <map>
#include <optional>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/compact/noop_compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/mergetree/compact/aggregate/aggregate_merge_function.h"
#include "paimon/core/mergetree/compact/force_up_level0_compaction.h"
#include "paimon/core/mergetree/compact/lookup_merge_tree_compact_rewriter.h"
#include "paimon/core/mergetree/compact/merge_tree_compact_manager.h"
#include "paimon/core/mergetree/compact/merge_tree_compact_rewriter.h"
#include "paimon/core/mergetree/compact/universal_compaction.h"
#include "paimon/core/mergetree/levels.h"
#include "paimon/core/mergetree/lookup/persist_empty_processor.h"
#include "paimon/core/mergetree/lookup/persist_position_processor.h"
#include "paimon/core/mergetree/lookup/persist_value_and_pos_processor.h"
#include "paimon/core/mergetree/lookup/persist_value_processor.h"
#include "paimon/core/mergetree/lookup/positioned_key_value.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/key_value_file_store_scan.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/primary_key_table_utils.h"
#include "paimon/core/utils/snapshot_manager.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class DataFilePathFactory;
class Executor;
class MemoryPool;
class SchemaManager;
struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

KeyValueFileStoreWrite::KeyValueFileStoreWrite(
    const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager, const std::string& commit_user,
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::Schema>& partition_schema,
    const std::shared_ptr<BucketedDvMaintainer::Factory>& dv_maintainer_factory,
    const std::shared_ptr<IOManager>& io_manager,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
    const CoreOptions& options, bool ignore_previous_files, bool is_streaming_mode,
    bool ignore_num_bucket_check, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool)
    : AbstractFileStoreWrite(file_store_path_factory, snapshot_manager, schema_manager, commit_user,
                             root_path, table_schema, schema, /*write_schema=*/schema,
                             partition_schema, dv_maintainer_factory, io_manager, options,
                             ignore_previous_files, is_streaming_mode, ignore_num_bucket_check,
                             executor, pool),
      key_comparator_(key_comparator),
      user_defined_seq_comparator_(user_defined_seq_comparator),
      merge_function_wrapper_(merge_function_wrapper),
      logger_(Logger::GetLogger("KeyValueFileStoreWrite")) {}

Result<std::unique_ptr<FileStoreScan>> KeyValueFileStoreWrite::CreateFileStoreScan(
    const std::shared_ptr<ScanFilter>& scan_filter) const {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                             options_.GetManifestCompression(), file_store_path_factory_, pool_));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                             options_.GetManifestCompression(), file_store_path_factory_,
                             options_.GetManifestTargetFileSize(), pool_, options_,
                             partition_schema_));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan,
                           KeyValueFileStoreScan::Create(
                               snapshot_manager_, schema_manager_, manifest_list, manifest_file,
                               table_schema_, schema_, scan_filter, options_, executor_, pool_));
    return scan;
}

Result<std::shared_ptr<BatchWriter>> KeyValueFileStoreWrite::CreateWriter(
    const BinaryRow& partition, int32_t bucket,
    const std::vector<std::shared_ptr<DataFileMeta>>& restore_data_files,
    int64_t restore_max_seq_number, const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer) {
    PAIMON_LOG_DEBUG(logger_, "Creating key value writer for partition %s, bucket %d",
                     partition.ToString().c_str(), bucket);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           file_store_path_factory_->CreateDataFilePathFactory(partition, bucket));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema_->TrimmedPrimaryKeys());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<Levels> levels,
        Levels::Create(key_comparator_, restore_data_files, options_.GetNumLevels()));
    auto compact_strategy = CreateCompactStrategy(options_);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CompactManager> compact_manager,
                           CreateCompactManager(partition, bucket, compact_strategy,
                                                compact_executor_, levels, dv_maintainer));

    auto writer = std::make_shared<MergeTreeWriter>(
        restore_max_seq_number, trimmed_primary_keys, data_file_path_factory, key_comparator_,
        user_defined_seq_comparator_, merge_function_wrapper_, table_schema_->Id(), schema_,
        options_, compact_manager, pool_);
    return std::shared_ptr<BatchWriter>(writer);
}

std::shared_ptr<CompactStrategy> KeyValueFileStoreWrite::CreateCompactStrategy(
    const CoreOptions& options) const {
    auto universal = std::make_shared<UniversalCompaction>(
        options.GetCompactionMaxSizeAmplificationPercent(), options.GetCompactionSizeRatio(),
        options.GetNumSortedRunsCompactionTrigger(), EarlyFullCompaction::Create(options),
        OffPeakHours::Create(options));

    if (options.NeedLookup()) {
        std::optional<int32_t> compact_max_interval = std::nullopt;
        switch (options.GetLookupCompactMode()) {
            case LookupCompactMode::GENTLE:
                compact_max_interval = options.GetLookupCompactMaxInterval();
                break;
            case LookupCompactMode::RADICAL:
                break;
        }
        return std::make_shared<ForceUpLevel0Compaction>(universal, compact_max_interval);
    }

    if (options.CompactionForceUpLevel0()) {
        return std::make_shared<ForceUpLevel0Compaction>(universal,
                                                         /*max_compact_interval=*/std::nullopt);
    }
    return universal;
}

Result<std::shared_ptr<CompactManager>> KeyValueFileStoreWrite::CreateCompactManager(
    const BinaryRow& partition, int32_t bucket,
    const std::shared_ptr<CompactStrategy>& compact_strategy,
    const std::shared_ptr<Executor>& compact_executor, const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer) {
    if (options_.WriteOnly()) {
        return std::make_shared<NoopCompactManager>();
    }

    auto cancellation_controller = std::make_shared<CancellationController>();
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<CompactRewriter> rewriter,
        CreateRewriter(partition, bucket, levels, dv_maintainer, cancellation_controller));
    auto reporter =
        compaction_metrics_ ? compaction_metrics_->CreateReporter(partition, bucket) : nullptr;

    return std::make_shared<MergeTreeCompactManager>(
        levels, compact_strategy, key_comparator_,
        options_.GetCompactionFileSize(/*has_primary_key=*/true),
        options_.GetNumSortedRunsStopTrigger(), rewriter, reporter, dv_maintainer,
        /*lazy_gen_deletion_file=*/false, options_.GetLookupStrategy().need_lookup,
        options_.CompactionForceRewriteAllFiles(),
        /*force_keep_delete=*/false, cancellation_controller, compact_executor);
}

Result<std::shared_ptr<CompactRewriter>> KeyValueFileStoreWrite::CreateRewriter(
    const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
    const std::shared_ptr<CancellationController>& cancellation_controller) {
    auto path_factory_cache =
        std::make_shared<FileStorePathFactoryCache>(root_path_, table_schema_, options_, pool_);
    if (options_.GetChangelogProducer() == ChangelogProducer::FULL_COMPACTION) {
        return Status::NotImplemented("not support full changelog merge tree compact rewriter");
    } else if (options_.GetLookupStrategy().need_lookup) {
        int32_t max_level = options_.GetNumLevels() - 1;
        return CreateLookupRewriter(partition, bucket, levels, dv_maintainer, max_level,
                                    options_.GetLookupStrategy(), path_factory_cache,
                                    cancellation_controller);
    } else {
        auto dv_factory = DeletionVector::CreateFactory(dv_maintainer);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MergeTreeCompactRewriter> rewriter,
                               MergeTreeCompactRewriter::Create(
                                   bucket, partition, table_schema_, dv_factory, path_factory_cache,
                                   options_, pool_, cancellation_controller));
        return std::shared_ptr<CompactRewriter>(std::move(rewriter));
    }
}

Result<std::shared_ptr<CompactRewriter>> KeyValueFileStoreWrite::CreateLookupRewriter(
    const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
    const LookupStrategy& lookup_strategy,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    const std::shared_ptr<CancellationController>& cancellation_controller) {
    if (lookup_strategy.is_first_row) {
        if (options_.DeletionVectorsEnabled()) {
            return Status::Invalid(
                "First row merge engine does not need deletion vectors because there is no "
                "deletion of old data in this merge engine.");
        }
        auto processor_factory = std::make_shared<PersistEmptyProcessor::Factory>();
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<LookupLevels<bool>> lookup_levels,
            CreateLookupLevels<bool>(partition, bucket, levels, processor_factory, dv_maintainer));
        auto merge_function_wrapper_factory =
            [lookup_levels_ptr = lookup_levels.get(), ignore_delete = options_.IgnoreDelete()](
                int32_t output_level) -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
            std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper =
                LookupMergeTreeCompactRewriter<bool>::CreateFirstRowMergeFunctionWrapper(
                    std::make_unique<FirstRowMergeFunction>(ignore_delete), output_level,
                    lookup_levels_ptr);
            return merge_function_wrapper;
        };
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<LookupMergeTreeCompactRewriter<bool>> rewriter,
            LookupMergeTreeCompactRewriter<bool>::Create(
                max_level, std::move(lookup_levels), dv_maintainer,
                std::move(merge_function_wrapper_factory), bucket, partition, table_schema_,
                path_factory_cache, options_, pool_, cancellation_controller));
        return std::shared_ptr<CompactRewriter>(std::move(rewriter));
    } else if (lookup_strategy.deletion_vector) {
        return CreateLookupRewriterWithDeletionVector(partition, bucket, levels, dv_maintainer,
                                                      max_level, lookup_strategy,
                                                      path_factory_cache, cancellation_controller);
    } else {
        return CreateLookupRewriterWithoutDeletionVector(
            partition, bucket, levels, dv_maintainer, max_level, lookup_strategy,
            path_factory_cache, cancellation_controller);
    }
}

Result<std::shared_ptr<CompactRewriter>>
KeyValueFileStoreWrite::CreateLookupRewriterWithDeletionVector(
    const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
    const LookupStrategy& lookup_strategy,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    const std::shared_ptr<CancellationController>& cancellation_controller) {
    auto merge_engine = options_.GetMergeEngine();
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema_->TrimmedPrimaryKeys());
    if (lookup_strategy.produce_changelog || merge_engine != MergeEngine::DEDUPLICATE ||
        !options_.GetSequenceField().empty()) {
        auto processor_factory = std::make_shared<PersistValueAndPosProcessor::Factory>(schema_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LookupLevels<PositionedKeyValue>> lookup_levels,
                               CreateLookupLevels<PositionedKeyValue>(
                                   partition, bucket, levels, processor_factory, dv_maintainer));
        auto merge_function_wrapper_factory =
            [data_schema = schema_, table_schema = table_schema_, options = options_,
             trimmed_primary_keys, lookup_levels_ptr = lookup_levels.get(), lookup_strategy,
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
                path_factory_cache, options_, pool_, cancellation_controller));
        return std::shared_ptr<CompactRewriter>(std::move(rewriter));
    }

    auto processor_factory = std::make_shared<PersistPositionProcessor::Factory>();
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LookupLevels<FilePosition>> lookup_levels,
                           CreateLookupLevels<FilePosition>(partition, bucket, levels,
                                                            processor_factory, dv_maintainer));
    auto merge_function_wrapper_factory =
        [data_schema = schema_, table_schema = table_schema_, options = options_,
         trimmed_primary_keys, lookup_levels_ptr = lookup_levels.get(), lookup_strategy,
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

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<LookupMergeTreeCompactRewriter<FilePosition>> rewriter,
        LookupMergeTreeCompactRewriter<FilePosition>::Create(
            max_level, std::move(lookup_levels), dv_maintainer,
            std::move(merge_function_wrapper_factory), bucket, partition, table_schema_,
            path_factory_cache, options_, pool_, cancellation_controller));
    return std::shared_ptr<CompactRewriter>(std::move(rewriter));
}

Result<std::shared_ptr<CompactRewriter>>
KeyValueFileStoreWrite::CreateLookupRewriterWithoutDeletionVector(
    const BinaryRow& partition, int32_t bucket, const std::shared_ptr<Levels>& levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
    const LookupStrategy& lookup_strategy,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    const std::shared_ptr<CancellationController>& cancellation_controller) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema_->TrimmedPrimaryKeys());
    auto processor_factory = std::make_shared<PersistValueProcessor::Factory>(schema_);
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<LookupLevels<KeyValue>> lookup_levels,
        CreateLookupLevels<KeyValue>(partition, bucket, levels, processor_factory, dv_maintainer));
    auto merge_function_wrapper_factory =
        [data_schema = schema_, table_schema = table_schema_, options = options_,
         trimmed_primary_keys, lookup_levels_ptr = lookup_levels.get(), lookup_strategy,
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
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<LookupMergeTreeCompactRewriter<KeyValue>> rewriter,
        LookupMergeTreeCompactRewriter<KeyValue>::Create(
            max_level, std::move(lookup_levels), dv_maintainer,
            std::move(merge_function_wrapper_factory), bucket, partition, table_schema_,
            path_factory_cache, options_, pool_, cancellation_controller));
    return std::shared_ptr<CompactRewriter>(std::move(rewriter));
}

}  // namespace paimon
