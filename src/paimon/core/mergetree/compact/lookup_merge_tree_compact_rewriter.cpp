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

#include "paimon/core/mergetree/compact/lookup_merge_tree_compact_rewriter.h"

#include "paimon/common/table/special_fields.h"
#include "paimon/core/mergetree/compact/first_row_merge_function_wrapper.h"
#include "paimon/core/mergetree/compact/lookup_changelog_merge_function_wrapper.h"
#include "paimon/core/mergetree/lookup/file_position.h"
#include "paimon/core/mergetree/lookup/positioned_key_value.h"

namespace paimon {
template <typename T>
LookupMergeTreeCompactRewriter<T>::LookupMergeTreeCompactRewriter(
    std::unique_ptr<LookupLevels<T>>&& lookup_levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
    const BinaryRow& partition, int32_t bucket, int64_t schema_id,
    const std::vector<std::string>& trimmed_primary_keys, const CoreOptions& options,
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
    MergeFunctionWrapperFactory merge_function_wrapper_factory,
    const std::shared_ptr<CancellationController>& cancellation_controller,
    const std::shared_ptr<RemoteLookupFileManager>& remote_lookup_file_manager,
    const std::shared_ptr<MemoryPool>& pool)
    : ChangelogMergeTreeRewriter(
          max_level, /*force_drop_delete=*/dv_maintainer != nullptr, partition, bucket, schema_id,
          trimmed_primary_keys, options, data_schema, write_schema,
          DeletionVector::CreateFactory(dv_maintainer), path_factory_cache,
          std::move(merge_file_split_read), std::move(merge_function_wrapper_factory),
          cancellation_controller, pool),
      lookup_levels_(std::move(lookup_levels)),
      dv_maintainer_(dv_maintainer),
      remote_lookup_file_manager_(remote_lookup_file_manager) {}

template <typename T>
Result<std::unique_ptr<LookupMergeTreeCompactRewriter<T>>>
LookupMergeTreeCompactRewriter<T>::Create(
    int32_t max_level, std::unique_ptr<LookupLevels<T>>&& lookup_levels,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
    MergeFunctionWrapperFactory merge_function_wrapper_factory, int32_t bucket,
    const BinaryRow& partition, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    const CoreOptions& options,
    const std::shared_ptr<CancellationController>& cancellation_controller,
    const std::shared_ptr<RemoteLookupFileManager>& remote_lookup_file_manager,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema->TrimmedPrimaryKeys());
    auto data_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    auto write_schema = SpecialFields::CompleteSequenceAndValueKindField(data_schema);

    // TODO(xinyu.lxy): set executor
    ReadContextBuilder read_context_builder(path_factory_cache->RootPath());
    read_context_builder.SetOptions(options.ToMap())
        .WithFileSystem(options.GetFileSystem())
        .EnablePrefetch(true)
        .SetPrefetchMaxParallelNum(1)
        .SetPrefetchBatchCount(3)
        .WithMemoryPool(pool);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context,
                           read_context_builder.Finish());
    // TODO(xinyu.lxy): temporarily disabled pre-buffer for parquet, which may cause high memory
    // usage during compaction. Will fix via parquet format refactor.
    auto new_options = options.ToMap();
    if (new_options.find("parquet.read.enable-pre-buffer") == new_options.end()) {
        new_options["parquet.read.enable-pre-buffer"] = "false";
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> internal_context,
                           InternalReadContext::Create(read_context, table_schema, new_options));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        path_factory_cache->GetOrCreatePathFactory(options.GetFileFormat()->Identifier()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<MergeFileSplitRead> merge_file_split_read,
        MergeFileSplitRead::Create(path_factory, internal_context, pool, CreateDefaultExecutor()));
    return std::unique_ptr<LookupMergeTreeCompactRewriter>(new LookupMergeTreeCompactRewriter(
        std::move(lookup_levels), dv_maintainer, max_level, partition, bucket, table_schema->Id(),
        trimmed_primary_keys, options, data_schema, write_schema, path_factory_cache,
        std::move(merge_file_split_read), std::move(merge_function_wrapper_factory),
        cancellation_controller, remote_lookup_file_manager, pool));
}

template <typename T>
std::shared_ptr<MergeFunctionWrapper<KeyValue>>
LookupMergeTreeCompactRewriter<T>::CreateFirstRowMergeFunctionWrapper(
    std::unique_ptr<FirstRowMergeFunction>&& merge_func, int32_t output_level,
    LookupLevels<bool>* lookup_levels) {
    auto contains = [output_level,
                     lookup_levels](const std::shared_ptr<InternalRow>& key) -> Result<bool> {
        PAIMON_ASSIGN_OR_RAISE(std::optional<bool> contain,
                               lookup_levels->Lookup(key, output_level + 1));
        return contain != std::nullopt;
    };
    return std::make_shared<FirstRowMergeFunctionWrapper>(std::move(merge_func),
                                                          std::move(contains));
}

template <typename T>
Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>>
LookupMergeTreeCompactRewriter<T>::CreateLookupMergeFunctionWrapper(
    std::unique_ptr<LookupMergeFunction>&& merge_func, int32_t output_level,
    const std::shared_ptr<BucketedDvMaintainer>& deletion_vectors_maintainer,
    const LookupStrategy& lookup_strategy,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    LookupLevels<T>* lookup_levels) {
    auto lookup = [output_level, lookup_levels](
                      const std::shared_ptr<InternalRow>& key) -> Result<std::optional<T>> {
        return lookup_levels->Lookup(key, output_level + 1);
    };
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MergeFunctionWrapper<KeyValue>> wrapper,
                           LookupChangelogMergeFunctionWrapper<T>::Create(
                               std::move(merge_func), std::move(lookup), lookup_strategy,
                               deletion_vectors_maintainer, user_defined_seq_comparator));
    return wrapper;
}

template <typename T>
ChangelogMergeTreeRewriter::UpgradeStrategy
LookupMergeTreeCompactRewriter<T>::GenerateUpgradeStrategy(
    int32_t output_level, const std::shared_ptr<DataFileMeta>& file) const {
    if (file->level != 0) {
        return ChangelogMergeTreeRewriter::UpgradeStrategy::NoChangelogNoRewrite();
    }
    // forcing rewriting when upgrading from level 0 to level x with different file formats
    if (options_.GetWriteFileFormat(file->level)->Identifier() !=
        options_.GetWriteFileFormat(output_level)->Identifier()) {
        return ChangelogMergeTreeRewriter::UpgradeStrategy::ChangelogWithRewrite();
    }

    // In deletionVector mode, since drop delete is required, when delete row count > 0 rewrite
    // is required.
    if (dv_maintainer_ && (!file->delete_row_count || file->delete_row_count.value() > 0)) {
        return ChangelogMergeTreeRewriter::UpgradeStrategy::ChangelogWithRewrite();
    }

    if (output_level == max_level_) {
        return ChangelogMergeTreeRewriter::UpgradeStrategy::ChangelogNoRewrite();
    }

    // DEDUPLICATE retains the latest records as the final result, so merging has no impact on
    // it at all.
    if (options_.GetMergeEngine() == MergeEngine::DEDUPLICATE &&
        options_.GetSequenceField().empty()) {
        return ChangelogMergeTreeRewriter::UpgradeStrategy::ChangelogNoRewrite();
    }
    // other merge engines must rewrite file, because some records that are already at higher
    // level may be merged
    // See LookupMergeFunction, it just returns newly records.
    return ChangelogMergeTreeRewriter::UpgradeStrategy::ChangelogWithRewrite();
}

template <typename T>
void LookupMergeTreeCompactRewriter<T>::NotifyRewriteCompactBefore(
    const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    if (dv_maintainer_) {
        for (const auto& file : files) {
            dv_maintainer_->RemoveDeletionVectorOf(file->file_name);
        }
    }
}

template <typename T>
Result<std::vector<std::shared_ptr<DataFileMeta>>>
LookupMergeTreeCompactRewriter<T>::NotifyRewriteCompactAfter(
    const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    if (!remote_lookup_file_manager_) {
        return files;
    }

    std::vector<std::shared_ptr<DataFileMeta>> result;
    result.reserve(files.size());
    for (const auto& file : files) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFileMeta> new_meta,
            remote_lookup_file_manager_->GenRemoteLookupFile(file, lookup_levels_.get()));
        result.push_back(std::move(new_meta));
    }
    return result;
}

template class LookupMergeTreeCompactRewriter<KeyValue>;
template class LookupMergeTreeCompactRewriter<FilePosition>;
template class LookupMergeTreeCompactRewriter<PositionedKeyValue>;
template class LookupMergeTreeCompactRewriter<bool>;
}  // namespace paimon
