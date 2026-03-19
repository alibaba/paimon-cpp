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
#include "arrow/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/changelog_merge_tree_rewriter.h"
#include "paimon/core/mergetree/compact/first_row_merge_function.h"
#include "paimon/core/mergetree/compact/first_row_merge_function_wrapper.h"
#include "paimon/core/mergetree/compact/lookup_changelog_merge_function_wrapper.h"
#include "paimon/core/mergetree/compact/lookup_merge_function.h"
#include "paimon/core/mergetree/lookup_levels.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/operation/merge_file_split_read.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
namespace paimon {
/// A `MergeTreeCompactRewriter` which produces changelog files by lookup for the compaction
/// involving level 0 files.
template <typename T>
class LookupMergeTreeCompactRewriter : public ChangelogMergeTreeRewriter {
 public:
    static Result<std::unique_ptr<LookupMergeTreeCompactRewriter>> Create(
        int32_t max_level, std::unique_ptr<LookupLevels<T>>&& lookup_levels,
        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
        MergeFunctionWrapperFactory merge_function_wrapper_factory, int32_t bucket,
        const BinaryRow& partition, const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
        const CoreOptions& options, const std::shared_ptr<MemoryPool>& pool) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                               table_schema->TrimmedPrimaryKeys());
        auto data_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        auto write_schema = SpecialFields::CompleteSequenceAndValueKindField(data_schema);

        // TODO(xinyu.lxy): set executor
        ReadContextBuilder read_context_builder(path_factory_cache->RootPath());
        read_context_builder.SetOptions(options.ToMap()).EnablePrefetch(true).WithMemoryPool(pool);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context,
                               read_context_builder.Finish());

        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<InternalReadContext> internal_context,
            InternalReadContext::Create(read_context, table_schema, options.ToMap()));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                               path_factory_cache->GetOrCreatePathFactory(
                                   options.GetWriteFileFormat(/*level=*/0)->Identifier()));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MergeFileSplitRead> merge_file_split_read,
                               MergeFileSplitRead::Create(path_factory, internal_context, pool,
                                                          CreateDefaultExecutor()));
        return std::unique_ptr<LookupMergeTreeCompactRewriter>(new LookupMergeTreeCompactRewriter(
            std::move(lookup_levels), dv_maintainer, max_level, partition, bucket,
            table_schema->Id(), trimmed_primary_keys, options, data_schema, write_schema,
            path_factory_cache, std::move(merge_file_split_read),
            std::move(merge_function_wrapper_factory), pool));
    }

    Status Close() override {
        return lookup_levels_->Close();
    }

    static std::shared_ptr<MergeFunctionWrapper<KeyValue>> CreateFirstRowMergeFunctionWrapper(
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

    static Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> CreateLookupMergeFunctionWrapper(
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

 private:
    LookupMergeTreeCompactRewriter(
        std::unique_ptr<LookupLevels<T>>&& lookup_levels,
        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t max_level,
        const BinaryRow& partition, int32_t bucket, int64_t schema_id,
        const std::vector<std::string>& trimmed_primary_keys, const CoreOptions& options,
        const std::shared_ptr<arrow::Schema>& data_schema,
        const std::shared_ptr<arrow::Schema>& write_schema,
        const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
        std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
        MergeFunctionWrapperFactory merge_function_wrapper_factory,
        const std::shared_ptr<MemoryPool>& pool)
        : ChangelogMergeTreeRewriter(max_level, /*force_drop_delete=*/dv_maintainer_ != nullptr,
                                     partition, bucket, schema_id, trimmed_primary_keys, options,
                                     data_schema, write_schema, path_factory_cache,
                                     std::move(merge_file_split_read),
                                     std::move(merge_function_wrapper_factory), pool),
          lookup_levels_(std::move(lookup_levels)),
          dv_maintainer_(dv_maintainer) {}

    bool RewriteChangelog(int32_t output_level, bool drop_delete,
                          const std::vector<std::vector<SortedRun>>& sections) const override {
        return RewriteLookupChangelog(output_level, sections);
    }

    UpgradeStrategy GenerateUpgradeStrategy(
        int32_t output_level, const std::shared_ptr<DataFileMeta>& file) const override {
        if (file->level != 0) {
            return UpgradeStrategy::NoChangelogNoRewrite();
        }
        // forcing rewriting when upgrading from level 0 to level x with different file formats
        if (options_.GetWriteFileFormat(file->level)->Identifier() !=
            options_.GetWriteFileFormat(output_level)->Identifier()) {
            return UpgradeStrategy::ChangelogWithRewrite();
        }

        // In deletionVector mode, since drop delete is required, when delete row count > 0 rewrite
        // is required.
        if (dv_maintainer_ && (!file->delete_row_count || file->delete_row_count.value() > 0)) {
            return UpgradeStrategy::ChangelogWithRewrite();
        }

        if (output_level == max_level_) {
            return UpgradeStrategy::ChangelogNoRewrite();
        }

        // DEDUPLICATE retains the latest records as the final result, so merging has no impact on
        // it at all.
        if (options_.GetMergeEngine() == MergeEngine::DEDUPLICATE &&
            options_.GetSequenceField().empty()) {
            return UpgradeStrategy::ChangelogNoRewrite();
        }
        // other merge engines must rewrite file, because some records that are already at higher
        // level may be merged
        // See LookupMergeFunction, it just returns newly records.
        return UpgradeStrategy::ChangelogWithRewrite();
    }

    void NotifyRewriteCompactBefore(
        const std::vector<std::shared_ptr<DataFileMeta>>& files) override {
        if (dv_maintainer_) {
            for (const auto& file : files) {
                dv_maintainer_->RemoveDeletionVectorOf(file->file_name);
            }
        }
    }

    std::vector<std::shared_ptr<DataFileMeta>> NotifyRewriteCompactAfter(
        const std::vector<std::shared_ptr<DataFileMeta>>& files) override {
        // TODO(xinyu.lxy): support remoteLookupFileManager
        return files;
    }

 private:
    std::unique_ptr<LookupLevels<T>> lookup_levels_;
    std::shared_ptr<BucketedDvMaintainer> dv_maintainer_;
};
}  // namespace paimon
