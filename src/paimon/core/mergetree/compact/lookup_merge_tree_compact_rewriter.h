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
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/mergetree/compact/changelog_merge_tree_rewriter.h"
#include "paimon/core/mergetree/compact/first_row_merge_function.h"
#include "paimon/core/mergetree/compact/lookup_merge_function.h"
#include "paimon/core/mergetree/lookup/remote_lookup_file_manager.h"
#include "paimon/core/mergetree/lookup_levels.h"
#include "paimon/core/schema/table_schema.h"
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
        const CoreOptions& options,
        const std::shared_ptr<CancellationController>& cancellation_controller,
        const std::shared_ptr<RemoteLookupFileManager>& remote_lookup_file_manager,
        const std::shared_ptr<MemoryPool>& pool);

    Status Close() override {
        return lookup_levels_->Close();
    }

    static std::shared_ptr<MergeFunctionWrapper<KeyValue>> CreateFirstRowMergeFunctionWrapper(
        std::unique_ptr<FirstRowMergeFunction>&& merge_func, int32_t output_level,
        LookupLevels<bool>* lookup_levels);

    static Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> CreateLookupMergeFunctionWrapper(
        std::unique_ptr<LookupMergeFunction>&& merge_func, int32_t output_level,
        const std::shared_ptr<BucketedDvMaintainer>& deletion_vectors_maintainer,
        const LookupStrategy& lookup_strategy,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        LookupLevels<T>* lookup_levels);

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
        const std::shared_ptr<CancellationController>& cancellation_controller,
        const std::shared_ptr<RemoteLookupFileManager>& remote_lookup_file_manager,
        const std::shared_ptr<MemoryPool>& pool);

    bool RewriteChangelog(int32_t output_level, bool drop_delete,
                          const std::vector<std::vector<SortedRun>>& sections) const override {
        return RewriteLookupChangelog(output_level, sections);
    }

    UpgradeStrategy GenerateUpgradeStrategy(
        int32_t output_level, const std::shared_ptr<DataFileMeta>& file) const override;

    void NotifyRewriteCompactBefore(
        const std::vector<std::shared_ptr<DataFileMeta>>& files) override;

    Result<std::vector<std::shared_ptr<DataFileMeta>>> NotifyRewriteCompactAfter(
        const std::vector<std::shared_ptr<DataFileMeta>>& files) override;

 private:
    std::unique_ptr<LookupLevels<T>> lookup_levels_;
    std::shared_ptr<BucketedDvMaintainer> dv_maintainer_;
    std::shared_ptr<RemoteLookupFileManager> remote_lookup_file_manager_;
};
}  // namespace paimon
