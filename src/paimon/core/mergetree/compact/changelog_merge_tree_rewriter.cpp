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

#include "paimon/core/mergetree/compact/changelog_merge_tree_rewriter.h"
namespace paimon {
ChangelogMergeTreeRewriter::ChangelogMergeTreeRewriter(
    int32_t max_level, bool force_drop_delete, const BinaryRow& partition, int32_t bucket,
    int64_t schema_id, const std::vector<std::string>& trimmed_primary_keys,
    const CoreOptions& options, const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& write_schema, DeletionVector::Factory dv_factory,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
    MergeFunctionWrapperFactory merge_function_wrapper_factory,
    const std::shared_ptr<MemoryPool>& pool)
    : MergeTreeCompactRewriter(partition, bucket, schema_id, trimmed_primary_keys, options,
                               data_schema, write_schema, std::move(dv_factory), path_factory_cache,
                               std::move(merge_file_split_read),
                               std::move(merge_function_wrapper_factory), pool),
      max_level_(max_level),
      force_drop_delete_(force_drop_delete) {}

Result<CompactResult> ChangelogMergeTreeRewriter::Rewrite(
    int32_t output_level, bool drop_delete, const std::vector<std::vector<SortedRun>>& sections) {
    if (RewriteChangelog(output_level, drop_delete, sections)) {
        return RewriteOrProduceChangelog(output_level, sections, drop_delete,
                                         /*rewrite_compact_file=*/true);
    } else {
        return RewriteCompaction(output_level, drop_delete, sections);
    }
}

Result<CompactResult> ChangelogMergeTreeRewriter::Upgrade(
    int32_t output_level, const std::shared_ptr<DataFileMeta>& file) {
    UpgradeStrategy upgrade_strategy = GenerateUpgradeStrategy(output_level, file);
    if (upgrade_strategy.changelog) {
        return RewriteOrProduceChangelog(output_level, {{SortedRun::FromSingle(file)}},
                                         force_drop_delete_, upgrade_strategy.rewrite);
    } else {
        return MergeTreeCompactRewriter::Upgrade(output_level, file);
    }
}

bool ChangelogMergeTreeRewriter::RewriteLookupChangelog(
    int32_t output_level, const std::vector<std::vector<SortedRun>>& sections) const {
    if (output_level == 0) {
        return false;
    }
    for (const auto& runs : sections) {
        for (const auto& run : runs) {
            for (const auto& file : run.Files()) {
                if (file->level == 0) {
                    return true;
                }
            }
        }
    }
    return false;
}

Result<CompactResult> ChangelogMergeTreeRewriter::RewriteOrProduceChangelog(
    int32_t output_level, const std::vector<std::vector<SortedRun>>& sections, bool drop_delete,
    bool rewrite_compact_file) {
    PAIMON_ASSIGN_OR_RAISE(MergeTreeCompactRewriter::KeyValueConsumerCreator create_consumer,
                           GenerateKeyValueConsumer());
    std::vector<std::shared_ptr<MergeTreeCompactRewriter::KeyValueMergeReader>> reader_holders;

    std::unique_ptr<MergeTreeCompactRewriter::KeyValueRollingFileWriter> compact_file_writer;
    if (rewrite_compact_file) {
        compact_file_writer = CreateRollingRowWriter(output_level);
    }
    // TODO(xinyu.lxy): produce changelog
    ScopeGuard write_guard([&]() -> void {
        if (compact_file_writer) {
            compact_file_writer->Abort();
        }
        merge_file_split_read_.reset();
        for (const auto& reader : reader_holders) {
            reader->Close();
        }
    });

    for (const auto& section : sections) {
        PAIMON_RETURN_NOT_OK(MergeReadAndWrite(output_level, drop_delete, section, create_consumer,
                                               compact_file_writer.get(), &reader_holders));
    }
    if (compact_file_writer) {
        PAIMON_RETURN_NOT_OK(compact_file_writer->Close());
    }
    auto before = ExtractFilesFromSections(sections);
    std::vector<std::shared_ptr<DataFileMeta>> after;
    if (compact_file_writer) {
        PAIMON_ASSIGN_OR_RAISE(after, compact_file_writer->GetResult());
    } else {
        after.reserve(before.size());
        for (const auto& file : before) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> new_file,
                                   file->Upgrade(output_level));
            after.emplace_back(std::move(new_file));
        }
    }
    if (rewrite_compact_file) {
        NotifyRewriteCompactBefore(before);
    }
    write_guard.Release();

    after = NotifyRewriteCompactAfter(after);
    return CompactResult(before, after);
}

}  // namespace paimon
