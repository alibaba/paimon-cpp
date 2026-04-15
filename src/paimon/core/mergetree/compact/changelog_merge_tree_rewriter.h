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
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_tree_compact_rewriter.h"
namespace paimon {
/// A `MergeTreeCompactRewriter` which produces changelog files while performing compaction.
class ChangelogMergeTreeRewriter : public MergeTreeCompactRewriter {
 public:
    Result<CompactResult> Rewrite(int32_t output_level, bool drop_delete,
                                  const std::vector<std::vector<SortedRun>>& sections) override;

    Result<CompactResult> Upgrade(int32_t output_level,
                                  const std::shared_ptr<DataFileMeta>& file) override;

 protected:
    ChangelogMergeTreeRewriter(
        int32_t max_level, bool force_drop_delete, const BinaryRow& partition, int32_t bucket,
        int64_t schema_id, const std::vector<std::string>& trimmed_primary_keys,
        const CoreOptions& options, const std::shared_ptr<arrow::Schema>& data_schema,
        const std::shared_ptr<arrow::Schema>& write_schema, DeletionVector::Factory dv_factory,
        const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
        std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
        MergeFunctionWrapperFactory merge_function_wrapper_factory,
        const std::shared_ptr<CancellationController>& cancellation_controller,
        const std::shared_ptr<MemoryPool>& pool);

    struct UpgradeStrategy {
        static UpgradeStrategy NoChangelogNoRewrite() {
            static const UpgradeStrategy ret = {false, false};
            return ret;
        }
        static UpgradeStrategy ChangelogNoRewrite() {
            static const UpgradeStrategy ret = {true, false};
            return ret;
        }
        static UpgradeStrategy ChangelogWithRewrite() {
            static const UpgradeStrategy ret = {true, true};
            return ret;
        }

        bool operator==(const UpgradeStrategy& other) const {
            if (this == &other) {
                return true;
            }
            return changelog == other.changelog && rewrite == other.rewrite;
        }
        bool changelog;
        bool rewrite;
    };

    virtual UpgradeStrategy GenerateUpgradeStrategy(
        int32_t output_level, const std::shared_ptr<DataFileMeta>& file) const = 0;

    virtual bool RewriteChangelog(int32_t output_level, bool drop_delete,
                                  const std::vector<std::vector<SortedRun>>& sections) const = 0;

    bool RewriteLookupChangelog(int32_t output_level,
                                const std::vector<std::vector<SortedRun>>& sections) const;

    int32_t max_level_;
    bool force_drop_delete_;

 private:
    Result<CompactResult> RewriteOrProduceChangelog(
        int32_t output_level, const std::vector<std::vector<SortedRun>>& sections, bool drop_delete,
        bool rewrite_compact_file);
};
}  // namespace paimon
