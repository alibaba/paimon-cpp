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

#pragma once
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "paimon/common/utils/bin_packing.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/mergetree/sorted_run.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/table/source/split_generator.h"
#include "paimon/result.h"

namespace paimon {
class FieldsComparator;
class SortedRun;
enum class MergeEngine;

/// Merge tree implementation of `SplitGenerator`.
class MergeTreeSplitGenerator : public SplitGenerator {
 public:
    MergeTreeSplitGenerator(int64_t target_split_size, int64_t open_file_cost,
                            bool deletion_vectors_enabled, const MergeEngine& merge_engine,
                            const std::shared_ptr<FieldsComparator>& key_comparator);

    Result<std::vector<SplitGroup>> SplitForBatch(
        std::vector<std::shared_ptr<DataFileMeta>>&& input) const override;

    Result<std::vector<SplitGroup>> SplitForStreaming(
        std::vector<std::shared_ptr<DataFileMeta>>&& files) const override {
        // We don't split streaming scan files
        return std::vector<SplitGroup>({SplitGroup::RawConvertibleGroup(std::move(files))});
    }

 private:
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> PackSplits(
        std::vector<std::vector<std::shared_ptr<DataFileMeta>>>&& sections) const;

    static std::vector<std::shared_ptr<DataFileMeta>> FlatFiles(
        std::vector<std::vector<std::shared_ptr<DataFileMeta>>>&& section);

    static std::vector<std::shared_ptr<DataFileMeta>> FlatRun(std::vector<SortedRun>&& section);

    static bool WithoutDeleteRow(const std::shared_ptr<DataFileMeta>& meta);

 private:
    int64_t target_split_size_;
    int64_t open_file_cost_;
    bool deletion_vectors_enabled_;
    MergeEngine merge_engine_;
    std::shared_ptr<FieldsComparator> key_comparator_;
};
}  // namespace paimon
