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

#include "paimon/core/table/source/data_evolution_split_generator.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iterator>

#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/bin_packing.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/core/io/data_file_meta.h"

namespace paimon {
Result<std::vector<SplitGenerator::SplitGroup>> DataEvolutionSplitGenerator::SplitForBatch(
    std::vector<std::shared_ptr<DataFileMeta>>&& input) const {
    RangeHelper<std::shared_ptr<DataFileMeta>> range_helper(
        [](const std::shared_ptr<DataFileMeta>& meta) -> Result<int64_t> {
            return meta->NonNullFirstRowId();
        },
        [](const std::shared_ptr<DataFileMeta>& meta) -> Result<int64_t> {
            PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, meta->NonNullFirstRowId());
            return first_row_id + meta->row_count - 1;
        });

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> ranges,
                           range_helper.MergeOverlappingRanges(std::move(input)));

    auto weight_func = [open_file_cost = open_file_cost_, count_blob_size = count_blob_size_](
                           const std::vector<std::shared_ptr<DataFileMeta>>& metas) -> int64_t {
        int64_t file_size_sum = 0;
        for (const auto& meta : metas) {
            if (BlobUtils::IsBlobFile(meta->file_name)) {
                file_size_sum += count_blob_size ? meta->file_size : open_file_cost;
            } else {
                file_size_sum += meta->file_size;
            }
        }
        return std::max(file_size_sum, open_file_cost);
    };

    auto packed = BinPacking::PackForOrdered<std::vector<std::shared_ptr<DataFileMeta>>>(
        std::move(ranges), weight_func, target_split_size_);

    std::vector<SplitGenerator::SplitGroup> ret;
    ret.reserve(packed.size());
    for (auto& pack : packed) {
        bool raw_convertible = true;
        std::vector<std::shared_ptr<DataFileMeta>> flat_meta;
        for (auto& with_same_row_id : pack) {
            if (with_same_row_id.size() != 1) {
                raw_convertible = false;
            }
            flat_meta.insert(flat_meta.end(), std::make_move_iterator(with_same_row_id.begin()),
                             std::make_move_iterator(with_same_row_id.end()));
        }
        if (raw_convertible) {
            ret.push_back(SplitGenerator::SplitGroup::RawConvertibleGroup(std::move(flat_meta)));
        } else {
            ret.push_back(SplitGenerator::SplitGroup::NonRawConvertibleGroup(std::move(flat_meta)));
        }
    }
    return ret;
}
}  // namespace paimon
