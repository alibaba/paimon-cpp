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

#include "paimon/core/table/source/append_count_reader.h"

#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/status.h"

namespace paimon {

Result<int64_t> AppendCountReader::CountRows() {
    int64_t total = 0;
    for (const auto& split : splits_) {
        PAIMON_ASSIGN_OR_RAISE(int64_t split_count, CountSingleSplit(split));
        total += split_count;
    }
    return total;
}

Result<int64_t> AppendCountReader::CountSingleSplit(const std::shared_ptr<Split>& split) const {
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
    if (!data_split) {
        return Status::Invalid("split cannot be cast to DataSplitImpl");
    }

    if (data_split->DataFiles().empty()) {
        return 0;
    }

    return MetadataCount(data_split);
}

Result<int64_t> AppendCountReader::MetadataCount(
    const std::shared_ptr<DataSplitImpl>& split) const {
    PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> partial_count, split->MergedRowCount());
    if (partial_count.has_value()) {
        return partial_count.value();
    }

    return split->RowCount();
}

}  // namespace paimon
