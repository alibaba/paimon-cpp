/*
 * Copyright 2025-present Alibaba Inc.
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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/table/source/data_split.h"
#include "paimon/utils/range.h"
#include "paimon/visibility.h"

namespace paimon {
/// Indexed split for global index reading operation.
class PAIMON_EXPORT IndexedSplit : public Split {
 public:
    /// @returns The underlying physical data split containing actual data file details.
    virtual std::shared_ptr<DataSplit> GetDataSplit() const = 0;

    /// @returns A list of row intervals [start, end] indicating which rows
    ///          are relevant (e.g., passed predicate pushdown).
    virtual const std::vector<Range>& RowRanges() const = 0;

    /// @returns A score for **each individual row** included in `RowRanges()`,
    ///          in the order they appear when traversing the ranges.
    virtual const std::vector<float>& Scores() const = 0;
};
}  // namespace paimon
