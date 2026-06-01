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

#include <cstdint>
#include <memory>
#include <vector>

#include "paimon/reader/count_reader.h"
#include "paimon/result.h"

namespace paimon {
class DataSplitImpl;
class Split;

class AppendCountReader : public CountReader {
 public:
    explicit AppendCountReader(std::vector<std::shared_ptr<Split>> splits)
        : splits_(std::move(splits)) {}

    Result<int64_t> CountRows() override;

 private:
    Result<int64_t> CountSingleSplit(const std::shared_ptr<Split>& split) const;
    Result<int64_t> MetadataCount(const std::shared_ptr<DataSplitImpl>& split) const;

 private:
    std::vector<std::shared_ptr<Split>> splits_;
};

}  // namespace paimon
