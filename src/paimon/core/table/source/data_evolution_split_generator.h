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
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "paimon/core/table/source/split_generator.h"
#include "paimon/result.h"

namespace paimon {
struct DataFileMeta;

/// Append data evolution table split generator, which implementation of `SplitGenerator`.
class DataEvolutionSplitGenerator : public SplitGenerator {
 public:
    DataEvolutionSplitGenerator(int64_t target_split_size, int64_t open_file_cost,
                                bool count_blob_size)
        : target_split_size_(target_split_size),
          open_file_cost_(open_file_cost),
          count_blob_size_(count_blob_size) {}

    Result<std::vector<SplitGroup>> SplitForBatch(
        std::vector<std::shared_ptr<DataFileMeta>>&& input) const override;

    Result<std::vector<SplitGroup>> SplitForStreaming(
        std::vector<std::shared_ptr<DataFileMeta>>&& files) const override {
        return SplitForBatch(std::move(files));
    }

 private:
    int64_t target_split_size_;
    int64_t open_file_cost_;
    bool count_blob_size_;
};

}  // namespace paimon
