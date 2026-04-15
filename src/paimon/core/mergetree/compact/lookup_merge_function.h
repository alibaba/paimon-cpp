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
#include <optional>
#include <utility>
#include <vector>

#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// A `MergeFunction` for lookup, this wrapper only considers the latest high level record,
/// because each merge will query the old merged record, so the latest high level record should be
/// the final merged value.
class LookupMergeFunction : public MergeFunction {
 public:
    explicit LookupMergeFunction(std::unique_ptr<MergeFunction>&& merge_function)
        : merge_function_(std::move(merge_function)) {}

    void Reset() override {
        candidates_.clear();
        current_key_ = nullptr;
        contain_level0_ = false;
    }

    Status Add(KeyValue&& kv) override {
        current_key_ = kv.key;
        if (kv.level == 0) {
            contain_level0_ = true;
        }
        candidates_.emplace_back(std::move(kv));
        return Status::OK();
    }

    bool ContainLevel0() const {
        return contain_level0_;
    }

    const std::shared_ptr<InternalRow>& GetKey() const {
        return current_key_;
    }

    Result<std::optional<KeyValue>> GetResult() override {
        merge_function_->Reset();
        std::optional<int32_t> high_level_idx = PickHighLevelIdx();
        for (int32_t i = 0; i < static_cast<int32_t>(candidates_.size()); ++i) {
            // records that has not been stored on the disk yet, such as the data in the write
            // buffer being at level -1
            if (candidates_[i].level <= 0 || i == high_level_idx.value()) {
                PAIMON_RETURN_NOT_OK(merge_function_->Add(std::move(candidates_[i])));
            }
        }
        return merge_function_->GetResult();
    }

    void InsertInto(std::optional<KeyValue>&& high_level,
                    std::function<bool(const KeyValue& o1, const KeyValue& o2)> cmp_function) {
        if (!high_level) {
            return;
        }
        candidates_.push_back(std::move(high_level.value()));
        std::sort(candidates_.begin(), candidates_.end(), cmp_function);
    }

    std::optional<int32_t> PickHighLevelIdx() const {
        std::optional<int32_t> high_level_idx;
        for (int32_t i = 0; i < static_cast<int32_t>(candidates_.size()); i++) {
            const auto& kv = candidates_[i];
            // records that has not been stored on the disk yet, such as the data in the write
            // buffer being at level -1
            if (kv.level <= 0) {
                continue;
            }
            // For high-level comparison logic (not involving Level 0), only the value of the
            // minimum Level should be selected
            if (!high_level_idx || kv.level < candidates_[high_level_idx.value()].level) {
                high_level_idx = i;
            }
        }
        return high_level_idx;
    }

 private:
    std::unique_ptr<MergeFunction> merge_function_;
    std::vector<KeyValue> candidates_;
    std::shared_ptr<InternalRow> current_key_;
    bool contain_level0_ = false;
};
}  // namespace paimon
