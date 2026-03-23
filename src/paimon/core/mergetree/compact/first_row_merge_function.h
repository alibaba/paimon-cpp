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
#include <optional>
#include <utility>

#include "paimon/common/types/row_kind.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// A `MergeFunction` where key is primary key (unique) and value is the full record, only keep
/// the first one.
class FirstRowMergeFunction : public MergeFunction {
 public:
    explicit FirstRowMergeFunction(bool ignore_delete) : ignore_delete_(ignore_delete) {}

    void Reset() override {
        first_kv_ = std::nullopt;
        contains_high_level_ = false;
    }

    bool ContainsHighLevel() const {
        return contains_high_level_;
    }

    Status Add(KeyValue&& moved_kv) override {
        KeyValue kv = std::move(moved_kv);
        if (kv.value_kind->IsRetract()) {
            // In 0.7- versions, the delete records might be written into data file even when
            // ignore-delete configured, so ignoreDelete still needs to be checked
            if (ignore_delete_) {
                return Status::OK();
            } else {
                return Status::Invalid(
                    "By default, First row merge engine can not accept DELETE/UPDATE_BEFORE "
                    "records. You can config 'first-row.ignore-delete' to ignore the "
                    "DELETE/UPDATE_BEFORE records.");
            }
        }

        if (kv.level > 0) {
            contains_high_level_ = true;
        }
        if (first_kv_ == std::nullopt) {
            first_kv_ = std::move(kv);
        }
        return Status::OK();
    }

    Result<std::optional<KeyValue>> GetResult() override {
        return std::move(first_kv_);
    }

 private:
    bool ignore_delete_;
    bool contains_high_level_ = false;
    std::optional<KeyValue> first_kv_;
};
}  // namespace paimon
