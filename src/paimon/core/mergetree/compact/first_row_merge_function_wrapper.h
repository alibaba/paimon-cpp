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

#include <cassert>
#include <functional>
#include <memory>
#include <optional>
#include <utility>

#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/first_row_merge_function.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// Wrapper for `MergeFunction`s to produce changelog by lookup for first row.
class FirstRowMergeFunctionWrapper : public MergeFunctionWrapper<KeyValue> {
 public:
    FirstRowMergeFunctionWrapper(
        std::unique_ptr<FirstRowMergeFunction>&& merge_function,
        std::function<Result<bool>(const std::shared_ptr<InternalRow>&)> contains)
        : merge_function_(std::move(merge_function)), contains_(std::move(contains)) {}

    void Reset() override {
        merge_function_->Reset();
    }

    Status Add(KeyValue&& kv) override {
        return merge_function_->Add(std::move(kv));
    }

    Result<std::optional<KeyValue>> GetResult() override {
        PAIMON_ASSIGN_OR_RAISE(std::optional<KeyValue> result, merge_function_->GetResult());
        if (merge_function_->ContainsHighLevel()) {
            Reset();
            return result;
        }
        if (!result) {
            Reset();
            return Status::Invalid(
                "In FirstRowMergeFunctionWrapper when call GetResult, there must have a result");
        }
        PAIMON_ASSIGN_OR_RAISE(bool contains, contains_(result.value().key));
        if (contains) {
            // empty
            Reset();
            return std::optional<KeyValue>();
        }
        // new record, output changelog
        // TODO(xinyu.lxy) support changelog
        Reset();
        return result;
    }

 private:
    std::unique_ptr<FirstRowMergeFunction> merge_function_;
    std::function<Result<bool>(const std::shared_ptr<InternalRow>&)> contains_;
};

}  // namespace paimon
