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

#include <memory>
#include <optional>
#include <utility>

#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// Wrapper for `MergeFunction`s which works like a reducer.
///
/// A reducer is a type of function. If there is only one input the result is equal to that input;
/// Otherwise the result is calculated by merging all the inputs in some way.
///
/// This wrapper optimize the wrapped `MergeFunction`. If there is only one input, the input
/// will be stored and the inner merge function will not be called, thus saving some computing time.
class ReducerMergeFunctionWrapper : public MergeFunctionWrapper<KeyValue> {
 public:
    explicit ReducerMergeFunctionWrapper(std::unique_ptr<MergeFunction>&& merge_function)
        : merge_function_(std::move(merge_function)) {}

    /// Resets the `MergeFunction` helper to its default state.
    void Reset() override {
        initial_kv_ = std::nullopt;
        merge_function_->Reset();
        is_initialized_ = false;
    }

    /// Adds the given `KeyValue` to the `MergeFunction` helper.
    Status Add(KeyValue&& kv) override {
        if (!initial_kv_) {
            initial_kv_ = std::move(kv);
        } else {
            if (!is_initialized_) {
                PAIMON_RETURN_NOT_OK(Merge(std::move(initial_kv_).value()));
                is_initialized_ = true;
            }
            PAIMON_RETURN_NOT_OK(Merge(std::move(kv)));
        }
        return Status::OK();
    }

    /// Get current value of the `MergeFunction` helper.
    Result<std::optional<KeyValue>> GetResult() override {
        std::optional<KeyValue> result;
        if (is_initialized_) {
            PAIMON_ASSIGN_OR_RAISE(result, merge_function_->GetResult());
        } else {
            result = std::move(initial_kv_);
        }
        Reset();
        return result;
    }

 private:
    Status Merge(KeyValue&& kv) {
        return merge_function_->Add(std::move(kv));
    }

 private:
    std::unique_ptr<MergeFunction> merge_function_;
    std::optional<KeyValue> initial_kv_;
    bool is_initialized_ = false;
};

}  // namespace paimon
