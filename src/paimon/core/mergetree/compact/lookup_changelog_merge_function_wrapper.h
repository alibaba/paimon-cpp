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

#include <algorithm>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/lookup_merge_function.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/core/mergetree/lookup/file_position.h"
#include "paimon/core/mergetree/lookup/positioned_key_value.h"
#include "paimon/core/options/lookup_strategy.h"
#include "paimon/result.h"
#include "paimon/status.h"
namespace paimon {
/// Wrapper for `MergeFunction`s to produce changelog by lookup during the compaction involving
/// level 0 files.
///
/// Changelog records are generated in the process of the level-0 file participating in the
/// compaction, if during the compaction processing:
///
///  Without level-0 records, no changelog.
///  With level-0 record, with level-x (x > 0) record, level-x record should be BEFORE, level-0
///       should be AFTER.
///  With level-0 record, without level-x record, need to lookup the history value of the upper
///       level as BEFORE.
/// TODO(xinyu.lxy) : add changelog
template <typename T>
class LookupChangelogMergeFunctionWrapper : public MergeFunctionWrapper<KeyValue> {
 public:
    static Result<std::unique_ptr<LookupChangelogMergeFunctionWrapper>> Create(
        std::unique_ptr<LookupMergeFunction>&& merge_function,
        std::function<Result<std::optional<T>>(const std::shared_ptr<InternalRow>&)> lookup,
        const LookupStrategy& lookup_strategy,
        const std::shared_ptr<BucketedDvMaintainer>& deletion_vectors_maintainer,
        const std::shared_ptr<FieldsComparator>& comparator) {
        if (lookup_strategy.deletion_vector && !deletion_vectors_maintainer) {
            return Status::Invalid("deletionVectorsMaintainer should not be null, there is a bug.");
        }
        return std::unique_ptr<LookupChangelogMergeFunctionWrapper>(
            new LookupChangelogMergeFunctionWrapper(std::move(merge_function), std::move(lookup),
                                                    lookup_strategy, deletion_vectors_maintainer,
                                                    comparator));
    }
    void Reset() override {
        merge_function_->Reset();
    }

    Status Add(KeyValue&& kv) override {
        return merge_function_->Add(std::move(kv));
    }

    Result<std::optional<KeyValue>> GetResult() override {
        // 1. Find the latest high level record and compute containLevel0
        std::optional<int32_t> high_level_idx = merge_function_->PickHighLevelIdx();

        // 2. Lookup if latest high level record is absent
        if (high_level_idx == std::nullopt) {
            std::optional<KeyValue> lookup_high_level;
            PAIMON_ASSIGN_OR_RAISE(std::optional<T> lookup_result,
                                   lookup_(merge_function_->GetKey()));
            if (lookup_result) {
                std::string file_name;
                int64_t row_position = -1;
                if constexpr (std::is_same_v<T, PositionedKeyValue>) {
                    lookup_high_level = std::move(lookup_result->key_value);
                    file_name = lookup_result->file_name;
                    row_position = lookup_result->row_position;
                } else if constexpr (std::is_same_v<T, FilePosition>) {
                    file_name = lookup_result->file_name;
                    row_position = lookup_result->row_position;
                } else if constexpr (std::is_same_v<T, KeyValue>) {
                    lookup_high_level = std::move(lookup_result);
                } else {
                    return Status::Invalid(
                        "deletion vector mode must have PositionedKeyValue or FilePosition "
                        "lookup result");
                }
                if (lookup_strategy_.deletion_vector) {
                    PAIMON_RETURN_NOT_OK(
                        deletion_vectors_maintainer_->NotifyNewDeletion(file_name, row_position));
                }
            }
            if (lookup_high_level) {
                merge_function_->InsertInto(std::move(lookup_high_level), comparator_);
            }
        }

        // 3. Calculate result
        PAIMON_ASSIGN_OR_RAISE(std::optional<KeyValue> result, merge_function_->GetResult());
        Reset();
        // 4. Set changelog when there's level-0 records
        // TODO(liancheng.lsz): setChangelog
        return result;
    }

 private:
    LookupChangelogMergeFunctionWrapper(
        std::unique_ptr<LookupMergeFunction>&& merge_function,
        std::function<Result<std::optional<T>>(const std::shared_ptr<InternalRow>&)> lookup,
        const LookupStrategy& lookup_strategy,
        const std::shared_ptr<BucketedDvMaintainer>& deletion_vectors_maintainer,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator)
        : merge_function_(std::move(merge_function)),
          lookup_(std::move(lookup)),
          lookup_strategy_(lookup_strategy),
          deletion_vectors_maintainer_(deletion_vectors_maintainer),
          comparator_(CreateSequenceComparator(user_defined_seq_comparator)) {}

    static std::function<bool(const KeyValue& o1, const KeyValue& o2)> CreateSequenceComparator(
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator) {
        auto cmp_func = [user_defined_seq_comparator](const KeyValue& o1, const KeyValue& o2) {
            if (user_defined_seq_comparator == nullptr) {
                return o1.sequence_number < o2.sequence_number;
            }
            auto user_defined_result =
                user_defined_seq_comparator->CompareTo(*(o1.value), *(o2.value));
            if (user_defined_result != 0) {
                return user_defined_result < 0;
            }
            return o1.sequence_number < o2.sequence_number;
        };
        return cmp_func;
    }

 private:
    std::unique_ptr<LookupMergeFunction> merge_function_;
    std::function<Result<std::optional<T>>(const std::shared_ptr<InternalRow>&)> lookup_;
    LookupStrategy lookup_strategy_;
    std::shared_ptr<BucketedDvMaintainer> deletion_vectors_maintainer_;
    std::function<bool(const KeyValue& o1, const KeyValue& o2)> comparator_;
};

}  // namespace paimon
