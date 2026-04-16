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

#include "paimon/core/mergetree/compact/sort_merge_reader_with_loser_tree.h"

#include <cassert>
#include <cstdint>

#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/key_value_record_reader.h"

namespace paimon {
SortMergeReaderWithLoserTree::SortMergeReaderWithLoserTree(
    std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
    const std::shared_ptr<FieldsComparator>& user_key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper)
    : merge_function_wrapper_(merge_function_wrapper) {
    // if lhs and rhs are both null, it doesn't matter who becomes the new winner. But if
    // first_comparator returns 0, it means that second_comparator must be used to compare
    // again.
    // lhs and rhs are swapped when compare to generate loser tree pop smallest first
    auto first_comparator = [user_key_comparator](const std::optional<KeyValue>& lhs,
                                                  const std::optional<KeyValue>& rhs) -> int32_t {
        if (lhs == std::nullopt) {
            return -1;
        }
        if (rhs == std::nullopt) {
            return 1;
        }
        return user_key_comparator->CompareTo(*(rhs.value().key), *(lhs.value().key));
    };
    auto second_comparator = [user_defined_seq_comparator](
                                 const std::optional<KeyValue>& lhs,
                                 const std::optional<KeyValue>& rhs) -> int32_t {
        if (lhs == std::nullopt) {
            return -1;
        }
        if (rhs == std::nullopt) {
            return 1;
        }
        if (user_defined_seq_comparator != nullptr) {
            int32_t result =
                user_defined_seq_comparator->CompareTo(*(rhs.value().value), *(lhs.value().value));
            if (result != 0) {
                return result;
            }
        }
        assert(lhs.value().sequence_number != rhs.value().sequence_number);
        return rhs.value().sequence_number < lhs.value().sequence_number ? -1 : 1;
    };
    loser_tree_ =
        std::make_unique<LoserTree>(std::move(readers), first_comparator, second_comparator);
}

Result<bool> SortMergeReaderWithLoserTree::Iterator::HasNext() {
    while (true) {
        PAIMON_RETURN_NOT_OK(reader_->loser_tree_->AdjustForNextLoop());
        std::optional<KeyValue> winner = reader_->loser_tree_->PopWinner();
        if (winner == std::nullopt) {
            return false;
        }
        reader_->merge_function_wrapper_->Reset();
        PAIMON_RETURN_NOT_OK(reader_->merge_function_wrapper_->Add(std::move(winner.value())));
        PAIMON_RETURN_NOT_OK(Merge());
        if (result_ != std::nullopt) {
            return true;
        }
    }
    return false;
}

}  // namespace paimon
