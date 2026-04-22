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
#include <memory>
#include <optional>

#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/mergetree/compact/loser_tree.h"

namespace paimon {

/// Utility class for creating LoserTree comparator functions used in merge operations.
class MergeUtils {
 public:
    MergeUtils() = delete;
    ~MergeUtils() = delete;

    /// Create a LoserTree key comparator from a FieldsComparator.
    /// The returned function compares two KeyValues by their keys.
    static LoserTree::CompareFunc CreateKeyComparator(
        const std::shared_ptr<FieldsComparator>& key_comparator) {
        return [key_comparator](const std::optional<KeyValue>& lhs,
                                const std::optional<KeyValue>& rhs) -> int32_t {
            if (lhs == std::nullopt) {
                return -1;
            }
            if (rhs == std::nullopt) {
                return 1;
            }
            return key_comparator->CompareTo(*(rhs.value().key), *(lhs.value().key));
        };
    }

    /// Create a LoserTree sequence comparator from an optional user-defined sequence comparator.
    /// The returned function compares two KeyValues by user-defined sequence fields first,
    /// then falls back to the built-in sequence number.
    static LoserTree::CompareFunc CreateSequenceComparator(
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator) {
        return [user_defined_seq_comparator](const std::optional<KeyValue>& lhs,
                                             const std::optional<KeyValue>& rhs) -> int32_t {
            if (lhs == std::nullopt) {
                return -1;
            }
            if (rhs == std::nullopt) {
                return 1;
            }
            if (user_defined_seq_comparator != nullptr) {
                int32_t result = user_defined_seq_comparator->CompareTo(*(rhs.value().value),
                                                                        *(lhs.value().value));
                if (result != 0) {
                    return result;
                }
            }
            assert(lhs.value().sequence_number != rhs.value().sequence_number);
            return rhs.value().sequence_number < lhs.value().sequence_number ? -1 : 1;
        };
    }
};

}  // namespace paimon
