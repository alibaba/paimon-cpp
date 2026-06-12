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
#include <map>
#include <string>
#include <unordered_map>

namespace paimon {

/// File-level field name <-> field_id dictionary for shared-shredding MAP.
/// Assigns monotonically increasing ids to new field names within one file.
class MapSharedShreddingFieldDict {
 public:
    MapSharedShreddingFieldDict() = default;

    /// Looks up or assigns a field_id for the given field name.
    /// New names get the next available id.
    int32_t GetOrAssign(const std::string& name) {
        auto iterator = name_to_id_.find(name);
        if (iterator != name_to_id_.end()) {
            return iterator->second;
        }
        int32_t field_id = next_id_++;
        name_to_id_[name] = field_id;
        return field_id;
    }

    /// Returns the complete name -> field_id dictionary.
    /// Used to populate MapSharedShreddingFieldMeta::name_to_id at file close.
    const std::map<std::string, int32_t>& GetNameToId() const {
        return name_to_id_;
    }

    /// Returns the number of distinct field names seen so far.
    int32_t Size() const {
        return next_id_;
    }

    /// Resets all state. Called when rolling to a new file.
    void Reset() {
        name_to_id_.clear();
        next_id_ = 0;
    }

 private:
    std::map<std::string, int32_t> name_to_id_;
    int32_t next_id_ = 0;
};

}  // namespace paimon
