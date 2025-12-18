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

#include <limits>
#include <memory>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"

namespace paimon {
/// A key value, including user key, sequence number, value kind and value. This object can be
/// reused.
/// e.g., primary key: k0, k1
/// table fields: k0, k1, v0, v1, v2
/// InternalRow of key in KeyValue Object: k0, k1
/// InternalRow of value in KeyValue Object: k0, k1, v0, v1, v2
struct KeyValue {
    static constexpr int32_t UNKNOWN_LEVEL = -1;
    static constexpr int32_t UNKNOWN_SEQUENCE = -1;

    KeyValue(const RowKind* _value_kind, int64_t _sequence_number, int32_t _level,
             std::shared_ptr<InternalRow>&& _key, std::unique_ptr<InternalRow>&& _value)
        : value_kind(_value_kind),
          sequence_number(_sequence_number),
          level(_level),
          key(std::move(_key)),
          value(std::move(_value)) {}

    KeyValue(KeyValue&& other) noexcept {
        *this = std::move(other);
    }

    KeyValue& operator=(KeyValue&& other) noexcept {
        if (&other == this) {
            return *this;
        }
        value_kind = other.value_kind;
        sequence_number = other.sequence_number;
        level = other.level;
        key = std::move(other.key);
        value = std::move(other.value);
        return *this;
    }

    const RowKind* value_kind = nullptr;
    // determined after written into memory table or read from file
    int64_t sequence_number = -1;
    // determined after read from file
    int32_t level = -1;
    std::shared_ptr<InternalRow> key;
    std::unique_ptr<InternalRow> value;
};

struct KeyValueBatch {
    KeyValueBatch() = default;
    KeyValueBatch(KeyValueBatch&& other) noexcept {
        *this = std::move(other);
    }
    KeyValueBatch& operator=(KeyValueBatch&& other) noexcept {
        if (&other == this) {
            return *this;
        }
        delete_row_count = other.delete_row_count;
        min_sequence_number = other.min_sequence_number;
        max_sequence_number = other.max_sequence_number;
        min_key = std::move(other.min_key);
        max_key = std::move(other.max_key);
        batch = std::move(other.batch);
        other.delete_row_count = 0;
        return *this;
    }
    ~KeyValueBatch() {
        if (batch) {
            ArrowArrayRelease(batch.get());
        }
    }
    int64_t delete_row_count = 0;
    int64_t min_sequence_number = std::numeric_limits<int64_t>::max();
    int64_t max_sequence_number = std::numeric_limits<int64_t>::min();
    std::shared_ptr<InternalRow> min_key;
    std::shared_ptr<InternalRow> max_key;
    std::unique_ptr<ArrowArray> batch;
};
}  // namespace paimon
