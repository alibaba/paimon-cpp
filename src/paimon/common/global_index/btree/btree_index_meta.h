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
#include <memory>

#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/memory/bytes.h"

namespace paimon {
/// Index metadata for each BTree index file.
///
/// Empty serialized keys are valid, so null boundary keys are encoded separately with flags.
class BTreeIndexMeta {
 public:
    static std::shared_ptr<BTreeIndexMeta> Deserialize(const std::shared_ptr<Bytes>& meta,
                                                       paimon::MemoryPool* pool);
    std::shared_ptr<Bytes> Serialize(paimon::MemoryPool* pool) const;

 public:
    BTreeIndexMeta(const std::shared_ptr<Bytes>& first_key, const std::shared_ptr<Bytes>& last_key,
                   bool has_nulls)
        : first_key_(first_key), last_key_(last_key), has_nulls_(has_nulls) {}

    const std::shared_ptr<Bytes>& FirstKey() const {
        return first_key_;
    }

    const std::shared_ptr<Bytes>& LastKey() const {
        return last_key_;
    }

    bool HasNulls() const {
        return has_nulls_;
    }

    bool OnlyNulls() const {
        return !(first_key_ || last_key_);
    }

 private:
    int32_t Size() const {
        // 11 bytes => key lengths (8) + has_nulls (1) + format version (1) + null flags (1).
        return (first_key_ ? first_key_->size() : 0) + (last_key_ ? last_key_->size() : 0) + 11;
    }

 private:
    static constexpr int8_t kFormatVersionWithNullFlags = 1;
    static constexpr int8_t kFirstKeyIsNull = 1;
    static constexpr int8_t kLastKeyIsNull = 1 << 1;

    std::shared_ptr<Bytes> first_key_;
    std::shared_ptr<Bytes> last_key_;
    bool has_nulls_;
};

}  // namespace paimon
