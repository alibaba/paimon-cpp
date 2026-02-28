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

#include <fmt/format.h>

#include <memory>

#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {

class Chunk {
 public:
    virtual ~Chunk() = default;

    virtual Result<bool> TryAdd(const Literal& key) = 0;

    virtual Result<int32_t> Find(const Literal& key) {
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp_with_key, Key().CompareTo(key));
        if (cmp_with_key == 0) {
            return Code();
        }
        int32_t low = 0;
        int32_t high = Size() - 1;
        const int32_t base = Code() + 1;
        while (low <= high) {
            const int32_t mid = low + (high - low) / 2;
            PAIMON_ASSIGN_OR_RAISE(Literal key_at_mid, GetKey(mid));
            PAIMON_ASSIGN_OR_RAISE(int32_t cmp, key_at_mid.CompareTo(key));
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return base + mid;
            }
        }
        return -(base + low + 1);
    }

    virtual Result<Literal> Find(const int32_t code) {
        const auto current = Code();
        if (current == code) {
            return Key();
        }
        const auto index = code - current - 1;
        if (index < 0 || index >= Size()) {
            return Status::Invalid(fmt::format("Invalid Code {}", code));
        }
        return GetKey(index);
    }

    virtual const Literal& Key() const = 0;

    virtual int32_t Code() const = 0;

    virtual int32_t Offset() const = 0;

    virtual void SetOffset(int32_t offset) = 0;

    virtual int32_t Size() const = 0;

    virtual Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeChunk() const = 0;

    virtual Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeKeys() const = 0;

 protected:
    virtual Result<Literal> GetKey(int32_t index) = 0;
};

}  // namespace paimon
