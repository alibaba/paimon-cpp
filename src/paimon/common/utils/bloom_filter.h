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
#include <cstdint>
#include <functional>
#include <memory>

#include "paimon/common/utils/bit_set.h"
#include "paimon/memory/bytes.h"
#include "paimon/visibility.h"

namespace paimon {

/// Bloom filter based on MemorySegment.
class PAIMON_EXPORT BloomFilter {
 public:
    BloomFilter(int64_t expected_entries, int32_t byte_length);

    int32_t GetNumHashFunctions() const {
        return num_hash_functions_;
    }

    int64_t GetExpectedEntries() const {
        return expected_entries_;
    }

    std::shared_ptr<BitSet> GetBitSet() {
        return bit_set_;
    }

    Status AddHash(int64_t hash64);

    bool TestHash(int64_t hash64) const;

    void Reset();

 private:
    static constexpr int32_t BYTE_SIZE = 8;

 private:
    int64_t expected_entries_;
    int32_t num_hash_functions_ = -1;
    std::shared_ptr<BitSet> bit_set_;
};
}  // namespace paimon
