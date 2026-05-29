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

#include "paimon/common/utils/bloom_filter64.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <utility>

#include "paimon/memory/bytes.h"

namespace paimon {
class MemoryPool;

BloomFilter64::BitSet::BitSet(const std::shared_ptr<Bytes>& bytes, int32_t offset)
    : offset_(offset), bytes_(bytes) {
    assert(bytes_->size() > 0);
    assert(offset_ >= 0);
}

void BloomFilter64::BitSet::Set(int32_t index) {
    char* data = bytes_->data();
    data[(static_cast<uint32_t>(index) >> 3) + offset_] |=
        static_cast<char>(1u << (index & BloomFilter64::BitSet::MASK));
}

bool BloomFilter64::BitSet::Get(int32_t index) const {
    const char* data = bytes_->data();
    return (data[(static_cast<uint32_t>(index) >> 3) + offset_] &
            static_cast<char>(1u << (index & BloomFilter64::BitSet::MASK))) != 0;
}

int32_t BloomFilter64::BitSet::BitSize() const {
    return (bytes_->size() - offset_) * BloomFilter64::BYTE_SIZE;
}

BloomFilter64::BloomFilter64(int64_t items, double fpp, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool) {
    auto nb = static_cast<int32_t>(-items * std::log(fpp) / (std::log(2) * std::log(2)));
    num_bits_ = nb + (BloomFilter64::BYTE_SIZE - (nb % BloomFilter64::BYTE_SIZE));
    num_hash_functions_ = std::max(
        1, static_cast<int32_t>(std::round(static_cast<double>(num_bits_) / items * std::log(2))));
    auto bytes = std::make_shared<Bytes>(num_bits_ / BloomFilter64::BYTE_SIZE, pool_.get());
    bit_set_ = std::make_unique<BitSet>(bytes, /*offset=*/0);
}

BloomFilter64::BloomFilter64(int32_t num_hash_functions, std::unique_ptr<BitSet>&& bit_set)
    : num_bits_(bit_set->BitSize()),
      num_hash_functions_(num_hash_functions),
      bit_set_(std::move(bit_set)) {}

void BloomFilter64::AddHash(int64_t hash64) {
    auto hash1 = static_cast<int32_t>(hash64);
    auto hash2 = static_cast<int32_t>(static_cast<uint64_t>(hash64) >> 32);

    for (int32_t i = 1; i <= num_hash_functions_; i++) {
        int32_t combined_hash = hash1 + (i * hash2);
        // hashcode should be positive, flip all the bits if it's negative
        if (combined_hash < 0) {
            combined_hash = ~combined_hash;
        }
        int32_t pos = combined_hash % num_bits_;
        bit_set_->Set(pos);
    }
}

bool BloomFilter64::TestHash(int64_t hash64) const {
    auto hash1 = static_cast<int32_t>(hash64);
    auto hash2 = static_cast<int32_t>(static_cast<uint64_t>(hash64) >> 32);

    for (int32_t i = 1; i <= num_hash_functions_; i++) {
        int32_t combined_hash = hash1 + (i * hash2);
        // hashcode should be positive, flip all the bits if it's negative
        if (combined_hash < 0) {
            combined_hash = ~combined_hash;
        }
        int32_t pos = combined_hash % num_bits_;
        if (!bit_set_->Get(pos)) {
            return false;
        }
    }
    return true;
}

}  // namespace paimon
