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

#include "paimon/common/utils/bloom_filter.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <utility>

namespace paimon {

BloomFilter::BloomFilter(int64_t expected_entries, int32_t byte_length)
    : expected_entries_(expected_entries) {
    bit_set_ = std::make_shared<BitSet>(byte_length);
}

Status BloomFilter::AddHash(int64_t hash1) {
    int hash2 = hash1 >> 16;

    for (int32_t i = 1; i <= num_hash_functions_; i++) {
        int32_t combined_hash = hash1 + (i * hash2);
        // hashcode should be positive, flip all the bits if it's negative
        if (combined_hash < 0) {
            combined_hash = ~combined_hash;
        }
        int32_t pos = combined_hash % bit_set_->GetBitLength();
        bit_set_->Set(pos);
    }
    return Status::OK();
}

bool BloomFilter::TestHash(int64_t hash64) const {
    auto hash1 = static_cast<int32_t>(hash64);
    auto hash2 = static_cast<int32_t>(static_cast<uint64_t>(hash64) >> 32);

    for (int32_t i = 1; i <= num_hash_functions_; i++) {
        int32_t combined_hash = hash1 + (i * hash2);
        // hashcode should be positive, flip all the bits if it's negative
        if (combined_hash < 0) {
            combined_hash = ~combined_hash;
        }
        int32_t pos = combined_hash % bit_set_->GetBitLength();
        if (!bit_set_->Get(pos)) {
            return false;
        }
    }
    return true;
}

void BloomFilter::Reset() {
    bit_set_->Clear();
}

}  // namespace paimon
