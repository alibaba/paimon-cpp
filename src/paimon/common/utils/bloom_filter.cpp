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

int32_t BloomFilter::OptimalNumOfBits(int64_t expect_entries, double fpp) {
    if (expect_entries <= 0 || fpp <= 0.0 || fpp >= 1.0) {
        return 0;
    }
    double result = -static_cast<double>(expect_entries) * log(fpp) / (log(2) * log(2));
    if (result > INT32_MAX) return INT32_MAX;
    if (result < 0) return 0;
    return static_cast<int32_t>(result);
}

int32_t BloomFilter::OptimalNumOfHashFunctions(int64_t expect_entries, int64_t bit_size) {
    if (expect_entries <= 0) {
        return 1;
    }
    double ratio = static_cast<double>(bit_size) / static_cast<double>(expect_entries);
    double result = ratio * std::log(2.0);
    return std::max(1, static_cast<int>(std::round(result)));
}

std::shared_ptr<BloomFilter> BloomFilter::Create(int64_t expect_entries, double fpp) {
    int bytes = static_cast<int>(ceil(BloomFilter::OptimalNumOfBits(expect_entries, fpp) / 8.0));
    return std::make_shared<BloomFilter>(expect_entries, bytes);
}

BloomFilter::BloomFilter(int64_t expected_entries, int32_t byte_length)
    : expected_entries_(expected_entries) {
    num_hash_functions_ =
        OptimalNumOfHashFunctions(expected_entries, static_cast<int64_t>(byte_length) << 3);
    bit_set_ = std::make_shared<BitSet>(byte_length);
}

Status BloomFilter::AddHash(int32_t hash1) {
    int hash2 = hash1 >> 16;

    for (int32_t i = 1; i <= num_hash_functions_; i++) {
        int32_t combined_hash = hash1 + (i * hash2);
        // hashcode should be positive, flip all the bits if it's negative
        if (combined_hash < 0) {
            combined_hash = ~combined_hash;
        }
        int32_t pos = combined_hash % bit_set_->BitSize();
        PAIMON_RETURN_NOT_OK(bit_set_->Set(pos));
    }
    return Status::OK();
}

bool BloomFilter::TestHash(int32_t hash1) const {
    int32_t hash2 = hash1 >> 16;

    for (int i = 1; i <= num_hash_functions_; i++) {
        int32_t combined_hash = hash1 + (i * hash2);
        // hashcode should be positive, flip all the bits if it's negative
        if (combined_hash < 0) {
            combined_hash = ~combined_hash;
        }
        int32_t pos = combined_hash % bit_set_->BitSize();
        if (!bit_set_->Get(pos)) {
            return false;
        }
    }
    return true;
}

Status BloomFilter::SetMemorySegment(std::shared_ptr<MemorySegment> segment, int32_t offset) {
    return bit_set_->SetMemorySegment(segment, offset);
}

void BloomFilter::Reset() {
    bit_set_->Clear();
}

}  // namespace paimon
