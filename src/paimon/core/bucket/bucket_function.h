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

namespace paimon {

class BinaryRow;

/// Abstract interface for bucket functions.
/// A bucket function determines which bucket a row should be assigned to.
class BucketFunction {
 public:
    virtual ~BucketFunction() = default;

    /// Compute the bucket for the given row.
    /// @param row The binary row to compute the bucket for.
    /// @param num_buckets The total number of buckets.
    /// @return The bucket index (0-based).
    virtual int32_t Bucket(const BinaryRow& row, int32_t num_buckets) const = 0;
};

}  // namespace paimon
