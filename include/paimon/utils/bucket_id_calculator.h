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
#include <memory>

#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

struct ArrowSchema;
struct ArrowArray;

namespace paimon {
class MemoryPool;

/// Calculator for determining bucket ids based on the given bucket keys.
///
/// @note `BucketIdCalculator` is compatible with the Java implementation and uses
/// hash-based distribution to ensure even data distribution across buckets.
class PAIMON_EXPORT BucketIdCalculator {
 public:
    /// Create `BucketIdCalculator` with custom memory pool.
    /// @param is_pk_table Whether this is for a primary key table.
    /// @param num_buckets Number of buckets.
    /// @param pool Memory pool for memory allocation.
    static Result<std::unique_ptr<BucketIdCalculator>> Create(
        bool is_pk_table, int32_t num_buckets, const std::shared_ptr<MemoryPool>& pool);

    /// Create `BucketIdCalculator` with default memory pool.
    /// @param is_pk_table Whether this is for a primary key table.
    /// @param num_buckets Number of buckets.
    static Result<std::unique_ptr<BucketIdCalculator>> Create(bool is_pk_table,
                                                              int32_t num_buckets);
    /// Calculate bucket ids for the given bucket keys.
    /// @param bucket_keys Arrow struct array containing the bucket key values.
    /// @param bucket_schema Arrow schema describing the structure of bucket_keys.
    /// @param bucket_ids Output array to store calculated bucket ids.
    /// @note 1. bucket_keys is a struct array, the order of fields needs to be consistent with
    /// "bucket-key" options in table schema. 2. bucket_keys and bucket_schema match each other. 3.
    /// bucket_ids is allocated enough space, at least >= bucket_keys->length
    Status CalculateBucketIds(ArrowArray* bucket_keys, ArrowSchema* bucket_schema,
                              int32_t* bucket_ids) const;

 private:
    BucketIdCalculator(int32_t num_buckets, const std::shared_ptr<MemoryPool>& pool)
        : num_buckets_(num_buckets), pool_(pool) {}

 private:
    int32_t num_buckets_;
    std::shared_ptr<MemoryPool> pool_;
};
}  // namespace paimon
