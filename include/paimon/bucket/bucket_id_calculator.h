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
#include <vector>

#include "paimon/bucket/bucket_function_type.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

struct ArrowSchema;
struct ArrowArray;

namespace paimon {
class BucketFunction;
class MemoryPool;

/// Calculator for determining bucket ids based on the given bucket keys.
///
/// @note `BucketIdCalculator` is compatible with the Java implementation and uses
/// hash-based distribution to ensure even data distribution across buckets.
class PAIMON_EXPORT BucketIdCalculator {
 public:
    /// Create `BucketIdCalculator` with default bucket function.
    /// @param is_pk_table Whether this is for a primary key table.
    /// @param num_buckets Number of buckets.
    /// @param pool Memory pool for memory allocation.
    static Result<std::unique_ptr<BucketIdCalculator>> Create(
        bool is_pk_table, int32_t num_buckets, const std::shared_ptr<MemoryPool>& pool);

    /// Create `BucketIdCalculator` with a custom bucket function.
    /// @param is_pk_table Whether this is for a primary key table.
    /// @param num_buckets Number of buckets.
    /// @param bucket_function The bucket function to use for bucket assignment.
    /// @param pool Memory pool for memory allocation.
    static Result<std::unique_ptr<BucketIdCalculator>> Create(
        bool is_pk_table, int32_t num_buckets, std::unique_ptr<BucketFunction> bucket_function,
        const std::shared_ptr<MemoryPool>& pool);

    /// Create `BucketIdCalculator` with MOD bucket function.
    /// @param is_pk_table Whether this is for a primary key table.
    /// @param num_buckets Number of buckets.
    /// @param bucket_key_type The type of the single bucket key field. Must be INT or BIGINT.
    /// @param pool Memory pool for memory allocation.
    static Result<std::unique_ptr<BucketIdCalculator>> CreateMod(
        bool is_pk_table, int32_t num_buckets, FieldType bucket_key_type,
        const std::shared_ptr<MemoryPool>& pool);

    /// Create `BucketIdCalculator` with HIVE bucket function.
    /// @param is_pk_table Whether this is for a primary key table.
    /// @param num_buckets Number of buckets.
    /// @param field_infos The detailed type info of all fields in the bucket key row.
    /// @param pool Memory pool for memory allocation.
    static Result<std::unique_ptr<BucketIdCalculator>> CreateHive(
        bool is_pk_table, int32_t num_buckets, const std::vector<HiveFieldInfo>& field_infos,
        const std::shared_ptr<MemoryPool>& pool);

    /// Calculate bucket ids for the given bucket keys.
    /// @param bucket_keys Arrow struct array containing the bucket key values.
    /// @param bucket_schema Arrow schema describing the structure of bucket_keys.
    /// @param bucket_ids Output array to store calculated bucket ids.
    /// @note 1. bucket_keys is a struct array, the order of fields needs to be consistent with
    /// "bucket-key" options in table schema. 2. bucket_keys and bucket_schema match each other. 3.
    /// bucket_ids is allocated enough space, at least >= bucket_keys->length
    Status CalculateBucketIds(ArrowArray* bucket_keys, ArrowSchema* bucket_schema,
                              int32_t* bucket_ids) const;

    /// Destructor
    ~BucketIdCalculator();

 private:
    BucketIdCalculator(int32_t num_buckets, std::unique_ptr<BucketFunction> bucket_function,
                       const std::shared_ptr<MemoryPool>& pool);

 private:
    int32_t num_buckets_;
    std::unique_ptr<BucketFunction> bucket_function_;
    std::shared_ptr<MemoryPool> pool_;
};
}  // namespace paimon
