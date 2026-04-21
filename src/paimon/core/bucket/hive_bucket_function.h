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
#include <vector>

#include "paimon/bucket/bucket_function_type.h"
#include "paimon/core/bucket/bucket_function.h"
#include "paimon/result.h"

namespace paimon {

/// Hive-compatible bucket function.
/// This implements the same bucket assignment logic as Hive, using Hive's hash functions
/// to ensure compatibility between Paimon and Hive bucketed tables.
///
/// The hash is computed by iterating over all fields in the row:
///   hash = (31 * hash) + computeHash(field_value)
/// Then the bucket is: (hash & INT32_MAX) % numBuckets
class HiveBucketFunction : public BucketFunction {
 public:
    /// Create a HiveBucketFunction with the given field types.
    /// @param field_types The types of all fields in the bucket key row.
    /// @return A Result containing the HiveBucketFunction or an error status.
    static Result<std::unique_ptr<HiveBucketFunction>> Create(
        const std::vector<FieldType>& field_types);

    /// Create a HiveBucketFunction with detailed field info (including decimal precision/scale).
    /// @param field_infos The detailed type info of all fields in the bucket key row.
    /// @return A Result containing the HiveBucketFunction or an error status.
    static Result<std::unique_ptr<HiveBucketFunction>> Create(
        const std::vector<HiveFieldInfo>& field_infos);

    int32_t Bucket(const BinaryRow& row, int32_t num_buckets) const override;

 private:
    explicit HiveBucketFunction(const std::vector<HiveFieldInfo>& field_infos);

    /// Compute the Hive hash for a single field value.
    int32_t ComputeHash(const BinaryRow& row, int32_t field_index) const;

    /// Mod operation that always returns non-negative result.
    static int32_t Mod(int32_t value, int32_t divisor);

    std::vector<HiveFieldInfo> field_infos_;
};

}  // namespace paimon
