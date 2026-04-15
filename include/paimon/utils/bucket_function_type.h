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

#include "paimon/defs.h"
#include "paimon/visibility.h"

namespace paimon {

/// Specifies the bucket function type for paimon bucket.
/// This determines how rows are assigned to buckets during data writing.
enum class BucketFunctionType {
    /// The default bucket function which will use arithmetic:
    /// bucket_id = abs(hash_bucket_binary_row % numBuckets) to get bucket.
    DEFAULT = 1,
    /// The modulus bucket function which will use modulus arithmetic:
    /// bucket_id = floorMod(bucket_key_value, numBuckets) to get bucket.
    /// Note: the bucket key must be a single field of INT or BIGINT datatype.
    MOD = 2,
    /// The hive bucket function which will use hive-compatible hash arithmetic to get bucket.
    HIVE = 3
};

/// Describes a field's type information needed for Hive hashing.
struct PAIMON_EXPORT HiveFieldInfo {
    FieldType type;
    int32_t precision = 0;  // Used for DECIMAL type
    int32_t scale = 0;      // Used for DECIMAL type

    explicit HiveFieldInfo(FieldType t) : type(t) {}
    HiveFieldInfo(FieldType t, int32_t p, int32_t s) : type(t), precision(p), scale(s) {}
};

}  // namespace paimon
