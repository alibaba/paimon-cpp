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

#include "paimon/core/bucket/bucket_function.h"
#include "paimon/defs.h"
#include "paimon/result.h"

namespace paimon {

/// Mod bucket function that uses modulo operation on the bucket key value.
/// The bucket key must be a single field of INT or BIGINT type.
/// This implements Java's Math.floorMod semantics for negative numbers.
class ModBucketFunction : public BucketFunction {
 public:
    /// Create a ModBucketFunction with the given bucket key type.
    /// @param bucket_key_type The type of the single bucket key field. Must be INT or BIGINT.
    /// @return A Result containing the ModBucketFunction or an error status.
    static Result<std::unique_ptr<ModBucketFunction>> Create(FieldType bucket_key_type);

    int32_t Bucket(const BinaryRow& row, int32_t num_buckets) const override;

 private:
    explicit ModBucketFunction(FieldType bucket_key_type);

    FieldType bucket_key_type_;
};

}  // namespace paimon
