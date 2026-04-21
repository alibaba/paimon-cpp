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

#include "paimon/core/bucket/mod_bucket_function.h"

#include <cassert>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/status.h"

namespace paimon {

namespace {

/// Equivalent to Java's Math.floorMod semantics.
/// The result always has the same sign as the divisor (y), or is zero.
/// Works for both int32_t and int64_t as T.
template <typename T>
inline int32_t FloorMod(T x, int32_t y) {
    auto mod = static_cast<int64_t>(x) % static_cast<int64_t>(y);
    // If the signs of mod and y differ and mod is not zero, adjust.
    if ((mod ^ static_cast<int64_t>(y)) < 0 && mod != 0) {
        mod += y;
    }
    return static_cast<int32_t>(mod);
}

}  // namespace

ModBucketFunction::ModBucketFunction(FieldType bucket_key_type)
    : bucket_key_type_(bucket_key_type) {}

Result<std::unique_ptr<ModBucketFunction>> ModBucketFunction::Create(FieldType bucket_key_type) {
    if (bucket_key_type != FieldType::INT && bucket_key_type != FieldType::BIGINT) {
        return Status::Invalid(
            fmt::format("ModBucketFunction only supports INT or BIGINT bucket key type, but got {}",
                        FieldTypeUtils::FieldTypeToString(bucket_key_type)));
    }
    return std::unique_ptr<ModBucketFunction>(new ModBucketFunction(bucket_key_type));
}

int32_t ModBucketFunction::Bucket(const BinaryRow& row, int32_t num_buckets) const {
    switch (bucket_key_type_) {
        case FieldType::INT:
            return FloorMod(row.GetInt(0), num_buckets);
        case FieldType::BIGINT:
            return FloorMod(row.GetLong(0), num_buckets);
        default:
            // This should never happen since Create() validates the type.
            assert(false);
            return 0;
    }
}

}  // namespace paimon
