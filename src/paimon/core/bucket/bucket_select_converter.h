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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/bucket/bucket_function_type.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {

class BinaryRowWriter;
class BucketFunction;
class MemoryPool;
class Predicate;

/// Converts predicates on bucket key fields to a target bucket ID.
/// When all bucket key fields have EQUAL predicates, the converter computes
/// which bucket the data must reside in, enabling bucket pruning during scan.
class BucketSelectConverter {
 public:
    BucketSelectConverter() = delete;
    ~BucketSelectConverter() = delete;

    /// Convert predicates to a target bucket ID.
    /// @param predicate The predicate (possibly compound AND) to analyze.
    /// @param bucket_key_names Ordered bucket key field names.
    /// @param bucket_key_arrow_types Ordered Arrow data types for bucket key fields.
    ///        FieldType is derived from these automatically.
    /// @param bucket_function_type The bucket function type (DEFAULT, MOD, HIVE).
    /// @param num_buckets The total number of buckets.
    /// @param pool Memory pool for BinaryRow construction.
    /// @return The target bucket ID, or nullopt if predicates don't fully constrain all bucket
    /// keys.
    static Result<std::optional<int32_t>> Convert(
        const std::shared_ptr<Predicate>& predicate,
        const std::vector<std::string>& bucket_key_names,
        const std::vector<std::shared_ptr<arrow::DataType>>& bucket_key_arrow_types,
        BucketFunctionType bucket_function_type, int32_t num_buckets, MemoryPool* pool);

 private:
    /// Extract single literal per bucket key field from EQUAL predicates.
    /// Splits the predicate by AND and looks for EQUAL leaf predicates on bucket key fields.
    /// @return A map from field name to literal, or nullopt if not all bucket keys are constrained.
    static std::optional<std::map<std::string, Literal>> ExtractEqualLiterals(
        const std::shared_ptr<Predicate>& predicate,
        const std::vector<std::string>& bucket_key_names);

    /// Write a Literal value to a BinaryRowWriter at the given position.
    static Status WriteLiteralToRow(int32_t pos, const Literal& literal, FieldType field_type,
                                    const std::shared_ptr<arrow::DataType>& arrow_type,
                                    BinaryRowWriter* writer);

    /// Create the appropriate BucketFunction for the given type.
    static Result<std::unique_ptr<BucketFunction>> CreateBucketFunction(
        BucketFunctionType type, const std::vector<FieldType>& bucket_key_types,
        const std::vector<std::shared_ptr<arrow::DataType>>& bucket_key_arrow_types);
};

}  // namespace paimon
