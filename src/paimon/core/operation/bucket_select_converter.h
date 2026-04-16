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
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "paimon/result.h"

namespace paimon {
class MemoryPool;
class Predicate;
class TableSchema;

/// Derives target bucket IDs from predicates on bucket key columns.
///
/// For a point query like `pk = 'xxx'`, this converter extracts the equality predicate,
/// computes the bucket hash (compatible with Java Paimon), and returns the matching bucket ID.
/// This allows the scan to skip files from non-matching buckets.
///
/// Algorithm (mirrors Java BucketSelectConverter):
/// 1. Split predicate by AND
/// 2. For each AND-child, split by OR
/// 3. Extract EQUAL/IN predicates on bucket key columns
/// 4. Cartesian product of values across all bucket key columns
/// 5. Hash each combination to get bucket IDs
class BucketSelectConverter {
 public:
    /// Convert a predicate into a set of matching bucket IDs.
    /// Returns nullopt if the predicate cannot be used to derive buckets
    /// (e.g., missing bucket key columns, too many combinations, or non-equality predicates).
    static Result<std::optional<std::set<int32_t>>> Convert(
        const std::shared_ptr<Predicate>& predicate, const std::vector<std::string>& bucket_keys,
        int32_t num_buckets, const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<MemoryPool>& pool);

 private:
    static constexpr int32_t kMaxValues = 1000;
};

}  // namespace paimon
