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

namespace paimon {

/// Specifies the bucket function type for paimon bucket.
/// This determines how rows are assigned to buckets during data writing.
enum class BucketFunctionType {
    /// Default bucket function using hash code.
    /// Computes the hash code of the bucket key row and assigns the bucket
    /// based on the absolute value of (hashCode % numBuckets).
    DEFAULT = 1,
    /// Mod bucket function using modulo operation on bucket key.
    /// Applies Java's Math.floorMod semantics on the bucket key value (INT or BIGINT)
    /// to determine the bucket assignment.
    MOD = 2,
    /// Hive-compatible bucket function.
    /// Uses Hive's ObjectInspectorUtils hash implementation to ensure
    /// consistent bucket assignment between Paimon and Hive bucketed tables.
    HIVE = 3
};

}  // namespace paimon
