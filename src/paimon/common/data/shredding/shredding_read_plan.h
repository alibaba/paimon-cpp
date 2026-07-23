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

#include <memory>

#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class Field;
class MemoryPool;
}  // namespace arrow

namespace paimon {

/// A per-column read plan translating between a column's logical shape and the physical
/// (shredded, possibly pruned) shape stored in one file: the physical field is pushed down to
/// the format reader and the physical batches are assembled back into logical arrays.
class ShreddingColumnReadPlan {
 public:
    virtual ~ShreddingColumnReadPlan() = default;

    /// The logical field restored on the assembled output.
    virtual const std::shared_ptr<arrow::Field>& LogicalField() const = 0;

    /// The physical file field (possibly a pruned subtree) to read from the file.
    virtual const std::shared_ptr<arrow::Field>& PhysicalField() const = 0;

    /// Assembles the physical column array back into the logical column array.
    virtual Result<std::shared_ptr<arrow::Array>> Assemble(
        const std::shared_ptr<arrow::Array>& physical, arrow::MemoryPool* pool) const = 0;
};

}  // namespace paimon
