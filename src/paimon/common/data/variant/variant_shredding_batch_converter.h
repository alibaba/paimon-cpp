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

#include "paimon/common/data/shredding/shredding_batch_converter.h"
#include "paimon/common/data/variant/variant_shredding_write_plan.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

struct ArrowArray;

namespace arrow {
class MemoryPool;
class Schema;
}  // namespace arrow

namespace paimon {

/// Converts logical batches containing VARIANT columns into physical batches where each planned
/// variant column is replaced by its shredded struct representation. Unplanned columns are
/// passed through unchanged.
class VariantShreddingBatchConverter : public ShreddingBatchConverter {
 public:
    static Result<std::shared_ptr<VariantShreddingBatchConverter>> Create(
        const std::shared_ptr<VariantShreddingWritePlan>& plan,
        const std::shared_ptr<MemoryPool>& pool);

    /// The physical schema produced by this converter.
    const std::shared_ptr<arrow::Schema>& GetPhysicalSchema() const override;

    /// Converts a logical batch to a physical batch.
    /// @param logical_batch Input ArrowArray (C ABI) with the logical schema. Consumed on
    ///        success.
    /// @return Owned physical ArrowArray (C ABI) with the physical schema.
    Result<std::unique_ptr<ArrowArray>> Convert(ArrowArray* logical_batch) override;

 private:
    VariantShreddingBatchConverter(const std::shared_ptr<VariantShreddingWritePlan>& plan,
                                   const std::shared_ptr<MemoryPool>& pool);

    std::shared_ptr<VariantShreddingWritePlan> plan_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

}  // namespace paimon
