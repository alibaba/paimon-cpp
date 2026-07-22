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
#include <string>
#include <vector>

#include "paimon/common/data/shredding/shredding_write_plan_factory.h"
#include "paimon/core/core_options.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class Schema;
}  // namespace arrow

namespace paimon {

class MapSharedShreddingContext;

/// Creates MAP shared-shredding batch converters driven by the cross-file adaptive-K context.
/// The write plan is never inferred from samples; per-file field metadata is persisted into the
/// file footer by the metadata finalizer.
class MapSharedShreddingWritePlanFactory : public ShreddingWritePlanFactory {
 public:
    MapSharedShreddingWritePlanFactory(const CoreOptions& options,
                                       const std::shared_ptr<arrow::Schema>& write_schema,
                                       const std::shared_ptr<MapSharedShreddingContext>& context,
                                       const std::shared_ptr<MemoryPool>& pool);

    bool ShouldCreateWritePlan() const override;

    bool ShouldInferWritePlan() const override;

    int32_t InferBufferRowCount() const override;

    Result<std::shared_ptr<ShreddingBatchConverter>> CreateConverter(
        const std::string& file_format_identifier,
        const std::vector<std::shared_ptr<arrow::Array>>& sample_batches) const override;

    MetadataFinalizer CreateMetadataFinalizer(
        const std::shared_ptr<ShreddingBatchConverter>& converter) const override;

 private:
    CoreOptions options_;
    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<MapSharedShreddingContext> context_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
