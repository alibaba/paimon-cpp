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
#include <optional>
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

/// Creates VARIANT shredding batch converters, either from the configured
/// `variant.shreddingSchema` or, when `variant.inferShreddingSchema` is enabled, by inferring a
/// shredding schema from sampled rows buffered per file.
class VariantShreddingWritePlanFactory : public ShreddingWritePlanFactory {
 public:
    VariantShreddingWritePlanFactory(const CoreOptions& options,
                                     const std::shared_ptr<arrow::Schema>& write_schema,
                                     const std::shared_ptr<MemoryPool>& pool);

    bool ShouldCreateWritePlan() const override;

    bool ShouldInferWritePlan() const override;

    int32_t InferBufferRowCount() const override;

    Result<std::shared_ptr<ShreddingBatchConverter>> CreateConverter(
        const std::string& file_format_identifier,
        const std::vector<std::shared_ptr<arrow::Array>>& sample_batches) const override;

 private:
    bool HasConfiguredShreddingSchema() const;
    bool ContainsVariantField() const;

    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<MemoryPool> pool_;

    // Option values resolved at construction; a parse failure is deferred to CreateConverter.
    std::optional<std::string> configured_schema_;
    bool infer_enabled_ = false;
    int32_t max_schema_width_ = 0;
    int32_t max_schema_depth_ = 0;
    double min_field_cardinality_ratio_ = 0.0;
    int32_t max_infer_buffer_row_ = 0;
    Status config_status_ = Status::OK();
};

}  // namespace paimon
