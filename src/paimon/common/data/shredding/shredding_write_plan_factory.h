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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/data/shredding/shredding_batch_converter.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class Schema;
}  // namespace arrow

namespace paimon {

/// Decides whether write batches must be rewritten into a physical (shredded) layout before
/// they reach the format writer, and creates the per-file batch converter -- either immediately
/// from configuration or, for inference, from sampled logical batches buffered by the writer.
class ShreddingWritePlanFactory {
 public:
    /// Finalizes per-file shredding metadata; the returned schema (or nullptr) is persisted into
    /// the file footer right before the file is finished.
    using MetadataFinalizer = std::function<Result<std::shared_ptr<arrow::Schema>>()>;

    virtual ~ShreddingWritePlanFactory() = default;

    /// Whether a write plan (immediate or inferred) applies to the write schema.
    virtual bool ShouldCreateWritePlan() const = 0;

    /// Whether the write plan must be inferred from sampled rows instead of configuration.
    virtual bool ShouldInferWritePlan() const = 0;

    /// The number of rows buffered per file to sample the inferred write plan from.
    virtual int32_t InferBufferRowCount() const = 0;

    /// Creates the per-file batch converter. `sample_batches` holds the logical batches sampled
    /// for inference and is empty when the plan comes from configuration. Returns nullptr when
    /// no conversion is useful for this file (the file is written with the logical schema).
    virtual Result<std::shared_ptr<ShreddingBatchConverter>> CreateConverter(
        const std::string& file_format_identifier,
        const std::vector<std::shared_ptr<arrow::Array>>& sample_batches) const = 0;

    /// The per-file metadata finalizer persisted into the file footer, or nullptr when the
    /// physical schema is self-describing (as it is for VARIANT shredding).
    virtual MetadataFinalizer CreateMetadataFinalizer(
        const std::shared_ptr<ShreddingBatchConverter>& converter) const {
        return nullptr;
    }
};

}  // namespace paimon
