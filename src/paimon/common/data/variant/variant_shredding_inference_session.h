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
#include <optional>

#include "paimon/common/data/variant/infer_variant_shredding_schema.h"
#include "paimon/result.h"

namespace paimon {

/// Rolling-writer-scoped Variant inference state. Evidence is bounded and is committed only
/// after the corresponding file has closed successfully.
class VariantShreddingInferenceSession {
 public:
    VariantShreddingInferenceSession(InferVariantShreddingSchema inferrer,
                                     int32_t effective_sample_size, double admission_ratio,
                                     double retention_ratio);

    bool HasPrior() const {
        return has_committed_evidence_;
    }

    /// Infers one complete physical row schema and retains the corresponding evidence as pending
    /// state until the file using this schema has completed successfully.
    Result<std::shared_ptr<arrow::Schema>> InferSchema(
        const InferVariantShreddingSchema::SampleBatches& samples);

    Status CommitPendingInference();

 private:
    InferVariantShreddingSchema inferrer_;
    int32_t effective_sample_size_;
    double admission_ratio_;
    double retention_ratio_;

    bool has_committed_evidence_ = false;
    InferVariantShreddingSchema::InferenceEvidence committed_evidence_;
    InferVariantShreddingSchema::SelectedSchemas committed_selected_schemas_;
    std::optional<InferVariantShreddingSchema::AdaptiveInferenceResult> pending_result_;
};

}  // namespace paimon
