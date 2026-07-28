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

#include "paimon/common/data/variant/variant_shredding_inference_session.h"

#include <utility>

namespace paimon {

VariantShreddingInferenceSession::VariantShreddingInferenceSession(
    InferVariantShreddingSchema inferrer, int32_t effective_sample_size, double admission_ratio,
    double retention_ratio)
    : inferrer_(std::move(inferrer)),
      effective_sample_size_(effective_sample_size),
      admission_ratio_(admission_ratio),
      retention_ratio_(retention_ratio) {}

Result<std::shared_ptr<arrow::Schema>> VariantShreddingInferenceSession::InferSchema(
    const InferVariantShreddingSchema::SampleBatches& samples) {
    InferVariantShreddingSchema::AdaptiveInferenceResult result;
    if (!has_committed_evidence_) {
        PAIMON_ASSIGN_OR_RAISE(result, inferrer_.InferInitial(samples, effective_sample_size_));
    } else {
        PAIMON_ASSIGN_OR_RAISE(
            result,
            inferrer_.InferAdaptive(committed_evidence_, committed_selected_schemas_, samples,
                                    effective_sample_size_, admission_ratio_, retention_ratio_));
    }
    std::shared_ptr<arrow::Schema> physical_schema = result.physical_schema;
    pending_result_ = std::move(result);
    return physical_schema;
}

Status VariantShreddingInferenceSession::CommitPendingInference() {
    if (!pending_result_.has_value()) {
        return Status::Invalid("No pending Variant inference to commit.");
    }
    committed_evidence_ = std::move(pending_result_->evidence);
    committed_selected_schemas_ = std::move(pending_result_->selected_schemas);
    pending_result_.reset();
    has_committed_evidence_ = true;
    return Status::OK();
}

}  // namespace paimon
