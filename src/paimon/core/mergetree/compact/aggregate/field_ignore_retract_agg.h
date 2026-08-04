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

#include <memory>
#include <utility>

#include "paimon/common/data/data_define.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"
#include "paimon/result.h"

namespace paimon {
/// An aggregator which ignores retraction messages.
class FieldIgnoreRetractAgg : public FieldAggregator {
 public:
    explicit FieldIgnoreRetractAgg(std::unique_ptr<FieldAggregator>&& agg)
        : FieldAggregator(agg->GetName(), agg->GetFieldType(), agg->GetPool()),
          agg_(std::move(agg)) {}

    Result<VariantType> Agg(const VariantType& accumulator,
                            const VariantType& input_field) override {
        return agg_->Agg(accumulator, input_field);
    }

    Result<VariantType> Retract(const VariantType& accumulator,
                                const VariantType& input_field) const override {
        return accumulator;
    }

    void Reset() override {
        agg_->Reset();
    }

 private:
    std::unique_ptr<FieldAggregator> agg_;
};
}  // namespace paimon
