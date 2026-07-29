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

#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"

namespace paimon {

/// Unions serialized 64-bit Roaring Bitmap fields.
class FieldRoaringBitmap64Agg : public FieldAggregator {
 public:
    static constexpr char NAME[] = "rbm64";

    /// Create an rbm64 aggregator for a binary field.
    ///
    /// @param field_type Type of the aggregated field.
    /// @param field_name Name of the aggregated field.
    /// @return An rbm64 aggregator, or an error Status.
    static Result<std::unique_ptr<FieldRoaringBitmap64Agg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type, const std::string& field_name);

    VariantType Agg(const VariantType& accumulator, const VariantType& input_field) override;
    Result<VariantType> AggResult(const VariantType& accumulator,
                                  const VariantType& input_field) override;

 private:
    explicit FieldRoaringBitmap64Agg(const std::shared_ptr<arrow::DataType>& field_type)
        : FieldAggregator(NAME, field_type) {}
};

}  // namespace paimon
