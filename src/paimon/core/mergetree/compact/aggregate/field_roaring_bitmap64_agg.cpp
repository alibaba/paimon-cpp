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

#include "paimon/core/mergetree/compact/aggregate/field_roaring_bitmap64_agg.h"

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregate_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

Result<std::unique_ptr<FieldRoaringBitmap64Agg>> FieldRoaringBitmap64Agg::Create(
    const std::shared_ptr<arrow::DataType>& field_type, const std::string& field_name) {
    if (field_type->id() != arrow::Type::BINARY) {
        return Status::Invalid(
            fmt::format("invalid field type {} for field '{}' of {}, supposed to be binary",
                        field_type->ToString(), field_name, NAME));
    }
    return std::unique_ptr<FieldRoaringBitmap64Agg>(new FieldRoaringBitmap64Agg(field_type));
}

VariantType FieldRoaringBitmap64Agg::Agg(const VariantType& accumulator,
                                         const VariantType& input_field) {
    Result<VariantType> result = AggResult(accumulator, input_field);
    return result.ok() ? std::move(result).value() : accumulator;
}

Result<VariantType> FieldRoaringBitmap64Agg::AggResult(const VariantType& accumulator,
                                                       const VariantType& input_field) {
    bool accumulator_null = DataDefine::IsVariantNull(accumulator);
    bool input_null = DataDefine::IsVariantNull(input_field);
    if (accumulator_null || input_null) {
        return accumulator_null ? input_field : FieldAggregateUtils::OwnedBinary(accumulator);
    }
    std::string_view accumulator_bytes = DataDefine::GetStringView(accumulator);
    std::string_view input_bytes = DataDefine::GetStringView(input_field);
    RoaringBitmap64 accumulator_bitmap;
    RoaringBitmap64 input_bitmap;
    PAIMON_RETURN_NOT_OK(
        accumulator_bitmap.Deserialize(accumulator_bytes.data(), accumulator_bytes.size()));
    PAIMON_RETURN_NOT_OK(input_bitmap.Deserialize(input_bytes.data(), input_bytes.size()));
    accumulator_bitmap |= input_bitmap;
    return VariantType(std::shared_ptr<Bytes>(accumulator_bitmap.Serialize(/*pool=*/nullptr)));
}

}  // namespace paimon
