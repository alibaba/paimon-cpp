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

#include "paimon/core/mergetree/compact/aggregate/field_sum_agg.h"

#include <cassert>
#include <cstdint>

#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/data/decimal.h"
#include "paimon/status.h"

namespace paimon {
Result<FieldSumAgg::FieldSumFunc> FieldSumAgg::CreateSumFunc(
    const std::shared_ptr<arrow::DataType>& field_type) {
    arrow::Type::type type = field_type->id();
    switch (type) {
        case arrow::Type::type::INT8:
            return FieldSumFunc(
                [](const VariantType& accumulator, const VariantType& input_field) -> VariantType {
                    char sum = DataDefine::GetVariantValue<char>(accumulator) +
                               DataDefine::GetVariantValue<char>(input_field);
                    return sum;
                });
        case arrow::Type::type::INT16:
            return FieldSumFunc(
                [](const VariantType& accumulator, const VariantType& input_field) -> VariantType {
                    int16_t sum = DataDefine::GetVariantValue<int16_t>(accumulator) +
                                  DataDefine::GetVariantValue<int16_t>(input_field);
                    return sum;
                });
        case arrow::Type::type::INT32:
            return FieldSumFunc(
                [](const VariantType& accumulator, const VariantType& input_field) -> VariantType {
                    int32_t sum = DataDefine::GetVariantValue<int32_t>(accumulator) +
                                  DataDefine::GetVariantValue<int32_t>(input_field);
                    return sum;
                });
        case arrow::Type::type::INT64:
            return FieldSumFunc(
                [](const VariantType& accumulator, const VariantType& input_field) -> VariantType {
                    int64_t sum = DataDefine::GetVariantValue<int64_t>(accumulator) +
                                  DataDefine::GetVariantValue<int64_t>(input_field);
                    return sum;
                });
        case arrow::Type::type::FLOAT:
            return FieldSumFunc(
                [](const VariantType& accumulator, const VariantType& input_field) -> VariantType {
                    float sum = DataDefine::GetVariantValue<float>(accumulator) +
                                DataDefine::GetVariantValue<float>(input_field);
                    return sum;
                });
        case arrow::Type::type::DOUBLE:
            return FieldSumFunc(
                [](const VariantType& accumulator, const VariantType& input_field) -> VariantType {
                    double sum = DataDefine::GetVariantValue<double>(accumulator) +
                                 DataDefine::GetVariantValue<double>(input_field);
                    return sum;
                });
        case arrow::Type::type::DECIMAL128: {
            return FieldSumFunc(
                [](const VariantType& accumulator, const VariantType& input_field) -> VariantType {
                    auto v1 = DataDefine::GetVariantValue<Decimal>(accumulator);
                    auto v2 = DataDefine::GetVariantValue<Decimal>(input_field);
                    assert(v1.Precision() == v2.Precision() && v1.Scale() == v2.Scale());
                    return Decimal(v1.Precision(), v1.Scale(), v1.Value() + v2.Value());
                });
        }
        default:
            return Status::Invalid(
                fmt::format("type {} not support in FieldSumAgg", field_type->ToString()));
    }
}

Result<FieldSumAgg::FieldNegFunc> FieldSumAgg::CreateNegFunc(
    const std::shared_ptr<arrow::DataType>& field_type) {
    arrow::Type::type type = field_type->id();
    switch (type) {
        case arrow::Type::type::INT8:
            return FieldNegFunc([](const VariantType& input_field) -> VariantType {
                char value = DataDefine::GetVariantValue<char>(input_field);
                return static_cast<char>(-value);
            });
        case arrow::Type::type::INT16:
            return FieldNegFunc([](const VariantType& input_field) -> VariantType {
                auto value = DataDefine::GetVariantValue<int16_t>(input_field);
                return static_cast<int16_t>(-value);
            });
        case arrow::Type::type::INT32:
            return FieldNegFunc([](const VariantType& input_field) -> VariantType {
                auto value = DataDefine::GetVariantValue<int32_t>(input_field);
                return (-value);
            });
        case arrow::Type::type::INT64:
            return FieldNegFunc([](const VariantType& input_field) -> VariantType {
                auto value = DataDefine::GetVariantValue<int64_t>(input_field);
                return (-value);
            });
        case arrow::Type::type::FLOAT:
            return FieldNegFunc([](const VariantType& input_field) -> VariantType {
                auto value = DataDefine::GetVariantValue<float>(input_field);
                return (-value);
            });
        case arrow::Type::type::DOUBLE:
            return FieldNegFunc([](const VariantType& input_field) -> VariantType {
                auto value = DataDefine::GetVariantValue<double>(input_field);
                return (-value);
            });
        case arrow::Type::type::DECIMAL128: {
            return FieldNegFunc([](const VariantType& input_field) -> VariantType {
                auto value = DataDefine::GetVariantValue<Decimal>(input_field);
                return Decimal(value.Precision(), value.Scale(), -value.Value());
            });
        }
        default:
            return Status::Invalid(
                fmt::format("type {} not support in FieldSumAgg", field_type->ToString()));
    }
}

}  // namespace paimon
