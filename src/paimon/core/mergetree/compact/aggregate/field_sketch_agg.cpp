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

#include "paimon/core/mergetree/compact/aggregate/field_sketch_agg.h"

#include <cstring>
#include <exception>
#include <utility>
#include <vector>

#include "DataSketches/hll.hpp"
#include "DataSketches/theta_sketch.hpp"
#include "DataSketches/theta_union.hpp"
#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregate_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
namespace {

template <typename T>
std::shared_ptr<Bytes> CopyBytes(const std::vector<T>& serialized, MemoryPool* pool) {
    pooled_unique_ptr<Bytes> result = Bytes::AllocateBytes(serialized.size() * sizeof(T), pool);
    if (!serialized.empty()) {
        std::memcpy(result->data(), serialized.data(), result->size());
    }
    return std::shared_ptr<Bytes>(std::move(result));
}

Status ValidateSketchType(const std::shared_ptr<arrow::DataType>& field_type,
                          const std::string& field_name, const char* name) {
    if (field_type->id() != arrow::Type::BINARY) {
        return Status::Invalid(
            fmt::format("invalid field type {} for field '{}' of {}, supposed to be binary",
                        field_type->ToString(), field_name, name));
    }
    return Status::OK();
}

}  // namespace

Result<std::unique_ptr<FieldHllSketchAgg>> FieldHllSketchAgg::Create(
    const std::shared_ptr<arrow::DataType>& field_type, const std::string& field_name,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_RETURN_NOT_OK(ValidateSketchType(field_type, field_name, NAME));
    return std::unique_ptr<FieldHllSketchAgg>(new FieldHllSketchAgg(field_type, pool));
}

Result<VariantType> FieldHllSketchAgg::Agg(const VariantType& accumulator,
                                           const VariantType& input_field) {
    bool accumulator_null = DataDefine::IsVariantNull(accumulator);
    bool input_null = DataDefine::IsVariantNull(input_field);
    if (accumulator_null && input_null) {
        return VariantType(NullType());
    }
    if (accumulator_null || input_null) {
        // AggReversed swaps the arguments, so either side may be the row-owned accumulator
        return FieldAggregateUtils::OwnedBinary(accumulator_null ? input_field : accumulator,
                                                pool_.get());
    }
    std::string_view accumulator_bytes = DataDefine::GetStringView(accumulator);
    std::string_view input_bytes = DataDefine::GetStringView(input_field);
    try {
        datasketches::hll_sketch accumulator_sketch = datasketches::hll_sketch::deserialize(
            accumulator_bytes.data(), accumulator_bytes.size());
        datasketches::hll_sketch input_sketch =
            datasketches::hll_sketch::deserialize(input_bytes.data(), input_bytes.size());
        datasketches::hll_union sketch_union(input_sketch.get_lg_config_k());
        sketch_union.update(input_sketch);
        sketch_union.update(accumulator_sketch);
        datasketches::hll_sketch result = sketch_union.get_result(datasketches::HLL_4);
        return VariantType(CopyBytes(result.serialize_compact(), pool_.get()));
    } catch (const std::exception& exception) {
        return Status::Invalid(
            fmt::format("Unable to deserialize or union HLL sketch: {}", exception.what()));
    } catch (...) {
        return Status::Invalid("Unable to deserialize or union HLL sketch");
    }
}

Result<std::unique_ptr<FieldThetaSketchAgg>> FieldThetaSketchAgg::Create(
    const std::shared_ptr<arrow::DataType>& field_type, const std::string& field_name,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_RETURN_NOT_OK(ValidateSketchType(field_type, field_name, NAME));
    return std::unique_ptr<FieldThetaSketchAgg>(new FieldThetaSketchAgg(field_type, pool));
}

Result<VariantType> FieldThetaSketchAgg::Agg(const VariantType& accumulator,
                                             const VariantType& input_field) {
    bool accumulator_null = DataDefine::IsVariantNull(accumulator);
    bool input_null = DataDefine::IsVariantNull(input_field);
    if (accumulator_null && input_null) {
        return VariantType(NullType());
    }
    if (accumulator_null || input_null) {
        // AggReversed swaps the arguments, so either side may be the row-owned accumulator
        return FieldAggregateUtils::OwnedBinary(accumulator_null ? input_field : accumulator,
                                                pool_.get());
    }
    std::string_view accumulator_bytes = DataDefine::GetStringView(accumulator);
    std::string_view input_bytes = DataDefine::GetStringView(input_field);
    try {
        datasketches::compact_theta_sketch accumulator_sketch =
            datasketches::compact_theta_sketch::deserialize(accumulator_bytes.data(),
                                                            accumulator_bytes.size());
        datasketches::compact_theta_sketch input_sketch =
            datasketches::compact_theta_sketch::deserialize(input_bytes.data(), input_bytes.size());
        datasketches::theta_union sketch_union = datasketches::theta_union::builder().build();
        sketch_union.update(accumulator_sketch);
        sketch_union.update(input_sketch);
        return VariantType(
            CopyBytes(sketch_union.get_result(/*ordered=*/true).serialize(), pool_.get()));
    } catch (const std::exception& exception) {
        return Status::Invalid(
            fmt::format("Unable to deserialize or union theta sketch: {}", exception.what()));
    } catch (...) {
        return Status::Invalid("Unable to deserialize or union theta sketch");
    }
}

}  // namespace paimon
