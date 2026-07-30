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

#include "paimon/common/data/variant/variant_schema.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {

/// Utils for converting between shredding schemas (`VariantSchema`) and their physical Arrow
/// representation, mirroring the Java `PaimonShreddingUtils` schema functions.
class VariantShreddingUtils {
 public:
    VariantShreddingUtils() = delete;
    ~VariantShreddingUtils() = delete;

    /// Given an expected schema of a Variant value, returns a suitable physical schema for
    /// shredding, by inserting appropriate intermediate value/typed_value fields at each level.
    /// For example, to represent the JSON `{"a": 1, "b": "hello"}`, the schema
    /// `struct{a: int32, b: string}` could be passed into this function, and it would return the
    /// shredding schema: `struct{metadata: binary, value: binary, typed_value: struct{a:
    /// struct{value: binary, typed_value: int32}, b: struct{value: binary, typed_value:
    /// string}}}`.
    static Result<std::shared_ptr<arrow::DataType>> VariantShreddingSchema(
        const std::shared_ptr<arrow::DataType>& shredding_type);

    /// Builds a `VariantSchema` from the physical shredded struct type (the inverse of
    /// `VariantShreddingSchema`), validating field names and types.
    static Result<std::shared_ptr<VariantSchema>> BuildVariantSchema(
        const std::shared_ptr<arrow::DataType>& struct_type);

    /// The Arrow type of the typed_value column for a scalar shredding schema.
    static Result<std::shared_ptr<arrow::DataType>> ScalarSchemaToArrowType(
        const VariantSchema::ScalarType& scalar);

    /// Whether the physical struct type of a variant field in a data file is shredded (contains
    /// a `typed_value` child).
    static bool IsShreddedFileType(const std::shared_ptr<arrow::DataType>& file_variant_type);

    /// Whether the physical struct uses the untyped inference layout `{metadata, value}`.
    static bool IsUntypedPhysicalVariantType(
        const std::shared_ptr<arrow::DataType>& file_variant_type);
};

}  // namespace paimon
