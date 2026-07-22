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
#include <unordered_map>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace arrow {
class DataType;
class Field;
class KeyValueMetadata;
class Schema;
}  // namespace arrow

namespace paimon {

/// Utils for the Paimon Variant type, whose underlying Arrow representation is
/// `struct<value: binary not null, metadata: binary not null>` marked with the
/// `paimon.extension.type = paimon.type.variant` field metadata (see `VariantDefs`).
class PAIMON_EXPORT VariantTypeUtils {
 public:
    VariantTypeUtils() = delete;
    ~VariantTypeUtils() = delete;

    /// Whether the field is a Paimon Variant field (a struct with the variant metadata marker).
    static bool IsVariantField(const std::shared_ptr<arrow::Field>& field);

    /// Whether `type` is the unshredded variant physical type
    /// `struct<value: binary not null, metadata: binary not null>` (field metadata ignored).
    static bool IsUnshreddedVariantType(const std::shared_ptr<arrow::DataType>& type);

    /// Whether the metadata carries the Paimon Variant extension type marker.
    static bool IsVariantMetadata(const std::shared_ptr<const arrow::KeyValueMetadata>& metadata);

    /// The unshredded physical Arrow type of a variant field:
    /// `struct<value: binary not null, metadata: binary not null>` with paimon field ids 0/1 on
    /// the children.
    static std::shared_ptr<arrow::DataType> UnshreddedStructType();

    /// Creates a Variant Arrow field with the variant metadata marker.
    static std::shared_ptr<arrow::Field> ToArrowField(
        const std::string& field_name, bool nullable = true,
        std::unordered_map<std::string, std::string> metadata = {});

    /// Validates that a variant-marked field has the expected physical shape:
    /// `struct<value: binary not null, metadata: binary not null>`.
    static Status ValidateVariantShape(const std::shared_ptr<arrow::Field>& field);

    /// Whether the schema contains a variant field, at the top level or nested inside structs,
    /// arrays or maps.
    static bool ContainsVariantField(const std::shared_ptr<arrow::Schema>& schema);

    /// Whether the field contains a variant field, itself included, at any nesting level.
    static bool ContainsVariantField(const std::shared_ptr<arrow::Field>& field);
};

}  // namespace paimon
