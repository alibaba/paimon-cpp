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

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed
 * by the Apache Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. */

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace paimon {

/// Defines a valid shredding schema, as described in the parquet-format VariantShredding.md
/// specification. A shredding schema contains a `value` and an optional `typed_value` field. If a
/// `typed_value` is an array or struct, it recursively contains its own shredding schema for
/// elements and fields, respectively. The schema also contains a `metadata` field at the top
/// level, but not in recursively shredded fields.
class VariantSchema {
 public:
    enum class ScalarKind {
        kBoolean,
        kByte,
        kShort,
        kInt,
        kLong,
        kFloat,
        kDouble,
        kString,
        kBinary,
        kDecimal,
        kDate,
        kTimestampLtz,
        kTimestampNtz,
        kUuid,
    };

    struct ScalarType {
        ScalarKind kind;
        // Only meaningful when `kind` is `kDecimal`.
        int32_t precision = 0;
        int32_t scale = 0;
    };

    /// Represents one field of an object in the shredding schema.
    struct ObjectField {
        std::string name;
        std::shared_ptr<VariantSchema> schema;
    };

    /// The index of the typed_value, value, and metadata fields in the schema, respectively. If a
    /// given field is not in the schema, its value must be set to -1 to indicate that it is
    /// invalid. The indices of valid fields are contiguous and start from 0.
    int32_t typed_idx = -1;
    int32_t variant_idx = -1;
    /// Must be non-negative in the top-level schema, and -1 at all other nesting levels.
    int32_t top_level_metadata_idx = -1;
    /// The number of fields in the schema, i.e. a value between 1 and 3, depending on which of
    /// value, typed_value and metadata are present.
    int32_t num_fields = 0;

    /// Exactly one of the following describes typed_value (or none if there is no typed_value).
    std::optional<ScalarType> scalar_schema;
    bool has_object_schema = false;
    std::vector<ObjectField> object_schema;
    /// Fast lookup of object fields by name; values are indices into `object_schema`.
    std::unordered_map<std::string, int32_t> object_schema_map;
    std::shared_ptr<VariantSchema> array_schema;

    /// Whether the variant column is unshredded. The user is not required to do anything special,
    /// but can have certain optimizations for unshredded variants.
    bool IsUnshredded() const {
        return top_level_metadata_idx >= 0 && variant_idx >= 0 && typed_idx < 0;
    }
};

}  // namespace paimon
