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
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "paimon/arrow/abi.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {

/// Parsed file-level meta for one shared-shredding MAP column.
struct PAIMON_EXPORT MapSharedShreddingFieldMeta {
    /// field_name -> field_id
    std::map<std::string, int32_t> name_to_id;
    /// field_id -> ordered physical column indices
    std::map<int32_t, std::vector<int32_t>> field_to_columns;
    /// Set of field_ids that ever spilled into __overflow
    std::set<int32_t> overflow_field_set;
    /// Number of physical columns K in this file
    int32_t num_columns = 0;
    /// Maximum row width observed in this file
    int32_t max_row_width = 0;

    bool operator==(const MapSharedShreddingFieldMeta& other) const {
        if (this == &other) {
            return true;
        }
        return name_to_id == other.name_to_id && field_to_columns == other.field_to_columns &&
               overflow_field_set == other.overflow_field_set && num_columns == other.num_columns &&
               max_row_width == other.max_row_width;
    }
};

class PAIMON_EXPORT MapSharedShreddingSchemaUtils {
 public:
    MapSharedShreddingSchemaUtils() = delete;
    ~MapSharedShreddingSchemaUtils() = delete;

    /// Converts a logical schema to a physical schema by replacing shredding MAP columns
    /// with their physical Struct representation.
    /// @param logical_schema The original Arrow C schema with MAP<STRING, T> columns.
    ///        Ownership of schema resources is transferred to this method.
    /// @param field_to_num_columns Map from field name to its physical column count K.
    ///        Each shredding column can have its own width.
    /// @return The exported Arrow C schema for file writing.
    static Result<std::unique_ptr<::ArrowSchema>> LogicalToPhysicalSchema(
        std::unique_ptr<::ArrowSchema> logical_schema,
        const std::map<std::string, int32_t>& field_to_num_columns);

    /// Attaches shared-shredding metadata to fields in a physical schema.
    /// @param physical_schema The Arrow C physical schema whose fields should receive metadata.
    ///        Ownership of schema resources is transferred to this method.
    /// @param field_name_to_meta Map from physical field name to its shared-shredding metadata.
    ///        Existing shared-shredding metadata keys on matching fields are overwritten.
    /// @param compression Compression codec name for field_dict serialization.
    /// @return A new Arrow C schema with shared-shredding metadata attached to matching fields.
    static Result<std::unique_ptr<::ArrowSchema>> AttachMetadataToSchema(
        std::unique_ptr<::ArrowSchema> physical_schema,
        const std::map<std::string, MapSharedShreddingFieldMeta>& field_name_to_meta,
        const std::string& compression);

    /// Extracts shared-shredding metadata from a named field in a physical schema.
    /// @param physical_schema The Arrow C physical schema that contains the target field.
    ///        Ownership of schema resources is transferred to this method.
    /// @param field_name The physical field name whose metadata should be extracted.
    /// @return Parsed shared-shredding metadata for the field.
    static Result<MapSharedShreddingFieldMeta> ExtractMetadataFromField(
        std::unique_ptr<::ArrowSchema> physical_schema, const std::string& field_name);
};

}  // namespace paimon
