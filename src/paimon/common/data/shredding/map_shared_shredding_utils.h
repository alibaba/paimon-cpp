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
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/data/shredding/map_shared_shredding_schema_utils.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class KeyValueMetadata;
class Schema;
}  // namespace arrow

namespace paimon {

class CoreOptions;
class MapSharedShreddingBatchConverter;
class MapSharedShreddingContext;

/// Utility functions for shared-shredding MAP storage layout.
class MapSharedShreddingUtils {
 public:
    MapSharedShreddingUtils() = delete;
    ~MapSharedShreddingUtils() = delete;

    // ---- Column detection ----

    /// Checks whether a given arrow field is MAP<STRING, T> (the type prerequisite for shredding).
    /// @param arrow_type The Arrow data type of the column.
    /// @return true if the type is MAP<STRING, T>.
    static bool IsShreddingKeyMap(const std::shared_ptr<arrow::DataType>& arrow_type);

    /// Creates a MapSharedShreddingContext for the given schema and options.
    /// Returns nullptr if no shredding MAP columns are detected.
    /// @param schema The logical Arrow schema.
    /// @param options CoreOptions containing per-column configuration.
    /// @return Shared context, or nullptr if no shredding columns.
    static Result<std::shared_ptr<MapSharedShreddingContext>> CreateShreddingContext(
        const std::shared_ptr<arrow::Schema>& schema, const CoreOptions& options);
    // ---- Schema conversion ----

    /// Converts a logical schema to a physical schema by replacing shredding MAP columns
    /// with their physical Struct representation.
    /// @param logical_schema The original schema with MAP<STRING, T> columns.
    /// @param field_to_num_columns Map from field name to its physical column count K.
    ///        Each shredding column can have its own width.
    /// @return The physical schema for file writing.
    static Result<std::shared_ptr<arrow::Schema>> LogicalToPhysicalSchema(
        const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::map<std::string, int32_t>& field_to_num_columns);

    /// Builds the physical Arrow type for one shredding MAP column with physical_col_ids.
    /// @param value_type The value type of the original MAP.
    /// @param physical_col_ids The set of physical column ids to include.
    /// @param value_nullable Whether the MAP's value field is nullable.
    /// @param include_overflow Whether to include __overflow column.
    static std::shared_ptr<arrow::DataType> BuildSpecificPhysicalStructType(
        const std::shared_ptr<arrow::DataType>& value_type,
        const std::set<int32_t>& physical_col_ids, bool value_nullable, bool include_overflow);

    // ---- Metadata serialization ----

    /// Serializes shredding metadata and appends entries to an existing KeyValueMetadata.
    /// @param field_meta The field-level shredding metadata to serialize.
    /// @param compression Compression codec name for field_dict compression.
    /// @param[out] metadata The KeyValueMetadata to append entries to.
    static Status SerializeMetadata(const MapSharedShreddingFieldMeta& field_meta,
                                    const std::string& compression,
                                    arrow::KeyValueMetadata* metadata);

    /// Deserializes shredding metadata from file footer KeyValueMetadata (per field).
    /// @param metadata The KeyValueMetadata from file footer.
    /// @param compression Compression codec name.
    /// @return Parsed MapSharedShreddingFieldMeta, or error if metadata is missing/malformed.
    static Result<MapSharedShreddingFieldMeta> DeserializeMetadata(
        const std::shared_ptr<arrow::KeyValueMetadata>& metadata, const std::string& compression);

    /// Checks whether a KeyValueMetadata contains shredding MAP metadata.
    static bool HasShreddingMetadata(const std::shared_ptr<arrow::KeyValueMetadata>& metadata);

    /// Checks whether a field in MapSharedShreddingFieldMeta is a overflow field.
    static Result<bool> IsOverflowField(const MapSharedShreddingFieldMeta& meta,
                                        const std::string& name);

    // ---- Writer helpers ----

    /// Builds a MetadataFinalizer that serializes shredding metadata into per-field
    /// KeyValueMetadata and reports file stats back to context for K adaptation.
    /// Shared by DataFileWriter (append-only) and KeyValueDataFileWriter (PK table).
    /// @param converter The batch converter that holds field-dict state for BuildFieldMeta.
    /// @param compression Compression codec name for field_dict serialization (e.g. "zstd").
    /// @param context The cross-file shared context for K adaptation.
    /// @param physical_schema The physical schema used for writing.
    /// @return A callable that produces the updated schema with shredding metadata
    ///         and reports file stats to context.
    static std::function<Result<std::shared_ptr<arrow::Schema>>()> BuildMetadataFinalizer(
        const std::shared_ptr<MapSharedShreddingBatchConverter>& converter,
        const std::string& compression, const std::shared_ptr<MapSharedShreddingContext>& context,
        const std::shared_ptr<arrow::Schema>& physical_schema);

 private:
    /// Returns the physical column indices for the given field name from the shredding meta.
    /// @param meta The shredding field meta parsed from file footer.
    /// @param name The field name to look up.
    /// @return Vector of physical column indices assigned to this field,
    ///         or Status::Invalid if the field name or field id is not found.
    static Result<std::vector<int32_t>> GetPhysicalColumnIndices(
        const MapSharedShreddingFieldMeta& meta, const std::string& name);

    /// Finds all shredding MAP field names in a schema by checking per-column config
    /// via CoreOptions.
    /// @param schema The logical Arrow schema.
    /// @param options CoreOptions containing per-column configuration.
    /// @return Vector of field names whose map.storage-layout is "shared-shredding", or error
    ///         if validation fails.
    static Result<std::vector<std::string>> DetectShreddingColumns(
        const std::shared_ptr<arrow::Schema>& schema, const CoreOptions& options);

    /// Builds shared-shredding max column counts from DetectShreddingColumns result and
    /// CoreOptions.
    /// @param shredding_field_names Field names returned by DetectShreddingColumns.
    /// @param options CoreOptions containing per-column shared-shredding config.
    /// @return Map from field name to its configured maximum physical width.
    static Result<std::map<std::string, int32_t>> BuildColumnToNumColumns(
        const std::vector<std::string>& shredding_field_names, const CoreOptions& options);

    /// Builds the physical Arrow type for one shredding MAP column.
    /// @param value_type The value type of the original MAP.
    /// @param num_columns Number of physical columns K.
    /// @param value_nullable Whether the MAP's value field is nullable.
    static std::shared_ptr<arrow::DataType> BuildPhysicalStructType(
        const std::shared_ptr<arrow::DataType>& value_type, int32_t num_columns,
        bool value_nullable);

    /// Builds the physical Arrow type for one shredding MAP column with sorted_cols.
    /// @param value_type The value type of the original MAP.
    /// @param sorted_cols The vector of physical column ids to include.
    /// @param value_nullable Whether the MAP's value field is nullable.
    /// @param include_overflow Whether to include __overflow column.
    static std::shared_ptr<arrow::DataType> InnerBuildSpecificPhysicalStructType(
        const std::shared_ptr<arrow::DataType>& value_type, const std::vector<int32_t>& sorted_cols,
        bool value_nullable, bool include_overflow);
};

}  // namespace paimon
