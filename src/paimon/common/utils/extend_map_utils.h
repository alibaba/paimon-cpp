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
#include <string>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/utils/extend_map_defs.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class KeyValueMetadata;
class Schema;
}  // namespace arrow

namespace paimon {

class CoreOptions;

/// Utility functions for columnar-extend MAP storage layout.
class ExtendMapUtils {
 public:
    ExtendMapUtils() = delete;
    ~ExtendMapUtils() = delete;

    // ---- Column detection ----

    /// Checks whether a given arrow field is MAP<STRING, T> (the type prerequisite for extend).
    /// @param arrow_type The Arrow data type of the column.
    /// @return true if the type is MAP<STRING, T>.
    static bool IsStringKeyMap(const std::shared_ptr<arrow::DataType>& arrow_type);

    /// Finds all extend MAP column indices in a schema by checking per-column config
    /// via CoreOptions.
    /// @param schema The logical Arrow schema.
    /// @param options CoreOptions containing per-column configuration.
    /// @return Vector of column indices whose map-storage-layout is "extend", or error
    ///         if validation fails.
    static Result<std::vector<int32_t>> DetectExtendColumns(
        const std::shared_ptr<arrow::Schema>& schema, const CoreOptions& options);

    // ---- Schema conversion ----

    /// Converts a logical schema to a physical schema by replacing extend MAP columns
    /// with their physical Struct representation.
    /// @param logical_schema The original schema with MAP<STRING, T> columns.
    /// @param column_to_num_columns Map from column index to its physical column count K.
    ///        Each extend column can have its own width.
    /// @return The physical schema for file writing.
    static Result<std::shared_ptr<arrow::Schema>> LogicalToPhysicalSchema(
        const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::map<int32_t, int32_t>& column_to_num_columns);

    /// Builds column_to_num_columns map from DetectExtendColumns result and CoreOptions.
    /// @param extend_column_indices Indices returned by DetectExtendColumns.
    /// @param schema The logical Arrow schema (used to get field names).
    /// @param options CoreOptions containing per-column max-columns config.
    /// @return Map from column index to K (max physical columns for that column).
    static Result<std::map<int32_t, int32_t>> BuildColumnToNumColumns(
        const std::vector<int32_t>& extend_column_indices,
        const std::shared_ptr<arrow::Schema>& schema, const CoreOptions& options);

    // ---- Metadata serialization ----

    /// Serializes extend metadata and appends entries to an existing KeyValueMetadata.
    /// @param file_meta The file-level extend metadata to serialize.
    /// @param compression Compression codec name for field_dict compression.
    /// @param[out] metadata The KeyValueMetadata to append entries to.
    static Status SerializeMetadata(const ExtendMapFileMeta& file_meta,
                                    const std::string& compression,
                                    arrow::KeyValueMetadata* metadata);

    /// Deserializes extend metadata from file footer KeyValueMetadata.
    /// @param metadata The KeyValueMetadata from file footer.
    /// @param compression Compression codec name.
    /// @return Parsed ExtendMapFileMeta, or error if metadata is missing/malformed.
    static Result<ExtendMapFileMeta> DeserializeMetadata(
        const std::shared_ptr<arrow::KeyValueMetadata>& metadata, const std::string& compression);

    /// Checks whether a KeyValueMetadata contains extend MAP metadata.
    static bool HasExtendMetadata(const std::shared_ptr<arrow::KeyValueMetadata>& metadata);

 private:
    /// Builds the physical Arrow type for one extend MAP column.
    /// @param value_type The value type of the original MAP.
    /// @param num_columns Number of physical columns K.
    /// @param value_nullable Whether the MAP's value field is nullable.
    static std::shared_ptr<arrow::DataType> BuildPhysicalStructType(
        const std::shared_ptr<arrow::DataType>& value_type, int32_t num_columns,
        bool value_nullable);
};

}  // namespace paimon
