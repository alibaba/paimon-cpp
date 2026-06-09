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
#include <set>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "fmt/format.h"

namespace paimon {

/// Constants for the columnar-extend MAP storage layout.
/// Includes file footer meta keys and physical sub-column names.
struct ExtendMapDefine {
    // ---- File footer meta keys ----

    /// Version of the extend-map meta format.
    static constexpr const char* kVersion = "paimon.map-extend.version";
    /// Current meta format version.
    static constexpr int32_t kCurrentVersion = 1;

    /// Marker key indicating this file uses extend layout. Value is the layout type string.
    static constexpr const char* kStorageLayout = "paimon.map-extend.storage-layout";
    /// Value for kStorageLayout when using extend layout.
    static constexpr const char* kStorageLayoutExtend = "extend";
    /// JSON-encoded field name <-> field id dictionary (may be compressed).
    static constexpr const char* kFieldDict = "paimon.map-extend.field-dict";
    /// Original (uncompressed) size of field_dict value.
    static constexpr const char* kFieldDictOriginalSize =
        "paimon.map-extend.field-dict-original-size";
    /// JSON-encoded field_id -> set of physical column indices.
    static constexpr const char* kFieldColumns = "paimon.map-extend.field-columns";
    /// JSON-encoded set of field_ids that ever spilled into __overflow.
    static constexpr const char* kOverflowSet = "paimon.map-extend.overflow-set";
    /// The number of physical columns K used in this file.
    static constexpr const char* kNumColumns = "paimon.map-extend.num-columns";
    /// The maximum row width observed in this file.
    static constexpr const char* kMaxRowWidth = "paimon.map-extend.max-row-width";

    // ---- Physical sub-column names ----

    /// Per-row field mapping column name.
    static constexpr const char* kFieldMapping = "__field_mapping";
    /// Overflow column name.
    static constexpr const char* kOverflow = "__overflow";

    /// Returns the name of the i-th physical column: "__col_0", "__col_1", etc.
    static std::string PhysicalColumnName(int32_t index) {
        return fmt::format("__col_{}", index);
    }
};

/// Parsed file-level meta for one columnar-extend MAP column.
struct ExtendMapFileMeta {
    /// field_name -> field_id
    std::map<std::string, int32_t> name_to_id;
    /// field_id -> set of physical column indices S
    std::map<int32_t, std::vector<int32_t>> field_to_columns;
    /// Set of field_ids that ever spilled into __overflow
    std::set<int32_t> overflow_field_set;
    /// Number of physical columns K in this file
    int32_t num_columns = 0;
    /// Maximum row width observed in this file
    int32_t max_row_width = 0;

    bool operator==(const ExtendMapFileMeta& other) const {
        return name_to_id == other.name_to_id && field_to_columns == other.field_to_columns &&
               overflow_field_set == other.overflow_field_set && num_columns == other.num_columns &&
               max_row_width == other.max_row_width;
    }
};

}  // namespace paimon
