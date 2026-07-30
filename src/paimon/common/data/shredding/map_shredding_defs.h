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
#include <string>

#include "paimon/data/shredding/map_shared_shredding_schema_utils.h"

namespace paimon {

/// Constants for MAP storage layout marker.
struct MapShreddingDefine {
    /// Marker key indicating this file uses a specific MAP storage layout.
    static constexpr const char* kStorageLayout = "paimon.map.storage-layout";
    /// Value for kStorageLayout when using shared-shredding layout.
    static constexpr const char* kStorageLayoutSharedShredding = "shared-shredding";
};

/// Constants for the shared-shredding MAP storage layout.
/// Includes file footer meta keys and physical sub-column names.
struct MapSharedShreddingDefine {
    // ---- File footer meta keys ----

    /// Version of the shared-shredding meta format.
    static constexpr const char* kVersion = "paimon.map.shared-shredding.version";
    /// Current meta format version.
    static constexpr int32_t kCurrentVersion = 1;
    /// JSON-encoded field name <-> field id dictionary (may be compressed).
    static constexpr const char* kFieldDict = "paimon.map.shared-shredding.field-dict";
    /// Compression codec used by field_dict. Missing in legacy files, which default to zstd.
    static constexpr const char* kFieldDictCompression =
        "paimon.map.shared-shredding.field-dict-compression";
    /// Original (uncompressed) size of field_dict value.
    static constexpr const char* kFieldDictOriginalSize =
        "paimon.map.shared-shredding.field-dict-original-size";
    /// JSON-encoded field_id -> set of physical column indices.
    static constexpr const char* kFieldColumns = "paimon.map.shared-shredding.field-columns";
    /// JSON-encoded set of field_ids that ever spilled into __overflow.
    static constexpr const char* kOverflowSet = "paimon.map.shared-shredding.overflow-set";
    /// The number of physical columns K used in this file.
    static constexpr const char* kNumColumns = "paimon.map.shared-shredding.num-columns";
    /// The maximum row width observed in this file.
    static constexpr const char* kMaxRowWidth = "paimon.map.shared-shredding.max-row-width";

    // ---- Physical sub-column names ----

    /// Per-row field mapping column name.
    static constexpr const char* kFieldMapping = "__field_mapping";
    /// Overflow column name.
    static constexpr const char* kOverflow = "__overflow";

    /// Default compression codec for field_dict serialization.
    static constexpr const char* kDefaultDictCompression = "zstd";

    /// Returns the name of the i-th physical column: "__col_0", "__col_1", etc.
    static std::string PhysicalColumnName(int32_t index) {
        return "__col_" + std::to_string(index);
    }
};

}  // namespace paimon
