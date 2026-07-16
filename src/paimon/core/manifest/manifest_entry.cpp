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

#include "paimon/core/manifest/manifest_entry.h"

#include "arrow/type_fwd.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
const std::shared_ptr<arrow::DataType>& ManifestEntry::DataType() {
    static std::shared_ptr<arrow::DataType> data_type =
        arrow::struct_({arrow::field("_KIND", arrow::int8(), /*nullable=*/false),
                        arrow::field("_PARTITION", arrow::binary(), /*nullable=*/false),
                        arrow::field("_BUCKET", arrow::int32(), /*nullable=*/false),
                        arrow::field("_TOTAL_BUCKETS", arrow::int32(), /*nullable=*/false),
                        arrow::field("_FILE", DataFileMeta::DataType(), /*nullable=*/false)});
    return data_type;
}

int64_t ManifestEntry::RecordCount(const std::vector<ManifestEntry>& entries) {
    int64_t record_count = 0;
    for (const auto& entry : entries) {
        record_count += entry.File()->row_count;
    }
    return record_count;
}

std::optional<int64_t> ManifestEntry::NullableRecordCount(
    const std::vector<ManifestEntry>& entries) {
    if (entries.empty()) {
        return std::nullopt;
    }
    return RecordCount(entries);
}

int64_t ManifestEntry::RecordCountAdd(const std::vector<ManifestEntry>& entries) {
    int64_t record_count = 0;
    for (const auto& entry : entries) {
        if (entry.Kind() == FileKind::Add()) {
            record_count += entry.File()->row_count;
        }
    }
    return record_count;
}

int64_t ManifestEntry::RecordCountDelete(const std::vector<ManifestEntry>& entries) {
    int64_t record_count = 0;
    for (const auto& entry : entries) {
        if (entry.Kind() == FileKind::Delete()) {
            record_count += entry.File()->row_count;
        }
    }
    return record_count;
}

}  // namespace paimon
