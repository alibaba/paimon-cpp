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

#include "paimon/core/manifest/manifest_file_meta_serializer.h"

#include <optional>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/status.h"

namespace paimon {

Result<BinaryRow> ManifestFileMetaSerializer::ToRow(const ManifestFileMeta& record) const {
    BinaryRow row(GetDataType()->num_fields());
    BinaryRowWriter writer(&row, 32 * 1024, pool_.get());

    writer.WriteInt(0, GetVersion());
    writer.WriteString(1, BinaryString::FromString(record.FileName(), pool_.get()));
    writer.WriteLong(2, record.FileSize());
    writer.WriteLong(3, record.NumAddedFiles());
    writer.WriteLong(4, record.NumDeletedFiles());
    writer.WriteRow(5, record.PartitionStats().ToRow());
    writer.WriteLong(6, record.SchemaId());

    auto min_bucket = record.MinBucket();
    if (!min_bucket) {
        writer.SetNullAt(7);
    } else {
        writer.WriteInt(7, min_bucket.value());
    }

    auto max_bucket = record.MaxBucket();
    if (!max_bucket) {
        writer.SetNullAt(8);
    } else {
        writer.WriteInt(8, max_bucket.value());
    }

    auto min_level = record.MinLevel();
    if (!min_level) {
        writer.SetNullAt(9);
    } else {
        writer.WriteInt(9, min_level.value());
    }

    auto max_level = record.MaxLevel();
    if (!max_level) {
        writer.SetNullAt(10);
    } else {
        writer.WriteInt(10, max_level.value());
    }
    auto min_row_id = record.MinRowId();
    if (!min_row_id) {
        writer.SetNullAt(11);
    } else {
        writer.WriteLong(11, min_row_id.value());
    }

    auto max_row_id = record.MaxRowId();
    if (!max_row_id) {
        writer.SetNullAt(12);
    } else {
        writer.WriteLong(12, max_row_id.value());
    }
    writer.Complete();
    return row;
}

Result<ManifestFileMeta> ManifestFileMetaSerializer::ConvertFrom(int32_t version,
                                                                 const InternalRow& row) const {
    if (version != VERSION_2) {
        if (version == VERSION_1) {
            return Status::Invalid(
                fmt::format("The current version {} is not compatible with the "
                            "version {}, please recreate the table.",
                            GetVersion(), version));
        }
        return Status::Invalid(fmt::format("Unsupported version: {}", version));
    }
    auto file_name = row.GetString(0);
    auto file_size = row.GetLong(1);
    auto num_added_files = row.GetLong(2);
    auto num_deleted_files = row.GetLong(3);
    auto partition_stats_row = row.GetRow(4, 3);
    if (partition_stats_row == nullptr) {
        return Status::Invalid(
            "ManifestFileMeta convert from row failed, with null partition stats");
    }
    PAIMON_ASSIGN_OR_RAISE(SimpleStats partition_stats,
                           SimpleStats::FromRow(partition_stats_row.get(), pool_.get()));

    auto schema_id = row.GetLong(5);
    std::optional<int32_t> min_bucket;
    if (!row.IsNullAt(6)) {
        min_bucket = row.GetInt(6);
    }
    std::optional<int32_t> max_bucket;
    if (!row.IsNullAt(7)) {
        max_bucket = row.GetInt(7);
    }
    std::optional<int32_t> min_level;
    if (!row.IsNullAt(8)) {
        min_level = row.GetInt(8);
    }
    std::optional<int32_t> max_level;
    if (!row.IsNullAt(9)) {
        max_level = row.GetInt(9);
    }
    std::optional<int64_t> min_row_id;
    if (!row.IsNullAt(10)) {
        min_row_id = row.GetLong(10);
    }
    std::optional<int64_t> max_row_id;
    if (!row.IsNullAt(11)) {
        max_row_id = row.GetLong(11);
    }
    return ManifestFileMeta(file_name.ToString(), file_size, num_added_files, num_deleted_files,
                            partition_stats, schema_id, min_bucket, max_bucket, min_level,
                            max_level, min_row_id, max_row_id);
}

}  // namespace paimon
