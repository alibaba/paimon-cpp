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

#include "paimon/core/manifest/manifest_entry_serializer.h"

#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;
struct DataFileMeta;

Result<ManifestEntry> ManifestEntrySerializer::ConvertFrom(int32_t version,
                                                           const InternalRow& row) const {
    if (version != VERSION_2) {
        if (version == VERSION_1) {
            return Status::Invalid(
                fmt::format("The current version {} is not compatible with the version {}, "
                            "please recreate the table.",
                            GetVersion(), version));
        }
        return Status::Invalid("Unsupported version", std::to_string(version));
    }
    auto kind = row.GetByte(0);
    PAIMON_ASSIGN_OR_RAISE(FileKind file_kind, FileKind::FromByteValue(kind));
    auto partition_bytes = row.GetBinary(1);
    PAIMON_ASSIGN_OR_RAISE(BinaryRow partition,
                           SerializationUtils::DeserializeBinaryRow(partition_bytes));
    auto bucket = row.GetInt(2);
    auto total_buckets = row.GetInt(3);
    auto file = row.GetRow(4, data_file_meta_serializer_.NumFields());
    if (!file) {
        return Status::Invalid("ManifestEntry convert from row failed, with null DataFileMeta");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> meta,
                           data_file_meta_serializer_.FromRow(*file));
    return ManifestEntry(file_kind, partition, bucket, total_buckets, meta);
}

Result<BinaryRow> ManifestEntrySerializer::ToRow(const ManifestEntry& record) const {
    BinaryRow row(GetDataType()->num_fields());
    BinaryRowWriter writer(&row, 32 * 1024, pool_.get());

    writer.WriteInt(0, GetVersion());
    writer.WriteByte(1, record.Kind().ToByteValue());
    auto partition_bytes = SerializationUtils::SerializeBinaryRow(record.Partition(), pool_.get());
    assert(partition_bytes);
    writer.WriteBinary(2, *partition_bytes);
    writer.WriteInt(3, record.Bucket());
    writer.WriteInt(4, record.TotalBuckets());
    PAIMON_ASSIGN_OR_RAISE(BinaryRow data_file_meta_row,
                           data_file_meta_serializer_.ToRow(record.File()));
    writer.WriteRow(5, data_file_meta_row);
    writer.Complete();
    return row;
}

}  // namespace paimon
