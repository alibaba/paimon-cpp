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
#include "paimon/core/index/index_file_meta_serializer.h"

#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/index/index_file_meta_v2_deserializer.h"

namespace paimon {
Result<BinaryRow> IndexFileMetaSerializer::ToRow(const std::shared_ptr<IndexFileMeta>& meta) const {
    BinaryRow row(IndexFileMeta::NUM_FIELDS);
    BinaryRowWriter writer(&row, 32 * 1024, pool_.get());
    WriteIndexFileMeta(/*start_pos=*/0, meta, &writer, pool_.get());
    writer.Complete();
    return row;
}

Result<std::shared_ptr<IndexFileMeta>> IndexFileMetaSerializer::FromRow(
    const InternalRow& row) const {
    auto file_type = row.GetString(0);
    auto file_name = row.GetString(1);
    auto file_size = row.GetLong(2);
    auto row_count = row.GetLong(3);
    std::optional<LinkedHashMap<std::string, DeletionVectorMeta>> dv_ranges;
    if (!row.IsNullAt(4)) {
        dv_ranges = IndexFileMetaV2Deserializer::RowArrayDataToDvRanges(row.GetArray(4).get());
    }
    std::optional<std::string> external_path;
    if (!row.IsNullAt(5)) {
        external_path = row.GetString(5).ToString();
    }
    std::optional<GlobalIndexMeta> global_index_meta;
    if (!row.IsNullAt(6)) {
        std::shared_ptr<InternalRow> global_index_meta_row =
            row.GetRow(6, GlobalIndexMeta::NUM_FIELDS);
        assert(global_index_meta_row);
        PAIMON_ASSIGN_OR_RAISE(global_index_meta, GlobalIndexMeta::FromRow(*global_index_meta_row));
    }

    return std::make_shared<IndexFileMeta>(file_type.ToString(), file_name.ToString(), file_size,
                                           row_count, dv_ranges, external_path, global_index_meta);
}

void IndexFileMetaSerializer::WriteIndexFileMeta(int32_t start_pos,
                                                 const std::shared_ptr<IndexFileMeta>& meta,
                                                 BinaryRowWriter* writer, MemoryPool* pool) {
    writer->WriteString(start_pos + 0, BinaryString::FromString(meta->IndexType(), pool));
    writer->WriteString(start_pos + 1, BinaryString::FromString(meta->FileName(), pool));
    writer->WriteLong(start_pos + 2, meta->FileSize());
    writer->WriteLong(start_pos + 3, meta->RowCount());
    const auto& dv_ranges = meta->DvRanges();
    if (dv_ranges == std::nullopt) {
        writer->SetNullAt(start_pos + 4);
    } else {
        auto array = DvRangesToRowArrayData(dv_ranges.value(), pool);
        writer->WriteArray(start_pos + 4, array);
    }
    auto external_path = meta->ExternalPath();
    if (external_path == std::nullopt) {
        writer->SetNullAt(start_pos + 5);
    } else {
        writer->WriteString(start_pos + 5, BinaryString::FromString(external_path.value(), pool));
    }
    auto global_index_meta = meta->GetGlobalIndexMeta();
    if (global_index_meta == std::nullopt) {
        writer->SetNullAt(start_pos + 6);
    } else {
        writer->WriteRow(start_pos + 6, global_index_meta.value().ToRow(pool));
    }
}

BinaryArray IndexFileMetaSerializer::DvRangesToRowArrayData(
    const LinkedHashMap<std::string, DeletionVectorMeta>& dv_metas, MemoryPool* pool) {
    BinaryArray array;
    BinaryArrayWriter array_writer(&array, dv_metas.size(), /*element_size=*/8, pool);
    int32_t pos = 0;
    for (const auto& dv_meta : dv_metas) {
        const auto& meta = dv_meta.second;
        BinaryRow dv_data(4);
        BinaryRowWriter writer(&dv_data, 1024, pool);
        writer.WriteString(/*pos=*/0, BinaryString::FromString(meta.GetDataFileName(), pool));
        writer.WriteInt(/*pos=*/1, meta.GetOffset());
        writer.WriteInt(/*pos=*/2, meta.GetLength());
        if (meta.GetCardinality()) {
            writer.WriteLong(/*pos=*/3, meta.GetCardinality().value());
        } else {
            writer.SetNullAt(3);
        }
        writer.Complete();
        array_writer.WriteRow(pos++, dv_data);
    }
    array_writer.Complete();
    return array;
}

}  // namespace paimon
