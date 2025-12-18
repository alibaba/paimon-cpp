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

#include "paimon/core/index/global_index_meta.h"

#include <memory>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row_writer.h"

namespace paimon {
GlobalIndexMeta::GlobalIndexMeta(int64_t _row_range_start, int64_t _row_range_end,
                                 int32_t _index_field_id,
                                 const std::optional<std::vector<int32_t>>& _extra_field_ids,
                                 const std::shared_ptr<Bytes>& _index_meta)
    : row_range_start(_row_range_start),
      row_range_end(_row_range_end),
      index_field_id(_index_field_id),
      extra_field_ids(_extra_field_ids),
      index_meta(_index_meta) {}

bool GlobalIndexMeta::operator==(const GlobalIndexMeta& other) const {
    if (this == &other) {
        return true;
    }
    if ((index_meta && !other.index_meta) || (!index_meta && other.index_meta)) {
        return false;
    }
    if (index_meta && other.index_meta && !(*index_meta == *other.index_meta)) {
        return false;
    }
    return row_range_start == other.row_range_start && row_range_end == other.row_range_end &&
           index_field_id == other.index_field_id && extra_field_ids == other.extra_field_ids;
}

std::string GlobalIndexMeta::ToString() const {
    std::string extra_field_ids_str =
        extra_field_ids == std::nullopt
            ? "null"
            : fmt::format("{}", fmt::join(extra_field_ids.value(), ", "));

    std::string index_meta_str =
        index_meta == nullptr ? "null" : std::string(index_meta->data(), index_meta->size());
    return fmt::format(
        "{{row_range_start={}, row_range_end={}, index_field_id={}, extra_field_ids={}, "
        "index_meta={}}}",
        row_range_start, row_range_end, index_field_id, extra_field_ids_str, index_meta_str);
}

BinaryRow GlobalIndexMeta::ToRow(MemoryPool* pool) const {
    BinaryRow row(5);
    BinaryRowWriter writer(&row, 32 * 1024, pool);
    writer.WriteLong(0, row_range_start);
    writer.WriteLong(1, row_range_end);
    writer.WriteInt(2, index_field_id);
    if (!extra_field_ids) {
        writer.SetNullAt(3);
    } else {
        writer.WriteArray(3, BinaryArray::FromIntArray(extra_field_ids.value(), pool));
    }
    if (index_meta == nullptr) {
        writer.SetNullAt(4);
    } else {
        writer.WriteBinary(4, *index_meta);
    }
    writer.Complete();
    return row;
}

Result<GlobalIndexMeta> GlobalIndexMeta::FromRow(const InternalRow& row) {
    int64_t row_range_start = row.GetLong(0);
    int64_t row_range_end = row.GetLong(1);
    int32_t index_field_id = row.GetInt(2);
    std::optional<std::vector<int32_t>> extra_field_ids;
    if (!row.IsNullAt(3)) {
        std::shared_ptr<InternalArray> array = row.GetArray(3);
        if (!array) {
            return Status::Invalid("GlobalIndexMeta FromRow failed with nullptr extra field ids");
        }
        PAIMON_ASSIGN_OR_RAISE(extra_field_ids, array->ToIntArray());
    }
    std::shared_ptr<Bytes> index_meta;
    if (!row.IsNullAt(4)) {
        index_meta = row.GetBinary(4);
        assert(index_meta);
    }
    return GlobalIndexMeta(row_range_start, row_range_end, index_field_id, extra_field_ids,
                           index_meta);
}

const std::shared_ptr<arrow::DataType>& GlobalIndexMeta::DataType() {
    static std::shared_ptr<arrow::DataType> schema = arrow::struct_({
        arrow::field("_ROW_RANGE_START", arrow::int64(), /*nullable=*/false),
        arrow::field("_ROW_RANGE_END", arrow::int64(), /*nullable=*/false),
        arrow::field("_INDEX_FIELD_ID", arrow::int32(), /*nullable=*/false),
        arrow::field("_EXTRA_FIELD_IDS",
                     arrow::list(arrow::field("item", arrow::int32(), /*nullable=*/false)),
                     /*nullable=*/true),
        arrow::field("_INDEX_META", arrow::binary(), /*nullable=*/true),
    });
    return schema;
}

}  // namespace paimon
