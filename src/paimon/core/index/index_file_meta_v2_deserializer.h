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

#pragma once

#include <memory>
#include <string>

#include "paimon/common/data/internal_array.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/utils/object_serializer.h"

namespace paimon {
/// Serializer for `IndexFileMeta` with 1.2 version.
class IndexFileMetaV2Deserializer : public ObjectSerializer<std::shared_ptr<IndexFileMeta>> {
 public:
    static const std::shared_ptr<arrow::DataType>& DataType() {
        static std::shared_ptr<arrow::DataType> schema = arrow::struct_(
            {arrow::field("_INDEX_TYPE", arrow::utf8(), false),
             arrow::field("_FILE_NAME", arrow::utf8(), false),
             arrow::field("_FILE_SIZE", arrow::int64(), false),
             arrow::field("_ROW_COUNT", arrow::int64(), false),
             arrow::field("_DELETIONS_VECTORS_RANGES",
                          arrow::list(arrow::field("item", DeletionVectorMeta::DataType(), true)),
                          true)});
        return schema;
    }

    explicit IndexFileMetaV2Deserializer(const std::shared_ptr<MemoryPool>& pool)
        : ObjectSerializer<std::shared_ptr<IndexFileMeta>>(DataType(), pool) {}

    Result<BinaryRow> ToRow(const std::shared_ptr<IndexFileMeta>& meta) const override {
        assert(false);
        return Status::Invalid("IndexFileMetaV2Deserializer to row is not valid");
    }

    Result<std::shared_ptr<IndexFileMeta>> FromRow(const InternalRow& row) const override {
        auto file_type = row.GetString(0);
        auto file_name = row.GetString(1);
        auto file_size = row.GetLong(2);
        auto row_count = row.GetLong(3);
        std::optional<LinkedHashMap<std::string, DeletionVectorMeta>> dv_ranges;
        if (!row.IsNullAt(4)) {
            dv_ranges = RowArrayDataToDvRanges(row.GetArray(4).get());
        }
        return std::make_shared<IndexFileMeta>(file_type.ToString(), file_name.ToString(),
                                               file_size, row_count, dv_ranges,
                                               /*external_path=*/std::nullopt);
    }

    static LinkedHashMap<std::string, DeletionVectorMeta> RowArrayDataToDvRanges(
        const InternalArray* array_data) {
        LinkedHashMap<std::string, DeletionVectorMeta> dv_metas;
        for (int32_t i = 0; i < array_data->Size(); i++) {
            auto row =
                array_data->GetRow(i, /*num_fields=*/DeletionVectorMeta::DataType()->num_fields());
            std::string file_name = row->GetString(0).ToString();
            std::optional<int64_t> cardinality =
                row->IsNullAt(3) ? std::nullopt : std::optional<int64_t>(row->GetLong(3));
            dv_metas.insert_or_assign(file_name, DeletionVectorMeta(file_name, row->GetInt(1),
                                                                    row->GetInt(2), cardinality));
        }
        return dv_metas;
    }
};

}  // namespace paimon
