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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {

class SharedShreddingFileReader : public FileBatchReader {
 public:
    static Result<std::unique_ptr<SharedShreddingFileReader>> Create(
        std::unique_ptr<FileBatchReader>&& reader, const std::shared_ptr<MemoryPool>& pool);

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Result<ReadBatch> NextBatch() override;

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override;

    void Close() override;

    Result<uint64_t> GetPreviousBatchFirstRowNumber() const override;

    Result<uint64_t> GetNumberOfRows() const override;

    bool SupportPreciseBitmapSelection() const override;

 private:
    SharedShreddingFileReader(
        std::unique_ptr<FileBatchReader>&& reader,
        const std::map<std::string, MapSharedShreddingFieldMeta>& shared_shredding_name_to_meta,
        const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<arrow::Array>> RebuildLogicalMapArray(
        const std::shared_ptr<arrow::Field>& physical_field,
        const std::shared_ptr<arrow::StructArray>& physical_struct_array) const;

    static std::vector<std::pair<std::string, int32_t>> ResolveSelectedKeyIds(
        const MapSharedShreddingFieldMeta& meta, const std::vector<std::string>& selected_keys);

    static void CollectPhysicalColumns(
        const std::shared_ptr<arrow::StructArray>& physical_struct_array,
        std::map<std::string, std::shared_ptr<arrow::Array>>* physical_column_name_to_array,
        std::shared_ptr<arrow::MapArray>* overflow_array);

    static Result<std::shared_ptr<arrow::Field>> ToLogicalMapField(
        const std::shared_ptr<arrow::Field>& physical_field);

 private:
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<FileBatchReader> reader_;
    std::map<std::string, MapSharedShreddingFieldMeta> shared_shredding_name_to_meta_;
    std::map<std::string, std::vector<std::string>> shared_shredding_name_to_selected_keys_;
    std::map<std::string, std::shared_ptr<arrow::MapType>> shared_shredding_name_to_map_type_;
};

}  // namespace paimon
