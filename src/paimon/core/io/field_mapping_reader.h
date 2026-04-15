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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/core/partition/partition_info.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
}  // namespace arrow

namespace paimon {
class DataField;
class MemoryPool;
class Metrics;
struct FieldMapping;

class FieldMappingReader : public FileBatchReader {
 public:
    FieldMappingReader(int32_t field_count, std::unique_ptr<FileBatchReader>&& reader,
                       const BinaryRow& partition, std::unique_ptr<FieldMapping>&& mapping,
                       const std::shared_ptr<MemoryPool>& pool);

    Result<ReadBatch> NextBatch() override {
        return Status::Invalid(
            "paimon inner reader FieldMappingReader should use NextBatchWithBitmap");
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
        return Status::Invalid("FieldMappingReader does not support GetFileSchema");
    }

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override {
        return Status::Invalid("FieldMappingReader does not support SetReadSchema");
    }

    Result<uint64_t> GetPreviousBatchFirstRowNumber() const override {
        return reader_->GetPreviousBatchFirstRowNumber();
    }

    Result<uint64_t> GetNumberOfRows() const override {
        return reader_->GetNumberOfRows();
    }

    bool SupportPreciseBitmapSelection() const override {
        return reader_->SupportPreciseBitmapSelection();
    }

 private:
    Result<std::shared_ptr<arrow::Array>> GenerateSinglePartitionArray(int32_t idx,
                                                                       int32_t batch_size) const;

    Result<std::shared_ptr<arrow::Array>> GeneratePartitionArray(int32_t batch_size) const;

    Result<std::shared_ptr<arrow::Array>> GenerateNonExistArray(int32_t batch_size) const;
    Result<std::shared_ptr<arrow::Array>> CastNonPartitionArrayIfNeed(
        const std::shared_ptr<arrow::Array>& src_array) const;

    static void MappingFields(const std::shared_ptr<arrow::Array>& src_array,
                              const std::vector<DataField>& read_fields_of_data_array,
                              const std::vector<int32_t>& idx_in_target_schema,
                              arrow::ArrayVector* target_array,
                              std::vector<std::string>* target_field_names);

 private:
    bool need_mapping_ = false;
    bool need_casting_ = false;
    int32_t field_count_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<FileBatchReader> reader_;
    BinaryRow partition_ = BinaryRow::EmptyRow();

    std::optional<PartitionInfo> partition_info_;
    NonPartitionInfo non_partition_info_;
    std::optional<NonExistFieldInfo> non_exist_field_info_;

    std::shared_ptr<arrow::Array> partition_array_;
    std::shared_ptr<arrow::Array> non_exist_array_;
};
}  // namespace paimon
