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
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/reader/file_batch_reader.h"
namespace paimon {

// Complete special fields:
// when _ROW_ID is not exist or null, convert to first row id + row idx in file
// when _SEQUENCE_NUMBER is not exist or null, convert to snapshot id
// Precondition: read_schema has special fields
class CompleteRowTrackingFieldsBatchReader : public FileBatchReader {
 public:
    CompleteRowTrackingFieldsBatchReader(std::unique_ptr<FileBatchReader>&& reader,
                                         const std::optional<int64_t>& first_row_id,
                                         int64_t snapshot_id,
                                         const std::shared_ptr<MemoryPool>& pool);

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
        return Status::Invalid(
            "CompleteRowTrackingFieldsBatchReader does not support GetFileSchema");
    }

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Result<ReadBatch> NextBatch() override {
        return Status::Invalid(
            "paimon inner reader CompleteRowTrackingFieldsBatchReader should use "
            "NextBatchWithBitmap");
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

    uint64_t GetPreviousBatchFirstRowNumber() const override {
        return reader_->GetPreviousBatchFirstRowNumber();
    }

    Result<uint64_t> GetNumberOfRows() const override {
        return reader_->GetNumberOfRows();
    }

    bool SupportPreciseBitmapSelection() const override {
        return reader_->SupportPreciseBitmapSelection();
    }

 private:
    Status ConvertRowTrackingField(int64_t array_length, int64_t init_value,
                                   const std::function<Result<int64_t>(int32_t)>& convert_func,
                                   std::shared_ptr<arrow::Array>* special_array_ptr) const;

 private:
    std::optional<int64_t> first_row_id_;
    int64_t snapshot_id_ = -1;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<arrow::Schema> read_schema_;
    std::unique_ptr<FileBatchReader> reader_;
};
}  // namespace paimon
