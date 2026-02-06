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

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/reader/prefetch_file_batch_reader.h"
namespace paimon::test {

class MockFileBatchReader : public PrefetchFileBatchReader {
 public:
    MockFileBatchReader(const std::shared_ptr<arrow::Array>& data,
                        const std::shared_ptr<arrow::DataType>& file_schema,
                        int32_t read_batch_size)
        : data_(data),
          file_schema_(file_schema),
          read_schema_(arrow::schema(file_schema->fields())),
          batch_size_(read_batch_size) {
        // add all valid bitmap
        int32_t data_length = data_ ? data_->length() : 0;
        bitmap_ = RoaringBitmap32();
        bitmap_.AddRange(0, data_length);
        read_end_pos_ = data_length;
        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
    }

    MockFileBatchReader(const std::shared_ptr<arrow::Array>& data,
                        const std::shared_ptr<arrow::DataType>& schema,
                        const RoaringBitmap32& bitmap, int32_t read_batch_size)
        : MockFileBatchReader(data, schema, read_batch_size) {
        bitmap_ = bitmap;
    }

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*file_schema_, c_schema.get()));
        return c_schema;
    }

    void SetNextBatchStatus(const Status& status) {
        next_batch_status_ = status;
    }

    void EnableRandomizeBatchSize(bool enabled) {
        enable_randomize_batch_size_ = enabled;
    }

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override {
        // Noted that SetReadSchema only change inner read_schema_, but take no effective on
        // NextBatch
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                          arrow::ImportSchema(read_schema));
        read_schema_ = arrow_schema;
        return Status::OK();
    }

    Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        bool* need_prefetch) const override {
        uint64_t begin_row_num = 0;
        PAIMON_ASSIGN_OR_RAISE(uint64_t end_row_num, GetNumberOfRows());
        std::vector<std::pair<uint64_t, uint64_t>> read_ranges;
        for (uint64_t begin = begin_row_num; begin < end_row_num; begin += batch_size_) {
            uint64_t end = std::min(begin + batch_size_, end_row_num);
            read_ranges.emplace_back(begin, end);
        }
        *need_prefetch = true;
        return read_ranges;
    }

    Status SeekToRow(uint64_t row_number) override {
        current_pos_ = row_number;
        return Status::OK();
    }

    Status SetReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) override {
        read_ranges_ = read_ranges;
        return Status::OK();
    }

    Result<ReadBatch> NextBatch() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch_with_bitmap, NextBatchWithBitmap());
        return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap),
                                                   arrow::default_memory_pool());
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        while (true) {
            PAIMON_RETURN_NOT_OK(next_batch_status_);
            if (current_pos_ >= read_end_pos_) {
                previous_batch_first_row_num_ = current_pos_;
                return BatchReader::MakeEofBatchWithBitmap();
            }
            int32_t actual_batch_size = batch_size_;
            if (enable_randomize_batch_size_) {
                actual_batch_size = std::rand() % batch_size_ + 1;
            }
            int32_t batch_end_pos = std::min(read_end_pos_, current_pos_ + actual_batch_size);
            auto slice = data_->Slice(current_pos_, batch_end_pos - current_pos_);
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> concat_slice,
                arrow::Concatenate({slice}, arrow::default_memory_pool()));
            RoaringBitmap32 bitmap;
            for (auto iter = bitmap_.EqualOrLarger(current_pos_);
                 iter != bitmap_.End() && *iter < batch_end_pos; ++iter) {
                bitmap.Add(*iter - current_pos_);
            }
            previous_batch_first_row_num_ = current_pos_;
            current_pos_ = batch_end_pos;
            if (bitmap.IsEmpty()) {
                continue;
            }
            std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
            std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                arrow::ExportArray(*concat_slice, c_array.get(), c_schema.get()));
            return std::make_pair(std::make_pair(std::move(c_array), std::move(c_schema)),
                                  std::move(bitmap));
        }
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        auto metrics = std::make_shared<MetricsImpl>();
        metrics->SetCounter("mock.number.of.rows", GetNumberOfRows().value_or(-1));
        return metrics;
    }

    uint64_t GetPreviousBatchFirstRowNumber() const override {
        return previous_batch_first_row_num_;
    }

    Result<uint64_t> GetNumberOfRows() const override {
        return data_ ? data_->length() : 0;
    }
    uint64_t GetNextRowToRead() const override {
        return current_pos_;
    }
    void Close() override {}

    std::vector<std::pair<uint64_t, uint64_t>> GetReadRanges() const {
        return read_ranges_;
    }

    bool SupportPreciseBitmapSelection() const override {
        return false;
    }

 private:
    std::shared_ptr<arrow::Array> data_;
    std::shared_ptr<arrow::DataType> file_schema_;
    std::shared_ptr<arrow::Schema> read_schema_;
    RoaringBitmap32 bitmap_;
    int32_t batch_size_ = 0;
    int32_t current_pos_ = 0;
    int32_t read_end_pos_ = 0;
    int32_t previous_batch_first_row_num_ = -1;
    Status next_batch_status_;
    bool enable_randomize_batch_size_ = true;
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges_;
};

}  // namespace paimon::test
