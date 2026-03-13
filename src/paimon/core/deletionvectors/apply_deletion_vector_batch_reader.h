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
#include <cassert>
#include <cstdint>
#include <memory>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class Metrics;

class ApplyDeletionVectorBatchReader : public FileBatchReader {
 public:
    ApplyDeletionVectorBatchReader(std::unique_ptr<FileBatchReader>&& reader,
                                   PAIMON_UNIQUE_PTR<DeletionVector>&& deletion_vector)
        : reader_(std::move(reader)), deletion_vector_(std::move(deletion_vector)) {
        assert(reader_);
    }

    Result<ReadBatch> NextBatch() override {
        return Status::Invalid(
            "paimon inner reader ApplyDeletionVectorBatchReader should use NextBatchWithBitmap");
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch_with_bitmap,
                                   reader_->NextBatchWithBitmap());
            if (BatchReader::IsEofBatch(batch_with_bitmap)) {
                return batch_with_bitmap;
            }
            auto& [batch, bitmap] = batch_with_bitmap;
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 valid_bitmap, Filter(batch.first->length));
            bitmap &= valid_bitmap;
            if (bitmap.IsEmpty()) {
                ReaderUtils::ReleaseReadBatch(std::move(batch));
                continue;
            }
            return batch_with_bitmap;
        }
    }

    void Close() override {
        return reader_->Close();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
        return reader_->GetFileSchema();
    }

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override {
        return Status::Invalid("ApplyDeletionVectorBatchReader does not support SetReadSchema");
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
    Result<RoaringBitmap32> Filter(int32_t batch_size) const {
        PAIMON_ASSIGN_OR_RAISE(uint64_t previous_batch_first_row_number,
                               reader_->GetPreviousBatchFirstRowNumber());
        return deletion_vector_->IsValid(previous_batch_first_row_number, batch_size);
    }

 private:
    std::unique_ptr<FileBatchReader> reader_;
    PAIMON_UNIQUE_PTR<DeletionVector> deletion_vector_;
};
}  // namespace paimon
