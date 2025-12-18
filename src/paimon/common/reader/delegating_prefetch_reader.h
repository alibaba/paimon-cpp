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

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "paimon/common/reader/prefetch_file_batch_reader.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {

class DelegatingPrefetchReader : public FileBatchReader {
 public:
    explicit DelegatingPrefetchReader(std::unique_ptr<PrefetchFileBatchReader> prefetch_reader)
        : prefetch_reader_(std::move(prefetch_reader)) {}

    Result<ReadBatch> NextBatch() override {
        return Status::Invalid(
            "paimon inner reader DelegatingPrefetchReader should use NextBatchWithBitmap");
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        return GetReader()->NextBatchWithBitmap();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return GetReader()->GetReaderMetrics();
    }

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
        return GetReader()->GetFileSchema();
    }
    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override {
        return prefetch_reader_->SetReadSchema(read_schema, predicate, selection_bitmap);
    }

    Status SeekToRow(uint64_t row_number) override {
        assert(false);
        return Status::NotImplemented("not support seek to row for delegate reader");
    }
    uint64_t GetPreviousBatchFirstRowNumber() const override {
        return GetReader()->GetPreviousBatchFirstRowNumber();
    }
    uint64_t GetNumberOfRows() const override {
        return GetReader()->GetNumberOfRows();
    }
    uint64_t GetNextRowToRead() const override {
        return GetReader()->GetNextRowToRead();
    }

    Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        bool* need_prefetch) const override {
        assert(false);
        return Status::NotImplemented("gen read ranges not implemented");
    }

    void Close() override {
        return prefetch_reader_->Close();
    }
    Status SetReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) override {
        assert(false);
        return Status::NotImplemented("not support set read ranges for delegate reader");
    }
    bool SupportPreciseBitmapSelection() const override {
        return GetReader()->SupportPreciseBitmapSelection();
    }

 private:
    inline FileBatchReader* GetReader() const {
        assert(prefetch_reader_);
        if (prefetch_reader_->NeedPrefetch()) {
            return prefetch_reader_.get();
        } else {
            return prefetch_reader_->GetFirstReader();
        }
    }

    std::unique_ptr<PrefetchFileBatchReader> prefetch_reader_;
};

}  // namespace paimon
