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
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "lance_lib/lance_api.h"
#include "paimon/metrics.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon::lance {
class LanceFileBatchReader : public FileBatchReader {
 public:
    static Result<std::unique_ptr<LanceFileBatchReader>> Create(const std::string& file_path,
                                                                int32_t batch_size,
                                                                int32_t batch_readahead);
    ~LanceFileBatchReader() override;

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Result<ReadBatch> NextBatch() override;

    uint64_t GetPreviousBatchFirstRowNumber() const override {
        // TODO(xinyu.lxy): support function
        assert(false);
        return -1;
    }

    Result<uint64_t> GetNumberOfRows() const override {
        return num_rows_;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        // TODO(xinyu.lxy): support metrics in reader
        return metrics_;
    }

    void Close() override {
        return DoClose();
    }

    bool SupportPreciseBitmapSelection() const override {
        return true;
    }

 private:
    LanceFileBatchReader(LanceFileReader* file_reader, int32_t batch_size, int32_t batch_readahead,
                         uint64_t num_rows, std::string&& error_message);

    void DoClose();

 private:
    int32_t batch_size_ = -1;
    int32_t batch_readahead_ = -1;
    uint64_t num_rows_ = 0;
    mutable std::string error_message_;
    LanceFileReader* file_reader_ = nullptr;
    LanceReaderAdapter* stream_reader_ = nullptr;
    std::vector<std::string> read_field_names_;
    std::vector<uint32_t> read_row_ids_;
    std::shared_ptr<Metrics> metrics_;
};

}  // namespace paimon::lance
