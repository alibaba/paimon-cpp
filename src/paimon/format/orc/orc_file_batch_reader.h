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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate.h"
#include "paimon/reader/file_batch_reader.h"

namespace orc {
class InputStream;
}  // namespace orc

namespace paimon::orc {
class OrcFileBatchReader : public FileBatchReader {
 public:
    static Result<std::unique_ptr<OrcFileBatchReader>> Create(
        std::unique_ptr<::orc::InputStream>&& input_stream, const std::shared_ptr<MemoryPool>& pool,
        const std::map<std::string, std::string>& options, int32_t batch_size);

    // For timestamp type, precision info is missing from file
    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    // Important: output ArrowArray is allocated on arrow_pool_ whose lifecycle holds in
    // OrcFileBatchReader. Therefore, we need to hold BatchReader when using output ArrowArray.
    Result<ReadBatch> NextBatch() override;

    uint64_t GetPreviousBatchFirstRowNumber() const override {
        return row_reader_->getRowNumber();
    }

    uint64_t GetNumberOfRows() const override {
        return reader_->getNumberOfRows();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override;

    void Close() override {
        metrics_ = GetReaderMetrics();
        row_reader_.reset();
        reader_.reset();
        reader_metrics_.reset();
    }

    bool SupportPreciseBitmapSelection() const override {
        return false;
    }

 private:
    OrcFileBatchReader(const std::string& file_name, int32_t batch_size,
                       std::unique_ptr<::orc::ReaderMetrics>&& reader_metrics,
                       std::unique_ptr<::orc::Reader>&& reader,
                       const std::map<std::string, std::string>& options,
                       std::unique_ptr<arrow::MemoryPool>&& arrow_pool,
                       const std::shared_ptr<::orc::MemoryPool>& orc_pool);

    static Result<::orc::RowReaderOptions> CreateRowReaderOptions(
        const ::orc::Type* src_type, const ::orc::Type* target_type,
        std::unique_ptr<::orc::SearchArgument>&& search_arg,
        const std::map<std::string, std::string>& options);

 private:
    std::string file_name_;
    int32_t batch_size_;
    std::map<std::string, std::string> options_;
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<::orc::MemoryPool> orc_pool_;
    std::unique_ptr<::orc::ReaderMetrics> reader_metrics_;
    std::unique_ptr<::orc::Reader> reader_;
    std::unique_ptr<::orc::RowReader> row_reader_;
    std::shared_ptr<arrow::DataType> target_type_;
    std::shared_ptr<Metrics> metrics_;
    bool has_error_ = false;
};
}  // namespace paimon::orc
