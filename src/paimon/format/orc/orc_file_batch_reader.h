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

#include <list>
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
#include "paimon/format/orc/orc_reader_wrapper.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate.h"
#include "paimon/reader/prefetch_file_batch_reader.h"

namespace orc {
class InputStream;
}  // namespace orc

namespace paimon::orc {

class OrcFileBatchReader : public PrefetchFileBatchReader {
 public:
    ~OrcFileBatchReader() override = default;
    static Result<std::unique_ptr<OrcFileBatchReader>> Create(
        std::unique_ptr<::orc::InputStream>&& input_stream, const std::shared_ptr<MemoryPool>& pool,
        const std::map<std::string, std::string>& options, int32_t batch_size);

    // For timestamp type, precision info is missing from file
    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Status SeekToRow(uint64_t row_number) override;

    Status SetReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) override {
        return reader_->SetReadRanges(read_ranges);
    }

    // Important: output ArrowArray is allocated on arrow_pool_ whose lifecycle holds in
    // OrcFileBatchReader. Therefore, we need to hold BatchReader when using output ArrowArray.
    Result<ReadBatch> NextBatch() override;

    uint64_t GetPreviousBatchFirstRowNumber() const override {
        return reader_->GetRowNumber();
    }

    Result<uint64_t> GetNumberOfRows() const override {
        return reader_->GetNumberOfRows();
    }

    uint64_t GetNextRowToRead() const override {
        return reader_->GetNextRowToRead();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override;

    Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        bool* need_prefetch) const override {
        PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, GetNumberOfRows());
        return reader_->GenReadRanges(target_column_ids_, 0, num_rows, need_prefetch);
    }

    void Close() override {
        metrics_ = GetReaderMetrics();
        reader_.reset();
        reader_metrics_.reset();
    }

    bool SupportPreciseBitmapSelection() const override {
        return false;
    }

    Result<std::vector<std::pair<uint64_t, uint64_t>>> PreBufferRange() override;

 private:
    OrcFileBatchReader(std::unique_ptr<::orc::ReaderMetrics>&& reader_metrics,
                       std::unique_ptr<OrcReaderWrapper>&& reader,
                       const std::map<std::string, std::string>& options,
                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                       const std::shared_ptr<::orc::MemoryPool>& orc_pool);

    static void GetSubColumnIds(const ::orc::Type* type, std::vector<uint64_t>* col_ids);

    static Result<::orc::RowReaderOptions> CreateRowReaderOptions(
        const ::orc::Type* src_type, const ::orc::Type* target_type,
        std::unique_ptr<::orc::SearchArgument>&& search_arg,
        const std::map<std::string, std::string>& options,
        std::vector<uint64_t>* target_column_ids);

    static Result<std::list<std::string>> GetAndCheckIncludedFields(
        const ::orc::Type* src_type, const ::orc::Type* target_type,
        std::vector<uint64_t>* target_column_ids);

    std::map<std::string, std::string> options_;

    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<::orc::MemoryPool> orc_pool_;

    std::unique_ptr<::orc::ReaderMetrics> reader_metrics_;
    std::unique_ptr<OrcReaderWrapper> reader_;
    std::shared_ptr<Metrics> metrics_;
    std::vector<uint64_t> target_column_ids_;
    std::vector<std::pair<uint64_t, uint64_t>> cache_ranges_;
};

}  // namespace paimon::orc
