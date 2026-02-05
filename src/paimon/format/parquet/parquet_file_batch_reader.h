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
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/parquet/file_reader_wrapper.h"
#include "paimon/reader/prefetch_file_batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "parquet/arrow/reader.h"
#include "parquet/properties.h"

namespace arrow {
class MemoryPool;

namespace io {
class RandomAccessFile;
}  // namespace io
}  // namespace arrow
namespace paimon {
class Metrics;
class Predicate;
class RoaringBitmap32;
}  // namespace paimon

namespace paimon::parquet {

class ParquetFileBatchReader : public PrefetchFileBatchReader {
 public:
    static Result<std::unique_ptr<ParquetFileBatchReader>> Create(
        std::shared_ptr<arrow::io::RandomAccessFile>&& input_stream,
        const std::shared_ptr<arrow::MemoryPool>& pool,
        const std::map<std::string, std::string>& options, int32_t batch_size);

    // For timestamp type, we return the schema stored in file, e.g., second in parquet file will
    // store as milli.
    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Status SeekToRow(uint64_t row_number) override {
        assert(reader_);
        return reader_->SeekToRow(row_number);
    }

    // Important: output ArrowArray is allocated on arrow_pool_ whose lifecycle holds in
    // ParquetFileBatchReader. Therefore, we need to hold BatchReader when using output
    // ArrowArray.
    Result<ReadBatch> NextBatch() override;

    Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        bool* need_prefetch) const override;

    uint64_t GetPreviousBatchFirstRowNumber() const override {
        assert(reader_);
        return reader_->GetPreviousBatchFirstRowNumber();
    }

    Result<uint64_t> GetNumberOfRows() const override {
        assert(reader_);
        return reader_->GetNumberOfRows();
    }

    uint64_t GetNextRowToRead() const override {
        assert(reader_);
        return reader_->GetNextRowToRead();
    }

    Status SetReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) override {
        read_ranges_ = read_ranges;
        PAIMON_ASSIGN_OR_RAISE(
            std::set<int32_t> ordered_row_groups,
            reader_->FilterRowGroupsByReadRanges(read_ranges_, read_row_groups_));
        return reader_->PrepareForReadingLazy(ordered_row_groups, read_column_indices_);
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        if (reader_) {
            auto status = reader_->Close();
            reader_.reset();
            (void)status;
        }
        input_stream_.reset();
    }

    bool SupportPreciseBitmapSelection() const override {
        return false;
    }

 private:
    ParquetFileBatchReader(std::shared_ptr<arrow::io::RandomAccessFile>&& input_stream,
                           std::unique_ptr<FileReaderWrapper>&& reader,
                           const std::map<std::string, std::string>& options,
                           const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    static Result<::parquet::ReaderProperties> CreateReaderProperties(
        const std::shared_ptr<arrow::MemoryPool>& pool,
        const std::map<std::string, std::string>& options);

    static Result<::parquet::ArrowReaderProperties> CreateArrowReaderProperties(
        const std::shared_ptr<arrow::MemoryPool>& pool,
        const std::map<std::string, std::string>& options, int32_t batch_size);

    static void FlattenSchema(const std::shared_ptr<arrow::DataType>& type, int32_t* index,
                              std::vector<int32_t>* index_vector) {
        if (type->id() == arrow::Type::STRUCT || type->id() == arrow::Type::LIST ||
            type->id() == arrow::Type::MAP) {
            for (int32_t i = 0; i < type->num_fields(); i++) {
                auto field = type->field(i);
                auto inner_type = field->type();
                FlattenSchema(inner_type, index, index_vector);
            }
        } else {
            index_vector->push_back((*index)++);
        }
    }

    // precondition: predicate supposed not be empty
    Result<std::vector<int32_t>> FilterRowGroupsByPredicate(
        const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<arrow::Schema> file_schema,
        const std::vector<int32_t>& src_row_groups) const;

    Result<std::vector<int32_t>> FilterRowGroupsByBitmap(
        const RoaringBitmap32& bitmap, const std::vector<int32_t>& src_row_groups) const;

 private:
    std::map<std::string, std::string> options_;
    // hold the lifecycle of arrow memory pool.
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;

    std::shared_ptr<arrow::io::RandomAccessFile> input_stream_;
    std::unique_ptr<FileReaderWrapper> reader_;

    std::shared_ptr<arrow::DataType> read_data_type_;
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges_;

    std::shared_ptr<Metrics> metrics_;

    // last time set read schema
    std::vector<int32_t> read_row_groups_;
    std::vector<int32_t> read_column_indices_;
};

}  // namespace paimon::parquet
