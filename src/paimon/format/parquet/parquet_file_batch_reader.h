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

#include <fmt/format.h>

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
#include "paimon/format/parquet/row_ranges.h"
#include "paimon/logging.h"
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
namespace parquet {
class FileMetaData;
}  // namespace parquet
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
        const std::map<std::string, std::string>& options, int32_t batch_size,
        std::shared_ptr<::parquet::FileMetaData> file_metadata,
        const std::shared_ptr<arrow::MemoryPool>& pool);

    static Result<::parquet::ReaderProperties> CreateReaderProperties(
        const std::shared_ptr<arrow::MemoryPool>& pool,
        const std::map<std::string, std::string>& options);

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

    Result<uint64_t> GetPreviousBatchGlobalRowId(uint64_t batch_row_id) const override {
        if (row_mapping_.size() == 0) {
            return Status::Invalid(
                "Last batch is not read or last batch is empty, cannot get previous batch global "
                "row id");
        }
        if (batch_row_id >= row_mapping_.size()) {
            return Status::Invalid(
                fmt::format("batch_row_id {} is out of range, last batch row count is {}",
                            batch_row_id, row_mapping_.size()));
        }
        return row_mapping_[batch_row_id];
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
        return reader_->ApplyReadRanges(read_ranges);
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

    /// Recursively collect leaf column indices for the sub-fields in read_type
    /// that match file_type by paimon field ID. Unmatched sub-fields in file_type
    /// have their leaf indices skipped. Partial projection inside LIST/MAP is
    /// not supported and will return Invalid.
    static Status CollectLeafIndices(const std::shared_ptr<arrow::DataType>& read_type,
                                     const std::shared_ptr<arrow::DataType>& file_type,
                                     int32_t* leaf_index, std::vector<int32_t>* indices);

    /// Skip over all leaf column indices of the given file_type without collecting.
    static void SkipLeafIndices(const std::shared_ptr<arrow::DataType>& file_type,
                                int32_t* leaf_index);

    /// Compute leaf column indices by recursively matching read_schema against
    /// file_schema using paimon field IDs. STRUCT supports sub-field projection
    /// (unmatched sub-fields are skipped). LIST/MAP require exact type match.
    static Result<std::vector<int32_t>> ComputeNestedColumnIndices(
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<arrow::Schema>& file_schema);

    Result<RowRanges> GetAllTargetRowRanges(const std::vector<TargetRowGroup>& target_row_groups);

    // precondition: predicate supposed not be empty
    Result<std::vector<int32_t>> FilterRowGroupsByPredicate(
        const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<arrow::Schema> file_schema,
        const std::vector<int32_t>& src_row_groups) const;

    Result<std::vector<int32_t>> FilterRowGroupsByBitmap(
        const RoaringBitmap32& bitmap, const std::vector<int32_t>& src_row_groups) const;

    // Apply page-level filtering using column index.
    // Returns (filtered row groups, per-row-group RowRanges for partial matches).
    Result<std::pair<std::vector<int32_t>, std::map<int32_t, RowRanges>>>
    FilterRowGroupsByPageIndex(const std::shared_ptr<Predicate>& predicate,
                               const std::map<std::string, int32_t>& column_name_to_index,
                               const std::vector<int32_t>& src_row_groups);

    Status GenerateRowMapping(int64_t batch_length);

 private:
    std::map<std::string, std::string> options_;
    // hold the lifecycle of arrow memory pool.
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;

    std::shared_ptr<arrow::io::RandomAccessFile> input_stream_;
    std::unique_ptr<FileReaderWrapper> reader_;

    std::shared_ptr<arrow::DataType> read_data_type_;

    std::shared_ptr<Metrics> metrics_;
    std::unique_ptr<Logger> logger_;

    uint64_t read_rows_ = 0;
    uint64_t read_batch_count_ = 0;

    RowRanges all_row_ranges_;
    std::vector<uint64_t> row_mapping_;
};

}  // namespace paimon::parquet
