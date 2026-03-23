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
#include <limits>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/compute/api.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "parquet/arrow/reader.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::parquet {

// The FileReaderWrapper is a decorator class designed to support seek functionality, as well as the
// methods GetPreviousBatchFirstRowNumber and GetNextRowToRead.
class FileReaderWrapper {
 public:
    static Result<std::unique_ptr<FileReaderWrapper>> Create(
        std::unique_ptr<::parquet::arrow::FileReader>&& reader);

    Status SeekToRow(uint64_t row_number);

    Result<std::shared_ptr<arrow::RecordBatch>> Next();

    Result<uint64_t> GetPreviousBatchFirstRowNumber() const {
        return previous_first_row_;
    }

    uint64_t GetNextRowToRead() const {
        return next_row_to_read_;
    }

    uint64_t GetNumberOfRows() const {
        return num_rows_;
    }

    int32_t GetNumberOfRowGroups() const {
        return file_reader_->num_row_groups();
    }

    ::parquet::arrow::FileReader* GetFileReader() const {
        return file_reader_.get();
    }

    const std::vector<std::pair<uint64_t, uint64_t>>& GetAllRowGroupRanges() const {
        return all_row_group_ranges_;
    }

    Result<std::shared_ptr<arrow::Schema>> GetSchema() const {
        std::shared_ptr<arrow::Schema> file_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetSchema(&file_schema));
        return file_schema;
    }

    Status Close() {
        if (batch_reader_) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(batch_reader_->Close());
        }
        return Status::OK();
    }

    Result<std::vector<std::pair<uint64_t, uint64_t>>> GetRowGroupRanges(
        const std::set<int32_t>& row_group_indices) const;

    Status PrepareForReadingLazy(const std::set<int32_t>& row_group_indices,
                                 const std::vector<int32_t>& column_indices);
    Status PrepareForReading(const std::set<int32_t>& row_group_indices,
                             const std::vector<int32_t>& column_indices);

    Result<std::set<int32_t>> FilterRowGroupsByReadRanges(
        const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges,
        const std::vector<int32_t>& src_row_groups) const;

 private:
    FileReaderWrapper(std::unique_ptr<::parquet::arrow::FileReader>&& file_reader,
                      const std::vector<std::pair<uint64_t, uint64_t>>& all_row_group_ranges,
                      uint64_t num_rows);

    Result<std::set<int32_t>> ReadRangesToRowGroupIds(
        const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) const;
    Result<int32_t> GetRowGroupId(std::pair<uint64_t, uint64_t> target_range) const;

    std::unique_ptr<::parquet::arrow::FileReader> file_reader_;
    std::unique_ptr<arrow::RecordBatchReader> batch_reader_;

    std::vector<std::pair<uint64_t, uint64_t>> all_row_group_ranges_;
    std::set<int32_t> target_row_group_indices_;
    std::vector<std::pair<uint64_t, uint64_t>> target_row_groups_;
    std::vector<int32_t> target_column_indices_;

    const uint64_t num_rows_;
    uint64_t next_row_to_read_ = std::numeric_limits<uint64_t>::max();
    uint64_t previous_first_row_ = std::numeric_limits<uint64_t>::max();
    uint64_t current_row_group_idx_ = 0;
    bool reader_initialized_ = false;
};

}  // namespace paimon::parquet
