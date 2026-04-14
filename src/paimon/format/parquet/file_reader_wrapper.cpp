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

#include "paimon/format/parquet/file_reader_wrapper.h"

#include <cassert>
#include <cstddef>

#include "arrow/io/interfaces.h"
#include "arrow/record_batch.h"
#include "arrow/util/range.h"
#include "fmt/format.h"
#include "paimon/format/parquet/column_index_filter.h"
#include "paimon/format/parquet/page_filtered_row_group_reader.h"
#include "paimon/macros.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/page_index.h"

namespace paimon::parquet {

Result<std::unique_ptr<FileReaderWrapper>> FileReaderWrapper::Create(
    std::unique_ptr<::parquet::arrow::FileReader>&& file_reader,
    ::arrow::MemoryPool* pool,
    int64_t batch_size) {
    if (file_reader == nullptr) {
        return Status::Invalid("file reader wrapper create failed. file reader is nullptr");
    }
    std::vector<std::pair<uint64_t, uint64_t>> all_row_group_ranges;
    auto meta_data = file_reader->parquet_reader()->metadata();
    // prepare [start_row_idx, end_row_idx) for all row groups
    uint64_t start_row_idx = 0;
    for (int32_t i = 0; i < meta_data->num_row_groups(); i++) {
        uint64_t end_row_idx = start_row_idx + meta_data->RowGroup(i)->num_rows();
        all_row_group_ranges.emplace_back(start_row_idx, end_row_idx);
        start_row_idx = end_row_idx;
    }
    uint64_t num_rows = file_reader->parquet_reader()->metadata()->num_rows();
    if (start_row_idx != num_rows) {
        assert(false);
        return Status::Invalid(
            fmt::format("unexpected error. row group ranges not match with num rows {}", num_rows));
    }
    std::vector<int32_t> row_groups_indices = arrow::internal::Iota(file_reader->num_row_groups());
    std::vector<int32_t> columns_indices =
        arrow::internal::Iota(file_reader->parquet_reader()->metadata()->num_columns());
    auto file_reader_wrapper = std::unique_ptr<FileReaderWrapper>(
        new FileReaderWrapper(std::move(file_reader), all_row_group_ranges, num_rows, pool,
                              batch_size));
    PAIMON_RETURN_NOT_OK(file_reader_wrapper->PrepareForReadingLazy(
        std::set<int32_t>(row_groups_indices.begin(), row_groups_indices.end()), columns_indices));
    return file_reader_wrapper;
}

FileReaderWrapper::~FileReaderWrapper() {
    WaitForPendingPreBuffer();
}

FileReaderWrapper::FileReaderWrapper(
    std::unique_ptr<::parquet::arrow::FileReader>&& file_reader,
    const std::vector<std::pair<uint64_t, uint64_t>>& all_row_group_ranges, uint64_t num_rows,
    ::arrow::MemoryPool* pool, int64_t batch_size)
    : file_reader_(std::move(file_reader)),
      all_row_group_ranges_(all_row_group_ranges),
      pool_(pool),
      batch_size_(batch_size),
      num_rows_(num_rows) {}

void FileReaderWrapper::WaitForPendingPreBuffer() {
    if (!prebuffered_row_groups_.empty() && file_reader_) {
        // Wait for all outstanding PreBuffer async reads to complete before destruction.
        // Without this, JindoSDK async pread callbacks may fire after the underlying
        // buffers and memory pool are freed, causing use-after-free crashes.
        auto status = file_reader_->parquet_reader()->WhenBuffered(
            prebuffered_row_groups_, prebuffered_columns_).status();
        (void)status;  // Best-effort; ignore errors during cleanup
        prebuffered_row_groups_.clear();
        prebuffered_columns_.clear();
    }
}

Status FileReaderWrapper::SeekToRow(uint64_t row_number) {
    // Reset any in-progress batched page-filtered consumption
    current_filtered_batch_.reset();
    filtered_batch_offset_ = 0;

    for (uint64_t i = 0; i < target_row_groups_.size(); i++) {
        if (row_number > target_row_groups_[i].first && row_number < target_row_groups_[i].second) {
            return Status::Invalid(fmt::format(
                "seek to row failed. row number {} should not be in the middle of readable range",
                row_number));
        }
        if (target_row_groups_[i].first >= row_number) {
            current_row_group_idx_ = i;
            next_row_to_read_ = target_row_groups_[i].first;

            // Clear pending filtered reads before seek position
            for (auto it = pending_filtered_reads_.begin(); it != pending_filtered_reads_.end();) {
                if (it->first < i) {
                    it = pending_filtered_reads_.erase(it);
                } else {
                    ++it;
                }
            }

            // Rebuild batch_reader_ only for non-page-filtered row groups at/after seek position
            std::vector<int32_t> target_row_group_indices;
            for (uint64_t j = i; j < target_row_groups_.size(); j++) {
                if (page_filtered_indices_.count(j) == 0) {
                    PAIMON_ASSIGN_OR_RAISE(int32_t row_group_id,
                                           GetRowGroupId(target_row_groups_[j]));
                    target_row_group_indices.push_back(row_group_id);
                }
            }
            if (!target_row_group_indices.empty()) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetRecordBatchReader(
                    target_row_group_indices, target_column_indices_, &batch_reader_));
            } else {
                batch_reader_.reset();
            }
            return Status::OK();
        }
    }
    next_row_to_read_ = num_rows_;
    current_row_group_idx_ = target_row_groups_.size();
    return Status::OK();
}

Result<std::shared_ptr<arrow::RecordBatch>> FileReaderWrapper::Next() {
    if (PAIMON_UNLIKELY(!reader_initialized_)) {
        PAIMON_RETURN_NOT_OK(PrepareForReading(target_row_group_indices_, target_column_indices_));
    }

    std::shared_ptr<arrow::RecordBatch> record_batch;

    // If we're still consuming slices from a page-filtered batch, return the next slice
    if (current_filtered_batch_) {
        int64_t remaining = current_filtered_batch_->num_rows() - filtered_batch_offset_;
        int64_t slice_len = (batch_size_ > 0 && remaining > batch_size_)
            ? batch_size_ : remaining;
        record_batch = current_filtered_batch_->Slice(filtered_batch_offset_, slice_len);
        filtered_batch_offset_ += slice_len;
        previous_first_row_ = next_row_to_read_;

        if (filtered_batch_offset_ >= current_filtered_batch_->num_rows()) {
            current_filtered_batch_.reset();
            filtered_batch_offset_ = 0;
            // Advance to next row group
            if (current_row_group_idx_ == target_row_groups_.size() - 1) {
                next_row_to_read_ = num_rows_;
            } else {
                current_row_group_idx_++;
                next_row_to_read_ = target_row_groups_[current_row_group_idx_].first;
            }
        }
        return record_batch;
    }

    if (current_row_group_idx_ >= target_row_groups_.size()) {
        previous_first_row_ = next_row_to_read_;
        return record_batch;  // nullptr - end of data
    }

    // Check if the current row group uses page-filtered reading (lazy on-demand)
    auto pending_it = pending_filtered_reads_.find(current_row_group_idx_);
    if (pending_it != pending_filtered_reads_.end()) {
        const auto& meta = pending_it->second;
        PAIMON_ASSIGN_OR_RAISE(
            auto full_batch,
            PageFilteredRowGroupReader::ReadFilteredRowGroup(
                file_reader_->parquet_reader(), meta.rg_index, meta.row_ranges,
                meta.column_indices, meta.read_schema, pool_, meta.cache_options,
                /*pre_buffered=*/true));
        pending_filtered_reads_.erase(pending_it);

        // If batch exceeds batch_size_, store and return first slice
        if (batch_size_ > 0 && full_batch && full_batch->num_rows() > batch_size_) {
            current_filtered_batch_ = full_batch;
            filtered_batch_offset_ = batch_size_;
            record_batch = full_batch->Slice(0, batch_size_);
        } else {
            record_batch = std::move(full_batch);
        }
    } else if (batch_reader_) {
        // Use the standard batch reader for fully matched row groups
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(record_batch, batch_reader_->Next());
    }

    if (record_batch) {
        int64_t num_rows = record_batch->num_rows();
        previous_first_row_ = next_row_to_read_;

        // For page-filtered batches, advance to the next row group
        // (unless we're in batched mode with slices remaining)
        if (page_filtered_indices_.count(current_row_group_idx_) > 0) {
            if (!current_filtered_batch_) {
                // Fully consumed or small enough for one batch, advance
                if (current_row_group_idx_ == target_row_groups_.size() - 1) {
                    next_row_to_read_ = num_rows_;
                } else {
                    current_row_group_idx_++;
                    next_row_to_read_ = target_row_groups_[current_row_group_idx_].first;
                }
            }
            // else: still consuming slices, stay on current row group
        } else if (next_row_to_read_ + num_rows <
                   target_row_groups_[current_row_group_idx_].second) {
            next_row_to_read_ += num_rows;
        } else if (next_row_to_read_ + num_rows ==
                   target_row_groups_[current_row_group_idx_].second) {
            if (current_row_group_idx_ == target_row_groups_.size() - 1) {
                next_row_to_read_ = num_rows_;
            } else {
                current_row_group_idx_++;
                next_row_to_read_ = target_row_groups_[current_row_group_idx_].first;
            }
        } else {
            return Status::Invalid(fmt::format(
                "Next failed. Unexpected error, next row to read {} + num rows just read {} "
                "should always be within current row group range or exactly equals to current "
                "row group end {}",
                next_row_to_read_, num_rows, target_row_groups_[current_row_group_idx_].second));
        }
    } else {
        previous_first_row_ = next_row_to_read_;
    }
    return record_batch;
}

Result<std::vector<std::pair<uint64_t, uint64_t>>> FileReaderWrapper::GetRowGroupRanges(
    const std::set<int32_t>& row_group_indices) const {
    std::vector<std::pair<uint64_t, uint64_t>> row_group_ranges;
    for (auto row_group_index : row_group_indices) {
        if (static_cast<size_t>(row_group_index) >= all_row_group_ranges_.size()) {
            return Status::Invalid(fmt::format("row group index {} is out of bound {}",
                                               row_group_index, all_row_group_ranges_.size()));
        }
        row_group_ranges.push_back(all_row_group_ranges_[row_group_index]);
    }
    return row_group_ranges;
}

Status FileReaderWrapper::PrepareForReadingLazy(const std::set<int32_t>& target_row_group_indices,
                                                const std::vector<int32_t>& column_indices) {
    target_row_group_indices_ = target_row_group_indices;
    target_column_indices_ = column_indices;
    reader_initialized_ = false;
    return Status::OK();
}

Status FileReaderWrapper::PrepareForReading(const std::set<int32_t>& target_row_group_indices,
                                            const std::vector<int32_t>& column_indices) {
    std::vector<std::pair<uint64_t, uint64_t>> target_row_groups;
    PAIMON_ASSIGN_OR_RAISE(target_row_groups, GetRowGroupRanges(target_row_group_indices));

    // Build position map: rg_index -> position in target_row_groups (O(1) lookup)
    std::map<int32_t, uint64_t> rg_idx_to_position;
    {
        uint64_t pos = 0;
        for (int32_t rg_idx : target_row_group_indices) {
            rg_idx_to_position[rg_idx] = pos++;
        }
    }

    // Separate row groups into fully matched (standard reader) and partially matched
    // (page-filtered, lazy on-demand reading)
    std::vector<int32_t> fully_matched_row_groups;
    pending_filtered_reads_.clear();
    page_filtered_indices_.clear();

    std::shared_ptr<arrow::Schema> read_schema;
    for (int32_t rg_idx : target_row_group_indices) {
        auto range_it = row_group_row_ranges_.find(rg_idx);
        if (range_it != row_group_row_ranges_.end()) {
            uint64_t pos = rg_idx_to_position[rg_idx];
            page_filtered_indices_.insert(pos);

            // Build read_schema lazily on first page-filtered row group
            if (!read_schema) {
                std::shared_ptr<arrow::Schema> schema;
                PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetSchema(&schema));
                std::vector<std::shared_ptr<arrow::Field>> fields;
                auto parquet_schema = file_reader_->parquet_reader()->metadata()->schema();
                for (int32_t col_idx : column_indices) {
                    const std::string& col_name = parquet_schema->Column(col_idx)->name();
                    auto field = schema->GetFieldByName(col_name);
                    if (field) {
                        fields.push_back(field);
                    }
                }
                read_schema = arrow::schema(fields);
            }

            // Store metadata for lazy on-demand reading instead of eager pre-read
            pending_filtered_reads_[pos] = PageFilteredRowGroupMeta{
                rg_idx, range_it->second, column_indices, read_schema,
                file_reader_->properties().cache_options()};
        } else {
            fully_matched_row_groups.push_back(rg_idx);
        }
    }


    // Wait for any previously pre-buffered data before starting new pre-buffer.
    WaitForPendingPreBuffer();

    // Create standard reader for fully matched row groups FIRST.
    // GetRecordBatchReader internally calls PreBuffer, but we'll override it below
    // with a single PreBuffer covering ALL row groups (page-filtered + fully-matched)
    // so that async I/O for all files starts in parallel.
    std::unique_ptr<arrow::RecordBatchReader> batch_reader;
    if (!fully_matched_row_groups.empty()) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetRecordBatchReader(
            fully_matched_row_groups, column_indices, &batch_reader));
    }

    // Single PreBuffer for ALL target row groups (both page-filtered and fully-matched).
    // This replaces the cache created by GetRecordBatchReader, but includes all ranges,
    // ensuring parallel I/O across all files/row groups.
    {
        std::vector<int> all_rg_vec;
        all_rg_vec.reserve(target_row_group_indices.size());
        for (int32_t rg_idx : target_row_group_indices) {
            all_rg_vec.push_back(rg_idx);
        }
        std::vector<int> col_vec(column_indices.begin(), column_indices.end());
        const auto& cache_opts = file_reader_->properties().cache_options();
        ::arrow::io::IOContext io_ctx(pool_);
        file_reader_->parquet_reader()->PreBuffer(all_rg_vec, col_vec, io_ctx, cache_opts);
        // Track for cleanup on destruction
        prebuffered_row_groups_ = all_rg_vec;
        prebuffered_columns_ = col_vec;
    }
    target_row_groups_ = target_row_groups;
    target_column_indices_ = column_indices;
    batch_reader_ = std::move(batch_reader);
    if (target_row_groups_.empty()) {
        next_row_to_read_ = num_rows_;
    } else {
        next_row_to_read_ = target_row_groups_[0].first;
    }
    previous_first_row_ = std::numeric_limits<uint64_t>::max();
    current_row_group_idx_ = 0;
    reader_initialized_ = true;
    return Status::OK();
}

Result<std::set<int32_t>> FileReaderWrapper::FilterRowGroupsByReadRanges(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges,
    const std::vector<int32_t>& src_row_groups) const {
    std::set<int32_t> target_row_groups;
    PAIMON_ASSIGN_OR_RAISE(std::set<int32_t> row_groups_to_read,
                           ReadRangesToRowGroupIds(read_ranges));
    for (const auto& row_group_id : src_row_groups) {
        if (row_groups_to_read.find(row_group_id) != row_groups_to_read.end()) {
            target_row_groups.emplace(row_group_id);
        }
    }
    return target_row_groups;
}

Result<std::set<int32_t>> FileReaderWrapper::ReadRangesToRowGroupIds(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) const {
    std::set<int32_t> selected_row_group_ids;
    for (const auto& read_range : read_ranges) {
        PAIMON_ASSIGN_OR_RAISE(int32_t row_group_id, GetRowGroupId(read_range));
        selected_row_group_ids.emplace(row_group_id);
    }
    return selected_row_group_ids;
}

Result<int32_t> FileReaderWrapper::GetRowGroupId(std::pair<uint64_t, uint64_t> target_range) const {
    for (size_t i = 0; i < all_row_group_ranges_.size(); i++) {
        if (all_row_group_ranges_[i] == target_range) {
            return i;
        }
    }
    return Status::Invalid(fmt::format(
        "not expected failure. target range bound '{},{}' not match with row group range bound",
        target_range.first, target_range.second));
}

std::shared_ptr<::parquet::PageIndexReader> FileReaderWrapper::GetPageIndexReader() {
    return file_reader_->parquet_reader()->GetPageIndexReader();
}

Result<RowRanges> FileReaderWrapper::CalculateFilteredRowRanges(
    int32_t row_group_index,
    const std::shared_ptr<Predicate>& predicate,
    const std::map<std::string, int32_t>& column_name_to_index) {
    if (!predicate) {
        auto meta_data = file_reader_->parquet_reader()->metadata();
        int64_t row_count = meta_data->RowGroup(row_group_index)->num_rows();
        return RowRanges::CreateSingle(row_count);
    }

    auto page_index_reader = GetPageIndexReader();
    if (!page_index_reader) {
        auto meta_data = file_reader_->parquet_reader()->metadata();
        int64_t row_count = meta_data->RowGroup(row_group_index)->num_rows();
        return RowRanges::CreateSingle(row_count);
    }

    auto meta_data = file_reader_->parquet_reader()->metadata();
    int64_t row_count = meta_data->RowGroup(row_group_index)->num_rows();

    return ColumnIndexFilter::CalculateRowRanges(
        predicate, page_index_reader, column_name_to_index, row_group_index, row_count);
}

}  // namespace paimon::parquet
