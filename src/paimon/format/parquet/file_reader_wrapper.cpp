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

#include <algorithm>
#include <cassert>
#include <cstddef>

#include "arrow/io/interfaces.h"
#include "arrow/record_batch.h"
#include "arrow/util/range.h"
#include "fmt/format.h"
#include "paimon/format/parquet/column_index_filter.h"
#include "paimon/format/parquet/page_filtered_row_group_reader.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/macros.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/page_index.h"

namespace paimon::parquet {

namespace {

// Merge overlapping or adjacent ReadRanges into a minimal set of non-overlapping ranges.
// PreBufferRanges requires non-overlapping ranges, so this is necessary when combining
// ranges from multiple sources (page-level ranges, column chunk ranges, etc.).
std::vector<::arrow::io::ReadRange> MergeOverlappingRanges(
    std::vector<::arrow::io::ReadRange> ranges) {
    if (ranges.empty()) {
        return ranges;
    }

    // Sort by offset
    std::sort(ranges.begin(), ranges.end(),
              [](const ::arrow::io::ReadRange& a, const ::arrow::io::ReadRange& b) {
                  return a.offset < b.offset;
              });

    std::vector<::arrow::io::ReadRange> merged;
    merged.push_back(ranges[0]);

    for (size_t i = 1; i < ranges.size(); ++i) {
        auto& last = merged.back();
        const auto& curr = ranges[i];
        // Check if current range overlaps or is adjacent to the last merged range
        int64_t last_end = last.offset + last.length;
        if (curr.offset <= last_end) {
            // Merge: extend the last range if current extends beyond it
            int64_t curr_end = curr.offset + curr.length;
            if (curr_end > last_end) {
                last.length = curr_end - last.offset;
            }
        } else {
            // No overlap, add as new range
            merged.push_back(curr);
        }
    }

    return merged;
}

}  // namespace

Result<std::unique_ptr<FileReaderWrapper>> FileReaderWrapper::Create(
    std::unique_ptr<::parquet::arrow::FileReader>&& file_reader, int64_t batch_size,
    std::shared_ptr<::arrow::MemoryPool> pool) {
    try {
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
            return Status::Invalid(fmt::format(
                "unexpected error. row group ranges not match with num rows {}", num_rows));
        }
        std::vector<int32_t> columns_indices =
            arrow::internal::Iota(file_reader->parquet_reader()->metadata()->num_columns());
        auto file_reader_wrapper = std::unique_ptr<FileReaderWrapper>(new FileReaderWrapper(
            std::move(file_reader), all_row_group_ranges, num_rows, batch_size, pool));
        std::vector<TargetRowGroup> all_target_row_groups;
        for (int32_t i = 0; i < file_reader_wrapper->GetNumberOfRowGroups(); i++) {
            all_target_row_groups.emplace_back(/*rg_index=*/i, /*page_filtered=*/false,
                                               /*ranges=*/RowRanges());
        }
        PAIMON_RETURN_NOT_OK(
            file_reader_wrapper->PrepareForReadingLazy(all_target_row_groups, columns_indices));
        return file_reader_wrapper;
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("FileReaderWrapper::Create")
}

FileReaderWrapper::~FileReaderWrapper() {
    WaitForPendingPreBuffer();
}

Result<std::shared_ptr<arrow::Schema>> FileReaderWrapper::GetSchema() const {
    try {
        std::shared_ptr<arrow::Schema> file_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetSchema(&file_schema));
        return file_schema;
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("FileReaderWrapper::GetSchema")
}

Status FileReaderWrapper::Close() {
    try {
        if (batch_reader_) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(batch_reader_->Close());
        }
        return Status::OK();
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("FileReaderWrapper::Close")
}

FileReaderWrapper::FileReaderWrapper(
    std::unique_ptr<::parquet::arrow::FileReader>&& file_reader,
    const std::vector<std::pair<uint64_t, uint64_t>>& all_row_group_ranges, uint64_t num_rows,
    int64_t batch_size, std::shared_ptr<::arrow::MemoryPool> pool)
    : file_reader_(std::move(file_reader)),
      all_row_group_ranges_(all_row_group_ranges),
      pool_(pool),
      batch_size_(batch_size),
      num_rows_(num_rows) {}

void FileReaderWrapper::WaitForPendingPreBuffer() {
    if (!prebuffered_ranges_.empty() && file_reader_) {
        // Wait for all outstanding PreBuffer async reads to complete before destruction.
        // Without this, JindoSDK async pread callbacks may fire after the underlying
        // buffers and memory pool are freed, causing use-after-free crashes.
        auto status =
            file_reader_->parquet_reader()->WhenBufferedRanges(prebuffered_ranges_).status();
        (void)status;  // Best-effort; ignore errors during cleanup
        prebuffered_ranges_.clear();
    }
}

void FileReaderWrapper::AdvanceToNextRowGroup() {
    current_row_group_idx_++;
    // Skip row groups excluded by read range.
    while (current_row_group_idx_ < target_row_groups_.size() &&
           target_row_groups_[current_row_group_idx_].excluded_by_read_range) {
        current_row_group_idx_++;
    }
    if (current_row_group_idx_ >= target_row_groups_.size()) {
        next_row_to_read_ = num_rows_;
    } else {
        next_row_to_read_ =
            all_row_group_ranges_[target_row_groups_[current_row_group_idx_].row_group_index].first;
    }
}

Status FileReaderWrapper::SeekToRow(uint64_t row_number) {
    try {
        current_page_filtered_reader_.reset();
        filtered_global_offset_ = 0;

        for (uint64_t i = 0; i < target_row_groups_.size(); i++) {
            if (target_row_groups_[i].excluded_by_read_range) {
                continue;
            }
            uint32_t rg_id = target_row_groups_[i].row_group_index;
            uint64_t rg_start = all_row_group_ranges_[rg_id].first;
            uint64_t rg_end = all_row_group_ranges_[rg_id].second;
            if (row_number > rg_start && row_number < rg_end) {
                return Status::Invalid(
                    fmt::format("seek to row failed. row number {} should not be in the middle of "
                                "readable range",
                                row_number));
            }
            if (rg_start >= row_number) {
                current_row_group_idx_ = i;
                next_row_to_read_ = rg_start;

                // Rebuild batch_reader_ for non-page-filtered RGs at/after seek position.
                std::vector<int32_t> fully_matched_indices;
                for (uint64_t j = i; j < target_row_groups_.size(); j++) {
                    if (!target_row_groups_[j].excluded_by_read_range &&
                        !target_row_groups_[j].is_partially_matched) {
                        fully_matched_indices.push_back(target_row_groups_[j].row_group_index);
                    }
                }
                if (!fully_matched_indices.empty()) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetRecordBatchReader(
                        fully_matched_indices, target_column_indices_, &batch_reader_));
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
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("FileReaderWrapper::SeekToRow")
}

Result<std::shared_ptr<arrow::RecordBatch>> FileReaderWrapper::NextPageFiltered() {
    int32_t rg_id = target_row_groups_[current_row_group_idx_].row_group_index;

    // Construct the per-RG streaming reader on demand.
    if (!current_page_filtered_reader_) {
        const auto& target_rg = target_row_groups_[current_row_group_idx_];
        auto page_ranges = PageFilteredRowGroupReader::ComputePageRanges(
            file_reader_->parquet_reader(), target_rg, target_column_indices_);
        bool pre_buffered = !prebuffered_ranges_.empty();
        int64_t max_chunksize = batch_size_ > 0 ? batch_size_ : std::numeric_limits<int64_t>::max();
        PAIMON_ASSIGN_OR_RAISE(
            current_page_filtered_reader_,
            PageFilteredRowGroupReader::ReadFilteredRowGroup(
                file_reader_->parquet_reader(), target_rg, target_column_indices_,
                page_filtered_read_schema_, file_reader_->properties().cache_options(),
                pre_buffered, page_ranges, max_chunksize, pool_));
        current_filtered_row_ranges_ = target_rg.row_ranges;
        current_filtered_rg_start_ = all_row_group_ranges_[rg_id].first;
        filtered_global_offset_ = 0;
    }

    std::shared_ptr<arrow::RecordBatch> record_batch;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(current_page_filtered_reader_->ReadNext(&record_batch));

    if (record_batch) {
        auto original_row =
            current_filtered_row_ranges_.MapFilteredIndexToOriginalRow(filtered_global_offset_);
        previous_first_row_ = original_row.has_value() ? current_filtered_rg_start_ +
                                                             static_cast<uint64_t>(*original_row)
                                                       : current_filtered_rg_start_;
        filtered_global_offset_ += record_batch->num_rows();
        return record_batch;
    }

    // RG exhausted — reset and advance.
    current_page_filtered_reader_.reset();
    filtered_global_offset_ = 0;
    AdvanceToNextRowGroup();
    return std::shared_ptr<arrow::RecordBatch>();
}

Result<std::shared_ptr<arrow::RecordBatch>> FileReaderWrapper::NextFullyMatched() {
    if (!batch_reader_) {
        return std::shared_ptr<arrow::RecordBatch>();
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::RecordBatch> record_batch,
                                      batch_reader_->Next());
    if (!record_batch) {
        return std::shared_ptr<arrow::RecordBatch>();
    }

    int32_t rg_id = target_row_groups_[current_row_group_idx_].row_group_index;
    uint64_t rg_end = all_row_group_ranges_[rg_id].second;
    int64_t num_rows = record_batch->num_rows();

    previous_first_row_ = next_row_to_read_;
    if (next_row_to_read_ + num_rows < rg_end) {
        next_row_to_read_ += num_rows;
    } else if (next_row_to_read_ + num_rows == rg_end) {
        AdvanceToNextRowGroup();
    } else {
        return Status::Invalid(
            fmt::format("Next failed. next_row_to_read {} + num_rows {} exceeds row group end {}",
                        next_row_to_read_, num_rows, rg_end));
    }
    return record_batch;
}

Result<std::shared_ptr<arrow::RecordBatch>> FileReaderWrapper::Next() {
    try {
        if (PAIMON_UNLIKELY(!reader_initialized_)) {
            PAIMON_RETURN_NOT_OK(PrepareForReading(target_row_groups_, target_column_indices_));
        }

        while (current_row_group_idx_ < target_row_groups_.size()) {
            bool is_page_filtered = target_row_groups_[current_row_group_idx_].is_partially_matched;
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch,
                                   is_page_filtered ? NextPageFiltered() : NextFullyMatched());
            if (batch) {
                return batch;
            } else if (!is_page_filtered) {
                // Null from fully-matched path means batch_reader_ is globally exhausted.
                break;
            }
            // current_row_group_idx_ has been advanced in NextPageFiltered() or NextFullyMatched(),
            // loop to try next RG.
        }

        previous_first_row_ = next_row_to_read_;
        return std::shared_ptr<arrow::RecordBatch>();
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("FileReaderWrapper::Next")
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

Status FileReaderWrapper::PrepareForReadingLazy(
    const std::vector<TargetRowGroup>& target_row_groups,
    const std::vector<int32_t>& column_indices) {
    target_row_groups_ = target_row_groups;
    target_column_indices_ = column_indices;
    reader_initialized_ = false;
    return Status::OK();
}

Status FileReaderWrapper::BuildPageFilteredSchema(const std::vector<int32_t>& column_indices) {
    if (page_filtered_read_schema_) {
        return Status::OK();
    }
    std::shared_ptr<arrow::Schema> schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetSchema(&schema));
    auto parquet_schema = file_reader_->parquet_reader()->metadata()->schema();
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int32_t col_idx : column_indices) {
        const std::string& col_name = parquet_schema->Column(col_idx)->name();
        auto field = schema->GetFieldByName(col_name);
        if (!field) {
            return Status::Invalid(fmt::format(
                "PrepareForReading: Parquet column {} ('{}') has no matching Arrow field in "
                "file schema",
                col_idx, col_name));
        }
        fields.push_back(field);
    }
    page_filtered_read_schema_ = arrow::schema(fields);
    return Status::OK();
}

std::vector<::arrow::io::ReadRange> FileReaderWrapper::CollectPreBufferRanges(
    const std::vector<int32_t>& column_indices) {
    std::vector<::arrow::io::ReadRange> ranges;
    auto file_metadata = file_reader_->parquet_reader()->metadata();

    for (const auto& trg : target_row_groups_) {
        if (trg.excluded_by_read_range) continue;

        if (trg.is_partially_matched) {
            // Page-filtered RGs: only matching page byte ranges.
            auto page_ranges = PageFilteredRowGroupReader::ComputePageRanges(
                file_reader_->parquet_reader(), trg, column_indices);
            ranges.insert(ranges.end(), std::make_move_iterator(page_ranges.begin()),
                          std::make_move_iterator(page_ranges.end()));
        } else {
            // Fully-matched RGs: entire column chunk ranges.
            auto rg_metadata = file_metadata->RowGroup(trg.row_group_index);
            for (int32_t col_idx : column_indices) {
                auto col_chunk = rg_metadata->ColumnChunk(col_idx);
                int64_t offset = col_chunk->data_page_offset();
                if (col_chunk->has_dictionary_page() && col_chunk->dictionary_page_offset() > 0 &&
                    offset > col_chunk->dictionary_page_offset()) {
                    offset = col_chunk->dictionary_page_offset();
                }
                ranges.push_back({offset, col_chunk->total_compressed_size()});
            }
        }
    }
    return ranges;
}

void FileReaderWrapper::DispatchPreBuffer(std::vector<::arrow::io::ReadRange> ranges) {
    const auto& cache_opts = file_reader_->properties().cache_options();
    ::arrow::io::IOContext io_ctx(pool_.get());
    auto merged_ranges = MergeOverlappingRanges(std::move(ranges));
    try {
        file_reader_->parquet_reader()->PreBufferRanges(merged_ranges, io_ctx, cache_opts);
        prebuffered_ranges_ = std::move(merged_ranges);
    } catch (const std::exception&) {
        prebuffered_ranges_.clear();
    }
}

Status FileReaderWrapper::PrepareForReading(const std::vector<TargetRowGroup>& target_row_groups,
                                            const std::vector<int32_t>& column_indices) {
    try {
        target_row_groups_ = target_row_groups;
        target_column_indices_ = column_indices;
        page_filtered_read_schema_.reset();

        // Partition into fully-matched and page-filtered row groups, skipping excluded ones.
        std::vector<int32_t> fully_matched_row_groups;
        uint64_t active_count = 0;
        for (const auto& trg : target_row_groups_) {
            if (trg.excluded_by_read_range) {
                continue;
            }
            active_count++;
            if (!trg.is_partially_matched) {
                fully_matched_row_groups.push_back(trg.row_group_index);
            }
        }

        bool has_page_filtered = fully_matched_row_groups.size() != active_count;
        if (has_page_filtered) {
            PAIMON_RETURN_NOT_OK(BuildPageFilteredSchema(column_indices));
        }

        WaitForPendingPreBuffer();

        // Create standard reader for fully-matched row groups.
        if (!fully_matched_row_groups.empty()) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetRecordBatchReader(
                fully_matched_row_groups, column_indices, &batch_reader_));
        } else {
            batch_reader_.reset();
        }

        // When page-filtered RGs exist, issue a single PreBuffer covering both kinds.
        // Otherwise GetRecordBatchReader already issued PreBuffer internally.
        if (has_page_filtered) {
            auto all_ranges = CollectPreBufferRanges(column_indices);
            DispatchPreBuffer(std::move(all_ranges));
        }

        // Reset read state. Find the first non-excluded row group.
        uint64_t first_active_idx = 0;
        while (first_active_idx < target_row_groups_.size() &&
               target_row_groups_[first_active_idx].excluded_by_read_range) {
            first_active_idx++;
        }
        if (first_active_idx >= target_row_groups_.size()) {
            next_row_to_read_ = num_rows_;
        } else {
            next_row_to_read_ =
                all_row_group_ranges_[target_row_groups_[first_active_idx].row_group_index].first;
        }
        previous_first_row_ = std::numeric_limits<uint64_t>::max();
        current_row_group_idx_ = first_active_idx;
        reader_initialized_ = true;
        return Status::OK();
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("FileReaderWrapper::PrepareForReading")
}

Status FileReaderWrapper::ApplyReadRanges(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) {
    if (read_ranges.empty()) {
        for (auto& trg : target_row_groups_) {
            trg.excluded_by_read_range = true;
        }
        reader_initialized_ = false;
        return Status::OK();
    }
    // Build a set of row group indices whose range matches one of the read ranges.
    std::set<int32_t> matching_rg_indices;
    for (const auto& read_range : read_ranges) {
        for (size_t i = 0; i < all_row_group_ranges_.size(); i++) {
            if (all_row_group_ranges_[i] == read_range) {
                matching_rg_indices.insert(static_cast<int32_t>(i));
            }
        }
    }
    // Mark each target row group as excluded or not based on the matching set.
    for (auto& trg : target_row_groups_) {
        trg.excluded_by_read_range = matching_rg_indices.count(trg.row_group_index) == 0;
    }
    reader_initialized_ = false;
    return Status::OK();
}

std::shared_ptr<::parquet::PageIndexReader> FileReaderWrapper::GetPageIndexReader() {
    try {
        return file_reader_->parquet_reader()->GetPageIndexReader();
    } catch (...) {
        // Page index is optional; degrade gracefully if the metadata read throws.
        return nullptr;
    }
}

Result<RowRanges> FileReaderWrapper::CalculateFilteredRowRanges(
    int32_t row_group_index, const std::shared_ptr<Predicate>& predicate,
    const std::map<std::string, int32_t>& column_name_to_index) {
    try {
        auto meta_data = file_reader_->parquet_reader()->metadata();
        int64_t row_count = meta_data->RowGroup(row_group_index)->num_rows();

        if (!predicate) {
            return RowRanges::CreateSingle(row_count);
        }

        auto page_index_reader = GetPageIndexReader();
        if (!page_index_reader) {
            return RowRanges::CreateSingle(row_count);
        }

        return ColumnIndexFilter::CalculateRowRanges(
            predicate, page_index_reader, column_name_to_index, row_group_index, row_count);
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("FileReaderWrapper::CalculateFilteredRowRanges")
}

}  // namespace paimon::parquet
