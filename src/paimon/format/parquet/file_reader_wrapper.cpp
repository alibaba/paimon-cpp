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
#include "paimon/macros.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/page_index.h"

// Convert any std::exception thrown by underlying Parquet/Arrow APIs into a
// Status. Used as the trailing catch clauses of a try block in every public
// method that calls into the parquet C++ API, so the read layer never throws.
#define PAIMON_PARQUET_CATCH_AND_RETURN_STATUS(context)                     \
    catch (const std::exception& e) {                                       \
        return Status::Invalid(fmt::format("{}: {}", (context), e.what())); \
    }                                                                       \
    catch (...) {                                                           \
        return Status::UnknownError((context), ": unknown error");          \
    }

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
    std::unique_ptr<::parquet::arrow::FileReader>&& file_reader, ::arrow::MemoryPool* pool,
    int64_t batch_size) {
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
        std::vector<int32_t> row_groups_indices =
            arrow::internal::Iota(file_reader->num_row_groups());
        std::vector<int32_t> columns_indices =
            arrow::internal::Iota(file_reader->parquet_reader()->metadata()->num_columns());
        auto file_reader_wrapper = std::unique_ptr<FileReaderWrapper>(new FileReaderWrapper(
            std::move(file_reader), all_row_group_ranges, num_rows, pool, batch_size));
        PAIMON_RETURN_NOT_OK(file_reader_wrapper->PrepareForReadingLazy(
            std::set<int32_t>(row_groups_indices.begin(), row_groups_indices.end()),
            columns_indices));
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
    ::arrow::MemoryPool* pool, int64_t batch_size)
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

Status FileReaderWrapper::SeekToRow(uint64_t row_number) {
    try {
        // Reset any in-progress page-filtered streaming
        current_page_filtered_reader_.reset();
        filtered_global_offset_ = 0;

        for (uint64_t i = 0; i < target_row_groups_.size(); i++) {
            if (row_number > target_row_groups_[i].first &&
                row_number < target_row_groups_[i].second) {
                return Status::Invalid(
                    fmt::format("seek to row failed. row number {} should not be in the middle of "
                                "readable range",
                                row_number));
            }
            if (target_row_groups_[i].first >= row_number) {
                current_row_group_idx_ = i;
                next_row_to_read_ = target_row_groups_[i].first;

                // Rebuild batch_reader_ only for non-page-filtered row groups at/after seek
                // position. Page-filtered RGs need no seek-side bookkeeping: their per-RG
                // reader is constructed on demand in Next() from row_group_row_ranges_ each
                // time, so backward seek "just works".
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
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("FileReaderWrapper::SeekToRow")
}

Result<std::shared_ptr<arrow::RecordBatch>> FileReaderWrapper::Next() {
    try {
        if (PAIMON_UNLIKELY(!reader_initialized_)) {
            PAIMON_RETURN_NOT_OK(
                PrepareForReading(target_row_group_indices_, target_column_indices_));
        }

        // Loop until we produce a batch or exhaust all row groups. A null from the active
        // per-RG reader means that RG is done; we advance and try the next RG without
        // surfacing a spurious null to the caller.
        while (current_row_group_idx_ < target_row_groups_.size()) {
            std::shared_ptr<arrow::RecordBatch> record_batch;
            bool is_page_filtered = page_filtered_indices_.count(current_row_group_idx_) > 0;

            if (is_page_filtered) {
                // Construct the per-RG streaming reader on demand. Inputs are recomputed each
                // time from existing wrapper fields (no per-RG meta cached on the wrapper),
                // mirroring how the fully-matched path delegates to Arrow's stateless
                // GetRecordBatchReader. This makes both forward and backward seeks work
                // uniformly: SeekToRow only resets current_page_filtered_reader_, and the
                // next Next() rebuilds from authoritative state.
                if (!current_page_filtered_reader_) {
                    PAIMON_ASSIGN_OR_RAISE(
                        int32_t rg_index,
                        GetRowGroupId(target_row_groups_[current_row_group_idx_]));
                    auto range_it = row_group_row_ranges_.find(rg_index);
                    if (range_it == row_group_row_ranges_.end()) {
                        return Status::Invalid(
                            fmt::format("page-filtered row group {} missing row ranges in "
                                        "row_group_row_ranges_",
                                        rg_index));
                    }
                    const RowRanges& row_ranges = range_it->second;
                    auto page_ranges = PageFilteredRowGroupReader::ComputePageRanges(
                        file_reader_->parquet_reader(), rg_index, row_ranges,
                        target_column_indices_);
                    bool pre_buffered = !prebuffered_ranges_.empty();
                    // batch_size_ == 0 means "no per-batch row cap" in the wrapper's contract,
                    // but TableBatchReader::set_chunksize(0) would loop forever emitting empty
                    // batches. Translate to int64_max so the reader produces one batch per
                    // underlying chunk boundary instead.
                    int64_t max_chunksize =
                        batch_size_ > 0 ? batch_size_ : std::numeric_limits<int64_t>::max();
                    PAIMON_ASSIGN_OR_RAISE(current_page_filtered_reader_,
                                           PageFilteredRowGroupReader::ReadFilteredRowGroup(
                                               file_reader_->parquet_reader(), rg_index, row_ranges,
                                               target_column_indices_, page_filtered_read_schema_,
                                               pool_, file_reader_->properties().cache_options(),
                                               pre_buffered, page_ranges, max_chunksize));
                    current_filtered_row_ranges_ = row_ranges;
                    current_filtered_rg_start_ = target_row_groups_[current_row_group_idx_].first;
                    filtered_global_offset_ = 0;
                }
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    current_page_filtered_reader_->ReadNext(&record_batch));
            } else if (batch_reader_) {
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(record_batch, batch_reader_->Next());
            }

            if (record_batch) {
                int64_t num_rows = record_batch->num_rows();
                if (is_page_filtered) {
                    // Map the cumulative filtered-row offset back to the original row index
                    // within this row group. Must be evaluated BEFORE incrementing the offset.
                    auto original_row = current_filtered_row_ranges_.MapFilteredIndexToOriginalRow(
                        filtered_global_offset_);
                    previous_first_row_ =
                        original_row.has_value()
                            ? current_filtered_rg_start_ + static_cast<uint64_t>(*original_row)
                            : current_filtered_rg_start_;
                    filtered_global_offset_ += num_rows;
                    // Stay on this RG; the next ReadNext will either return more data or null.
                } else {
                    previous_first_row_ = next_row_to_read_;
                    if (next_row_to_read_ + num_rows <
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
                            "Next failed. Unexpected error, next row to read {} + num rows just "
                            "read {} should always be within current row group range or exactly "
                            "equals to current row group end {}",
                            next_row_to_read_, num_rows,
                            target_row_groups_[current_row_group_idx_].second));
                    }
                }
                return record_batch;
            }

            // Null batch: current row group is exhausted (or fully-matched RGs hit a degenerate
            // EOF). Advance to the next row group and continue the loop.
            if (is_page_filtered) {
                current_page_filtered_reader_.reset();
                filtered_global_offset_ = 0;
                if (current_row_group_idx_ == target_row_groups_.size() - 1) {
                    next_row_to_read_ = num_rows_;
                    current_row_group_idx_ = target_row_groups_.size();
                } else {
                    current_row_group_idx_++;
                    next_row_to_read_ = target_row_groups_[current_row_group_idx_].first;
                }
            } else {
                // Fully-matched path: batch_reader_ is exhausted with no more RBs to align on
                // row counts. Stop here — remaining RGs (if any) should be page-filtered and
                // will be handled by re-entering the loop, but if we got here without advancing
                // first, treat as terminal to avoid an infinite loop.
                break;
            }
        }

        previous_first_row_ = next_row_to_read_;
        return std::shared_ptr<arrow::RecordBatch>();  // EOF
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

Status FileReaderWrapper::PrepareForReadingLazy(const std::set<int32_t>& target_row_group_indices,
                                                const std::vector<int32_t>& column_indices) {
    target_row_group_indices_ = target_row_group_indices;
    target_column_indices_ = column_indices;
    reader_initialized_ = false;
    return Status::OK();
}

Status FileReaderWrapper::PrepareForReading(const std::set<int32_t>& target_row_group_indices,
                                            const std::vector<int32_t>& column_indices) {
    try {
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

        // Separate row groups into fully matched (Arrow's standard reader) and partially
        // matched (page-filtered, per-RG reader constructed on demand in Next()).
        // Per-RG metadata for the page-filtered path is NOT cached on the wrapper — it's
        // recomputed on demand in Next() from row_group_row_ranges_ + target_column_indices_,
        // mirroring how the fully-matched path lets Arrow's FileReader own all metadata.
        std::vector<int32_t> fully_matched_row_groups;
        page_filtered_indices_.clear();
        page_filtered_read_schema_.reset();

        // Page-level byte ranges collected here only for the bulk PreBuffer call below;
        // discarded once PreBuffer is dispatched.
        std::vector<::arrow::io::ReadRange> page_filtered_byte_ranges;

        for (int32_t rg_idx : target_row_group_indices) {
            auto range_it = row_group_row_ranges_.find(rg_idx);
            if (range_it != row_group_row_ranges_.end()) {
                uint64_t pos = rg_idx_to_position[rg_idx];
                page_filtered_indices_.insert(pos);

                // Build the page-filter read_schema once on first encounter — it's identical
                // across all page-filtered RGs in this session.
                if (!page_filtered_read_schema_) {
                    std::shared_ptr<arrow::Schema> schema;
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetSchema(&schema));
                    std::vector<std::shared_ptr<arrow::Field>> fields;
                    auto parquet_schema = file_reader_->parquet_reader()->metadata()->schema();
                    for (int32_t col_idx : column_indices) {
                        const std::string& col_name = parquet_schema->Column(col_idx)->name();
                        auto field = schema->GetFieldByName(col_name);
                        if (!field) {
                            return Status::Invalid(fmt::format(
                                "PrepareForReading: Parquet column {} ('{}') has no matching Arrow "
                                "field in file schema",
                                col_idx, col_name));
                        }
                        fields.push_back(field);
                    }
                    page_filtered_read_schema_ = arrow::schema(fields);
                }

                auto page_ranges = PageFilteredRowGroupReader::ComputePageRanges(
                    file_reader_->parquet_reader(), rg_idx, range_it->second, column_indices);
                page_filtered_byte_ranges.insert(page_filtered_byte_ranges.end(),
                                                 std::make_move_iterator(page_ranges.begin()),
                                                 std::make_move_iterator(page_ranges.end()));
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

        // Collect all byte ranges for a single PreBufferRanges call.
        // Page-filtered RGs: only matching page ranges (from ComputePageRanges).
        // Fully-matched RGs: entire column chunk ranges.
        //
        // When there are no page-filtered RGs, skip the manual PreBufferRanges entirely:
        // GetRecordBatchReader has already issued PreBuffer internally (driven by
        // ArrowReaderProperties::pre_buffer=true), and a second PreBufferRanges call here
        // would tear down and rebuild cached_source_, redundantly re-issuing the same IO
        // on remote filesystems. The manual path is only needed to merge page-level ranges
        // with column-chunk ranges into a single PreBuffer covering both kinds of RGs.
        if (!page_filtered_indices_.empty()) {
            std::vector<::arrow::io::ReadRange> all_ranges = std::move(page_filtered_byte_ranges);

            // Fully-matched row groups: add entire column chunk ranges
            // The correct calculation follows Arrow's ColumnChunkMetaData::file_range():
            // - col_start = data_page_offset (or dictionary_page_offset if present and lower)
            // - col_length = total_compressed_size (includes all pages: dictionary + data)
            auto file_metadata = file_reader_->parquet_reader()->metadata();
            for (int32_t rg_idx : fully_matched_row_groups) {
                auto rg_metadata = file_metadata->RowGroup(rg_idx);
                for (int32_t col_idx : column_indices) {
                    auto col_chunk = rg_metadata->ColumnChunk(col_idx);
                    int64_t offset = col_chunk->data_page_offset();
                    if (col_chunk->has_dictionary_page() &&
                        col_chunk->dictionary_page_offset() > 0 &&
                        offset > col_chunk->dictionary_page_offset()) {
                        offset = col_chunk->dictionary_page_offset();
                    }
                    int64_t size = col_chunk->total_compressed_size();
                    all_ranges.push_back({offset, size});
                }
            }

            const auto& cache_opts = file_reader_->properties().cache_options();
            ::arrow::io::IOContext io_ctx(pool_);
            // Merge overlapping ranges before calling PreBufferRanges, which rejects overlapping
            // ranges.
            auto merged_ranges = MergeOverlappingRanges(std::move(all_ranges));
            // PreBuffer is an optimization - if it fails (e.g., IO error during testing),
            // continue without pre-buffering. Subsequent reads will fetch data on-demand.
            try {
                file_reader_->parquet_reader()->PreBufferRanges(merged_ranges, io_ctx, cache_opts);
                // Track for cleanup on destruction
                prebuffered_ranges_ = std::move(merged_ranges);
            } catch (const std::exception& e) {
                // Pre-buffering failed, clear ranges to indicate no pre-buffered data available.
                // Reading will fall back to on-demand I/O.
                prebuffered_ranges_.clear();
            }
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
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("FileReaderWrapper::PrepareForReading")
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
