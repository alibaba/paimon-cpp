/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/format/parquet/page_filtered_row_group_reader.h"

#include <algorithm>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/chunked_array.h"
#include "arrow/io/caching.h"
#include "arrow/io/interfaces.h"
#include "arrow/table.h"
#include "arrow/util/future.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/schema.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"

namespace paimon::parquet {

namespace {

/// Wraps an arrow::Table + TableBatchReader as a RecordBatchReader so the caller can
/// stream batches while ensuring every returned array offset is zero. The Table is held
/// to keep its ChunkedArrays alive for the inner TableBatchReader.
class TableRecordBatchReader : public arrow::RecordBatchReader {
 public:
    TableRecordBatchReader(std::shared_ptr<arrow::Table> table, int64_t chunksize,
                           std::shared_ptr<arrow::MemoryPool> pool)
        : table_(std::move(table)), inner_(*table_), pool_(std::move(pool)) {
        inner_.set_chunksize(chunksize);
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        return table_->schema();
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        ARROW_RETURN_NOT_OK(inner_.ReadNext(out));
        if (!*out) {
            return arrow::Status::OK();
        }

        // Page filtering may produce columns with different chunk boundaries. TableBatchReader
        // aligns them by slicing columns, which may leave non-zero child offsets.
        Result<std::shared_ptr<arrow::RecordBatch>> normalized_result =
            ArrowUtils::NormalizeRecordBatchOffsets(*out, pool_.get());
        if (!normalized_result.ok()) {
            return ToArrowStatus(normalized_result.status());
        }
        *out = std::move(normalized_result).value();
        return arrow::Status::OK();
    }

 private:
    std::shared_ptr<arrow::Table> table_;
    arrow::TableBatchReader inner_;
    std::shared_ptr<arrow::MemoryPool> pool_;
};

/// A FileColumnIterator that installs a data_page_filter on every PageReader it
/// produces, enabling I/O-level page skipping. The base class handles row group
/// iteration; this subclass only decorates the PageReader returned by NextChunk().
class PageFilteringColumnIterator : public ::parquet::arrow::FileColumnIterator {
 public:
    PageFilteringColumnIterator(
        int column_index, ::parquet::ParquetFileReader* reader, std::vector<int> row_groups,
        std::function<bool(const ::parquet::DataPageStats&)> data_page_filter)
        : FileColumnIterator(column_index, reader, std::move(row_groups)),
          data_page_filter_(std::move(data_page_filter)) {}

    std::unique_ptr<::parquet::PageReader> NextChunk() override {
        std::unique_ptr<::parquet::PageReader> page_reader = FileColumnIterator::NextChunk();
        if (page_reader && data_page_filter_) {
            page_reader->set_data_page_filter(data_page_filter_);
        }
        return page_reader;
    }

 private:
    std::function<bool(const ::parquet::DataPageStats&)> data_page_filter_;
};

}  // namespace

std::pair<int64_t, int64_t> PageFilteredRowGroupReader::GetPageRowRange(
    const std::vector<::parquet::PageLocation>& page_locations, int32_t page_idx,
    int64_t row_group_row_count) {
    int64_t first_row = page_locations[page_idx].first_row_index;
    int64_t last_row = (page_idx + 1 < static_cast<int32_t>(page_locations.size()))
                           ? page_locations[page_idx + 1].first_row_index - 1
                           : row_group_row_count - 1;
    return {first_row, last_row};
}

std::function<bool(const ::parquet::DataPageStats&)> PageFilteredRowGroupReader::MakePageFilter(
    const RowRanges& row_ranges, const std::shared_ptr<::parquet::OffsetIndex>& offset_index,
    int64_t row_group_row_count) {
    auto page_counter = std::make_shared<int32_t>(0);
    const auto& page_locations = offset_index->page_locations();
    auto num_pages = static_cast<int32_t>(page_locations.size());

    return [row_ranges, page_locations, num_pages, row_group_row_count,
            page_counter](const ::parquet::DataPageStats& /*stats*/) -> bool {
        int32_t page_idx = (*page_counter)++;
        if (page_idx >= num_pages) {
            return false;
        }
        auto [first_row, last_row] = GetPageRowRange(page_locations, page_idx, row_group_row_count);
        return !row_ranges.IsOverlapping(first_row, last_row);
    };
}

std::pair<RowRanges, int64_t> PageFilteredRowGroupReader::ComputeCompressedRowRanges(
    const RowRanges& original_ranges, const std::shared_ptr<::parquet::OffsetIndex>& offset_index,
    int64_t row_group_row_count) {
    const auto& page_locations = offset_index->page_locations();
    auto num_pages = static_cast<int32_t>(page_locations.size());
    const auto& ranges = original_ranges.GetRanges();

    RowRanges compressed;
    int64_t compressed_offset = 0;

    for (int32_t page_idx = 0; page_idx < num_pages; ++page_idx) {
        auto [page_from, page_to] = GetPageRowRange(page_locations, page_idx, row_group_row_count);
        int64_t page_size = page_to - page_from + 1;

        if (!original_ranges.IsOverlapping(page_from, page_to)) {
            // Page will be skipped by data_page_filter, not in compressed space
            continue;
        }

        for (const auto& range : ranges) {
            if (range.to < page_from) continue;
            if (range.from > page_to) break;
            int64_t overlap_from = std::max(range.from, page_from);
            int64_t overlap_to = std::min(range.to, page_to);
            compressed.Add(RowRanges::Range(compressed_offset + (overlap_from - page_from),
                                            compressed_offset + (overlap_to - page_from)));
        }

        compressed_offset += page_size;
    }

    return {compressed, compressed_offset};
}

Status PageFilteredRowGroupReader::ExecuteSkipReadPattern(
    ::parquet::arrow::ColumnReader* column_reader, int col_idx, const RowRanges& ranges,
    int64_t total) {
    PAIMON_RETURN_NOT_OK_FROM_ARROW(column_reader->ResetLeaf(col_idx, total));
    int64_t current = 0;
    for (const auto& range : ranges.GetRanges()) {
        int64_t skip = range.from > current ? range.from - current : 0;
        int64_t skipped = column_reader->SkipRecords(col_idx, skip);
        if (skipped != skip) {
            return Status::Invalid(fmt::format(
                "PageFilteredRowGroupReader: leaf {} expected to skip {} records but skipped {}",
                col_idx, skip, skipped));
        }
        int64_t to_read = range.Count();
        int64_t read = column_reader->ReadRecords(col_idx, to_read);
        if (read != to_read) {
            return Status::Invalid(fmt::format(
                "PageFilteredRowGroupReader: leaf {} expected to read {} records but read {}",
                col_idx, to_read, read));
        }
        current = range.to + 1;
    }
    return Status::OK();
}

Status PageFilteredRowGroupReader::WaitForPreBuffer(
    ::parquet::ParquetFileReader* parquet_reader, int32_t row_group_index,
    const std::vector<int32_t>& column_indices, const ::arrow::io::CacheOptions& cache_options,
    bool pre_buffered, const std::vector<::arrow::io::ReadRange>& page_ranges,
    std::shared_ptr<::arrow::MemoryPool> pool) {
    std::vector<int> rg_vec = {row_group_index};
    std::vector<int> col_vec(column_indices.begin(), column_indices.end());
    if (!pre_buffered) {
        ::arrow::io::IOContext io_ctx(pool.get());
        parquet_reader->PreBuffer(rg_vec, col_vec, io_ctx, cache_options);
    }
    if (!page_ranges.empty()) {
        auto status = parquet_reader->WhenBufferedRanges(page_ranges).status();
        if (!status.ok()) {
            ::arrow::io::IOContext io_ctx(pool.get());
            parquet_reader->PreBuffer(rg_vec, col_vec, io_ctx, cache_options);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(parquet_reader->WhenBuffered(rg_vec, col_vec).status());
        }
    } else {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(parquet_reader->WhenBuffered(rg_vec, col_vec).status());
    }
    return Status::OK();
}

Result<std::shared_ptr<arrow::ChunkedArray>> PageFilteredRowGroupReader::ReadFilteredField(
    ::parquet::arrow::FileReader* arrow_file_reader,
    const std::shared_ptr<::parquet::RowGroupPageIndexReader>& rg_page_index_reader,
    int32_t row_group_index, int32_t field_index, const std::vector<int32_t>& column_indices,
    const RowRanges& row_ranges, int64_t row_group_row_count) {
    // Factory: set data_page_filter on every leaf (per-leaf OffsetIndex).
    // data_page_filter enables I/O-level page skipping for all leaves.
    auto factory =
        [row_group_index, &rg_page_index_reader, &row_ranges, row_group_row_count](
            int col_idx,
            ::parquet::ParquetFileReader* reader) -> ::parquet::arrow::FileColumnIterator* {
        std::function<bool(const ::parquet::DataPageStats&)> data_page_filter;
        if (rg_page_index_reader) {
            auto offset_index = rg_page_index_reader->GetOffsetIndex(col_idx);
            if (offset_index) {
                data_page_filter = MakePageFilter(row_ranges, offset_index, row_group_row_count);
            }
        }
        return new PageFilteringColumnIterator(col_idx, reader, std::vector<int>{row_group_index},
                                               std::move(data_page_filter));
    };

    // Build reader tree with leaf column filtering
    std::unique_ptr<::parquet::arrow::ColumnReader> column_reader;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow_file_reader->GetColumn(field_index, column_indices, factory, &column_reader));

    if (!column_reader) {
        return Status::Invalid(
            fmt::format("PageFilteredRowGroupReader: field {} has no matching leaf columns "
                        "(row_group={})",
                        field_index, row_group_index));
    }

    // Since leaf columns may have misaligned pages, we compute compressed row ranges and drive each
    // leaf column independently
    int64_t effective_total = row_group_row_count;
    for (int col_idx : column_reader->LeafColumnIndices()) {
        RowRanges effective_ranges = row_ranges;
        if (rg_page_index_reader) {
            auto offset_index = rg_page_index_reader->GetOffsetIndex(col_idx);
            if (offset_index) {
                auto [compressed, total] =
                    ComputeCompressedRowRanges(row_ranges, offset_index, row_group_row_count);
                effective_ranges = std::move(compressed);
                effective_total = total;
            }
        }

        PAIMON_RETURN_NOT_OK(ExecuteSkipReadPattern(column_reader.get(), col_idx, effective_ranges,
                                                    effective_total));
    }

    // Build the Arrow array (TransferColumnData for leaves + assemble for nested)
    std::shared_ptr<arrow::ChunkedArray> chunked_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(column_reader->BuildArray(effective_total, &chunked_array));

    return chunked_array;
}

Result<std::unique_ptr<arrow::RecordBatchReader>> PageFilteredRowGroupReader::ReadFilteredRowGroup(
    ::parquet::arrow::FileReader* arrow_file_reader, const TargetRowGroup& target_row_group,
    const std::vector<int32_t>& column_indices, const ::arrow::io::CacheOptions& cache_options,
    bool pre_buffered, const std::vector<::arrow::io::ReadRange>& page_ranges,
    int64_t max_chunksize, std::shared_ptr<::arrow::MemoryPool> pool) {
    auto parquet_reader = arrow_file_reader->parquet_reader();
    const auto& row_ranges = target_row_group.GetRowRanges();
    int32_t row_group_index = target_row_group.GetRowGroupIndex();

    int64_t expected_rows = row_ranges.RowCount();

    if (!row_ranges.IsEmpty()) {
        PAIMON_RETURN_NOT_OK(WaitForPreBuffer(parquet_reader, row_group_index, column_indices,
                                              cache_options, pre_buffered, page_ranges, pool));
    }

    auto rg_metadata = parquet_reader->metadata()->RowGroup(row_group_index);
    int64_t row_group_row_count = rg_metadata->num_rows();

    // reuse RowGroupPageIndexReader for multiple columns in the same row group to avoid redundant
    // metadata reads
    std::shared_ptr<::parquet::RowGroupPageIndexReader> rg_page_index_reader;
    auto page_index_reader = parquet_reader->GetPageIndexReader();
    if (page_index_reader) {
        rg_page_index_reader = page_index_reader->RowGroup(row_group_index);
    }

    const auto& manifest = arrow_file_reader->manifest();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::vector<int> field_indices,
        manifest.GetFieldIndices(std::vector<int>(column_indices.begin(), column_indices.end())));

    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    columns.reserve(field_indices.size());

    for (int field_idx : field_indices) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::ChunkedArray> chunked_array,
            ReadFilteredField(arrow_file_reader, rg_page_index_reader, row_group_index, field_idx,
                              column_indices, row_ranges, row_group_row_count));

        if (chunked_array && chunked_array->length() != expected_rows) {
            return Status::Invalid(
                fmt::format("PageFilteredRowGroupReader: field {} produced {} rows but expected {} "
                            "(row_group={})",
                            field_idx, chunked_array->length(), expected_rows, row_group_index));
        }

        columns.push_back(std::move(chunked_array));
    }

    std::vector<std::shared_ptr<arrow::Field>> result_fields;
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& field_name = manifest.schema_fields[field_indices[i]].field->name();
        result_fields.push_back(arrow::field(field_name, columns[i]->type()));
    }
    auto result_schema = arrow::schema(result_fields);

    auto table = arrow::Table::Make(result_schema, std::move(columns), expected_rows);
    return std::make_unique<TableRecordBatchReader>(std::move(table), max_chunksize, pool);
}

std::vector<::arrow::io::ReadRange> PageFilteredRowGroupReader::ComputePageRanges(
    ::parquet::ParquetFileReader* parquet_reader, const TargetRowGroup& target_row_group,
    const std::vector<int32_t>& column_indices) {
    int32_t row_group_index = target_row_group.GetRowGroupIndex();
    const auto& row_ranges = target_row_group.GetRowRanges();

    std::vector<::arrow::io::ReadRange> ranges;
    auto file_metadata = parquet_reader->metadata();
    auto rg_metadata = file_metadata->RowGroup(row_group_index);
    int64_t row_group_row_count = rg_metadata->num_rows();

    auto page_index_reader = parquet_reader->GetPageIndexReader();
    std::shared_ptr<::parquet::RowGroupPageIndexReader> rg_page_index_reader;
    if (page_index_reader) {
        rg_page_index_reader = page_index_reader->RowGroup(row_group_index);
    }

    for (int32_t col_idx : column_indices) {
        auto col_chunk = rg_metadata->ColumnChunk(col_idx);
        int64_t data_page_offset = col_chunk->data_page_offset();
        int64_t data_page_compressed_size = col_chunk->total_compressed_size();
        // Dictionary page: always include if present
        if (col_chunk->has_dictionary_page()) {
            int64_t dict_offset = col_chunk->dictionary_page_offset();
            int64_t dict_size = data_page_offset - dict_offset;
            if (dict_size > 0) {
                // if dictionary exists, the data page size should be reduced by the dictionary
                data_page_compressed_size -= dict_size;
                ranges.push_back({dict_offset, dict_size});
            }
        }

        int64_t chunk_end = data_page_offset + data_page_compressed_size;

        // Try to get OffsetIndex for page-level ranges
        std::shared_ptr<::parquet::OffsetIndex> offset_index;
        if (rg_page_index_reader) {
            offset_index = rg_page_index_reader->GetOffsetIndex(col_idx);
        }

        if (!offset_index) {
            // No OffsetIndex: fall back to entire column chunk
            ranges.push_back({data_page_offset, data_page_compressed_size});
            continue;
        }

        const auto& page_locations = offset_index->page_locations();
        auto num_pages = static_cast<int32_t>(page_locations.size());

        for (int32_t page_idx = 0; page_idx < num_pages; ++page_idx) {
            auto [first_row, last_row] =
                GetPageRowRange(page_locations, page_idx, row_group_row_count);

            if (!row_ranges.IsOverlapping(first_row, last_row)) {
                continue;
            }

            int64_t page_offset = page_locations[page_idx].offset;
            int64_t page_size = (page_idx + 1 < num_pages)
                                    ? page_locations[page_idx + 1].offset - page_offset
                                    : chunk_end - page_offset;
            ranges.push_back({page_offset, page_size});
        }
    }

    return ranges;
}

}  // namespace paimon::parquet
