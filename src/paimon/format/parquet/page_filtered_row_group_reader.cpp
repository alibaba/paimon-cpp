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
#include "arrow/array/concatenate.h"
#include "arrow/builder.h"
#include "arrow/chunked_array.h"
#include "arrow/io/caching.h"
#include "arrow/io/interfaces.h"
#include "arrow/table.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/future.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/level_conversion.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"

namespace paimon::parquet {

namespace {

/// Wraps an arrow::Table + TableBatchReader as a RecordBatchReader so the caller can
/// stream zero-copy-sliced batches without deep-copying multi-chunk columns. The Table
/// is held to keep its ChunkedArrays alive for the inner TableBatchReader.
class TableRecordBatchReader : public arrow::RecordBatchReader {
 public:
    TableRecordBatchReader(std::shared_ptr<arrow::Table> table, int64_t chunksize)
        : table_(std::move(table)), inner_(*table_) {
        inner_.set_chunksize(chunksize);
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        return table_->schema();
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        return inner_.ReadNext(out);
    }

 private:
    std::shared_ptr<arrow::Table> table_;
    arrow::TableBatchReader inner_;
};

/// Check if a SchemaField or any of its descendants is a repeated type
/// (List, Map, FixedSizeList, LargeList). This mirrors
/// ColumnReaderImpl::IsOrHasRepeatedChild() to decide whether to use
/// DefRepLevelsToBitmap (true) or DefLevelsToBitmap (false) for Struct assembly.
bool HasRepeatedDescendant(const ::parquet::arrow::SchemaField& field) {
    if (field.is_leaf()) return false;
    for (const auto& child : field.children) {
        auto type_id = child.field->type()->id();
        if (type_id == ::arrow::Type::LIST || type_id == ::arrow::Type::MAP ||
            type_id == ::arrow::Type::FIXED_SIZE_LIST || type_id == ::arrow::Type::LARGE_LIST) {
            return true;
        }
        if (HasRepeatedDescendant(child)) return true;
    }
    return false;
}

/// Convert a ChunkedArray to a single ArrayData.
/// 0 chunks → null array; 1 chunk → that chunk's data; >1 → concatenate.
::arrow::Result<std::shared_ptr<::arrow::ArrayData>> ChunksToSingleArrayData(
    const ::arrow::ChunkedArray& chunked) {
    if (chunked.num_chunks() == 0) {
        ARROW_ASSIGN_OR_RAISE(auto array, ::arrow::MakeArrayOfNull(chunked.type(), 0));
        return array->data();
    }
    if (chunked.num_chunks() == 1) {
        return chunked.chunk(0)->data();
    }
    ARROW_ASSIGN_OR_RAISE(auto concatenated, ::arrow::Concatenate(chunked.chunks()));
    return concatenated->data();
}

}  // namespace

std::shared_ptr<arrow::Field> PageFilteredRowGroupReader::BuildProjectedField(
    const ::parquet::arrow::SchemaField& schema_field, const std::set<int32_t>& column_indices) {
    if (schema_field.is_leaf()) {
        if (column_indices.count(schema_field.column_index) > 0) {
            return schema_field.field;
        }
        return nullptr;
    }

    auto type = schema_field.field->type();
    auto type_id = type->id();

    if (type_id == ::arrow::Type::STRUCT) {
        std::vector<std::shared_ptr<arrow::Field>> child_fields;
        for (const auto& child : schema_field.children) {
            auto projected = BuildProjectedField(child, column_indices);
            if (projected) {
                child_fields.push_back(projected);
            }
        }
        if (child_fields.empty()) return nullptr;
        return arrow::field(schema_field.field->name(), arrow::struct_(child_fields),
                            schema_field.field->nullable());
    }

    if (type_id == ::arrow::Type::LIST || type_id == ::arrow::Type::LARGE_LIST ||
        type_id == ::arrow::Type::FIXED_SIZE_LIST || type_id == ::arrow::Type::MAP) {
        if (schema_field.children.empty()) return nullptr;
        auto projected_child = BuildProjectedField(schema_field.children[0], column_indices);
        if (!projected_child) return nullptr;
        auto child_type = projected_child->type();
        if (type_id == ::arrow::Type::LIST) {
            return arrow::field(schema_field.field->name(), arrow::list(child_type),
                                schema_field.field->nullable());
        }
        if (type_id == ::arrow::Type::LARGE_LIST) {
            return arrow::field(schema_field.field->name(), arrow::large_list(child_type),
                                schema_field.field->nullable());
        }
        if (type_id == ::arrow::Type::FIXED_SIZE_LIST) {
            auto& fsl_type = static_cast<const arrow::FixedSizeListType&>(*type);
            return arrow::field(schema_field.field->name(),
                                arrow::fixed_size_list(child_type, fsl_type.list_size()),
                                schema_field.field->nullable());
        }
        if (type_id == ::arrow::Type::MAP) {
            if (child_type->id() == ::arrow::Type::STRUCT && child_type->num_fields() == 2) {
                return arrow::field(
                    schema_field.field->name(),
                    arrow::map(child_type->field(0)->type(), child_type->field(1)->type()),
                    schema_field.field->nullable());
            }
            return arrow::field(schema_field.field->name(), arrow::list(child_type),
                                schema_field.field->nullable());
        }
    }

    return nullptr;
}

Result<std::shared_ptr<arrow::Schema>> PageFilteredRowGroupReader::BuildProjectedSchema(
    ::parquet::arrow::FileReader* arrow_file_reader, const std::vector<int32_t>& column_indices) {
    const auto& manifest = arrow_file_reader->manifest();
    std::vector<int> col_indices_vec(column_indices.begin(), column_indices.end());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::vector<int> field_indices,
                                      manifest.GetFieldIndices(col_indices_vec));

    std::set<int32_t> col_set(column_indices.begin(), column_indices.end());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int field_idx : field_indices) {
        auto projected = BuildProjectedField(manifest.schema_fields[field_idx], col_set);
        if (projected) {
            fields.push_back(projected);
        }
    }
    return arrow::schema(std::move(fields));
}

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
    const std::shared_ptr<::parquet::internal::RecordReader>& record_reader,
    const RowRanges& ranges, int64_t total_row_count, int32_t row_group_index,
    int32_t column_index) {
    int64_t current_row = 0;
    for (const auto& range : ranges.GetRanges()) {
        if (range.from > current_row) {
            int64_t to_skip = range.from - current_row;
            int64_t skipped = record_reader->SkipRecords(to_skip);
            if (skipped != to_skip) {
                return Status::Invalid(fmt::format(
                    "PageFilteredRowGroupReader: expected to skip {} records but skipped {} "
                    "(row_group={}, column={})",
                    to_skip, skipped, row_group_index, column_index));
            }
            current_row = range.from;
        }
        int64_t to_read = range.Count();
        int64_t read = record_reader->ReadRecords(to_read);
        if (read != to_read) {
            return Status::Invalid(
                fmt::format("PageFilteredRowGroupReader: expected to read {} records but read {} "
                            "(row_group={}, column={}, range=[{},{}])",
                            to_read, read, row_group_index, column_index, range.from, range.to));
        }
        current_row += to_read;
    }
    if (current_row < total_row_count) {
        record_reader->SkipRecords(total_row_count - current_row);
    }
    return Status::OK();
}

Result<std::pair<std::shared_ptr<arrow::ChunkedArray>,
                 std::shared_ptr<::parquet::internal::RecordReader>>>
PageFilteredRowGroupReader::ReadLeafColumn(
    const std::shared_ptr<::parquet::RowGroupReader>& row_group_reader,
    ::parquet::ParquetFileReader* parquet_reader,
    const std::shared_ptr<::parquet::RowGroupPageIndexReader>& rg_page_index_reader,
    int32_t row_group_index, int32_t column_index, const RowRanges& row_ranges,
    const std::shared_ptr<arrow::Field>& field, int64_t row_group_row_count,
    bool enable_page_filter, std::shared_ptr<::arrow::MemoryPool> pool) {
    auto file_metadata = parquet_reader->metadata();
    const auto* col_descriptor = file_metadata->schema()->Column(column_index);

    RowRanges effective_ranges = row_ranges;
    int64_t effective_row_count = row_group_row_count;

    std::shared_ptr<::parquet::OffsetIndex> offset_index;
    if (rg_page_index_reader) {
        offset_index = rg_page_index_reader->GetOffsetIndex(column_index);
    }

    auto page_reader = row_group_reader->GetColumnPageReader(column_index);

    if (enable_page_filter && offset_index) {
        page_reader->set_data_page_filter(
            MakePageFilter(row_ranges, offset_index, row_group_row_count));
        auto [compressed_ranges, compressed_total] =
            ComputeCompressedRowRanges(row_ranges, offset_index, row_group_row_count);
        effective_ranges = std::move(compressed_ranges);
        effective_row_count = compressed_total;
    }

    ::parquet::internal::LevelInfo leaf_info =
        ::parquet::internal::LevelInfo::ComputeLevelInfo(col_descriptor);
    auto record_reader =
        ::parquet::internal::RecordReader::Make(col_descriptor, leaf_info, pool.get());
    record_reader->SetPageReader(std::move(page_reader));

    PAIMON_RETURN_NOT_OK(ExecuteSkipReadPattern(
        record_reader, effective_ranges, effective_row_count, row_group_index, column_index));

    std::shared_ptr<arrow::ChunkedArray> chunked_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(::parquet::arrow::TransferColumnData(
        record_reader.get(), field, col_descriptor, pool.get(), &chunked_array));

    return std::make_pair(std::move(chunked_array), std::move(record_reader));
}

Result<std::shared_ptr<arrow::ChunkedArray>> PageFilteredRowGroupReader::ReadFilteredColumn(
    const std::shared_ptr<::parquet::RowGroupReader>& row_group_reader,
    ::parquet::ParquetFileReader* parquet_reader,
    const std::shared_ptr<::parquet::RowGroupPageIndexReader>& rg_page_index_reader,
    int32_t row_group_index, int32_t column_index, const RowRanges& row_ranges,
    const std::shared_ptr<arrow::Field>& field, int64_t row_group_row_count,
    std::shared_ptr<::arrow::MemoryPool> pool) {
    PAIMON_ASSIGN_OR_RAISE(auto result,
                           ReadLeafColumn(row_group_reader, parquet_reader, rg_page_index_reader,
                                          row_group_index, column_index, row_ranges, field,
                                          row_group_row_count, /*enable_page_filter=*/true, pool));
    return result.first;
}

Result<std::pair<std::shared_ptr<arrow::ChunkedArray>,
                 std::shared_ptr<::parquet::internal::RecordReader>>>
PageFilteredRowGroupReader::ReadAndAssembleField(
    const ::parquet::arrow::SchemaField& schema_field, ::parquet::ParquetFileReader* parquet_reader,
    const std::shared_ptr<::parquet::RowGroupReader>& row_group_reader,
    const std::shared_ptr<::parquet::RowGroupPageIndexReader>& rg_page_index_reader,
    int32_t row_group_index, const std::vector<int32_t>& column_indices,
    const RowRanges& row_ranges, const std::shared_ptr<arrow::Field>& field,
    int64_t row_group_row_count, int64_t expected_rows, std::shared_ptr<::arrow::MemoryPool> pool,
    bool is_top_level) {
    namespace bit_util = ::arrow::bit_util;

    // For leaf/flat fields, use ReadLeafColumn.
    // Top-level leaf columns get data_page_filter for I/O-level page skipping.
    // Nested leaf columns (is_top_level=false) skip data_page_filter to preserve
    // def/rep level synchronization across sibling leaf columns.
    if (schema_field.is_leaf()) {
        PAIMON_ASSIGN_OR_RAISE(
            auto result,
            ReadLeafColumn(row_group_reader, parquet_reader, rg_page_index_reader, row_group_index,
                           schema_field.column_index, row_ranges, field, row_group_row_count,
                           /*enable_page_filter=*/is_top_level, pool));
        return result;
    }

    auto type_id = field->type()->id();

    if (type_id == ::arrow::Type::STRUCT) {
        // === Struct Assembly (mimicking StructReader::BuildArray) ===
        std::vector<std::shared_ptr<::arrow::ArrayData>> child_data;
        std::shared_ptr<::parquet::internal::RecordReader> def_level_reader;
        std::set<int32_t> col_set(column_indices.begin(), column_indices.end());

        for (const auto& child : schema_field.children) {
            // Sub-column projection: skip children whose leaf columns are not requested.
            auto projected_child_field = BuildProjectedField(child, col_set);
            if (!projected_child_field) {
                continue;
            }

            PAIMON_ASSIGN_OR_RAISE(
                auto child_result,
                ReadAndAssembleField(child, parquet_reader, row_group_reader, rg_page_index_reader,
                                     row_group_index, column_indices, row_ranges,
                                     projected_child_field, row_group_row_count, expected_rows,
                                     pool, /*is_top_level=*/false));

            if (!def_level_reader) {
                def_level_reader = child_result.second;
            }

            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto array_data,
                                              ChunksToSingleArrayData(*child_result.first));
            child_data.push_back(std::move(array_data));
        }

        // Build validity bitmap from def levels.
        bool has_repeated_child = HasRepeatedDescendant(schema_field);
        std::shared_ptr<::arrow::ResizableBuffer> null_bitmap;
        ::parquet::internal::ValidityBitmapInputOutput validity_io;
        validity_io.values_read_upper_bound = expected_rows;
        validity_io.values_read = expected_rows;

        if (has_repeated_child) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                null_bitmap, ::arrow::AllocateResizableBuffer(bit_util::BytesForBits(expected_rows),
                                                              pool.get()));
            validity_io.valid_bits = null_bitmap->mutable_data();

            const int16_t* def_levels = def_level_reader->def_levels();
            const int16_t* rep_levels = def_level_reader->rep_levels();
            int64_t num_levels = def_level_reader->levels_position();
            ::parquet::internal::DefRepLevelsToBitmap(def_levels, rep_levels, num_levels,
                                                      schema_field.level_info, &validity_io);
        } else if (field->nullable()) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                null_bitmap, ::arrow::AllocateResizableBuffer(bit_util::BytesForBits(expected_rows),
                                                              pool.get()));
            validity_io.valid_bits = null_bitmap->mutable_data();

            const int16_t* def_levels = def_level_reader->def_levels();
            int64_t num_levels = def_level_reader->levels_position();
            ::parquet::internal::DefLevelsToBitmap(def_levels, num_levels, schema_field.level_info,
                                                   &validity_io);
        }

        if (null_bitmap) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                null_bitmap->Resize(bit_util::BytesForBits(validity_io.values_read)));
            null_bitmap->ZeroPadding();
        }

        if (!field->nullable() && !has_repeated_child && !child_data.empty()) {
            validity_io.values_read = child_data.front()->length;
        }

        std::vector<std::shared_ptr<::arrow::Buffer>> buffers{
            validity_io.null_count > 0 ? null_bitmap : nullptr};
        auto data = std::make_shared<::arrow::ArrayData>(field->type(), validity_io.values_read,
                                                         std::move(buffers), std::move(child_data));
        auto result = ::arrow::MakeArray(data);
        return std::make_pair(std::make_shared<arrow::ChunkedArray>(result), def_level_reader);
    }

    if (type_id == ::arrow::Type::LIST || type_id == ::arrow::Type::MAP ||
        type_id == ::arrow::Type::LARGE_LIST || type_id == ::arrow::Type::FIXED_SIZE_LIST) {
        // === List/Map Assembly (mimicking ListReader::BuildArray) ===
        // Map is stored as list<struct<key, value>> in Parquet, so use List assembly.
        const auto& child = schema_field.children[0];
        std::set<int32_t> col_set(column_indices.begin(), column_indices.end());
        auto projected_child_field = BuildProjectedField(child, col_set);
        if (!projected_child_field) {
            return Status::Invalid(fmt::format(
                "PageFilteredRowGroupReader: no leaf columns requested for list/map field '{}'",
                field->name()));
        }
        PAIMON_ASSIGN_OR_RAISE(
            auto child_result,
            ReadAndAssembleField(child, parquet_reader, row_group_reader, rg_page_index_reader,
                                 row_group_index, column_indices, row_ranges, projected_child_field,
                                 row_group_row_count, expected_rows, pool,
                                 /*is_top_level=*/false));

        auto& item_record_reader = child_result.second;
        const int16_t* def_levels = item_record_reader->def_levels();
        const int16_t* rep_levels = item_record_reader->rep_levels();
        int64_t num_levels = item_record_reader->levels_position();

        bool is_large_list = (type_id == ::arrow::Type::LARGE_LIST);
        bool is_fixed_size = (type_id == ::arrow::Type::FIXED_SIZE_LIST);

        std::shared_ptr<::arrow::ResizableBuffer> validity_buffer;
        ::parquet::internal::ValidityBitmapInputOutput validity_io;
        validity_io.values_read_upper_bound = expected_rows;

        if (field->nullable()) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                validity_buffer, ::arrow::AllocateResizableBuffer(
                                     bit_util::BytesForBits(expected_rows), pool.get()));
            validity_io.valid_bits = validity_buffer->mutable_data();
        }

        std::shared_ptr<::arrow::ResizableBuffer> offsets_buffer;

        if (is_large_list) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                offsets_buffer,
                ::arrow::AllocateResizableBuffer(
                    sizeof(int64_t) * std::max(int64_t{1}, expected_rows + 1), pool.get()));
            auto* offset_data = reinterpret_cast<int64_t*>(offsets_buffer->mutable_data());
            offset_data[0] = 0;
            ::parquet::internal::DefRepLevelsToList(def_levels, rep_levels, num_levels,
                                                    schema_field.level_info, &validity_io,
                                                    offset_data);
        } else {
            // LIST, MAP, and FIXED_SIZE_LIST all use int32 offsets initially.
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                offsets_buffer,
                ::arrow::AllocateResizableBuffer(
                    sizeof(int32_t) * std::max(int64_t{1}, expected_rows + 1), pool.get()));
            auto* offset_data = reinterpret_cast<int32_t*>(offsets_buffer->mutable_data());
            offset_data[0] = 0;
            ::parquet::internal::DefRepLevelsToList(def_levels, rep_levels, num_levels,
                                                    schema_field.level_info, &validity_io,
                                                    offset_data);
        }

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto item_data,
                                          ChunksToSingleArrayData(*child_result.first));

        // Resize buffers to actual size.
        if (offsets_buffer) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                offsets_buffer->Resize((validity_io.values_read + 1) *
                                       (is_large_list ? sizeof(int64_t) : sizeof(int32_t))));
        }
        if (validity_buffer) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                validity_buffer->Resize(bit_util::BytesForBits(validity_io.values_read)));
            validity_buffer->ZeroPadding();
        }

        std::vector<std::shared_ptr<::arrow::Buffer>> buffers;
        if (is_fixed_size) {
            // Validate each list has the expected fixed size, then drop offsets.
            const auto& fs_type = static_cast<const ::arrow::FixedSizeListType&>(*field->type());
            int32_t list_size = fs_type.list_size();
            const int32_t* offsets = reinterpret_cast<const int32_t*>(offsets_buffer->data());
            for (int64_t x = 1; x <= validity_io.values_read; ++x) {
                int32_t size = offsets[x] - offsets[x - 1];
                if (size != list_size) {
                    return Status::Invalid(
                        fmt::format("Expected all lists to be of size={} but index {} had size={}",
                                    list_size, x, size));
                }
            }
            // FixedSizeList only has a validity buffer (no offsets).
            buffers.push_back(validity_io.null_count > 0 ? validity_buffer : nullptr);
        } else {
            buffers.push_back(validity_io.null_count > 0 ? validity_buffer : nullptr);
            buffers.push_back(offsets_buffer);
        }

        auto data = std::make_shared<::arrow::ArrayData>(
            field->type(), validity_io.values_read, std::move(buffers),
            std::vector<std::shared_ptr<::arrow::ArrayData>>{item_data}, validity_io.null_count);

        if (type_id == ::arrow::Type::MAP) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(::arrow::MapArray::ValidateChildData(data->child_data));
        }

        auto result = ::arrow::MakeArray(data);
        return std::make_pair(std::make_shared<arrow::ChunkedArray>(result), item_record_reader);
    }

    return Status::Invalid(fmt::format("PageFilteredRowGroupReader: unsupported field type: {}",
                                       field->type()->ToString()));
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

Result<std::unique_ptr<arrow::RecordBatchReader>> PageFilteredRowGroupReader::ReadFilteredRowGroup(
    ::parquet::arrow::FileReader* arrow_file_reader, const TargetRowGroup& target_row_group,
    const std::vector<int32_t>& column_indices, const std::shared_ptr<arrow::Schema>& arrow_schema,
    const ::arrow::io::CacheOptions& cache_options, bool pre_buffered,
    const std::vector<::arrow::io::ReadRange>& page_ranges, int64_t max_chunksize,
    std::shared_ptr<::arrow::MemoryPool> pool) {
    const auto& row_ranges = target_row_group.row_ranges;
    int32_t row_group_index = target_row_group.row_group_index;

    if (row_ranges.IsEmpty()) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Table> empty_table,
                                          arrow::Table::MakeEmpty(arrow_schema, pool.get()));
        return std::make_unique<TableRecordBatchReader>(std::move(empty_table), max_chunksize);
    }

    int64_t expected_rows = row_ranges.RowCount();

    ::parquet::ParquetFileReader* parquet_reader = arrow_file_reader->parquet_reader();

    PAIMON_RETURN_NOT_OK(WaitForPreBuffer(parquet_reader, row_group_index, column_indices,
                                          cache_options, pre_buffered, page_ranges, pool));

    auto row_group_reader = parquet_reader->RowGroup(row_group_index);
    auto rg_metadata = parquet_reader->metadata()->RowGroup(row_group_index);
    int64_t row_group_row_count = rg_metadata->num_rows();

    // reuse RowGroupPageIndexReader for multiple columns in the same row group to avoid redundant
    // metadata reads
    std::shared_ptr<::parquet::RowGroupPageIndexReader> rg_page_index_reader;
    auto page_index_reader = parquet_reader->GetPageIndexReader();
    if (page_index_reader) {
        rg_page_index_reader = page_index_reader->RowGroup(row_group_index);
    }

    // Use SchemaManifest to group leaf columns by top-level field.
    // This allows us to handle nested types (Struct, List, Map) correctly by
    // building a reader tree for each top-level field instead of treating each
    // leaf column independently.
    const auto& manifest = arrow_file_reader->manifest();
    std::vector<int> col_indices_vec(column_indices.begin(), column_indices.end());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::vector<int> field_indices,
                                      manifest.GetFieldIndices(col_indices_vec));

    // Read each top-level field with page filtering
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    columns.reserve(field_indices.size());

    for (size_t i = 0; i < field_indices.size(); ++i) {
        const auto& schema_field = manifest.schema_fields[field_indices[i]];
        PAIMON_ASSIGN_OR_RAISE(
            auto field_result,
            ReadAndAssembleField(schema_field, parquet_reader, row_group_reader,
                                 rg_page_index_reader, row_group_index, column_indices, row_ranges,
                                 arrow_schema->field(static_cast<int>(i)), row_group_row_count,
                                 expected_rows, pool));

        auto& chunked_array = field_result.first;
        if (chunked_array->length() != expected_rows) {
            return Status::Invalid(fmt::format(
                "PageFilteredRowGroupReader: field {} produced {} rows but expected {} "
                "(row_group={})",
                field_indices[i], chunked_array->length(), expected_rows, row_group_index));
        }

        columns.push_back(std::move(chunked_array));
    }

    auto table = arrow::Table::Make(arrow_schema, std::move(columns), expected_rows);
    return std::make_unique<TableRecordBatchReader>(std::move(table), max_chunksize);
}

std::vector<::arrow::io::ReadRange> PageFilteredRowGroupReader::ComputePageRanges(
    ::parquet::ParquetFileReader* parquet_reader, const TargetRowGroup& target_row_group,
    const std::vector<int32_t>& column_indices) {
    int32_t row_group_index = target_row_group.row_group_index;
    const auto& row_ranges = target_row_group.row_ranges;

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
