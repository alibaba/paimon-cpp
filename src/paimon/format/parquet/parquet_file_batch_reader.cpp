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

#include "paimon/format/parquet/parquet_file_batch_reader.h"

#include <algorithm>
#include <cstddef>
#include <unordered_map>

#include "arrow/acero/options.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/api.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/io/caching.h"
#include "arrow/io/interfaces.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/range.h"
#include "arrow/util/thread_pool.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_schema_util.h"
#include "paimon/format/parquet/parquet_timestamp_converter.h"
#include "paimon/format/parquet/predicate_converter.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "parquet/arrow/reader.h"
#include "parquet/properties.h"

namespace arrow {
class MemoryPool;
}  // namespace arrow
namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::parquet {

ParquetFileBatchReader::ParquetFileBatchReader(
    std::shared_ptr<arrow::io::RandomAccessFile>&& input_stream,
    std::unique_ptr<FileReaderWrapper>&& reader, const std::map<std::string, std::string>& options,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
    std::shared_ptr<std::atomic<uint64_t>> storage_read_bytes)
    : options_(options),
      arrow_pool_(arrow_pool),
      input_stream_(std::move(input_stream)),
      reader_(std::move(reader)),
      metrics_(std::make_shared<MetricsImpl>()),
      storage_read_bytes_(std::move(storage_read_bytes)),
      logger_(Logger::GetLogger("ParquetFileBatchReader")) {}

Result<std::unique_ptr<ParquetFileBatchReader>> ParquetFileBatchReader::Create(
    std::shared_ptr<arrow::io::RandomAccessFile>&& input_stream,
    const std::map<std::string, std::string>& options, int32_t batch_size,
    std::shared_ptr<::parquet::FileMetaData> file_metadata,
    std::shared_ptr<std::atomic<uint64_t>> storage_read_bytes,
    const std::shared_ptr<arrow::MemoryPool>& pool) {
    try {
        assert(input_stream);
        PAIMON_ASSIGN_OR_RAISE(::parquet::ReaderProperties reader_properties,
                               CreateReaderProperties(pool, options));

        PAIMON_ASSIGN_OR_RAISE(::parquet::ArrowReaderProperties arrow_reader_properties,
                               CreateArrowReaderProperties(pool, options, batch_size));

        ::parquet::arrow::FileReaderBuilder file_reader_builder;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            file_reader_builder.Open(input_stream, reader_properties, std::move(file_metadata)));

        std::unique_ptr<::parquet::arrow::FileReader> file_reader;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_builder.memory_pool(pool.get())
                                            ->properties(arrow_reader_properties)
                                            ->Build(&file_reader));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileReaderWrapper> reader,
                               FileReaderWrapper::Create(std::move(file_reader),
                                                         static_cast<int64_t>(batch_size), pool));
        auto parquet_file_batch_reader = std::unique_ptr<ParquetFileBatchReader>(
            new ParquetFileBatchReader(std::move(input_stream), std::move(reader), options, pool,
                                       std::move(storage_read_bytes)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> file_schema,
                               parquet_file_batch_reader->GetFileSchema());
        PAIMON_RETURN_NOT_OK(parquet_file_batch_reader->SetReadSchema(
            file_schema.get(), /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt));
        return parquet_file_batch_reader;
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("ParquetFileBatchReader::Create")
}

Result<std::unique_ptr<::ArrowSchema>> ParquetFileBatchReader::GetFileSchema() const {
    try {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> file_schema, reader_->GetSchema());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> new_schema,
                               ParquetFieldIdConverter::GetPaimonIdsFromParquetIds(file_schema));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::DataType> new_type,
            ParquetTimestampConverter::AdjustTimezone(arrow::struct_(new_schema->fields())));

        auto c_schema = std::make_unique<::ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*new_type, c_schema.get()));
        return c_schema;
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("ParquetFileBatchReader::GetFileSchema")
}

Status ParquetFileBatchReader::SetReadSchema(
    ::ArrowSchema* schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    try {
        if (!schema) {
            return Status::Invalid("SetReadSchema failed: read schema cannot be nullptr");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> read_schema,
                                          arrow::ImportSchema(schema));

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> file_schema, reader_->GetSchema());

        // Recursively match read_schema against file_schema by field names.
        // STRUCT supports sub-field projection; LIST/MAP require exact type match.
        PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> column_indices,
                               ComputeNestedColumnIndices(read_schema, file_schema));

        // Build column name to index map for page-level filtering.
        // We still need the full per-top-level-field leaf indices for predicate pushdown.
        std::unordered_map<std::string, std::vector<int32_t>> field_index_map;
        int32_t flat_idx = 0;
        for (const auto& field : file_schema->fields()) {
            std::vector<int32_t> leaf_indices;
            FlattenSchema(field->type(), &flat_idx, &leaf_indices);
            field_index_map[field->name()] = leaf_indices;
        }

        TargetRowGroups target_row_groups =
            TargetRowGroup::MakeForAllRowGroups(reader_->GetAllRowGroupRanges());
        PAIMON_ASSIGN_OR_RAISE(
            bool enable_page_index_filter,
            OptionsUtils::GetValueFromMap<bool>(options_, PARQUET_READ_ENABLE_PAGE_INDEX_FILTER,
                                                DEFAULT_PARQUET_READ_ENABLE_PAGE_INDEX_FILTER));

        if (predicate) {
            PAIMON_ASSIGN_OR_RAISE(
                target_row_groups,
                FilterRowGroupsByPredicate(predicate, file_schema, target_row_groups));
        }
        if (selection_bitmap) {
            PAIMON_ASSIGN_OR_RAISE(
                target_row_groups,
                FilterRowGroupsByBitmap(selection_bitmap.value(), target_row_groups));
            // workaround: page index filter does not support nested fields for now, skip page index
            // bitmap pushdown if there is any nested field in the schema
            if (enable_page_index_filter) {
                // To decide which strategy to use, "trim" or "coalesce". "Coalesce" By default.
                PAIMON_ASSIGN_OR_RAISE(
                    std::string strategy,
                    OptionsUtils::GetValueFromMap<std::string>(
                        options_, PARQUET_READ_BITMAP_ROW_RANGE_REFINING_STRATEGY,
                        DEFAULT_PARQUET_READ_BITMAP_STRATEGY));
                if (strategy == "trim") {
                    PAIMON_ASSIGN_OR_RAISE(
                        target_row_groups,
                        RefineRowRangesByTrimming(selection_bitmap.value(), target_row_groups,
                                                  column_indices));
                } else if (strategy == "coalesce") {
                    PAIMON_ASSIGN_OR_RAISE(
                        target_row_groups,
                        RefineRowRangesByCoalescing(selection_bitmap.value(), target_row_groups));
                } else {
                    return Status::Invalid(
                        fmt::format("Invalid row range refining strategy :{}, valid strategies "
                                    "are: trim, coalesce",
                                    strategy));
                }
            }
        }
        // Apply page-level filtering after bitmap pruning so we don't read page index
        // pages for row groups that the bitmap already excluded.
        // If no predicate is provided, skip page-level filtering
        if (predicate && !target_row_groups.empty()) {
            if (enable_page_index_filter) {
                // Build column name to index map for page-level filtering.
                // For leaf columns, indices[0] is the correct leaf column index in Parquet.
                // For nested types (struct/list/map), FlattenSchema produces multiple leaf indices,
                // but predicate pushdown only targets leaf columns with simple types, so indices[0]
                // is always the correct single leaf index for predicate evaluation.
                std::map<std::string, int32_t> column_name_to_index;
                for (const auto& [name, indices] : field_index_map) {
                    if (!indices.empty()) {
                        column_name_to_index[name] = indices[0];
                    }
                }
                PAIMON_ASSIGN_OR_RAISE(
                    target_row_groups,
                    FilterRowGroupsByPageIndex(predicate, column_name_to_index, target_row_groups));
            }
        }

        read_data_type_ = arrow::struct_(read_schema->fields());

        metrics_->SetCounter(ParquetMetrics::READ_ROW_GROUPS_TOTAL,
                             reader_->GetNumberOfRowGroups());
        metrics_->SetCounter(ParquetMetrics::READ_ROW_GROUPS_AFTER_FILTER,
                             target_row_groups.size());

        PAIMON_RETURN_NOT_OK(UpdateAllTargetRowRanges(target_row_groups));
        PAIMON_RETURN_NOT_OK(reader_->PrepareForReadingLazy(target_row_groups, column_indices));
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("ParquetFileBatchReader::SetReadSchema")
    return Status::OK();
}

Result<TargetRowGroups> ParquetFileBatchReader::FilterRowGroupsByPredicate(
    const std::shared_ptr<Predicate>& predicate, const std::shared_ptr<arrow::Schema> file_schema,
    const TargetRowGroups& src_row_groups) const {
    if (!predicate) {
        return Status::Invalid("cannot pushdown an empty predicate");
    }
    // convert paimon predicate to arrow expression
    PAIMON_ASSIGN_OR_RAISE(
        uint32_t predicate_node_count_limit,
        OptionsUtils::GetValueFromMap<uint32_t>(options_, PARQUET_READ_PREDICATE_NODE_COUNT_LIMIT,
                                                DEFAULT_PARQUET_READ_PREDICATE_NODE_COUNT_LIMIT));
    PAIMON_ASSIGN_OR_RAISE(arrow::compute::Expression expr,
                           PredicateConverter::Convert(predicate, predicate_node_count_limit));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(arrow::Expression bind_expr, expr.Bind(*file_schema));

    // prepare file source
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(int64_t file_length, input_stream_->GetSize());
    auto file_source = arrow::dataset::FileSource(input_stream_, /*size=*/file_length);

    // filter row group by arrow expression and row group meta
    auto parquet_file_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::dataset::ParquetFileFragment> file_fragment,
        parquet_file_format->MakeFragment(
            file_source, /*partition_expression=*/PredicateConverter::AlwaysTrue(),
            /*physical_schema=*/nullptr,
            /*row_groups=*/TargetRowGroup::GetRowGroupIndices(src_row_groups)));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        file_fragment->EnsureCompleteMetadata(reader_->GetFileReader()));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(arrow::dataset::FragmentVector target_fragments,
                                      file_fragment->SplitByRowGroup(bind_expr));
    TargetRowGroups target_row_groups;
    target_row_groups.reserve(src_row_groups.size());
    for (const auto& fragment : target_fragments) {
        auto parquet_fragment = dynamic_cast<arrow::dataset::ParquetFileFragment*>(fragment.get());
        if (!parquet_fragment) {
            return Status::Invalid("cannot cast to ParquetFileFragment in ParquetFileBatchReader");
        }
        for (auto rg_index : parquet_fragment->row_groups()) {
            for (const auto& row_group : src_row_groups) {
                if (row_group.GetRowGroupIndex() == rg_index) {
                    target_row_groups.emplace_back(row_group);
                    break;
                }
            }
        }
    }
    return target_row_groups;
}

Result<TargetRowGroups> ParquetFileBatchReader::FilterRowGroupsByBitmap(
    const RoaringBitmap32& bitmap, const TargetRowGroups& src_row_groups) const {
    if (bitmap.IsEmpty()) {
        return Status::Invalid("cannot push down an empty bitmap to ParquetFileBatchReader");
    }

    const auto& all_row_group_ranges = reader_->GetAllRowGroupRanges();

    TargetRowGroups target_row_groups;
    for (const auto& row_group : src_row_groups) {
        int32_t row_group_idx = row_group.GetRowGroupIndex();
        if (static_cast<size_t>(row_group_idx) >= all_row_group_ranges.size()) {
            return Status::Invalid(
                fmt::format("src row group {} not in row group meta", row_group_idx));
        }
        // half open interval [start_row_idx, end_row_idx)
        const auto& [start_row_idx, end_row_idx] = all_row_group_ranges[row_group_idx];
        if (!bitmap.ContainsAny(start_row_idx, end_row_idx)) {
            continue;
        }
        target_row_groups.emplace_back(row_group);
    }
    return target_row_groups;
}

RowRanges ParquetFileBatchReader::CoalesceNearbyRanges(const RowRanges& input,
                                                       uint64_t hole_size_limit) {
    if (input.IsEmpty()) {
        return RowRanges();
    }

    const auto& ranges = input.GetRanges();
    RowRanges result;
    int64_t merge_start = ranges.front().from;
    int64_t merge_end = ranges.front().to;

    for (size_t i = 1; i < ranges.size(); ++i) {
        // Gap between [merge_start, merge_end] and [ranges[i].from, ranges[i].to]
        int64_t gap = ranges[i].from - merge_end - 1;
        if (static_cast<uint64_t>(gap) > hole_size_limit) {
            result.Add(RowRanges::Range(merge_start, merge_end));
            merge_start = ranges[i].from;
        }
        merge_end = ranges[i].to;
    }
    result.Add(RowRanges::Range(merge_start, merge_end));
    return result;
}

RowRanges ParquetFileBatchReader::BitmapToContiguousRanges(const RoaringBitmap32& bitmap,
                                                           uint64_t start_row, uint64_t end_row) {
    RowRanges ranges;
    if (bitmap.IsEmpty() || start_row >= end_row) {
        return ranges;
    }

    auto it = bitmap.EqualOrLarger(static_cast<int32_t>(start_row));
    const auto end = bitmap.End();
    if (it == end || static_cast<uint64_t>(*it) >= end_row) {
        return ranges;
    }

    auto run_start = static_cast<int64_t>(*it);
    auto prev = run_start;

    for (++it; it != end; ++it) {
        auto current = static_cast<int64_t>(*it);
        if (current >= static_cast<int64_t>(end_row)) {
            break;
        }
        if (current != prev + 1) {
            ranges.Add(RowRanges::Range(run_start - start_row, prev - start_row));
            run_start = current;
        }
        prev = current;
    }
    ranges.Add(RowRanges::Range(run_start - start_row, prev - start_row));
    return ranges;
}

Result<TargetRowGroups> ParquetFileBatchReader::RefineRowRangesByCoalescing(
    const RoaringBitmap32& bitmap, const TargetRowGroups& src_row_groups) const {
    PAIMON_ASSIGN_OR_RAISE(const uint64_t hole_size_limit,
                           OptionsUtils::GetValueFromMap<uint64_t>(
                               options_, PARQUET_READ_ROW_RANGES_COALESCE_HOLE_SIZE_LIMIT,
                               DEFAULT_PARQUET_READ_ROW_RANGES_COALESCE_HOLE_SIZE_LIMIT));

    const auto& all_row_group_ranges = reader_->GetAllRowGroupRanges();
    TargetRowGroups target_row_groups;
    target_row_groups.reserve(src_row_groups.size());

    for (const auto& row_group : src_row_groups) {
        int32_t rg_index = row_group.GetRowGroupIndex();
        uint64_t rg_start_row = all_row_group_ranges[rg_index].first;
        uint64_t rg_end_row = all_row_group_ranges[rg_index].second;

        // Step 1: bitmap -> contiguous ranges (relative to row group start).
        // Step 2: coalesce ranges with small gaps to reduce range count.
        RowRanges contiguous = BitmapToContiguousRanges(bitmap, rg_start_row, rg_end_row);
        RowRanges coalesced = CoalesceNearbyRanges(contiguous, hole_size_limit);

        auto rg_row_count = static_cast<int64_t>(rg_end_row - rg_start_row);
        if (coalesced.IsEmpty()) {
            continue;
        }
        if (coalesced.RowCount() == rg_row_count) {
            target_row_groups.emplace_back(row_group);
        } else {
            target_row_groups.emplace_back(rg_index, true, std::move(coalesced));
        }
    }
    return target_row_groups;
}

Result<TargetRowGroups> ParquetFileBatchReader::RefineRowRangesByTrimming(
    const RoaringBitmap32& bitmap, const TargetRowGroups& src_row_groups,
    const std::vector<int32_t>& column_indices) const {
    auto page_index_reader = reader_->GetPageIndexReader();
    if (!page_index_reader) {
        return src_row_groups;
    }

    TargetRowGroups target_row_groups;
    target_row_groups.reserve(src_row_groups.size());
    for (const auto& row_group : src_row_groups) {
        auto filtered =
            TrimRowGroupPageRanges(bitmap, row_group, column_indices, page_index_reader);
        if (!filtered.GetRowRanges().IsEmpty()) {
            target_row_groups.emplace_back(std::move(filtered));
        }
    }
    return target_row_groups;
}

TargetRowGroup ParquetFileBatchReader::TrimRowGroupPageRanges(
    const RoaringBitmap32& bitmap, const TargetRowGroup& row_group,
    const std::vector<int32_t>& column_indices,
    const std::shared_ptr<::parquet::PageIndexReader>& page_index_reader) const {
    int32_t row_group_idx = row_group.GetRowGroupIndex();
    auto rg_page_index_reader = page_index_reader->RowGroup(row_group_idx);
    if (!rg_page_index_reader) {
        return row_group;
    }

    const auto& all_row_group_ranges = reader_->GetAllRowGroupRanges();
    uint64_t rg_start_row = all_row_group_ranges[row_group_idx].first;
    uint64_t rg_row_count = all_row_group_ranges[row_group_idx].second - rg_start_row;

    RowRanges row_ranges = row_group.GetRowRanges();
    for (int32_t col_index : column_indices) {
        auto offset_index = rg_page_index_reader->GetOffsetIndex(col_index);
        if (!offset_index) {
            continue;
        }
        auto page_ranges = ComputeColumnPageRanges(bitmap, offset_index->page_locations(),
                                                   rg_start_row, rg_row_count);
        row_ranges = RowRanges::Intersection(row_ranges, page_ranges);
    }
    if (row_ranges.RowCount() == static_cast<int64_t>(rg_row_count)) {
        return row_group;
    } else {
        return TargetRowGroup(row_group_idx, true, std::move(row_ranges));
    }
}

RowRanges ParquetFileBatchReader::ComputeColumnPageRanges(
    const RoaringBitmap32& bitmap, const std::vector<::parquet::PageLocation>& page_locations,
    uint64_t rg_start_row, uint64_t rg_row_count) {
    RowRanges page_row_ranges;
    for (size_t page_idx = 0; page_idx < page_locations.size(); ++page_idx) {
        // half open interval [first_row, last_row)
        auto first_row = page_locations[page_idx].first_row_index;
        auto last_row = page_idx + 1 < page_locations.size()
                            ? page_locations[page_idx + 1].first_row_index
                            : rg_row_count;

        if (!bitmap.ContainsAny(rg_start_row + first_row, rg_start_row + last_row)) {
            continue;
        }
        // closed interval [range_start_row, range_end_row]
        auto range_start_row = bitmap.NextValue(rg_start_row + first_row);
        auto range_end_row = bitmap.PreviousValue(rg_start_row + last_row);
        if (!range_start_row.has_value() || !range_end_row.has_value()) {
            continue;
        }
        page_row_ranges.Add(
            Range(range_start_row.value() - rg_start_row, range_end_row.value() - rg_start_row));
    }
    return page_row_ranges;
}

// Uses page-level column index statistics to filter row groups and store per-row-group
// RowRanges for true page-level skipping. A row group is excluded if ALL its pages are
// determined to not match the predicate. For partially matched row groups, RowRanges
// are stored for page-level filtering during reading.
Result<TargetRowGroups> ParquetFileBatchReader::FilterRowGroupsByPageIndex(
    const std::shared_ptr<Predicate>& predicate,
    const std::map<std::string, int32_t>& column_name_to_index,
    const TargetRowGroups& src_row_groups) const {
    if (!predicate) {
        return src_row_groups;
    }

    auto page_index_reader = reader_->GetPageIndexReader();
    if (!page_index_reader) {
        PAIMON_LOG_DEBUG(logger_,
                         "Page index not available in file, skipping page-level filtering (%s)",
                         PARQUET_WRITE_ENABLE_PAGE_INDEX);
        return src_row_groups;
    }

    auto file_metadata = reader_->GetFileReader()->parquet_reader()->metadata();

    TargetRowGroups target_row_groups;

    for (const auto& row_group : src_row_groups) {
        int32_t row_group_idx = row_group.GetRowGroupIndex();
        auto result =
            reader_->CalculateFilteredRowRanges(row_group_idx, predicate, column_name_to_index);

        if (!result.ok()) {
            target_row_groups.emplace_back(row_group);
            continue;
        }

        const auto& row_ranges = result.value();
        if (!row_ranges.IsEmpty()) {
            int64_t rg_row_count = file_metadata->RowGroup(row_group_idx)->num_rows();
            auto intersection = row_group.IsPartiallyMatched()
                                    ? RowRanges::Intersection(row_group.GetRowRanges(), row_ranges)
                                    : row_ranges;
            if (intersection.IsEmpty()) {
                continue;
            }
            if (intersection.RowCount() < rg_row_count) {
                target_row_groups.emplace_back(row_group_idx, true, intersection);
            } else {
                target_row_groups.emplace_back(row_group);
            }
        }
    }

    return target_row_groups;
}

Result<BatchReader::ReadBatch> ParquetFileBatchReader::NextBatch() {
    try {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch, reader_->Next());
        if (batch == nullptr) {
            row_mapping_.clear();
            return BatchReader::MakeEofBatch();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          batch->ToStructArray());
        PAIMON_ASSIGN_OR_RAISE(bool need_cast, ParquetTimestampConverter::NeedCastArrayForTimestamp(
                                                   array->type(), read_data_type_));
        if (need_cast) {
            PAIMON_ASSIGN_OR_RAISE(array, ParquetTimestampConverter::CastArrayForTimestamp(
                                              array, read_data_type_, arrow_pool_));
        }
        PAIMON_ASSIGN_OR_RAISE(need_cast, ParquetTimestampConverter::NeedCastArrayForTimestamp(
                                              array->type(), read_data_type_));
        if (need_cast) {
            return Status::Invalid(fmt::format(
                "unexpected: in parquet, after CastArrayForTimestamp, output type {} not "
                "equal with read schema {}",
                array->type()->ToString(), read_data_type_->ToString()));
        }
        PAIMON_RETURN_NOT_OK(GenerateRowMapping(array->length()));
        std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));

        read_rows_ += array->length();
        read_batch_count_++;
        metrics_->SetCounter(ParquetMetrics::READ_ROWS, read_rows_);
        metrics_->SetCounter(ParquetMetrics::READ_BATCH_COUNT, read_batch_count_);

        return make_pair(std::move(c_array), std::move(c_schema));
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("ParquetFileBatchReader::NextBatch")
}

Result<std::vector<std::pair<uint64_t, uint64_t>>> ParquetFileBatchReader::GenReadRanges(
    bool* need_prefetch) const {
    try {
        *need_prefetch = true;
        return reader_->GetAllRowGroupRanges();
    }
    PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("ParquetFileBatchReader::GenReadRanges")
}

Result<::parquet::ReaderProperties> ParquetFileBatchReader::CreateReaderProperties(
    const std::shared_ptr<arrow::MemoryPool>& pool,
    const std::map<std::string, std::string>& options) {
    ::parquet::ReaderProperties reader_properties;
    // TODO(jinli.zjw): set more ReaderProperties (compare with java)
    PAIMON_ASSIGN_OR_RAISE(
        bool enable_pre_buffer,
        OptionsUtils::GetValueFromMap<bool>(options, PARQUET_READ_ENABLE_PRE_BUFFER, true));
    if (enable_pre_buffer) {
        reader_properties.enable_buffered_stream();
    } else {
        reader_properties.disable_buffered_stream();
    }
    return reader_properties;
}

Result<::parquet::ArrowReaderProperties> ParquetFileBatchReader::CreateArrowReaderProperties(
    const std::shared_ptr<arrow::MemoryPool>& pool,
    const std::map<std::string, std::string>& options, int32_t batch_size) {
    PAIMON_ASSIGN_OR_RAISE(
        uint32_t executor_thread_count,
        OptionsUtils::GetValueFromMap<uint32_t>(options, PARQUET_READ_EXECUTOR_THREAD_COUNT,
                                                DEFAULT_PARQUET_READ_EXECUTOR_THREAD_COUNT));

    ::parquet::ArrowReaderProperties arrow_reader_props;
    // TODO(jinli.zjw): set more ArrowReaderProperties (compare with java)
    PAIMON_ASSIGN_OR_RAISE(
        bool enable_pre_buffer,
        OptionsUtils::GetValueFromMap<bool>(options, PARQUET_READ_ENABLE_PRE_BUFFER, true));
    arrow_reader_props.set_pre_buffer(enable_pre_buffer);
    arrow_reader_props.set_batch_size(static_cast<int64_t>(batch_size));
    if (executor_thread_count != 0) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::SetCpuThreadPoolCapacity(executor_thread_count));
        arrow_reader_props.set_use_threads(true);
    } else {
        arrow_reader_props.set_use_threads(false);
    }
    PAIMON_ASSIGN_OR_RAISE(bool cache_lazy, OptionsUtils::GetValueFromMap<bool>(
                                                options, PARQUET_READ_CACHE_OPTION_LAZY, false));
    PAIMON_ASSIGN_OR_RAISE(
        int64_t cache_prefetch_limit,
        OptionsUtils::GetValueFromMap<int64_t>(options, PARQUET_READ_CACHE_OPTION_PREFETCH_LIMIT,
                                               DEFAULT_PARQUET_READ_CACHE_OPTION_PREFETCH_LIMIT));
    PAIMON_ASSIGN_OR_RAISE(
        int64_t cache_range_size_limit,
        OptionsUtils::GetValueFromMap<int64_t>(options, PARQUET_READ_CACHE_OPTION_RANGE_SIZE_LIMIT,
                                               DEFAULT_PARQUET_READ_CACHE_OPTION_RANGE_SIZE_LIMIT));
    auto cache_option = arrow::io::CacheOptions::Defaults();
    cache_option.lazy = cache_lazy;
    cache_option.prefetch_limit = cache_prefetch_limit;
    cache_option.range_size_limit = cache_range_size_limit;
    arrow_reader_props.set_cache_options(cache_option);
    return arrow_reader_props;
}

// Nested column index computation

Status ParquetFileBatchReader::CollectLeafIndices(const std::shared_ptr<arrow::DataType>& read_type,
                                                  const std::shared_ptr<arrow::DataType>& file_type,
                                                  int32_t* leaf_index,
                                                  std::vector<int32_t>* indices) {
    if (file_type->id() == arrow::Type::STRUCT) {
        for (const auto& file_child : file_type->fields()) {
            std::shared_ptr<arrow::Field> read_child = nullptr;
            for (const auto& candidate : read_type->fields()) {
                if (candidate->name() == file_child->name()) {
                    read_child = candidate;
                    break;
                }
            }
            if (read_child) {
                PAIMON_RETURN_NOT_OK(CollectLeafIndices(read_child->type(), file_child->type(),
                                                        leaf_index, indices));
            } else {
                SkipLeafIndices(file_child->type(), leaf_index);
            }
        }
    } else if (file_type->id() == arrow::Type::LIST || file_type->id() == arrow::Type::MAP) {
        // Keep behavior aligned with ORC path: list/map inner partial projection
        // is currently unsupported and should fail-fast.
        if (!read_type->Equals(file_type)) {
            return Status::Invalid(fmt::format(
                "Parquet does not support partial projection inside list/map: src {} vs target {}",
                file_type->ToString(), read_type->ToString()));
        }
        for (int32_t i = 0; i < file_type->num_fields(); i++) {
            PAIMON_RETURN_NOT_OK(CollectLeafIndices(
                read_type->field(i)->type(), file_type->field(i)->type(), leaf_index, indices));
        }
    } else {
        // Leaf column — collect its index.
        indices->push_back((*leaf_index)++);
    }
    return Status::OK();
}

void ParquetFileBatchReader::SkipLeafIndices(const std::shared_ptr<arrow::DataType>& file_type,
                                             int32_t* leaf_index) {
    if (file_type->id() == arrow::Type::STRUCT || file_type->id() == arrow::Type::LIST ||
        file_type->id() == arrow::Type::MAP) {
        for (int32_t i = 0; i < file_type->num_fields(); i++) {
            SkipLeafIndices(file_type->field(i)->type(), leaf_index);
        }
    } else {
        (*leaf_index)++;
    }
}

Result<std::vector<int32_t>> ParquetFileBatchReader::ComputeNestedColumnIndices(
    const std::shared_ptr<arrow::Schema>& read_schema,
    const std::shared_ptr<arrow::Schema>& file_schema) {
    std::vector<int32_t> indices;
    std::vector<int32_t> file_field_leaf_starts;
    file_field_leaf_starts.reserve(file_schema->num_fields());

    int32_t file_leaf_index = 0;
    for (const auto& file_field : file_schema->fields()) {
        file_field_leaf_starts.push_back(file_leaf_index);
        SkipLeafIndices(file_field->type(), &file_leaf_index);
    }

    const auto& file_fields = file_schema->fields();
    for (const auto& read_field : read_schema->fields()) {
        int32_t file_field_idx = -1;
        for (int32_t i = 0; i < static_cast<int32_t>(file_fields.size()); ++i) {
            if (file_fields[i]->name() == read_field->name()) {
                file_field_idx = i;
                break;
            }
        }
        if (file_field_idx < 0) {
            return Status::Invalid(
                fmt::format("Field '{}' in read schema does not exist in parquet file schema",
                            read_field->name()));
        }
        int32_t leaf_index = file_field_leaf_starts[file_field_idx];
        PAIMON_RETURN_NOT_OK(CollectLeafIndices(
            read_field->type(), file_fields[file_field_idx]->type(), &leaf_index, &indices));
    }
    return indices;
}

Status ParquetFileBatchReader::UpdateAllTargetRowRanges(
    const std::vector<TargetRowGroup>& target_row_groups) {
    row_mapping_.clear();
    auto all_row_group_ranges = reader_->GetAllRowGroupRanges();
    RowRanges all_ranges;
    for (const auto& target_row_group : target_row_groups) {
        auto row_group_idx = target_row_group.GetRowGroupIndex();
        for (const auto& range : target_row_group.GetRowRanges().GetRanges()) {
            all_ranges.Add(Range(all_row_group_ranges[row_group_idx].first + range.from,
                                 all_row_group_ranges[row_group_idx].first + range.to));
        }
    }
    all_row_ranges_ = std::move(all_ranges);
    return Status::OK();
}

Status ParquetFileBatchReader::GenerateRowMapping(int64_t batch_length) {
    const std::vector<Range>& all_ranges = all_row_ranges_.GetRanges();
    PAIMON_ASSIGN_OR_RAISE(int64_t batch_start_row, reader_->GetPreviousBatchFirstRowNumber());

    auto cur_range_it =
        std::upper_bound(all_ranges.begin(), all_ranges.end(), batch_start_row,
                         [](int64_t value, const Range& r) { return value < r.from; });
    if (cur_range_it == all_ranges.begin()) {
        return Status::Invalid("No range found!");
    }
    --cur_range_it;
    if (batch_start_row < cur_range_it->from || batch_start_row > cur_range_it->to) {
        return Status::Invalid(
            fmt::format("Batch start row {} is not in the current range [{}, {}]!", batch_start_row,
                        cur_range_it->from, cur_range_it->to));
    }

    std::vector<uint64_t> row_mapping;
    row_mapping.reserve(batch_length);
    int64_t global_row = batch_start_row;
    for (int64_t i = 0; i < batch_length; ++i) {
        if (global_row > cur_range_it->to) {
            ++cur_range_it;
            if (cur_range_it == all_ranges.end()) {
                return Status::Invalid("Batch length exceeds the total row ranges!");
            }
            global_row = cur_range_it->from;
        }
        row_mapping.push_back(global_row);
        global_row++;
    }
    row_mapping_ = std::move(row_mapping);
    return Status::OK();
}
}  // namespace paimon::parquet
