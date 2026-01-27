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
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_format_defs.h"
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
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : options_(options),
      arrow_pool_(arrow_pool),
      input_stream_(std::move(input_stream)),
      reader_(std::move(reader)),
      read_ranges_(reader_->GetAllRowGroupRanges()),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<ParquetFileBatchReader>> ParquetFileBatchReader::Create(
    std::shared_ptr<arrow::io::RandomAccessFile>&& input_stream,
    const std::shared_ptr<arrow::MemoryPool>& pool,
    const std::map<std::string, std::string>& options, int32_t batch_size) {
    assert(input_stream);
    PAIMON_ASSIGN_OR_RAISE(::parquet::ReaderProperties reader_properties,
                           CreateReaderProperties(pool, options));
    PAIMON_ASSIGN_OR_RAISE(::parquet::ArrowReaderProperties arrow_reader_properties,
                           CreateArrowReaderProperties(pool, options, batch_size));

    ::parquet::arrow::FileReaderBuilder file_reader_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_builder.Open(input_stream, reader_properties));

    std::unique_ptr<::parquet::arrow::FileReader> file_reader;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_builder.memory_pool(pool.get())
                                        ->properties(arrow_reader_properties)
                                        ->Build(&file_reader));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileReaderWrapper> reader,
                           FileReaderWrapper::Create(std::move(file_reader)));
    auto parquet_file_batch_reader = std::unique_ptr<ParquetFileBatchReader>(
        new ParquetFileBatchReader(std::move(input_stream), std::move(reader), options, pool));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> file_schema,
                           parquet_file_batch_reader->GetFileSchema());
    PAIMON_RETURN_NOT_OK(parquet_file_batch_reader->SetReadSchema(
        file_schema.get(), /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt));
    return parquet_file_batch_reader;
}

Result<std::unique_ptr<::ArrowSchema>> ParquetFileBatchReader::GetFileSchema() const {
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

Status ParquetFileBatchReader::SetReadSchema(
    ::ArrowSchema* schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!schema) {
        return Status::Invalid("SetReadSchema failed: read schema cannot be nullptr");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> read_schema,
                                      arrow::ImportSchema(schema));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> file_schema, reader_->GetSchema());
    std::unordered_map<std::string, std::vector<int32_t>> field_index_map;
    int32_t i = 0;
    for (const auto& field : file_schema->fields()) {
        std::vector<int32_t> v;
        FlattenSchema(field->type(), &i, &v);
        field_index_map[field->name()] = v;
    }

    std::vector<int32_t> column_indices;
    for (const auto& field : read_schema->field_names()) {
        if (field_index_map.find(field) != field_index_map.end()) {
            for (int32_t index : field_index_map[field]) {
                column_indices.push_back(index);
            }
        } else {
            return Status::Invalid(fmt::format("Field {} is not found in schema.", field));
        }
    }

    std::vector<int32_t> row_groups = arrow::internal::Iota(reader_->GetNumberOfRowGroups());
    if (predicate) {
        PAIMON_ASSIGN_OR_RAISE(row_groups,
                               FilterRowGroupsByPredicate(predicate, file_schema, row_groups));
    }
    if (selection_bitmap) {
        PAIMON_ASSIGN_OR_RAISE(row_groups,
                               FilterRowGroupsByBitmap(selection_bitmap.value(), row_groups));
    }

    read_data_type_ = arrow::struct_(read_schema->fields());
    read_row_groups_ = row_groups;
    read_column_indices_ = column_indices;

    PAIMON_ASSIGN_OR_RAISE(std::set<int32_t> ordered_row_groups,
                           reader_->FilterRowGroupsByReadRanges(read_ranges_, read_row_groups_));
    return reader_->PrepareForReadingLazy(ordered_row_groups, read_column_indices_);
}

Result<std::vector<int32_t>> ParquetFileBatchReader::FilterRowGroupsByPredicate(
    const std::shared_ptr<Predicate>& predicate, const std::shared_ptr<arrow::Schema> file_schema,
    const std::vector<int32_t>& src_row_groups) const {
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
            /*physical_schema=*/nullptr, /*row_groups=*/src_row_groups));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        file_fragment->EnsureCompleteMetadata(reader_->GetFileReader()));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(arrow::dataset::FragmentVector target_fragments,
                                      file_fragment->SplitByRowGroup(bind_expr));
    std::vector<int32_t> target_row_groups;
    target_row_groups.reserve(src_row_groups.size());
    for (const auto& fragment : target_fragments) {
        auto parquet_fragment = dynamic_cast<arrow::dataset::ParquetFileFragment*>(fragment.get());
        if (!parquet_fragment) {
            return Status::Invalid("cannot cast to ParquetFileFragment in ParquetFileBatchReader");
        }
        target_row_groups.insert(target_row_groups.end(), parquet_fragment->row_groups().begin(),
                                 parquet_fragment->row_groups().end());
    }
    return target_row_groups;
}

Result<std::vector<int32_t>> ParquetFileBatchReader::FilterRowGroupsByBitmap(
    const RoaringBitmap32& bitmap, const std::vector<int32_t>& src_row_groups) const {
    if (bitmap.IsEmpty()) {
        return Status::Invalid("cannot push down an empty bitmap to ParquetFileBatchReader");
    }
    const auto& all_row_group_ranges = reader_->GetAllRowGroupRanges();
    // filter row groups by row range
    std::vector<int32_t> target_row_groups;
    for (const auto& row_group_idx : src_row_groups) {
        if (static_cast<size_t>(row_group_idx) >= all_row_group_ranges.size()) {
            return Status::Invalid(
                fmt::format("src row group {} not in row group meta", row_group_idx));
        }
        const auto& [start_row_idx, end_row_idx] = all_row_group_ranges[row_group_idx];
        if (bitmap.ContainsAny(start_row_idx, end_row_idx)) {
            target_row_groups.push_back(row_group_idx);
        }
    }
    return target_row_groups;
}

Result<BatchReader::ReadBatch> ParquetFileBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch, reader_->Next());
    if (batch == nullptr) {
        return BatchReader::MakeEofBatch();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array, batch->ToStructArray());
    PAIMON_ASSIGN_OR_RAISE(bool need_cast, ParquetTimestampConverter::NeedCastArrayForTimestamp(
                                               array->type(), read_data_type_));
    if (need_cast) {
        PAIMON_ASSIGN_OR_RAISE(array, ParquetTimestampConverter::CastArrayForTimestamp(
                                          array, read_data_type_, arrow_pool_));
    }
    PAIMON_ASSIGN_OR_RAISE(need_cast, ParquetTimestampConverter::NeedCastArrayForTimestamp(
                                          array->type(), read_data_type_));
    if (need_cast) {
        return Status::Invalid(
            fmt::format("unexpected: in parquet, after CastArrayForTimestamp, output type {} not "
                        "equal with read schema {}",
                        array->type()->ToString(), read_data_type_->ToString()));
    }
    std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
    return make_pair(std::move(c_array), std::move(c_schema));
}

Result<std::vector<std::pair<uint64_t, uint64_t>>> ParquetFileBatchReader::GenReadRanges(
    bool* need_prefetch) const {
    *need_prefetch = true;
    return reader_->GetAllRowGroupRanges();
}

Result<::parquet::ReaderProperties> ParquetFileBatchReader::CreateReaderProperties(
    const std::shared_ptr<arrow::MemoryPool>& pool,
    const std::map<std::string, std::string>& options) {
    ::parquet::ReaderProperties reader_properties;
    // TODO(jinli.zjw): set more ReaderProperties (compare with java)
    reader_properties.enable_buffered_stream();
    return reader_properties;
}

Result<::parquet::ArrowReaderProperties> ParquetFileBatchReader::CreateArrowReaderProperties(
    const std::shared_ptr<arrow::MemoryPool>& pool,
    const std::map<std::string, std::string>& options, int32_t batch_size) {
    PAIMON_ASSIGN_OR_RAISE(bool use_threads,
                           OptionsUtils::GetValueFromMap<bool>(options, PARQUET_READ_USE_THREADS,
                                                               DEFAULT_PARQUET_READ_USE_THREADS));

    ::parquet::ArrowReaderProperties arrow_reader_props;
    // TODO(jinli.zjw): set more ArrowReaderProperties (compare with java)
    arrow_reader_props.set_pre_buffer(true);
    arrow_reader_props.set_batch_size(static_cast<int64_t>(batch_size));
    arrow_reader_props.set_use_threads(use_threads);
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

}  // namespace paimon::parquet
