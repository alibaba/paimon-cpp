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

#include "paimon/format/parquet/parquet_stats_extractor.h"

#include <cassert>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/parquet/parquet_schema_util.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"
#include "parquet/types.h"

namespace paimon {
class MemoryPool;
}  // namespace paimon

namespace paimon::parquet {

namespace {

template <typename ParquetTypedStatsType, typename R = typename ParquetTypedStatsType::T>
std::pair<std::optional<R>, std::optional<R>> CollectMinMaxStats(
    const std::shared_ptr<ParquetTypedStatsType>& typed_stats) {
    std::optional<R> min;
    std::optional<R> max;
    if (typed_stats && typed_stats->HasMinMax()) {
        min = typed_stats->min();
        max = typed_stats->max();
    }
    return std::make_pair(min, max);
}

Result<std::unique_ptr<ColumnStats>> ConvertStatsToColumnStats(
    const std::shared_ptr<::parquet::Statistics>& stats,
    const std::shared_ptr<::parquet::schema::PrimitiveNode>& primitive_node,
    const std::shared_ptr<arrow::DataType>& write_type, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> data_type,
                                      GetArrowType(*primitive_node));
    auto id = data_type->id();

    std::optional<int64_t> null_count;
    if (stats && stats->HasNullCount()) {
        null_count = stats->null_count();
    }
    switch (id) {
        case arrow::Type::BOOL: {
            auto typed_stats =
                arrow::internal::checked_pointer_cast<::parquet::BoolStatistics>(stats);
            auto [min, max] = CollectMinMaxStats(typed_stats);
            return ColumnStats::CreateBooleanColumnStats(min, max, null_count);
        }
        case arrow::Type::INT8: {
            auto typed_stats =
                arrow::internal::checked_pointer_cast<::parquet::Int32Statistics>(stats);
            std::optional<int8_t> min;
            std::optional<int8_t> max;
            if (typed_stats && typed_stats->HasMinMax()) {
                min = static_cast<int8_t>(typed_stats->min());
                max = static_cast<int8_t>(typed_stats->max());
            }
            return ColumnStats::CreateTinyIntColumnStats(min, max, null_count);
        }
        case arrow::Type::INT16: {
            auto typed_stats =
                arrow::internal::checked_pointer_cast<::parquet::Int32Statistics>(stats);
            std::optional<int16_t> min;
            std::optional<int16_t> max;
            if (typed_stats && typed_stats->HasMinMax()) {
                min = static_cast<int16_t>(typed_stats->min());
                max = static_cast<int16_t>(typed_stats->max());
            }
            return ColumnStats::CreateSmallIntColumnStats(min, max, null_count);
        }
        case arrow::Type::INT32: {
            auto typed_stats =
                arrow::internal::checked_pointer_cast<::parquet::Int32Statistics>(stats);
            auto [min, max] = CollectMinMaxStats(typed_stats);
            return ColumnStats::CreateIntColumnStats(min, max, null_count);
        }
        case arrow::Type::INT64: {
            auto typed_stats =
                arrow::internal::checked_pointer_cast<::parquet::Int64Statistics>(stats);
            auto [min, max] = CollectMinMaxStats(typed_stats);
            return ColumnStats::CreateBigIntColumnStats(min, max, null_count);
        }
        case arrow::Type::FLOAT: {
            auto typed_stats =
                arrow::internal::checked_pointer_cast<::parquet::FloatStatistics>(stats);
            auto [min, max] = CollectMinMaxStats(typed_stats);
            return ColumnStats::CreateFloatColumnStats(min, max, null_count);
        }
        case arrow::Type::DOUBLE: {
            auto typed_stats =
                arrow::internal::checked_pointer_cast<::parquet::DoubleStatistics>(stats);
            auto [min, max] = CollectMinMaxStats(typed_stats);
            return ColumnStats::CreateDoubleColumnStats(min, max, null_count);
        }
        case arrow::Type::STRING: {
            auto typed_stats =
                arrow::internal::checked_pointer_cast<::parquet::ByteArrayStatistics>(stats);
            std::optional<std::string> min;
            std::optional<std::string> max;
            if (typed_stats && typed_stats->HasMinMax()) {
                min = std::string(std::string_view{typed_stats->min()});
                max = std::string(std::string_view{typed_stats->max()});
            }
            return ColumnStats::CreateStringColumnStats(min, max, null_count);
        }
        case arrow::Type::BINARY: {
            return ColumnStats::CreateStringColumnStats(std::nullopt, std::nullopt, null_count);
        }
        case arrow::Type::DATE32: {
            auto typed_stats =
                arrow::internal::checked_pointer_cast<::parquet::Int32Statistics>(stats);
            auto [min, max] = CollectMinMaxStats<::parquet::Int32Statistics>(typed_stats);
            return ColumnStats::CreateDateColumnStats(min, max, null_count);
        }
        case arrow::Type::TIMESTAMP: {
            auto timestamp_type =
                arrow::internal::checked_pointer_cast<::arrow::TimestampType>(data_type);
            if (timestamp_type->unit() == arrow::TimeUnit::type::NANO) {
                // int96 does not have statistics
                return ColumnStats::CreateTimestampColumnStats(
                    std::nullopt, std::nullopt, std::nullopt, Timestamp::MAX_PRECISION);
            }
            auto typed_stats =
                arrow::internal::checked_pointer_cast<::parquet::Int64Statistics>(stats);
            auto [min, max] = CollectMinMaxStats(typed_stats);
            // while write type is ts(second), data type in parquet file will be ts(milli), correct
            // precision is supposed to be extracted from write type
            auto write_ts_type =
                arrow::internal::checked_pointer_cast<::arrow::TimestampType>(write_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(write_ts_type);
            if (!min || !max) {
                return ColumnStats::CreateTimestampColumnStats(std::nullopt, std::nullopt,
                                                               null_count, precision);
            }
            auto src_time_type = DateTimeUtils::GetTimeTypeFromArrowType(timestamp_type);
            auto [milli_min, nano_min] = DateTimeUtils::TimestampConverter(
                min.value(), src_time_type, DateTimeUtils::TimeType::MILLISECOND,
                DateTimeUtils::TimeType::NANOSECOND);
            auto [milli_max, nano_max] = DateTimeUtils::TimestampConverter(
                max.value(), src_time_type, DateTimeUtils::TimeType::MILLISECOND,
                DateTimeUtils::TimeType::NANOSECOND);
            return ColumnStats::CreateTimestampColumnStats(Timestamp(milli_min, nano_min),
                                                           Timestamp(milli_max, nano_max),
                                                           null_count, precision);
        }
        case arrow::Type::DECIMAL128: {
            auto decimal_type =
                arrow::internal::checked_pointer_cast<::arrow::Decimal128Type>(data_type);
            int32_t precision = decimal_type->precision();
            int32_t scale = decimal_type->scale();
            std::optional<Decimal> min_value;
            std::optional<Decimal> max_value;
            if (primitive_node->physical_type() == ::parquet::Type::INT32) {
                auto typed_stats =
                    arrow::internal::checked_pointer_cast<::parquet::Int32Statistics>(stats);
                if (typed_stats && typed_stats->HasMinMax()) {
                    min_value = Decimal(precision, scale, typed_stats->min());
                    max_value = Decimal(precision, scale, typed_stats->max());
                }
            } else if (primitive_node->physical_type() == ::parquet::Type::INT64) {
                auto typed_stats =
                    arrow::internal::checked_pointer_cast<::parquet::Int64Statistics>(stats);
                if (typed_stats && typed_stats->HasMinMax()) {
                    min_value = Decimal(precision, scale, typed_stats->min());
                    max_value = Decimal(precision, scale, typed_stats->max());
                }
            } else if (primitive_node->physical_type() == ::parquet::Type::FIXED_LEN_BYTE_ARRAY ||
                       primitive_node->physical_type() == ::parquet::Type::BYTE_ARRAY) {
                if (stats && stats->HasMinMax()) {
                    Bytes encode_min(stats->EncodeMin(), pool.get());
                    min_value = Decimal::FromUnscaledBytes(precision, scale, &encode_min);
                    Bytes encode_max(stats->EncodeMax(), pool.get());
                    max_value = Decimal::FromUnscaledBytes(precision, scale, &encode_max);
                }
            }
            return ColumnStats::CreateDecimalColumnStats(min_value, max_value, null_count,
                                                         precision, scale);
        }
        default:
            return Status::Invalid(
                fmt::format("cannot fetch statistics, invalid type {}", data_type->ToString()));
    }
}

template <typename T>
void MergeTypedStats(
    const std::string& column_name, const std::shared_ptr<::parquet::Statistics>& stats,
    std::unordered_map<std::string, std::shared_ptr<::parquet::Statistics>>* merged_stats) {
    auto& entry = (*merged_stats)[column_name];
    if (!entry) {
        entry = stats;
    } else {
        arrow::internal::checked_pointer_cast<T>(entry)->Merge(
            *arrow::internal::checked_pointer_cast<T>(stats));
    }
}

Status MergeStats(
    const std::string& column_name, const std::shared_ptr<::parquet::Statistics>& stats,
    std::unordered_map<std::string, std::shared_ptr<::parquet::Statistics>>* merged_stats) {
    switch (stats->physical_type()) {
        case ::parquet::Type::BOOLEAN:
            MergeTypedStats<::parquet::BoolStatistics>(column_name, stats, merged_stats);
            break;
        case ::parquet::Type::INT32:
            MergeTypedStats<::parquet::Int32Statistics>(column_name, stats, merged_stats);
            break;
        case ::parquet::Type::INT64:
            MergeTypedStats<::parquet::Int64Statistics>(column_name, stats, merged_stats);
            break;
        case ::parquet::Type::FLOAT:
            MergeTypedStats<::parquet::FloatStatistics>(column_name, stats, merged_stats);
            break;
        case ::parquet::Type::DOUBLE:
            MergeTypedStats<::parquet::DoubleStatistics>(column_name, stats, merged_stats);
            break;
        case ::parquet::Type::BYTE_ARRAY:
            MergeTypedStats<::parquet::ByteArrayStatistics>(column_name, stats, merged_stats);
            break;
        case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
            MergeTypedStats<::parquet::FLBAStatistics>(column_name, stats, merged_stats);
            break;
        default:
            return Status::Invalid(fmt::format("Unsupported parquet type {} for statistics merge",
                                               ::parquet::TypeToString(stats->physical_type())));
    }
    return Status::OK();
}

}  // namespace

Result<std::pair<ColumnStatsVector, FormatStatsExtractor::FileInfo>>
ParquetStatsExtractor::ExtractWithFileInfo(const std::shared_ptr<FileSystem>& file_system,
                                           const std::string& path,
                                           const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> input_stream, file_system->Open(path));
    assert(input_stream);
    PAIMON_ASSIGN_OR_RAISE(uint64_t file_length, input_stream->Length());
    std::shared_ptr<arrow::MemoryPool> parquet_memory_pool = GetArrowPool(pool);
    auto parquet_input_file = std::make_shared<ArrowInputStreamAdapter>(
        std::move(input_stream), parquet_memory_pool, file_length);
    ::parquet::ReaderProperties read_properties(parquet_memory_pool.get());
    read_properties.enable_buffered_stream();
    ::parquet::arrow::FileReaderBuilder file_reader_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_builder.Open(parquet_input_file, read_properties));

    std::shared_ptr<::parquet::FileMetaData> file_metadata =
        file_reader_builder.raw_reader()->metadata();
    int32_t field_count = file_metadata->schema()->group_node()->field_count();

    ColumnStatsVector result_stats;
    result_stats.reserve(field_count);

    std::unordered_map<std::string, std::shared_ptr<::parquet::Statistics>> merged_stats;

    for (int32_t row_group_idx = 0; row_group_idx < file_metadata->num_row_groups();
         ++row_group_idx) {
        for (int32_t col_idx = 0; col_idx < file_metadata->num_columns(); ++col_idx) {
            auto column_chunk = file_metadata->RowGroup(row_group_idx)->ColumnChunk(col_idx);
            if (!column_chunk->is_stats_set()) {
                continue;
            }
            auto stats = column_chunk->statistics();
            std::string column_name = column_chunk->path_in_schema()->ToDotString();
            PAIMON_RETURN_NOT_OK(MergeStats(column_name, stats, &merged_stats));
        }
    }

    for (int32_t field_idx = 0; field_idx < field_count; ++field_idx) {
        auto node = file_metadata->schema()->group_node()->field(field_idx);
        if (node->is_group()) {
            // nested type do not have parquet stats
            const auto& logical_type = node->logical_type();
            FieldType nested_type = FieldType::UNKNOWN;
            if (logical_type->is_list()) {
                nested_type = FieldType::ARRAY;
            } else if (logical_type->is_map()) {
                nested_type = FieldType::MAP;
            } else if (logical_type->is_none()) {
                nested_type = FieldType::STRUCT;
            }
            result_stats.push_back(ColumnStats::CreateNestedColumnStats(nested_type, std::nullopt));
        } else {
            auto primitive_node =
                arrow::internal::checked_pointer_cast<::parquet::schema::PrimitiveNode>(node);
            assert(primitive_node != nullptr);
            auto iter = merged_stats.find(node->name());
            const std::shared_ptr<::parquet::Statistics>& parquet_stats =
                iter == merged_stats.end() ? nullptr : iter->second;
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<ColumnStats> col_stats,
                ConvertStatsToColumnStats(parquet_stats, primitive_node,
                                          write_schema_->field(field_idx)->type(), pool));
            result_stats.push_back(col_stats);
        }
    }
    return std::make_pair(std::move(result_stats), FileInfo(file_metadata->num_rows()));
}

}  // namespace paimon::parquet
