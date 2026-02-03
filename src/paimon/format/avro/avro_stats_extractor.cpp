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

#include "paimon/format/avro/avro_stats_extractor.h"

#include <cstdint>
#include <optional>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/util/checked_cast.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/defs.h"
#include "paimon/format/avro/avro_file_format.h"
#include "paimon/status.h"

namespace paimon {
class FileSystem;
class MemoryPool;
}  // namespace paimon

namespace paimon::avro {

Result<std::pair<ColumnStatsVector, FormatStatsExtractor::FileInfo>>
AvroStatsExtractor::ExtractWithFileInfoInternal(const std::shared_ptr<FileSystem>& file_system,
                                                const std::string& path,
                                                const std::shared_ptr<MemoryPool>& pool,
                                                bool with_file_info) const {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> input_stream, file_system->Open(path));
    assert(input_stream);
    auto avro_file_format = std::make_unique<AvroFileFormat>(options_);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> avro_reader_builder,
                           avro_file_format->CreateReaderBuilder(core_options.GetReadBatchSize()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FileBatchReader> avro_reader,
        avro_reader_builder->WithMemoryPool(pool)->Build(std::move(input_stream)));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> c_schema, avro_reader->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_schema.get()));
    ColumnStatsVector result_stats;
    result_stats.reserve(arrow_schema->num_fields());
    for (const auto& arrow_field : arrow_schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ColumnStats> stats,
                               FetchColumnStatistics(arrow_field->type()));
        result_stats.push_back(std::move(stats));
    }
    if (!with_file_info) {
        // Do not return file info if not needed, because GetNumberOfRows in avro reader need I/O
        // and performance is poor.
        return std::make_pair(result_stats, FileInfo(-1));
    }
    PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, avro_reader->GetNumberOfRows());
    return std::make_pair(result_stats, FileInfo(num_rows));
}

Result<std::unique_ptr<ColumnStats>> AvroStatsExtractor::FetchColumnStatistics(
    const std::shared_ptr<arrow::DataType>& type) const {
    // TODO(jinli.zjw): support stats in avro
    arrow::Type::type kind = type->id();
    switch (kind) {
        case arrow::Type::type::BOOL:
            return ColumnStats::CreateBooleanColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::type::INT8:
            return ColumnStats::CreateTinyIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::type::INT16:
            return ColumnStats::CreateSmallIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::type::INT32:
            return ColumnStats::CreateIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::type::INT64:
            return ColumnStats::CreateBigIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::type::FLOAT:
            return ColumnStats::CreateFloatColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::type::DOUBLE:
            return ColumnStats::CreateDoubleColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::type::BINARY:
            return ColumnStats::CreateStringColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::type::STRING:
            return ColumnStats::CreateStringColumnStats(std::nullopt, std::nullopt, std::nullopt);

        case arrow::Type::type::DATE32:
            return ColumnStats::CreateDateColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::type::TIMESTAMP: {
            auto ts_type = arrow::internal::checked_pointer_cast<::arrow::TimestampType>(type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(ts_type);
            return ColumnStats::CreateTimestampColumnStats(std::nullopt, std::nullopt, std::nullopt,
                                                           precision);
        }
        case arrow::Type::type::DECIMAL128: {
            auto decimal_type =
                arrow::internal::checked_pointer_cast<::arrow::Decimal128Type>(type);
            int32_t precision = decimal_type->precision();
            int32_t scale = decimal_type->scale();
            return ColumnStats::CreateDecimalColumnStats(std::nullopt, std::nullopt, std::nullopt,
                                                         precision, scale);
        }
        case arrow::Type::type::STRUCT:
            return ColumnStats::CreateNestedColumnStats(FieldType::STRUCT, std::nullopt);
        case arrow::Type::type::LIST:
            return ColumnStats::CreateNestedColumnStats(FieldType::ARRAY, std::nullopt);
        case arrow::Type::type::MAP:
            return ColumnStats::CreateNestedColumnStats(FieldType::MAP, std::nullopt);
        default:
            return Status::Invalid("Unknown or unsupported arrow type: ", type->ToString());
    }
}
}  // namespace paimon::avro
