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

#include "paimon/format/lance/lance_stats_extractor.h"

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
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/lance/lance_file_batch_reader.h"
#include "paimon/format/lance/lance_format_defs.h"
#include "paimon/status.h"

namespace paimon {
class FileSystem;
class MemoryPool;
}  // namespace paimon

namespace paimon::lance {
Result<std::pair<ColumnStatsVector, FormatStatsExtractor::FileInfo>>
LanceStatsExtractor::ExtractWithFileInfo(const std::shared_ptr<FileSystem>& file_system,
                                         const std::string& path,
                                         const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<LanceFileBatchReader> lance_reader,
        LanceFileBatchReader::Create(path, core_options.GetReadBatchSize(),
                                     /*batch_readahead=*/DEFAULT_LANCE_READAHEAD_BATCH_COUNT));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> c_schema, lance_reader->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_schema.get()));
    ColumnStatsVector result_stats;
    result_stats.reserve(arrow_schema->num_fields());
    for (const auto& arrow_field : arrow_schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ColumnStats> stats,
                               FetchColumnStatistics(arrow_field->type()));
        result_stats.push_back(std::move(stats));
    }
    PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, lance_reader->GetNumberOfRows());
    return std::make_pair(result_stats, FileInfo(num_rows));
}

Result<std::unique_ptr<ColumnStats>> LanceStatsExtractor::FetchColumnStatistics(
    const std::shared_ptr<arrow::DataType>& type) const {
    // TODO(xinyu.lxy): support stats in lance
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
        default:
            return Status::Invalid("Unknown or unsupported arrow type: ", type->ToString());
    }
}
}  // namespace paimon::lance
