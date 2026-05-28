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

#include "paimon/format/orc/orc_stats_extractor.h"

#include <cassert>
#include <exception>

#include "arrow/type_fwd.h"
#include "fmt/format.h"
#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "orc/Statistics.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/casting/decimal_to_decimal_cast_executor.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/format/orc/orc_input_stream_impl.h"
#include "paimon/format/orc/orc_memory_pool.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/literal.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
}  // namespace arrow
namespace paimon {
class MemoryPool;
}  // namespace paimon

namespace paimon::orc {
Result<std::pair<ColumnStatsVector, FormatStatsExtractor::FileInfo>>
OrcStatsExtractor::ExtractWithFileInfo(const std::shared_ptr<FileSystem>& file_system,
                                       const std::string& path,
                                       const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream, file_system->Open(path));
    assert(input_stream);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OrcInputStreamImpl> orc_input_stream,
                           OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
    try {
        ::orc::ReaderOptions reader_options;
        auto orc_pool = std::make_shared<OrcMemoryPool>(pool);
        reader_options.setMemoryPool(*orc_pool);
        std::unique_ptr<::orc::Reader> reader =
            ::orc::createReader(std::move(orc_input_stream), reader_options);
        assert(reader);
        row_count_ = reader->getNumberOfRows();
        if (reader->getNumberOfStripeStatistics() == 0) {
            return Status::Invalid(fmt::format("statistics is disabled in {}", path));
        }
        auto statistics = reader->getStatistics();
        auto num_columns = statistics->getNumberOfColumns();
        auto sub_type_count = reader->getType().getSubtypeCount();
        if (reader->getType().getSubtype(sub_type_count - 1)->getMaximumColumnId() + 1 !=
            num_columns) {
            // flatten sub type count {reader->getType().getSubtype(sub_type_count -
            // 1)->getMaximumColumnId() + 1} mismatch number columns {num_columns} of statistics
            return Status::Invalid(
                fmt::format("sub type count {} mismatch number columns {} of statistics",
                            sub_type_count, num_columns));
        }
        ColumnStatsVector result_stats;
        result_stats.reserve(sub_type_count);
        for (uint32_t i = 0; i < sub_type_count; i++) {
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<ColumnStats> stats,
                FetchColumnStatistics(
                    statistics->getColumnStatistics(reader->getType().getSubtype(i)->getColumnId()),
                    reader->getType().getSubtype(i), write_schema_->field(i)->type()));
            result_stats.push_back(std::move(stats));
        }
        return std::make_pair(result_stats, FileInfo(row_count_));
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("extract file info failed for file {}, with {} error", path, e.what()));
    } catch (...) {
        return Status::UnknownError(
            fmt::format("extract file info failed for file {}, with unknown error", path));
    }
}

Result<std::unique_ptr<ColumnStats>> OrcStatsExtractor::FetchColumnStatistics(
    const ::orc::ColumnStatistics* column_stats, const ::orc::Type* type,
    const std::shared_ptr<arrow::DataType>& write_type) const {
    if (column_stats == nullptr || type == nullptr) {
        return Status::Invalid("column stats is null or orc type is null");
    }
    int64_t null_count = row_count_ - column_stats->getNumberOfValues();
    bool all_null = (null_count == row_count_);
    if (null_count > 0 && !column_stats->hasNull()) {
        return Status::Invalid(
            "invalid column statistics, null count greater than zero while has no null value");
    }
    ::orc::TypeKind kind = type->getKind();
    switch (kind) {
        case ::orc::BOOLEAN: {
            auto typed_stats = dynamic_cast<const ::orc::BooleanColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid(
                    "cannot cast to BooleanColumnStatistics for orc::BOOLEAN type");
            }
            if (all_null || !typed_stats->hasCount()) {
                return ColumnStats::CreateBooleanColumnStats(std::nullopt, std::nullopt,
                                                             null_count);
            }
            return ColumnStats::CreateBooleanColumnStats(
                typed_stats->getFalseCount() == 0, typed_stats->getTrueCount() != 0, null_count);
        }
        case ::orc::BYTE: {
            auto typed_stats = dynamic_cast<const ::orc::IntegerColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid("cannot cast to IntegerColumnStatistics for orc::BYTE type");
            }
            if (all_null || !typed_stats->hasMinimum()) {
                return ColumnStats::CreateTinyIntColumnStats(std::nullopt, std::nullopt,
                                                             null_count);
            }
            return ColumnStats::CreateTinyIntColumnStats(
                static_cast<int8_t>(typed_stats->getMinimum()),
                static_cast<int8_t>(typed_stats->getMaximum()), null_count);
        }
        case ::orc::SHORT: {
            auto typed_stats = dynamic_cast<const ::orc::IntegerColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid(
                    "cannot cast to IntegerColumnStatistics for orc::SHORT type");
            }
            if (all_null || !typed_stats->hasMinimum()) {
                return ColumnStats::CreateSmallIntColumnStats(std::nullopt, std::nullopt,
                                                              null_count);
            }
            return ColumnStats::CreateSmallIntColumnStats(
                static_cast<int16_t>(typed_stats->getMinimum()),
                static_cast<int16_t>(typed_stats->getMaximum()), null_count);
        }
        case ::orc::INT: {
            auto typed_stats = dynamic_cast<const ::orc::IntegerColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid("cannot cast to IntegerColumnStatistics for orc::INT type");
            }
            if (all_null || !typed_stats->hasMinimum()) {
                return ColumnStats::CreateIntColumnStats(std::nullopt, std::nullopt, null_count);
            }
            return ColumnStats::CreateIntColumnStats(
                static_cast<int32_t>(typed_stats->getMinimum()),
                static_cast<int32_t>(typed_stats->getMaximum()), null_count);
        }
        case ::orc::LONG: {
            auto typed_stats = dynamic_cast<const ::orc::IntegerColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid("cannot cast to IntegerColumnStatistics for orc::Long type");
            }
            if (all_null || !typed_stats->hasMinimum()) {
                return ColumnStats::CreateBigIntColumnStats(std::nullopt, std::nullopt, null_count);
            }
            return ColumnStats::CreateBigIntColumnStats(typed_stats->getMinimum(),
                                                        typed_stats->getMaximum(), null_count);
        }
        case ::orc::FLOAT: {
            auto typed_stats = dynamic_cast<const ::orc::DoubleColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid("cannot cast to DoubleColumnStatistics for orc::FLOAT type");
            }
            if (all_null || !typed_stats->hasMinimum()) {
                return ColumnStats::CreateFloatColumnStats(std::nullopt, std::nullopt, null_count);
            }
            return ColumnStats::CreateFloatColumnStats(
                static_cast<float>(typed_stats->getMinimum()),
                static_cast<float>(typed_stats->getMaximum()), null_count);
        }
        case ::orc::DOUBLE: {
            auto typed_stats = dynamic_cast<const ::orc::DoubleColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid(
                    "cannot cast to DoubleColumnStatistics for orc::DOUBLE type");
            }
            if (all_null || !typed_stats->hasMinimum()) {
                return ColumnStats::CreateDoubleColumnStats(std::nullopt, std::nullopt, null_count);
            }
            return ColumnStats::CreateDoubleColumnStats(typed_stats->getMinimum(),
                                                        typed_stats->getMaximum(), null_count);
        }
        case ::orc::CHAR:
        case ::orc::VARCHAR:
        case ::orc::STRING: {
            auto typed_stats = dynamic_cast<const ::orc::StringColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid(
                    "cannot cast to StringColumnStatistics for orc::CHAR/VARCHAR/STRING type");
            }
            if (all_null || !typed_stats->hasMinimum()) {
                return ColumnStats::CreateStringColumnStats(std::nullopt, std::nullopt, null_count);
            }
            return ColumnStats::CreateStringColumnStats(typed_stats->getMinimum(),
                                                        typed_stats->getMaximum(), null_count);
        }
        case ::orc::BINARY:
            return ColumnStats::CreateStringColumnStats(std::nullopt, std::nullopt, null_count);
        case ::orc::TIMESTAMP:
        case ::orc::TIMESTAMP_INSTANT: {
            auto typed_stats = dynamic_cast<const ::orc::TimestampColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid(
                    "cannot cast to TimestampColumnStatistics for orc::TIMESTAMP/TIMESTAMP_INSTANT "
                    "type");
            }
            auto write_ts_type =
                arrow::internal::checked_pointer_cast<::arrow::TimestampType>(write_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(write_ts_type);
            if (all_null || !typed_stats->hasMinimum()) {
                return ColumnStats::CreateTimestampColumnStats(std::nullopt, std::nullopt,
                                                               null_count, precision);
            }
            int64_t min_ms = typed_stats->getMinimum();
            int64_t max_ms = typed_stats->getMaximum();
            int32_t min_ns = typed_stats->getMinimumNanos();
            int32_t max_ns = typed_stats->getMaximumNanos();
            return ColumnStats::CreateTimestampColumnStats(
                Timestamp(min_ms, min_ns), Timestamp(max_ms, max_ns), null_count, precision);
        }
        case ::orc::DATE: {
            auto typed_stats = dynamic_cast<const ::orc::DateColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid(
                    "cannot cast to DateColumnStatistics for orc::Date "
                    "type");
            }
            if (all_null || !typed_stats->hasMinimum()) {
                return ColumnStats::CreateDateColumnStats(std::nullopt, std::nullopt, null_count);
            }
            return ColumnStats::CreateDateColumnStats(typed_stats->getMinimum(),
                                                      typed_stats->getMaximum(), null_count);
        }
        case ::orc::DECIMAL: {
            auto typed_stats = dynamic_cast<const ::orc::DecimalColumnStatistics*>(column_stats);
            if (typed_stats == nullptr) {
                return Status::Invalid(
                    "cannot cast to DecimalColumnStatistics for orc::DECIMAL "
                    "type");
            }
            int32_t precision = type->getPrecision();
            int32_t scale = type->getScale();
            if (all_null || !typed_stats->hasMinimum()) {
                return ColumnStats::CreateDecimalColumnStats(std::nullopt, std::nullopt, null_count,
                                                             precision, scale);
            }
            auto min_decimal = typed_stats->getMinimum();
            Decimal min_value(precision, min_decimal.scale,
                              static_cast<Decimal::int128_t>(
                                  static_cast<Decimal::uint128_t>(
                                      static_cast<uint64_t>(min_decimal.value.getHighBits()))
                                      << 64 |
                                  min_decimal.value.getLowBits()));
            std::shared_ptr<arrow::DataType> target_type = arrow::decimal128(precision, scale);
            auto cast_executor = std::make_shared<DecimalToDecimalCastExecutor>();
            if (min_decimal.scale != scale) {
                // need casting
                PAIMON_ASSIGN_OR_RAISE(Literal converted_min_value,
                                       cast_executor->Cast(Literal(min_value), target_type));
                min_value = converted_min_value.GetValue<Decimal>();
            }
            auto max_decimal = typed_stats->getMaximum();
            Decimal max_value(precision, max_decimal.scale,
                              static_cast<Decimal::int128_t>(
                                  static_cast<Decimal::uint128_t>(
                                      static_cast<uint64_t>(max_decimal.value.getHighBits()))
                                      << 64 |
                                  max_decimal.value.getLowBits()));
            if (max_decimal.scale != scale) {
                // need casting
                PAIMON_ASSIGN_OR_RAISE(Literal converted_max_value,
                                       cast_executor->Cast(Literal(max_value), target_type));
                max_value = converted_max_value.GetValue<Decimal>();
            }
            return ColumnStats::CreateDecimalColumnStats(min_value, max_value, null_count,
                                                         precision, scale);
        }
        case ::orc::LIST:
            return ColumnStats::CreateNestedColumnStats(FieldType::ARRAY, null_count);
        case ::orc::MAP:
            return ColumnStats::CreateNestedColumnStats(FieldType::MAP, null_count);
        case ::orc::STRUCT:
            return ColumnStats::CreateNestedColumnStats(FieldType::STRUCT, null_count);
        default:
            return Status::Invalid(
                fmt::format("cannot fetch statistics, invalid type {}", type->toString()));
    }
    return Status::Invalid(
        fmt::format("cannot fetch statistics, invalid type {}", type->toString()));
}
}  // namespace paimon::orc
