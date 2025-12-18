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

#include "paimon/core/stats/simple_stats_collector.h"

#include <cassert>
#include <cstdint>
#include <optional>
#include <string>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/format/column_stats.h"

namespace paimon {

SimpleStatsCollector::SimpleStatsCollector(const std::shared_ptr<arrow::Schema>& schema)
    : schema_(schema) {
    column_stats_.resize(schema_->num_fields());
}

Status SimpleStatsCollector::Collect(const BinaryRow& row) {
    assert(schema_);
    if (schema_->num_fields() != row.GetFieldCount()) {
        return Status::Invalid(fmt::format(
            "fields count {} in partition schema not equal to fields count {} in partition",
            schema_->num_fields(), row.GetFieldCount()));
    }
    for (int32_t i = 0; i < schema_->num_fields(); i++) {
        const auto& field = schema_->field(i);
        const auto& type = field->type()->id();
        switch (type) {
            case arrow::Type::BOOL: {
                if (column_stats_[i] == nullptr) {
                    column_stats_[i] = ColumnStats::CreateBooleanColumnStats(
                        std::nullopt, std::nullopt, std::nullopt);
                }
                auto typed_stats = dynamic_cast<BooleanColumnStats*>(column_stats_[i].get());
                if (typed_stats == nullptr) {
                    assert(false);
                    return Status::Invalid("cast typed stats failed");
                }
                if (!row.IsNullAt(i)) {
                    typed_stats->Collect(row.GetBoolean(i));
                } else {
                    typed_stats->Collect(std::nullopt);
                }
                break;
            }
            case arrow::Type::INT8: {
                if (column_stats_[i] == nullptr) {
                    column_stats_[i] = ColumnStats::CreateTinyIntColumnStats(
                        std::nullopt, std::nullopt, std::nullopt);
                }
                auto typed_stats = dynamic_cast<TinyIntColumnStats*>(column_stats_[i].get());
                if (typed_stats == nullptr) {
                    assert(false);
                    return Status::Invalid("cast typed stats failed");
                }
                if (!row.IsNullAt(i)) {
                    typed_stats->Collect(row.GetByte(i));
                } else {
                    typed_stats->Collect(std::nullopt);
                }
                break;
            }
            case arrow::Type::INT16: {
                if (column_stats_[i] == nullptr) {
                    column_stats_[i] = ColumnStats::CreateSmallIntColumnStats(
                        std::nullopt, std::nullopt, std::nullopt);
                }
                auto typed_stats = dynamic_cast<SmallIntColumnStats*>(column_stats_[i].get());
                if (typed_stats == nullptr) {
                    assert(false);
                    return Status::Invalid("cast typed stats failed");
                }
                if (!row.IsNullAt(i)) {
                    typed_stats->Collect(row.GetShort(i));
                } else {
                    typed_stats->Collect(std::nullopt);
                }
                break;
            }
            case arrow::Type::INT32: {
                if (column_stats_[i] == nullptr) {
                    column_stats_[i] =
                        ColumnStats::CreateIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
                }
                auto typed_stats = dynamic_cast<IntColumnStats*>(column_stats_[i].get());
                if (typed_stats == nullptr) {
                    assert(false);
                    return Status::Invalid("cast typed stats failed");
                }
                if (!row.IsNullAt(i)) {
                    typed_stats->Collect(row.GetInt(i));
                } else {
                    typed_stats->Collect(std::nullopt);
                }
                break;
            }
            case arrow::Type::INT64: {
                if (column_stats_[i] == nullptr) {
                    column_stats_[i] = ColumnStats::CreateBigIntColumnStats(
                        std::nullopt, std::nullopt, std::nullopt);
                }
                auto typed_stats = dynamic_cast<BigIntColumnStats*>(column_stats_[i].get());
                if (typed_stats == nullptr) {
                    assert(false);
                    return Status::Invalid("cast typed stats failed");
                }
                if (!row.IsNullAt(i)) {
                    typed_stats->Collect(row.GetLong(i));
                } else {
                    typed_stats->Collect(std::nullopt);
                }
                break;
            }
            case arrow::Type::FLOAT: {
                if (column_stats_[i] == nullptr) {
                    column_stats_[i] = ColumnStats::CreateFloatColumnStats(
                        std::nullopt, std::nullopt, std::nullopt);
                }
                auto typed_stats = dynamic_cast<FloatColumnStats*>(column_stats_[i].get());
                if (typed_stats == nullptr) {
                    assert(false);
                    return Status::Invalid("cast typed stats failed");
                }
                if (!row.IsNullAt(i)) {
                    typed_stats->Collect(row.GetFloat(i));
                } else {
                    typed_stats->Collect(std::nullopt);
                }
                break;
            }
            case arrow::Type::DOUBLE: {
                if (column_stats_[i] == nullptr) {
                    column_stats_[i] = ColumnStats::CreateDoubleColumnStats(
                        std::nullopt, std::nullopt, std::nullopt);
                }
                auto typed_stats = dynamic_cast<DoubleColumnStats*>(column_stats_[i].get());
                if (typed_stats == nullptr) {
                    assert(false);
                    return Status::Invalid("cast typed stats failed");
                }
                if (!row.IsNullAt(i)) {
                    typed_stats->Collect(row.GetDouble(i));
                } else {
                    typed_stats->Collect(std::nullopt);
                }
                break;
            }
            case arrow::Type::STRING:
            case arrow::Type::BINARY: {
                if (column_stats_[i] == nullptr) {
                    column_stats_[i] = ColumnStats::CreateStringColumnStats(
                        std::nullopt, std::nullopt, std::nullopt);
                }
                auto typed_stats = dynamic_cast<StringColumnStats*>(column_stats_[i].get());
                if (typed_stats == nullptr) {
                    assert(false);
                    return Status::Invalid("cast typed stats failed");
                }
                if (!row.IsNullAt(i)) {
                    typed_stats->Collect(row.GetString(i).ToString());
                } else {
                    typed_stats->Collect(std::nullopt);
                }
                break;
            }
            case arrow::Type::DATE32: {
                if (column_stats_[i] == nullptr) {
                    column_stats_[i] = ColumnStats::CreateDateColumnStats(
                        std::nullopt, std::nullopt, std::nullopt);
                }
                auto typed_stats = dynamic_cast<DateColumnStats*>(column_stats_[i].get());
                if (typed_stats == nullptr) {
                    assert(false);
                    return Status::Invalid("cast typed stats failed");
                }
                if (!row.IsNullAt(i)) {
                    typed_stats->Collect(row.GetDate(i));
                } else {
                    typed_stats->Collect(std::nullopt);
                }
                break;
            }
            default:
                return Status::NotImplemented(
                    fmt::format("Do not support arrow type {}", static_cast<int32_t>(type)));
        }
    }
    return Status::OK();
}

}  // namespace paimon
