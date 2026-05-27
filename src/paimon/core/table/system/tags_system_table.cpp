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

#include "paimon/core/table/system/tags_system_table.h"

#include <utility>

#include "arrow/api.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/tag_manager.h"
#include "paimon/data/timestamp.h"
#include "paimon/status.h"

namespace paimon {
namespace {

Result<int64_t> LocalDateTimePartsToTimestampMillis(const std::vector<int64_t>& parts) {
    if (parts.size() < 6) {
        return Status::Invalid("tag create time requires at least 6 date-time fields");
    }

    int64_t year = parts[0];
    int64_t month = parts[1];
    int64_t day = parts[2];
    int64_t hour = parts[3];
    int64_t minute = parts[4];
    int64_t second = parts[5];
    int64_t nanos = parts.size() > 6 ? parts[6] : 0;
    auto is_leap_year = [](int64_t value) {
        return value % 4 == 0 && (value % 100 != 0 || value % 400 == 0);
    };
    int64_t days_in_month[] = {31, is_leap_year(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30,
                               31};
    if (month < 1 || month > 12 || day < 1 || day > days_in_month[month - 1] || hour < 0 ||
        hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 || nanos < 0 ||
        nanos > 999999999) {
        return Status::Invalid("invalid tag create time fields");
    }

    year -= month <= 2 ? 1 : 0;
    int64_t era = (year >= 0 ? year : year - 399) / 400;
    auto year_of_era = static_cast<uint32_t>(year - era * 400);
    auto month_prime = static_cast<uint32_t>(month + (month > 2 ? -3 : 9));
    uint32_t day_of_year = (153 * month_prime + 2) / 5 + static_cast<uint32_t>(day) - 1;
    uint32_t day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    int64_t epoch_day = era * 146097 + static_cast<int64_t>(day_of_era) - 719468;
    return epoch_day * DateTimeUtils::MILLIS_PER_DAY + hour * 3600000 + minute * 60000 +
           second * 1000 + nanos / 1000000;
}

Result<std::optional<int64_t>> OptionalLocalDateTimePartsToTimestampMillis(
    const std::optional<std::vector<int64_t>>& parts) {
    if (!parts) {
        return std::optional<int64_t>();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t timestamp_millis,
                           LocalDateTimePartsToTimestampMillis(parts.value()));
    return std::optional<int64_t>(timestamp_millis);
}

std::optional<std::string> OptionalDoubleToString(const std::optional<double_t>& value) {
    if (!value) {
        return std::optional<std::string>();
    }
    return std::to_string(value.value());
}

VariantType OptionalTimestampMillisValue(const std::optional<int64_t>& value) {
    if (!value) {
        return NullType();
    }
    return Timestamp::FromEpochMillis(value.value());
}

}  // namespace

TagsSystemTable::TagsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                 std::string branch)
    : InMemorySystemTable(table_path),
      context_(SystemTableUtils::CreateContext(std::move(fs), std::move(table_path),
                                               std::move(branch))) {}

std::string TagsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> TagsSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("tag_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("snapshot_id", arrow::int64(), /*nullable=*/false),
        arrow::field("schema_id", arrow::int64(), /*nullable=*/false),
        arrow::field("commit_time", arrow::timestamp(arrow::TimeUnit::MILLI),
                     /*nullable=*/false),
        arrow::field("record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("create_time", arrow::timestamp(arrow::TimeUnit::MILLI),
                     /*nullable=*/true),
        arrow::field("time_retained", arrow::utf8(), /*nullable=*/true),
    });
}

Result<std::vector<GenericRow>> TagsSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    TagManager tag_manager(context_.fs, context_.table_path, context_.branch);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tag_names, tag_manager.ListTagNames());
    std::vector<GenericRow> rows;
    rows.reserve(tag_names.size());

    for (const auto& name : tag_names) {
        PAIMON_ASSIGN_OR_RAISE(Tag tag, tag_manager.GetOrThrow(name));
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> tag_create_time,
                               OptionalLocalDateTimePartsToTimestampMillis(tag.TagCreateTime()));
        GenericRow row(schema->num_fields());
        row.SetField(0, SystemTableUtils::StringValue(name));
        row.SetField(1, tag.Id());
        row.SetField(2, tag.SchemaId());
        PAIMON_ASSIGN_OR_RAISE(VariantType commit_time,
                               SystemTableUtils::LocalTimestampMillisValue(tag.TimeMillis()));
        row.SetField(3, commit_time);
        row.SetField(4, SystemTableUtils::OptionalInt64Value(tag.TotalRecordCount()));
        row.SetField(5, OptionalTimestampMillisValue(tag_create_time));
        row.SetField(6, SystemTableUtils::OptionalStringValue(
                            OptionalDoubleToString(tag.TagTimeRetained())));
        rows.push_back(std::move(row));
    }

    return rows;
}

}  // namespace paimon
