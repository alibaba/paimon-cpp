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

#include "paimon/core/io/data_file_meta.h"

#include <algorithm>
#include <cstddef>
#include <utility>

#include "arrow/type_fwd.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {

const BinaryRow& DataFileMeta::EmptyMinKey() {
    static const BinaryRow empty_row = BinaryRow::EmptyRow();
    return empty_row;
}

const BinaryRow& DataFileMeta::EmptyMaxKey() {
    static const BinaryRow empty_row = BinaryRow::EmptyRow();
    return empty_row;
}

Result<std::shared_ptr<DataFileMeta>> DataFileMeta::ForAppend(
    const std::string& file_name, int64_t file_size, int64_t row_count,
    const SimpleStats& row_stats, int64_t min_sequence_number, int64_t max_sequence_number,
    int64_t schema_id, const std::optional<FileSource>& file_source,
    const std::optional<std::vector<std::string>>& value_stats_cols,
    const std::optional<std::string>& external_path, const std::optional<int64_t>& first_row_id,
    const std::optional<std::vector<std::string>>& write_cols) {
    return ForAppend(file_name, file_size, row_count, row_stats, min_sequence_number,
                     max_sequence_number, schema_id, std::vector<std::optional<std::string>>(),
                     nullptr, file_source, value_stats_cols, external_path, first_row_id,
                     write_cols);
}

Result<std::shared_ptr<DataFileMeta>> DataFileMeta::ForAppend(
    const std::string& file_name, int64_t file_size, int64_t row_count,
    const SimpleStats& row_stats, int64_t min_sequence_number, int64_t max_sequence_number,
    int64_t schema_id, const std::vector<std::optional<std::string>>& extra_files,
    const std::shared_ptr<Bytes>& embedded_index, const std::optional<FileSource>& file_source,
    const std::optional<std::vector<std::string>>& value_stats_cols,
    const std::optional<std::string>& external_path, const std::optional<int64_t>& first_row_id,
    const std::optional<std::vector<std::string>>& write_cols) {
    PAIMON_ASSIGN_OR_RAISE(int64_t local_micro, DateTimeUtils::GetCurrentLocalTimeUs());
    return std::make_shared<DataFileMeta>(
        file_name, file_size, row_count, EmptyMinKey(), EmptyMaxKey(), SimpleStats::EmptyStats(),
        row_stats, min_sequence_number, max_sequence_number, schema_id, DUMMY_LEVEL, extra_files,
        Timestamp(/*millisecond=*/local_micro / 1000, /*nano_of_millisecond=*/0), 0ll,
        embedded_index, file_source, value_stats_cols, external_path, first_row_id, write_cols);
}

DataFileMeta::DataFileMeta(
    const std::string& _file_name, int64_t _file_size, int64_t _row_count,
    const BinaryRow& _min_key, const BinaryRow& _max_key, const SimpleStats& _key_stats,
    const SimpleStats& _value_stats, int64_t _min_sequence_number, int64_t _max_sequence_number,
    int64_t _schema_id, int32_t _level, const std::vector<std::optional<std::string>>& _extra_files,
    const Timestamp& _creation_time, const std::optional<int64_t>& _delete_row_count,
    const std::shared_ptr<Bytes>& _embedded_index, const std::optional<FileSource>& _file_source,
    const std::optional<std::vector<std::string>>& _value_stats_cols,
    const std::optional<std::string>& _external_path, const std::optional<int64_t>& _first_row_id,
    const std::optional<std::vector<std::string>>& _write_cols)
    : file_name(_file_name),
      file_size(_file_size),
      row_count(_row_count),
      min_key(_min_key),
      max_key(_max_key),
      key_stats(_key_stats),
      value_stats(_value_stats),
      min_sequence_number(_min_sequence_number),
      max_sequence_number(_max_sequence_number),
      schema_id(_schema_id),
      level(_level),
      extra_files(_extra_files),
      creation_time(_creation_time),
      delete_row_count(_delete_row_count),
      embedded_index(_embedded_index),
      file_source(_file_source),
      value_stats_cols(_value_stats_cols),
      external_path(_external_path),
      first_row_id(_first_row_id),
      write_cols(_write_cols) {}

Result<std::string> DataFileMeta::FileFormat() const {
    size_t last_dot_index = file_name.find_last_of(".");
    if (last_dot_index == std::string::npos || last_dot_index == file_name.length() - 1) {
        return Status::Invalid("cannot find format from file ", file_name);
    }
    return file_name.substr(last_dot_index + 1);
}

std::optional<std::string> DataFileMeta::ExternalPathDir() const {
    if (!external_path) {
        return std::nullopt;
    }
    return PathUtil::GetParentDirPath(external_path.value());
}

Result<int64_t> DataFileMeta::CreationTimeEpochMillis() const {
    PAIMON_ASSIGN_OR_RAISE(Timestamp utc_ts, DateTimeUtils::ToUTCTimestamp(creation_time));
    return utc_ts.GetMillisecond();
}

Result<std::optional<RoaringBitmap32>> DataFileMeta::ToFileSelection(
    const std::optional<std::vector<Range>>& row_ranges) const {
    if (!row_ranges) {
        return std::optional<RoaringBitmap32>();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t start, NonNullFirstRowId());
    int64_t end = start + row_count - 1;
    Range file_range(start, end);

    RoaringBitmap32 selection;
    for (const auto& row_range : row_ranges.value()) {
        auto intersect_result = Range::Intersection(file_range, row_range);
        if (intersect_result) {
            selection.AddRange(static_cast<int32_t>(intersect_result.value().from - start),
                               static_cast<int32_t>(intersect_result.value().to - start + 1));
        }
    }
    if (selection.Cardinality() == row_count) {
        // If all rows are selected, do not push down selection bitmap.
        return std::optional<RoaringBitmap32>();
    }
    return std::optional<RoaringBitmap32>(selection);
}

bool DataFileMeta::operator==(const DataFileMeta& other) const {
    if (this == &other) {
        return true;
    }
    if ((embedded_index && !other.embedded_index) || (!embedded_index && other.embedded_index)) {
        return false;
    }
    if (embedded_index && other.embedded_index && !(*embedded_index == *other.embedded_index)) {
        return false;
    }
    return file_name == other.file_name && file_size == other.file_size &&
           row_count == other.row_count && min_key == other.min_key && max_key == other.max_key &&
           key_stats == other.key_stats && value_stats == other.value_stats &&
           min_sequence_number == other.min_sequence_number &&
           max_sequence_number == other.max_sequence_number && schema_id == other.schema_id &&
           level == other.level && extra_files == other.extra_files &&
           creation_time == other.creation_time && delete_row_count == other.delete_row_count &&
           file_source == other.file_source && value_stats_cols == other.value_stats_cols &&
           external_path == other.external_path && first_row_id == other.first_row_id &&
           write_cols == other.write_cols;
}

bool DataFileMeta::TEST_Equal(const DataFileMeta& other) const {
    if (this == &other) {
        return true;
    }
    auto compare_optional_ignore_name = [](const std::optional<std::string>& lhs,
                                           const std::optional<std::string>& rhs) -> bool {
        if (lhs != rhs) {
            if (lhs == std::nullopt || rhs == std::nullopt) {
                return false;
            }
        }
        return true;
    };

    if (extra_files.size() != other.extra_files.size()) {
        return false;
    } else {
        for (size_t i = 0; i < extra_files.size(); ++i) {
            if (!compare_optional_ignore_name(extra_files[i], other.extra_files[i])) {
                return false;
            }
        }
    }

    if ((embedded_index && !other.embedded_index) || (!embedded_index && other.embedded_index)) {
        return false;
    }
    if (embedded_index && other.embedded_index && !(*embedded_index == *other.embedded_index)) {
        return false;
    }
    // ignore file_name, file_size, extra_files, creation_time and external path
    return row_count == other.row_count && min_key == other.min_key && max_key == other.max_key &&
           key_stats == other.key_stats && value_stats == other.value_stats &&
           min_sequence_number == other.min_sequence_number &&
           max_sequence_number == other.max_sequence_number && schema_id == other.schema_id &&
           level == other.level && delete_row_count == other.delete_row_count &&
           file_source == other.file_source && value_stats_cols == other.value_stats_cols &&
           compare_optional_ignore_name(external_path, other.external_path) &&
           first_row_id == other.first_row_id && write_cols == other.write_cols;
}

std::string DataFileMeta::ToString() const {
    std::vector<std::string> extra_files_str;
    for (const auto& file : extra_files) {
        if (file == std::nullopt) {
            extra_files_str.emplace_back("null");
        } else {
            extra_files_str.emplace_back(file.value());
        }
    }

    return fmt::format(
        "{{fileName: {}, fileSize: {}, rowCount: {}, embeddedIndex: {}, minKey: {}, maxKey: "
        "{}, "
        "keyStats: {}, valueStats: {}, minSequenceNumber: {}, maxSequenceNumber: {}, schemaId: "
        "{}, level: {}, extraFiles: {}, creationTime: {}, deleteRowCount: {}, fileSource: {}, "
        "valueStatsCols: {}, externalPath: {}, firstRowId: {}, writeCols: {}}}",
        file_name, file_size, row_count,
        embedded_index == nullptr ? "null"
                                  : std::string(embedded_index->data(), embedded_index->size()),
        min_key.ToString(), max_key.ToString(), key_stats.ToString(), value_stats.ToString(),
        min_sequence_number, max_sequence_number, schema_id, level, extra_files_str,
        creation_time.ToString(),
        delete_row_count == std::nullopt ? "null" : std::to_string(delete_row_count.value()),
        file_source == std::nullopt ? "null" : file_source.value().ToString(),
        value_stats_cols == std::nullopt
            ? "null"
            : fmt::format("{}", fmt::join(value_stats_cols.value(), ", ")),
        external_path == std::nullopt ? "null" : external_path.value(),
        first_row_id == std::nullopt ? "null" : std::to_string(first_row_id.value()),
        write_cols == std::nullopt ? "null" : fmt::format("{}", write_cols.value()));
}

int64_t DataFileMeta::GetMaxSequenceNumber(
    const std::vector<std::shared_ptr<DataFileMeta>>& file_metas) {
    int64_t ret = -1;
    for (const auto& meta : file_metas) {
        ret = std::max(ret, meta->max_sequence_number);
    }
    return ret;
}

const std::shared_ptr<arrow::DataType>& DataFileMeta::DataType() {
    static std::shared_ptr<arrow::DataType> schema = arrow::struct_(
        {arrow::field("_FILE_NAME", arrow::utf8(), /*nullable=*/false),
         arrow::field("_FILE_SIZE", arrow::int64(), /*nullable=*/false),
         arrow::field("_ROW_COUNT", arrow::int64(), /*nullable=*/false),
         arrow::field("_MIN_KEY", arrow::binary(), /*nullable=*/false),
         arrow::field("_MAX_KEY", arrow::binary(), /*nullable=*/false),
         arrow::field("_KEY_STATS", SimpleStats::DataType(), /*nullable=*/false),
         arrow::field("_VALUE_STATS", SimpleStats::DataType(), /*nullable=*/false),
         arrow::field("_MIN_SEQUENCE_NUMBER", arrow::int64(), /*nullable=*/false),
         arrow::field("_MAX_SEQUENCE_NUMBER", arrow::int64(), /*nullable=*/false),
         arrow::field("_SCHEMA_ID", arrow::int64(), /*nullable=*/false),
         arrow::field("_LEVEL", arrow::int32(), /*nullable=*/false),
         arrow::field("_EXTRA_FILES",
                      arrow::list(arrow::field("item", arrow::utf8(), /*nullable=*/false)),
                      /*nullable=*/false),
         arrow::field("_CREATION_TIME", arrow::timestamp(arrow::TimeUnit::MILLI),
                      /*nullable=*/true),
         arrow::field("_DELETE_ROW_COUNT", arrow::int64(), /*nullable=*/true),
         arrow::field("_EMBEDDED_FILE_INDEX", arrow::binary(), /*nullable=*/true),
         arrow::field("_FILE_SOURCE", arrow::int8(), /*nullable=*/true),
         arrow::field("_VALUE_STATS_COLS",
                      arrow::list(arrow::field("item", arrow::utf8(), /*nullable=*/false)),
                      /*nullable=*/true),
         arrow::field("_EXTERNAL_PATH", arrow::utf8(), /*nullable=*/true),
         arrow::field("_FIRST_ROW_ID", arrow::int64(), /*nullable=*/true),
         arrow::field("_WRITE_COLS",
                      arrow::list(arrow::field("item", arrow::utf8(), /*nullable=*/false)),
                      /*nullable=*/true)});
    return schema;
}

}  // namespace paimon
