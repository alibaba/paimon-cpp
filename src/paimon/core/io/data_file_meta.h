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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/range.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
class Bytes;

/// Metadata of a data file.
struct DataFileMeta {
    static const BinaryRow& EmptyMinKey();
    static const BinaryRow& EmptyMaxKey();
    static constexpr int32_t DUMMY_LEVEL = 0;

    DataFileMeta(const std::string& _file_name, int64_t _file_size, int64_t _row_count,
                 const BinaryRow& _min_key, const BinaryRow& _max_key,
                 const SimpleStats& _key_stats, const SimpleStats& _value_stats,
                 int64_t _min_sequence_number, int64_t _max_sequence_number, int64_t _schema_id,
                 int _level, const std::vector<std::optional<std::string>>& _extra_files,
                 const Timestamp& _creation_time, const std::optional<int64_t>& _delete_row_count,
                 const std::shared_ptr<Bytes>& _embedded_index,
                 const std::optional<FileSource>& _file_source,
                 const std::optional<std::vector<std::string>>& _value_stats_cols,
                 const std::optional<std::string>& _external_path,
                 const std::optional<int64_t>& _first_row_id,
                 const std::optional<std::vector<std::string>>& _write_cols);

    static Result<std::shared_ptr<DataFileMeta>> ForAppend(
        const std::string& file_name, int64_t file_size, int64_t row_count,
        const SimpleStats& row_stats, int64_t min_sequence_number, int64_t max_sequence_number,
        int64_t schema_id, const std::optional<FileSource>& file_source,
        const std::optional<std::vector<std::string>>& value_stats_cols,
        const std::optional<std::string>& external_path, const std::optional<int64_t>& first_row_id,
        const std::optional<std::vector<std::string>>& write_cols);

    static Result<std::shared_ptr<DataFileMeta>> ForAppend(
        const std::string& file_name, int64_t file_size, int64_t row_count,
        const SimpleStats& row_stats, int64_t min_sequence_number, int64_t max_sequence_number,
        int64_t schema_id, const std::vector<std::optional<std::string>>& extra_files,
        const std::shared_ptr<Bytes>& embedded_index, const std::optional<FileSource>& file_source,
        const std::optional<std::vector<std::string>>& value_stats_cols,
        const std::optional<std::string>& external_path, const std::optional<int64_t>& first_row_id,
        const std::optional<std::vector<std::string>>& write_cols);

    std::optional<int64_t> AddRowCount() const {
        return delete_row_count == std::nullopt ? std::optional<int64_t>()
                                                : row_count - delete_row_count.value();
    }

    Result<int64_t> CreationTimeEpochMillis() const;

    Result<std::string> FileFormat() const;
    std::optional<std::string> ExternalPathDir() const;

    bool operator==(const DataFileMeta& other) const;
    bool TEST_Equal(const DataFileMeta& other) const;
    std::string ToString() const;

    void AssignSequenceNumber(int64_t _min_sequence_number, int64_t _max_sequence_number) {
        min_sequence_number = _min_sequence_number;
        max_sequence_number = _max_sequence_number;
    }

    void AssignFirstRowId(int64_t _first_row_id) {
        first_row_id = _first_row_id;
    }

    Result<int64_t> NonNullFirstRowId() const {
        if (first_row_id) {
            return first_row_id.value();
        }
        return Status::Invalid(fmt::format("First row id of {} should not be null.", file_name));
    }

    // empty row_ranges indicates all rows in the file are needed, return null bitmap
    Result<std::optional<RoaringBitmap32>> ToFileSelection(
        const std::optional<std::vector<Range>>& row_ranges) const;

    static int64_t GetMaxSequenceNumber(
        const std::vector<std::shared_ptr<DataFileMeta>>& file_metas);

    static const std::shared_ptr<arrow::DataType>& DataType();

    std::string file_name;
    int64_t file_size;
    // total number of rows (including add & delete) in this file
    int64_t row_count;
    BinaryRow min_key;
    BinaryRow max_key;
    SimpleStats key_stats;
    SimpleStats value_stats;

    int64_t min_sequence_number;
    int64_t max_sequence_number;
    int64_t schema_id;
    int32_t level;

    /// Usage:
    ///
    /// Paimon 0.2
    /// Stores changelog files for `CoreOptions.ChangelogProducer#INPUT`. Changelog
    /// files are moved to `DataIncrement` since Paimon 0.3.
    std::vector<std::optional<std::string>> extra_files;
    Timestamp creation_time;

    // row_count = addRowCount + deleteRowCount
    // Why don't we keep addRowCount and deleteRowCount?
    // Because in previous versions of DataFileMeta, we only keep row_count.
    // We have to keep the compatibility.
    std::optional<int64_t> delete_row_count;

    // file index filter bytes, if it is small, store in data file meta
    std::shared_ptr<Bytes> embedded_index;
    std::optional<FileSource> file_source;
    std::optional<std::vector<std::string>> value_stats_cols;

    // external path of file, if it is null, it is in the default warehouse path.
    std::optional<std::string> external_path;

    std::optional<int64_t> first_row_id;

    std::optional<std::vector<std::string>> write_cols;
};
}  // namespace paimon
