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

#include "paimon/core/operation/commit/row_id_column_conflict_checker.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/status.h"

namespace paimon {

Result<std::shared_ptr<RowIdColumnConflictChecker>> RowIdColumnConflictChecker::FromDataFiles(
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::vector<std::shared_ptr<DataFileMeta>>& delta_files) {
    auto checker =
        std::shared_ptr<RowIdColumnConflictChecker>(new RowIdColumnConflictChecker(schema_manager));

    PAIMON_RETURN_NOT_OK(checker->BuildWriteRanges(delta_files));
    return checker;
}

Status RowIdColumnConflictChecker::BuildWriteRanges(
    const std::vector<std::shared_ptr<DataFileMeta>>& delta_files) {
    std::vector<std::shared_ptr<DataFileMeta>> row_id_files;
    row_id_files.reserve(delta_files.size());
    for (const auto& file : delta_files) {
        if (file && file->first_row_id.has_value()) {
            row_id_files.push_back(file);
        }
    }

    if (row_id_files.empty()) {
        write_ranges_.clear();
        return Status::OK();
    }

    // 1. merge overlapping ranges and calculate [Range, unordered_set<FieldId>] struct.
    RangeHelper<std::shared_ptr<DataFileMeta>> range_helper(
        [](const std::shared_ptr<DataFileMeta>& file) -> Result<int64_t> {
            return file->first_row_id.value();
        },
        [](const std::shared_ptr<DataFileMeta>& file) -> Result<int64_t> {
            return file->first_row_id.value() + file->row_count - 1;
        });
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::vector<std::shared_ptr<DataFileMeta>>> merged_range_groups,
        range_helper.MergeOverlappingRanges(std::move(row_id_files)));

    write_ranges_.clear();

    for (const auto& group : merged_range_groups) {
        Range merged_range = MergeRange(group);
        std::unordered_set<int32_t> field_ids;

        for (const auto& file : group) {
            PAIMON_RETURN_NOT_OK(AddWriteFieldIds(file, &field_ids));
        }

        write_ranges_.push_back(WriteRange{merged_range, std::move(field_ids)});
    }

    // 2. sort by range for binary search
    std::sort(write_ranges_.begin(), write_ranges_.end(),
              [](const WriteRange& a, const WriteRange& b) {
                  if (a.range.from != b.range.from) {
                      return a.range.from < b.range.from;
                  }
                  return a.range.to < b.range.to;
              });

    return Status::OK();
}

Range RowIdColumnConflictChecker::MergeRange(
    const std::vector<std::shared_ptr<DataFileMeta>>& files) const {
    int64_t from = std::numeric_limits<int64_t>::max();
    int64_t to = std::numeric_limits<int64_t>::min();
    for (const auto& file : files) {
        const int64_t file_from = file->first_row_id.value();
        const int64_t file_to = file_from + file->row_count - 1;
        from = std::min(from, file_from);
        to = std::max(to, file_to);
    }
    return Range(from, to);
}

Status RowIdColumnConflictChecker::AddWriteFieldIds(const std::shared_ptr<DataFileMeta>& file,
                                                    std::unordered_set<int32_t>* field_ids) {
    if (!file->write_cols.has_value()) {
        std::map<std::string, int32_t> field_id_by_name;
        PAIMON_ASSIGN_OR_RAISE(field_id_by_name, FieldIdByName(file->schema_id));
        for (const auto& entry : field_id_by_name) {
            field_ids->insert(entry.second);
        }
        return Status::OK();
    }

    for (const auto& write_col : file->write_cols.value()) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<int32_t> field_id, FieldId(file, write_col));
        if (field_id.has_value()) {
            field_ids->insert(field_id.value());
        }
    }

    return Status::OK();
}

Result<bool> RowIdColumnConflictChecker::ConflictsWith(
    const std::shared_ptr<DataFileMeta>& file) const {
    if (!file->first_row_id.has_value()) {
        return false;
    }

    Range range(file->first_row_id.value(), file->first_row_id.value() + file->row_count - 1);
    int32_t index = FirstPossibleRange(range);
    while (index < static_cast<int32_t>(write_ranges_.size())) {
        const auto& write_range = write_ranges_[index];
        if (write_range.range.from > range.to) {
            return false;
        }
        // overlapping row range and overlapping write fields
        if (Range::HasIntersection(write_range.range, range)) {
            PAIMON_ASSIGN_OR_RAISE(bool has_common_write_field,
                                   ContainsAnyWriteField(write_range.field_ids, file));
            if (has_common_write_field) {
                return true;
            }
        }
        ++index;
    }

    return false;
}

int32_t RowIdColumnConflictChecker::FirstPossibleRange(const Range& range) const {
    int32_t low = 0;
    int32_t high = static_cast<int32_t>(write_ranges_.size());
    while (low < high) {
        const int32_t mid = low + (high - low) / 2;
        if (write_ranges_[mid].range.to < range.from) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

Result<bool> RowIdColumnConflictChecker::ContainsAnyWriteField(
    const std::unordered_set<int32_t>& field_ids, const std::shared_ptr<DataFileMeta>& file) const {
    // If write cols == null, it's a full-schema write
    if (!file->write_cols.has_value()) {
        return true;
    }

    for (const auto& write_col : file->write_cols.value()) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<int32_t> field_id, FieldId(file, write_col));
        if (field_id.has_value() && field_ids.count(field_id.value()) > 0) {
            return true;
        }
    }
    return false;
}

Result<std::optional<int32_t>> RowIdColumnConflictChecker::FieldId(
    const std::shared_ptr<DataFileMeta>& file, const std::string& write_col) const {
    std::map<std::string, int32_t> field_id_by_name;
    PAIMON_ASSIGN_OR_RAISE(field_id_by_name, FieldIdByName(file->schema_id));
    auto it = field_id_by_name.find(write_col);
    if (it != field_id_by_name.end()) {
        return std::optional<int32_t>(it->second);
    }

    if (SpecialFields::IsSystemField(write_col)) {
        return std::optional<int32_t>();
    }

    return Status::Invalid("Cannot find write column '" + write_col + "' in schema " +
                           std::to_string(file->schema_id) + ".");
}

Result<std::map<std::string, int32_t>> RowIdColumnConflictChecker::FieldIdByName(
    int64_t schema_id) const {
    auto cache_it = field_id_by_name_cache_.find(schema_id);
    if (cache_it != field_id_by_name_cache_.end()) {
        return cache_it->second;
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema,
                           schema_manager_->ReadSchema(schema_id));
    std::map<std::string, int32_t> mapping;
    for (const auto& field : schema->Fields()) {
        mapping[field.Name()] = field.Id();
    }

    auto [it, inserted] = field_id_by_name_cache_.emplace(schema_id, std::move(mapping));
    (void)inserted;
    return it->second;
}

}  // namespace paimon
