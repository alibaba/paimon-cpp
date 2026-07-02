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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/utils/range.h"

namespace paimon {

/// Detects row-id range conflicts only when written field ids overlap. The detection process is as
/// below:
///
///   Merge delta files by row range and calculate updated columns.
///   Sort those items by range.
///   For each checking files, do binary search to find overlapping ranges. If their updated
///   columns also overlap, return conflicting result.
///
class RowIdColumnConflictChecker {
 public:
    static Result<std::shared_ptr<RowIdColumnConflictChecker>> FromDataFiles(
        const std::shared_ptr<SchemaManager>& schema_manager,
        const std::vector<std::shared_ptr<DataFileMeta>>& delta_files);

    bool IsEmpty() const {
        return write_ranges_.empty();
    }

    /// Check whether a committed incremental file entry conflicts with current committing delta
    /// files. If an existing file has both overlapping row range and overlapping write fields, then
    /// it conflicts.
    ///
    /// @param file committed incremental data file
    /// @return true if conflict
    Result<bool> ConflictsWith(const std::shared_ptr<DataFileMeta>& file) const;

 private:
    /// Range and field id Set.
    struct WriteRange {
        Range range;
        std::unordered_set<int32_t> field_ids;
    };

    explicit RowIdColumnConflictChecker(const std::shared_ptr<SchemaManager>& schema_manager)
        : schema_manager_(schema_manager) {}

    Status BuildWriteRanges(const std::vector<std::shared_ptr<DataFileMeta>>& delta_files);
    Status AddWriteFieldIds(const std::shared_ptr<DataFileMeta>& file,
                            std::unordered_set<int32_t>* field_ids);
    Range MergeRange(const std::vector<std::shared_ptr<DataFileMeta>>& files) const;
    int32_t FirstPossibleRange(const Range& range) const;
    Result<bool> ContainsAnyWriteField(const std::unordered_set<int32_t>& field_ids,
                                       const std::shared_ptr<DataFileMeta>& file) const;
    Result<std::optional<int32_t>> FieldId(const std::shared_ptr<DataFileMeta>& file,
                                           const std::string& write_col) const;
    Result<std::map<std::string, int32_t>> FieldIdByName(int64_t schema_id) const;

 private:
    std::shared_ptr<SchemaManager> schema_manager_;
    std::vector<WriteRange> write_ranges_;
    mutable std::map<int64_t, std::map<std::string, int32_t>> field_id_by_name_cache_;
};

}  // namespace paimon
