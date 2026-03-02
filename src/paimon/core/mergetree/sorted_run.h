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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/utils/fields_comparator.h"
namespace paimon {
/// A `SortedRun` is a list of files sorted by their keys. The key intervals [minKey, maxKey]
/// of these files do not overlap.
class SortedRun {
 public:
    static SortedRun FromSingle(const std::shared_ptr<DataFileMeta>& meta) {
        return SortedRun({meta});
    }
    static SortedRun FromSorted(const std::vector<std::shared_ptr<DataFileMeta>>& meta) {
        return SortedRun(meta);
    }
    const std::vector<std::shared_ptr<DataFileMeta>>& Files() const& {
        return files_;
    }

    std::vector<std::shared_ptr<DataFileMeta>>&& Files() && {
        return std::move(files_);
    }

    int64_t TotalSize() const {
        return total_size_;
    }

    bool IsValid(const std::shared_ptr<FieldsComparator>& comparator) const {
        for (size_t i = 1; i < files_.size(); ++i) {
            if (comparator->CompareTo(files_[i]->min_key, files_[i - 1]->max_key) <= 0) {
                return false;
            }
        }
        return true;
    }

    std::string ToString() const {
        std::vector<std::string> files_str;
        files_str.reserve(files_.size());
        for (const auto& file : files_) {
            files_str.push_back(file->ToString());
        }
        return fmt::format("{}", fmt::join(files_str, ", "));
    }

 private:
    explicit SortedRun(const std::vector<std::shared_ptr<DataFileMeta>>& files) : files_(files) {
        for (const auto& file : files) {
            total_size_ += file->file_size;
        }
    }

 private:
    int64_t total_size_ = 0;
    std::vector<std::shared_ptr<DataFileMeta>> files_;
};
}  // namespace paimon
