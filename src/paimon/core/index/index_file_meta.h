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

#include <memory>
#include <string>

#include "arrow/api.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/global_index_meta.h"

namespace paimon {
/// Metadata of index file.
class IndexFileMeta {
 public:
    static constexpr int32_t NUM_FIELDS = 7;

    IndexFileMeta(const std::string& index_type, const std::string& file_name, int64_t file_size,
                  int64_t row_count,
                  const std::optional<LinkedHashMap<std::string, DeletionVectorMeta>>& dv_ranges,
                  const std::optional<std::string>& external_path)
        : IndexFileMeta(index_type, file_name, file_size, row_count, dv_ranges, external_path,
                        /*global_index_meta=*/std::nullopt) {}

    IndexFileMeta(const std::string& index_type, const std::string& file_name, int64_t file_size,
                  int64_t row_count,
                  const std::optional<LinkedHashMap<std::string, DeletionVectorMeta>>& dv_ranges,
                  const std::optional<std::string>& external_path,
                  const std::optional<GlobalIndexMeta>& global_index_meta)
        : index_type_(index_type),
          file_name_(file_name),
          file_size_(file_size),
          row_count_(row_count),
          dv_ranges_(dv_ranges),
          external_path_(external_path),
          global_index_meta_(global_index_meta) {}

    const std::string& IndexType() const {
        return index_type_;
    }

    const std::string& FileName() const {
        return file_name_;
    }

    int64_t FileSize() const {
        return file_size_;
    }

    int64_t RowCount() const {
        return row_count_;
    }

    const std::optional<LinkedHashMap<std::string, DeletionVectorMeta>>& DvRanges() const {
        return dv_ranges_;
    }

    const std::optional<std::string>& ExternalPath() const {
        return external_path_;
    }

    const std::optional<GlobalIndexMeta>& GetGlobalIndexMeta() const {
        return global_index_meta_;
    }

    bool operator==(const IndexFileMeta& other) const {
        if (this == &other) {
            return true;
        }
        return index_type_ == other.index_type_ && file_name_ == other.file_name_ &&
               file_size_ == other.file_size_ && row_count_ == other.row_count_ &&
               dv_ranges_ == other.dv_ranges_ && external_path_ == other.external_path_ &&
               global_index_meta_ == other.global_index_meta_;
    }

    bool TEST_Equal(const IndexFileMeta& other) const {
        if (this == &other) {
            return true;
        }

        if ((dv_ranges_ && !other.dv_ranges_) || (!dv_ranges_ && other.dv_ranges_)) {
            return false;
        }
        if (dv_ranges_ && other.dv_ranges_) {
            if (dv_ranges_.value().size() != other.dv_ranges_.value().size()) {
                return false;
            }
            for (auto iter1 = dv_ranges_.value().begin(), iter2 = other.dv_ranges_.value().begin();
                 iter1 != dv_ranges_.value().end() && iter2 != other.dv_ranges_.value().end();
                 ++iter1, ++iter2) {
                if (!iter1->second.TEST_Equal(iter2->second)) {
                    return false;
                }
            }
        }

        if ((external_path_ && !other.external_path_) ||
            (!external_path_ && other.external_path_)) {
            return false;
        }

        // ignore file_name & file_size
        return index_type_ == other.index_type_ && row_count_ == other.row_count_ &&
               global_index_meta_ == other.global_index_meta_;
    }

    std::string ToString() const {
        std::string dv_str = dv_ranges_ == std::nullopt ? "null" : Format(dv_ranges_.value());
        std::string external_path_str =
            external_path_ == std::nullopt ? "null" : external_path_.value();
        std::string global_index_meta_str =
            global_index_meta_ == std::nullopt ? "null" : global_index_meta_.value().ToString();
        return fmt::format(
            "IndexManifestEntry{{indexType={}, fileName={}, fileSize={}, rowCount={}, "
            "dvRanges={}, externalPath={}, globalIndexMeta={}}}",
            index_type_, file_name_, file_size_, row_count_, dv_str, external_path_str,
            global_index_meta_str);
    }

    static const std::shared_ptr<arrow::DataType>& DataType() {
        static std::shared_ptr<arrow::DataType> schema = arrow::struct_({
            arrow::field("_INDEX_TYPE", arrow::utf8(), false),
            arrow::field("_FILE_NAME", arrow::utf8(), false),
            arrow::field("_FILE_SIZE", arrow::int64(), false),
            arrow::field("_ROW_COUNT", arrow::int64(), false),
            arrow::field("_DELETIONS_VECTORS_RANGES",
                         arrow::list(arrow::field("item", DeletionVectorMeta::DataType(), true)),
                         true),
            arrow::field("_EXTERNAL_PATH", arrow::utf8(), true),
            arrow::field("_GLOBAL_INDEX", GlobalIndexMeta::DataType(), true),
        });
        return schema;
    }

 private:
    static std::string Format(const LinkedHashMap<std::string, DeletionVectorMeta>& val) {
        std::string result = "{";
        for (const auto& iter : val) {
            result.append(fmt::format("{}: {};", iter.first, iter.second.ToString()));
        }
        if (!val.empty()) {
            result.pop_back();
        }
        result.append("}");
        return result;
    }

 private:
    std::string index_type_;
    std::string file_name_;
    int64_t file_size_ = 0;
    int64_t row_count_ = 0;

    /// Metadata only used by `DeletionVectorsIndexFile`, use LinkedHashMap to ensure that the
    /// order of DeletionVectorRanges and the written DeletionVectors is consistent.
    std::optional<LinkedHashMap<std::string, DeletionVectorMeta>> dv_ranges_;

    std::optional<std::string> external_path_;

    std::optional<GlobalIndexMeta> global_index_meta_;
};

}  // namespace paimon
