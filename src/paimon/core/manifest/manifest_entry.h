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
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_entry.h"
#include "paimon/core/manifest/file_kind.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
/// Entry of a manifest file, representing an addition / deletion of a data file.
class ManifestEntry : public FileEntry {
 public:
    static const std::shared_ptr<arrow::DataType>& DataType();
    static int64_t RecordCountAdd(const std::vector<ManifestEntry>& manifest_entries);
    static int64_t RecordCountDelete(const std::vector<ManifestEntry>& manifest_entries);

    ManifestEntry(const FileKind& kind, const BinaryRow& partition, int32_t bucket,
                  int32_t total_buckets, const std::shared_ptr<DataFileMeta>& file)
        : kind_(kind),
          partition_(partition),
          bucket_(bucket),
          total_buckets_(total_buckets),
          file_(file) {}

    ManifestEntry(const ManifestEntry& other) noexcept {
        *this = other;
    }

    ManifestEntry& operator=(const ManifestEntry& other) noexcept {
        if (&other == this) {
            return *this;
        }
        kind_ = other.kind_;
        partition_ = other.partition_;
        bucket_ = other.bucket_;
        total_buckets_ = other.total_buckets_;
        file_ = other.file_;
        return *this;
    }

    ManifestEntry(ManifestEntry&& other) noexcept {
        *this = std::move(other);
    }

    ManifestEntry& operator=(ManifestEntry&& other) noexcept {
        if (&other == this) {
            return *this;
        }
        kind_ = other.kind_;
        partition_ = other.partition_;
        bucket_ = other.bucket_;
        total_buckets_ = other.total_buckets_;
        file_ = std::move(other.file_);
        return *this;
    }

    const FileKind& Kind() const override {
        return kind_;
    }

    const BinaryRow& Partition() const override {
        return partition_;
    }

    int32_t Bucket() const override {
        return bucket_;
    }

    int32_t Level() const override {
        return file_->level;
    }

    const std::string& FileName() const override {
        return file_->file_name;
    }

    const std::optional<std::string>& ExternalPath() const override {
        return file_->external_path;
    }

    const BinaryRow& MinKey() const override {
        return file_->min_key;
    }

    const BinaryRow& MaxKey() const override {
        return file_->max_key;
    }

    FileEntry::Identifier CreateIdentifier() const override {
        return FileEntry::Identifier(partition_, bucket_, file_->level, file_->file_name,
                                     file_->external_path);
    }

    int32_t TotalBuckets() const {
        return total_buckets_;
    }

    const std::shared_ptr<DataFileMeta>& File() const {
        return file_;
    }

    bool operator==(const ManifestEntry& other) const {
        if (this == &other) {
            return true;
        }
        return kind_ == other.kind_ && partition_ == other.partition_ && bucket_ == other.bucket_ &&
               total_buckets_ == other.total_buckets_ && *file_ == *(other.file_);
    }

    std::string ToString() const {
        return fmt::format("{{{}, {}, {}, {}, {}}}", static_cast<int32_t>(kind_.ToByteValue()),
                           partition_.ToString(), bucket_, total_buckets_, file_->ToString());
    }

    void AssignSequenceNumber(int64_t min_sequence_number, int64_t max_sequence_number) {
        file_->AssignSequenceNumber(min_sequence_number, max_sequence_number);
    }

    void AssignFirstRowId(int64_t first_row_id) {
        file_->AssignFirstRowId(first_row_id);
    }

 private:
    FileKind kind_;
    // for tables without partition this field should be a row with 0 columns (not null)
    BinaryRow partition_;
    int32_t bucket_;
    int32_t total_buckets_;
    std::shared_ptr<DataFileMeta> file_;
};

}  // namespace paimon
