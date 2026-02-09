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
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include "paimon/common/utils/jsonizable.h"
#include "paimon/core/snapshot.h"
#include "paimon/result.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {
class FileSystem;

/// Snapshot with tagCreateTime and tagTimeRetained.
class Tag : public Snapshot {
 public:
    static constexpr char FIELD_TAG_CREATE_TIME[] = "tagCreateTime";
    static constexpr char FIELD_TAG_TIME_RETAINED[] = "tagTimeRetained";

    JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(Tag);

    Tag(const std::optional<int32_t>& version, int64_t id, int64_t schema_id,
        const std::string& base_manifest_list,
        const std::optional<int64_t>& base_manifest_list_size,
        const std::string& delta_manifest_list,
        const std::optional<int64_t>& delta_manifest_list_size,
        const std::optional<std::string>& changelog_manifest_list,
        const std::optional<int64_t>& changelog_manifest_list_size,
        const std::optional<std::string>& index_manifest, const std::string& commit_user,
        int64_t commit_identifier, CommitKind commit_kind, int64_t time_millis,
        const std::optional<std::map<int32_t, int64_t>>& log_offsets,
        const std::optional<int64_t>& total_record_count,
        const std::optional<int64_t>& delta_record_count,
        const std::optional<int64_t>& changelog_record_count,
        const std::optional<int64_t>& watermark, const std::optional<std::string>& statistics,
        const std::optional<std::map<std::string, std::string>>& properties,
        const std::optional<int64_t>& next_row_id,
        const std::optional<std::vector<int64_t>>& tag_create_time,
        const std::optional<double_t>& tag_time_retained);

    bool operator==(const Tag& other) const;
    bool TEST_Equal(const Tag& other) const;

    std::optional<std::vector<int64_t>> TagCreateTime() const {
        return tag_create_time_;
    }

    std::optional<double_t> TagTimeRetained() const {
        return tag_time_retained_;
    }

    Result<Snapshot> TrimToSnapshot() const;

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;

    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    static Result<Tag> FromPath(const std::shared_ptr<FileSystem>& fs, const std::string& path);

 private:
    std::optional<std::vector<int64_t>> tag_create_time_;
    std::optional<double_t> tag_time_retained_;
};
}  // namespace paimon
