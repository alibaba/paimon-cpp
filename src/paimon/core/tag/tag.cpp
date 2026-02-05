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

#include "paimon/core/tag/tag.h"

#include <cassert>

#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

Tag::Tag(const std::optional<int32_t>& version, const int64_t id, const int64_t schema_id,
         const std::string& base_manifest_list,
         const std::optional<int64_t>& base_manifest_list_size,
         const std::string& delta_manifest_list,
         const std::optional<int64_t>& delta_manifest_list_size,
         const std::optional<std::string>& changelog_manifest_list,
         const std::optional<int64_t>& changelog_manifest_list_size,
         const std::optional<std::string>& index_manifest, const std::string& commit_user,
         const int64_t commit_identifier, const CommitKind commit_kind, const int64_t time_millis,
         const std::optional<std::map<int32_t, int64_t>>& log_offsets,
         const std::optional<int64_t>& total_record_count,
         const std::optional<int64_t>& delta_record_count,
         const std::optional<int64_t>& changelog_record_count,
         const std::optional<int64_t>& watermark, const std::optional<std::string>& statistics,
         const std::optional<std::map<std::string, std::string>>& properties,
         const std::optional<int64_t>& next_row_id,
         const std::optional<std::vector<int64_t>>& tag_create_time,
         const std::optional<double_t>& tag_time_retained)
    : Snapshot(version, id, schema_id, base_manifest_list, base_manifest_list_size,
               delta_manifest_list, delta_manifest_list_size, changelog_manifest_list,
               changelog_manifest_list_size, index_manifest, commit_user, commit_identifier,
               commit_kind, time_millis, log_offsets, total_record_count, delta_record_count,
               changelog_record_count, watermark, statistics, properties, next_row_id),
      tag_create_time_(tag_create_time),
      tag_time_retained_(tag_time_retained) {}

bool Tag::operator==(const Tag& other) const {
    if (this == &other) {
        return true;
    }
    return Snapshot::operator==(other) && tag_create_time_ == other.tag_create_time_ &&
           tag_time_retained_ == other.tag_time_retained_;
}

bool Tag::TEST_Equal(const Tag& other) const {
    if (this == &other) {
        return true;
    }

    return Snapshot::TEST_Equal(other) && tag_create_time_ == other.tag_create_time_ &&
           tag_time_retained_ == other.tag_time_retained_;
}

Result<Snapshot> Tag::TrimToSnapshot() const {
    return Snapshot(Version(), Id(), SchemaId(), BaseManifestList(), BaseManifestListSize(),
                    DeltaManifestList(), DeltaManifestListSize(), ChangelogManifestList(),
                    ChangelogManifestListSize(), IndexManifest(), CommitUser(), CommitIdentifier(),
                    GetCommitKind(), TimeMillis(), LogOffsets(), TotalRecordCount(),
                    DeltaRecordCount(), ChangelogRecordCount(), Watermark(), Statistics(),
                    Properties(), NextRowId());
}

rapidjson::Value Tag::ToJson(rapidjson::Document::AllocatorType* allocator) const noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj = Snapshot::ToJson(allocator);
    if (tag_create_time_ != std::nullopt) {
        obj.AddMember(rapidjson::StringRef(FIELD_TAG_CREATE_TIME),
                      RapidJsonUtil::SerializeValue(tag_create_time_.value(), allocator).Move(),
                      *allocator);
    }
    if (tag_time_retained_ != std::nullopt) {
        obj.AddMember(rapidjson::StringRef(FIELD_TAG_TIME_RETAINED),
                      RapidJsonUtil::SerializeValue(tag_time_retained_.value(), allocator).Move(),
                      *allocator);
    }
    return obj;
}

void Tag::FromJson(const rapidjson::Value& obj) noexcept(false) {
    Snapshot::FromJson(obj);
    tag_create_time_ = RapidJsonUtil::DeserializeKeyValue<std::optional<std::vector<int64_t>>>(
        obj, FIELD_TAG_CREATE_TIME);
    tag_time_retained_ =
        RapidJsonUtil::DeserializeKeyValue<std::optional<double_t>>(obj, FIELD_TAG_TIME_RETAINED);
}

Result<Tag> Tag::FromPath(const std::shared_ptr<FileSystem>& fs, const std::string& path) {
    std::string json_str;
    PAIMON_RETURN_NOT_OK(fs->ReadFile(path, &json_str));
    Tag tag;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::FromJsonString(json_str, &tag));
    return tag;
}
}  // namespace paimon
