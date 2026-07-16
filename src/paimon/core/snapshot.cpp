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

#include "paimon/core/snapshot.h"

#include <cassert>
#include <stdexcept>
#include <utility>

#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

const Snapshot::CommitKind Snapshot::CommitKind::Append() {
    static const Snapshot::CommitKind commit_kind = Snapshot::CommitKind(0);
    return commit_kind;
}

const Snapshot::CommitKind Snapshot::CommitKind::Compact() {
    static const Snapshot::CommitKind commit_kind = Snapshot::CommitKind(1);
    return commit_kind;
}

const Snapshot::CommitKind Snapshot::CommitKind::Overwrite() {
    static const Snapshot::CommitKind commit_kind = Snapshot::CommitKind(2);
    return commit_kind;
}

const Snapshot::CommitKind Snapshot::CommitKind::Analyze() {
    static const Snapshot::CommitKind commit_kind = Snapshot::CommitKind(3);
    return commit_kind;
}

const Snapshot::CommitKind Snapshot::CommitKind::Unknown() {
    static const Snapshot::CommitKind commit_kind = Snapshot::CommitKind(-1);
    return commit_kind;
}
bool Snapshot::TEST_Equal(const Snapshot& other) const {
    if (this == &other) {
        return true;
    }

    if ((base_manifest_list_size_ && !other.base_manifest_list_size_) ||
        (!base_manifest_list_size_ && other.base_manifest_list_size_)) {
        return false;
    }
    if ((delta_manifest_list_size_ && !other.delta_manifest_list_size_) ||
        (!delta_manifest_list_size_ && other.delta_manifest_list_size_)) {
        return false;
    }
    if ((changelog_manifest_list_ && !other.changelog_manifest_list_) ||
        (!changelog_manifest_list_ && other.changelog_manifest_list_)) {
        return false;
    }
    if ((changelog_manifest_list_size_ && !other.changelog_manifest_list_size_) ||
        (!changelog_manifest_list_size_ && other.changelog_manifest_list_size_)) {
        return false;
    }

    return version_ == other.version_ && id_ == other.id_ && schema_id_ == other.schema_id_ &&
           index_manifest_ == other.index_manifest_ && commit_user_ == other.commit_user_ &&
           commit_identifier_ == other.commit_identifier_ && commit_kind_ == other.commit_kind_ &&
           total_record_count_ == other.total_record_count_ &&
           delta_record_count_ == other.delta_record_count_ &&
           changelog_record_count_ == other.changelog_record_count_ &&
           watermark_ == other.watermark_ && statistics_ == other.statistics_ &&
           properties_ == other.properties_ && next_row_id_ == other.next_row_id_;
}

bool Snapshot::operator==(const Snapshot& other) const {
    if (this == &other) {
        return true;
    }
    return version_ == other.version_ && id_ == other.id_ && schema_id_ == other.schema_id_ &&
           base_manifest_list_ == other.base_manifest_list_ &&
           base_manifest_list_size_ == other.base_manifest_list_size_ &&
           delta_manifest_list_ == other.delta_manifest_list_ &&
           delta_manifest_list_size_ == other.delta_manifest_list_size_ &&
           changelog_manifest_list_ == other.changelog_manifest_list_ &&
           changelog_manifest_list_size_ == other.changelog_manifest_list_size_ &&
           index_manifest_ == other.index_manifest_ && commit_user_ == other.commit_user_ &&
           commit_identifier_ == other.commit_identifier_ && commit_kind_ == other.commit_kind_ &&
           time_millis_ == other.time_millis_ && total_record_count_ == other.total_record_count_ &&
           delta_record_count_ == other.delta_record_count_ &&
           changelog_record_count_ == other.changelog_record_count_ &&
           watermark_ == other.watermark_ && statistics_ == other.statistics_ &&
           properties_ == other.properties_ && next_row_id_ == other.next_row_id_;
}

std::string Snapshot::CommitKind::ToString(const Snapshot::CommitKind& kind) {
    switch (kind.value_) {
        case 0:
            return "APPEND";
        case 1:
            return "COMPACT";
        case 2:
            return "OVERWRITE";
        case 3:
            return "ANALYZE";
        default:
            assert(false);
            return "UNKNOWN";
    }
}
Snapshot::CommitKind Snapshot::CommitKind::FromString(const std::string& kind) {
    if (kind == "APPEND") {
        return Append();
    } else if (kind == "COMPACT") {
        return Compact();
    } else if (kind == "OVERWRITE") {
        return Overwrite();
    } else if (kind == "ANALYZE") {
        return Analyze();
    }
    assert(false);
    return Unknown();
}

Snapshot::Snapshot(const std::optional<int32_t>& version, int64_t id, int64_t schema_id,
                   const std::string& base_manifest_list,
                   const std::optional<int64_t>& base_manifest_list_size,
                   const std::string& delta_manifest_list,
                   const std::optional<int64_t>& delta_manifest_list_size,
                   const std::optional<std::string>& changelog_manifest_list,
                   const std::optional<int64_t>& changelog_manifest_list_size,
                   const std::optional<std::string>& index_manifest, const std::string& commit_user,
                   int64_t commit_identifier, CommitKind commit_kind, int64_t time_millis,
                   int64_t total_record_count, int64_t delta_record_count,
                   const std::optional<int64_t>& changelog_record_count,
                   const std::optional<int64_t>& watermark,
                   const std::optional<std::string>& statistics,
                   const std::optional<std::map<std::string, std::string>>& properties,
                   const std::optional<int64_t>& next_row_id)
    : version_(version),
      id_(id),
      schema_id_(schema_id),
      base_manifest_list_(base_manifest_list),
      base_manifest_list_size_(base_manifest_list_size),
      delta_manifest_list_(delta_manifest_list),
      delta_manifest_list_size_(delta_manifest_list_size),
      changelog_manifest_list_(changelog_manifest_list),
      changelog_manifest_list_size_(changelog_manifest_list_size),
      index_manifest_(index_manifest),
      commit_user_(commit_user),
      commit_identifier_(commit_identifier),
      commit_kind_(commit_kind),
      time_millis_(time_millis),
      total_record_count_(total_record_count),
      delta_record_count_(delta_record_count),
      changelog_record_count_(changelog_record_count),
      watermark_(watermark),
      statistics_(statistics),
      properties_(properties),
      next_row_id_(next_row_id) {}

rapidjson::Value Snapshot::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(FIELD_VERSION),
                  RapidJsonUtil::SerializeValue(Version(), allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(FIELD_ID),
                  RapidJsonUtil::SerializeValue(id_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(FIELD_SCHEMA_ID),
                  RapidJsonUtil::SerializeValue(schema_id_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(FIELD_BASE_MANIFEST_LIST),
                  RapidJsonUtil::SerializeValue(base_manifest_list_, allocator).Move(), *allocator);
    if (base_manifest_list_size_) {
        obj.AddMember(
            rapidjson::StringRef(FIELD_BASE_MANIFEST_LIST_SIZE),
            RapidJsonUtil::SerializeValue(base_manifest_list_size_.value(), allocator).Move(),
            *allocator);
    }
    obj.AddMember(rapidjson::StringRef(FIELD_DELTA_MANIFEST_LIST),
                  RapidJsonUtil::SerializeValue(delta_manifest_list_, allocator).Move(),
                  *allocator);
    if (delta_manifest_list_size_) {
        obj.AddMember(rapidjson::StringRef(FIELD_DELTA_MANIFEST_LIST_SIZE),
                      RapidJsonUtil::SerializeValue(delta_manifest_list_size_, allocator).Move(),
                      *allocator);
    }
    obj.AddMember(rapidjson::StringRef(FIELD_CHANGELOG_MANIFEST_LIST),
                  RapidJsonUtil::SerializeValue(changelog_manifest_list_, allocator).Move(),
                  *allocator);
    if (changelog_manifest_list_size_) {
        obj.AddMember(
            rapidjson::StringRef(FIELD_CHANGELOG_MANIFEST_LIST_SIZE),
            RapidJsonUtil::SerializeValue(changelog_manifest_list_size_, allocator).Move(),
            *allocator);
    }
    if (index_manifest_ != std::nullopt) {
        obj.AddMember(rapidjson::StringRef(FIELD_INDEX_MANIFEST),
                      RapidJsonUtil::SerializeValue(index_manifest_.value(), allocator).Move(),
                      *allocator);
    }

    obj.AddMember(rapidjson::StringRef(FIELD_COMMIT_USER),
                  RapidJsonUtil::SerializeValue(commit_user_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(FIELD_COMMIT_IDENTIFIER),
                  RapidJsonUtil::SerializeValue(commit_identifier_, allocator).Move(), *allocator);
    obj.AddMember(
        rapidjson::StringRef(FIELD_COMMIT_KIND),
        RapidJsonUtil::SerializeValue(Snapshot::CommitKind::ToString(commit_kind_), allocator)
            .Move(),
        *allocator);

    obj.AddMember(rapidjson::StringRef(FIELD_TIME_MILLIS),
                  RapidJsonUtil::SerializeValue(time_millis_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(FIELD_TOTAL_RECORD_COUNT),
                  RapidJsonUtil::SerializeValue(total_record_count_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(FIELD_DELTA_RECORD_COUNT),
                  RapidJsonUtil::SerializeValue(delta_record_count_, allocator).Move(), *allocator);

    if (changelog_record_count_ != std::nullopt) {
        obj.AddMember(
            rapidjson::StringRef(FIELD_CHANGELOG_RECORD_COUNT),
            RapidJsonUtil::SerializeValue(changelog_record_count_.value(), allocator).Move(),
            *allocator);
    }

    if (watermark_ != std::nullopt) {
        obj.AddMember(rapidjson::StringRef(FIELD_WATERMARK),
                      RapidJsonUtil::SerializeValue(watermark_.value(), allocator).Move(),
                      *allocator);
    }

    if (statistics_ != std::nullopt) {
        obj.AddMember(rapidjson::StringRef(FIELD_STATISTICS),
                      RapidJsonUtil::SerializeValue(statistics_.value(), allocator).Move(),
                      *allocator);
    }
    if (properties_ != std::nullopt) {
        obj.AddMember(rapidjson::StringRef(FIELD_PROPERTIES),
                      RapidJsonUtil::SerializeValue(properties_.value(), allocator).Move(),
                      *allocator);
    }
    if (next_row_id_ != std::nullopt) {
        obj.AddMember(rapidjson::StringRef(FIELD_NEXT_ROW_ID),
                      RapidJsonUtil::SerializeValue(next_row_id_.value(), allocator).Move(),
                      *allocator);
    }

    return obj;
}

void Snapshot::FromJson(const rapidjson::Value& obj) noexcept(false) {
    version_ = RapidJsonUtil::DeserializeKeyValue<int32_t>(obj, FIELD_VERSION, -1);
    id_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, FIELD_ID);
    schema_id_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, FIELD_SCHEMA_ID);
    base_manifest_list_ =
        RapidJsonUtil::DeserializeKeyValue<std::string>(obj, FIELD_BASE_MANIFEST_LIST);
    base_manifest_list_size_ = RapidJsonUtil::DeserializeKeyValue<std::optional<int64_t>>(
        obj, FIELD_BASE_MANIFEST_LIST_SIZE);
    delta_manifest_list_ =
        RapidJsonUtil::DeserializeKeyValue<std::string>(obj, FIELD_DELTA_MANIFEST_LIST);
    delta_manifest_list_size_ = RapidJsonUtil::DeserializeKeyValue<std::optional<int64_t>>(
        obj, FIELD_DELTA_MANIFEST_LIST_SIZE);
    changelog_manifest_list_ = RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(
        obj, FIELD_CHANGELOG_MANIFEST_LIST);
    changelog_manifest_list_size_ = RapidJsonUtil::DeserializeKeyValue<std::optional<int64_t>>(
        obj, FIELD_CHANGELOG_MANIFEST_LIST_SIZE);
    index_manifest_ =
        RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(obj, FIELD_INDEX_MANIFEST);
    commit_user_ = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, FIELD_COMMIT_USER);
    commit_identifier_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, FIELD_COMMIT_IDENTIFIER);
    commit_kind_ = Snapshot::CommitKind::FromString(
        RapidJsonUtil::DeserializeKeyValue<std::string>(obj, FIELD_COMMIT_KIND));
    if (commit_kind_ == Snapshot::CommitKind::Unknown()) {
        throw std::invalid_argument("deserialize CommitKind failed");
    }
    time_millis_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, FIELD_TIME_MILLIS);
    total_record_count_ =
        RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, FIELD_TOTAL_RECORD_COUNT);
    delta_record_count_ =
        RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, FIELD_DELTA_RECORD_COUNT);
    changelog_record_count_ = RapidJsonUtil::DeserializeKeyValue<std::optional<int64_t>>(
        obj, FIELD_CHANGELOG_RECORD_COUNT);
    watermark_ = RapidJsonUtil::DeserializeKeyValue<std::optional<int64_t>>(obj, FIELD_WATERMARK);
    statistics_ =
        RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(obj, FIELD_STATISTICS);
    properties_ =
        RapidJsonUtil::DeserializeKeyValue<std::optional<std::map<std::string, std::string>>>(
            obj, FIELD_PROPERTIES);
    next_row_id_ =
        RapidJsonUtil::DeserializeKeyValue<std::optional<int64_t>>(obj, FIELD_NEXT_ROW_ID);
}

Result<Snapshot> Snapshot::FromPath(const std::shared_ptr<FileSystem>& fs,
                                    const std::string& path) {
    std::string json_str;
    PAIMON_RETURN_NOT_OK(fs->ReadFile(path, &json_str));
    PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, Snapshot::FromJsonString(json_str));
    return snapshot;
}

}  // namespace paimon
