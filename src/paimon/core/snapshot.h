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
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include "paimon/common/utils/jsonizable.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {
class FileSystem;

// This file is the entrance to all data committed at some specific time point.
class Snapshot : public Jsonizable<Snapshot> {
 public:
    class CommitKind {
     public:
        explicit CommitKind(int8_t kind) : value_(kind) {}
        /// Changes flushed from the mem table.
        static const CommitKind Append();

        /// Changes by compacting existing data files.
        static const CommitKind Compact();

        /// Changes that clear up the whole partition and then add new records.
        static const CommitKind Overwrite();

        /// Collect statistics.
        static const CommitKind Analyze();

        static const CommitKind Unknown();

        bool operator==(const CommitKind& other) const {
            return value_ == other.value_;
        }
        static std::string ToString(const CommitKind& kind);
        static CommitKind FromString(const std::string& kind);

     private:
        int8_t value_;
    };

    static constexpr char FIELD_VERSION[] = "version";
    static constexpr char FIELD_ID[] = "id";
    static constexpr char FIELD_SCHEMA_ID[] = "schemaId";
    static constexpr char FIELD_BASE_MANIFEST_LIST[] = "baseManifestList";
    static constexpr char FIELD_BASE_MANIFEST_LIST_SIZE[] = "baseManifestListSize";
    static constexpr char FIELD_DELTA_MANIFEST_LIST[] = "deltaManifestList";
    static constexpr char FIELD_DELTA_MANIFEST_LIST_SIZE[] = "deltaManifestListSize";
    static constexpr char FIELD_CHANGELOG_MANIFEST_LIST[] = "changelogManifestList";
    static constexpr char FIELD_CHANGELOG_MANIFEST_LIST_SIZE[] = "changelogManifestListSize";
    static constexpr char FIELD_INDEX_MANIFEST[] = "indexManifest";
    static constexpr char FIELD_COMMIT_USER[] = "commitUser";
    static constexpr char FIELD_COMMIT_IDENTIFIER[] = "commitIdentifier";
    static constexpr char FIELD_COMMIT_KIND[] = "commitKind";
    static constexpr char FIELD_TIME_MILLIS[] = "timeMillis";
    static constexpr char FIELD_LOG_OFFSETS[] = "logOffsets";
    static constexpr char FIELD_TOTAL_RECORD_COUNT[] = "totalRecordCount";
    static constexpr char FIELD_DELTA_RECORD_COUNT[] = "deltaRecordCount";
    static constexpr char FIELD_CHANGELOG_RECORD_COUNT[] = "changelogRecordCount";
    static constexpr char FIELD_WATERMARK[] = "watermark";
    static constexpr char FIELD_STATISTICS[] = "statistics";
    static constexpr char FIELD_PROPERTIES[] = "properties";
    static constexpr char FIELD_NEXT_ROW_ID[] = "nextRowId";

    JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(Snapshot);

    Snapshot(int64_t id, int64_t schema_id, const std::string& base_manifest_list,
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
             const std::optional<int64_t>& next_row_id)
        : Snapshot(CURRENT_VERSION, id, schema_id, base_manifest_list, base_manifest_list_size,
                   delta_manifest_list, delta_manifest_list_size, changelog_manifest_list,
                   changelog_manifest_list_size, index_manifest, commit_user, commit_identifier,
                   commit_kind, time_millis, log_offsets, total_record_count, delta_record_count,
                   changelog_record_count, watermark, statistics, properties, next_row_id) {}

    Snapshot(const std::optional<int32_t>& version, int64_t id, int64_t schema_id,
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
             const std::optional<int64_t>& next_row_id);

    bool operator==(const Snapshot& other) const;
    bool TEST_Equal(const Snapshot& other) const;

 public:
    static constexpr int64_t FIRST_SNAPSHOT_ID = 1;
    static constexpr int32_t TABLE_STORE_02_VERSION = 1;
    static constexpr int32_t CURRENT_VERSION = 3;

 public:
    int32_t Version() const {
        // there is no version field for paimon <= 0.2
        return version_ == std::nullopt ? TABLE_STORE_02_VERSION : version_.value();
    }

    int64_t Id() const {
        return id_;
    }

    int64_t SchemaId() const {
        return schema_id_;
    }

    const std::string& BaseManifestList() const {
        return base_manifest_list_;
    }

    const std::optional<int64_t>& BaseManifestListSize() const {
        return base_manifest_list_size_;
    }

    const std::string& DeltaManifestList() const {
        return delta_manifest_list_;
    }

    const std::optional<int64_t>& DeltaManifestListSize() const {
        return delta_manifest_list_size_;
    }

    const std::optional<std::string>& ChangelogManifestList() const {
        return changelog_manifest_list_;
    }

    const std::optional<int64_t>& ChangelogManifestListSize() const {
        return changelog_manifest_list_size_;
    }

    const std::optional<std::string>& IndexManifest() const {
        return index_manifest_;
    }

    const std::string& CommitUser() const {
        return commit_user_;
    }

    int64_t CommitIdentifier() const {
        return commit_identifier_;
    }

    CommitKind GetCommitKind() const {
        return commit_kind_;
    }

    int64_t TimeMillis() const {
        return time_millis_;
    }

    const std::optional<std::map<int32_t, int64_t>>& LogOffsets() const {
        return log_offsets_;
    }

    const std::optional<int64_t>& TotalRecordCount() const {
        return total_record_count_;
    }

    const std::optional<int64_t>& DeltaRecordCount() const {
        return delta_record_count_;
    }

    const std::optional<int64_t>& ChangelogRecordCount() const {
        return changelog_record_count_;
    }

    const std::optional<int64_t>& Watermark() const {
        return watermark_;
    }

    const std::optional<std::string>& Statistics() const {
        return statistics_;
    }

    const std::optional<std::map<std::string, std::string>>& Properties() const {
        return properties_;
    }

    const std::optional<int64_t>& NextRowId() const {
        return next_row_id_;
    }

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;

    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    static Result<Snapshot> FromPath(const std::shared_ptr<FileSystem>& fs,
                                     const std::string& path);

 private:
    // version of snapshot
    // null for paimon <= 0.2
    std::optional<int32_t> version_;
    int64_t id_ = -1;
    int64_t schema_id_ = -1;

    // a manifest list recording all changes from the previous snapshots
    std::string base_manifest_list_;
    std::optional<int64_t> base_manifest_list_size_;

    // a manifest list recording all new changes occurred in this snapshot
    // for faster expire and streaming reads
    std::string delta_manifest_list_;
    std::optional<int64_t> delta_manifest_list_size_;

    // a manifest list recording all changelog produced in this snapshot
    // null if no changelog is produced, or for paimon <= 0.2
    std::optional<std::string> changelog_manifest_list_;
    std::optional<int64_t> changelog_manifest_list_size_;

    // a manifest recording all index files of this table
    // null if no index file
    std::optional<std::string> index_manifest_;

    std::string commit_user_;

    // Mainly for snapshot deduplication.
    //
    // If multiple snapshots have the same commitIdentifier, reading from any of these
    // snapshots must produce the same table.
    //
    // If snapshot A has a smaller commitIdentifier than snapshot B, then snapshot A must
    // be committed before snapshot B, and thus snapshot A must contain older records than
    // snapshot B.
    int64_t commit_identifier_ = std::numeric_limits<int64_t>::min();

    CommitKind commit_kind_ = CommitKind::Unknown();

    int64_t time_millis_;

    std::optional<std::map<int32_t, int64_t>> log_offsets_;

    // record count of all changes occurred in this snapshot
    // null for paimon <= 0.3
    std::optional<int64_t> total_record_count_;

    // record count of all new changes occurred in this snapshot
    // null for paimon <= 0.3
    std::optional<int64_t> delta_record_count_;

    // record count of all changelog produced in this snapshot
    // null for paimon <= 0.3
    std::optional<int64_t> changelog_record_count_;

    // watermark for input records
    // null for paimon <= 0.3
    // null if there is no watermark in new committing, and the previous snapshot does not
    // Have a watermark
    std::optional<int64_t> watermark_;

    // stats file name for statistics of this table
    // null if no stats file
    std::optional<std::string> statistics_;

    // properties
    // null for paimon <= 1.1 or empty properties
    std::optional<std::map<std::string, std::string>> properties_;

    std::optional<int64_t> next_row_id_;
};

}  // namespace paimon
