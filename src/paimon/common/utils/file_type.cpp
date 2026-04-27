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

#include "paimon/common/utils/file_type.h"

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon {

namespace {

constexpr char SNAPSHOT_PREFIX[] = "snapshot-";
constexpr char SCHEMA_PREFIX[] = "schema-";
constexpr char STATISTICS_PREFIX[] = "stat-";
constexpr char TAG_PREFIX[] = "tag-";
constexpr char CONSUMER_PREFIX[] = "consumer-";
constexpr char SERVICE_PREFIX[] = "service-";
constexpr char INDEX_PATH_SUFFIX[] = ".index";
constexpr char INDEX_PREFIX[] = "index-";
constexpr char CHANGELOG_PREFIX[] = "changelog-";

constexpr char MANIFEST[] = "manifest";
constexpr char CHANGELOG_DIR[] = "changelog";
constexpr char GLOBAL_INDEX_INFIX[] = "global-index-";
constexpr char TEMP_FILE_SUFFIX[] = ".tmp";

std::string UnwrapTempFileName(const std::string& name) {
    // format: .{originalName}.{UUID}.tmp
    // suffix ".{UUID}.tmp" is 41 chars: 1(dot) + 36(UUID) + 4(.tmp)
    // minimum total: 1(leading dot) + 1(name) + 41(suffix) = 43
    if (name.size() < 43 || name[0] != '.' || !StringUtils::EndsWith(name, TEMP_FILE_SUFFIX)) {
        return name;
    }

    size_t dot_before_uuid = name.size() - 41;
    if (name[dot_before_uuid] != '.') {
        return name;
    }

    return name.substr(1, dot_before_uuid - 1);
}

}  // namespace

bool FileTypeUtils::IsIndex(FileType file_type) {
    return file_type == FileType::kBucketIndex || file_type == FileType::kGlobalIndex ||
           file_type == FileType::kFileIndex;
}

std::string FileTypeUtils::ToString(FileType file_type) {
    switch (file_type) {
        case FileType::kMeta:
            return "meta";
        case FileType::kData:
            return "data";
        case FileType::kBucketIndex:
            return "bucket_index";
        case FileType::kGlobalIndex:
            return "global_index";
        case FileType::kFileIndex:
            return "file_index";
    }
    return "data";
}

FileType FileTypeUtils::Classify(const std::string& file_path) {
    std::string name = PathUtil::GetName(file_path);
    name = UnwrapTempFileName(name);

    if (StringUtils::StartsWith(name, SNAPSHOT_PREFIX) ||
        StringUtils::StartsWith(name, SCHEMA_PREFIX) ||
        StringUtils::StartsWith(name, STATISTICS_PREFIX) ||
        StringUtils::StartsWith(name, TAG_PREFIX) ||
        StringUtils::StartsWith(name, CONSUMER_PREFIX) ||
        StringUtils::StartsWith(name, SERVICE_PREFIX)) {
        return FileType::kMeta;
    }

    if (StringUtils::EndsWith(name, INDEX_PATH_SUFFIX)) {
        if (name.find(GLOBAL_INDEX_INFIX) != std::string::npos) {
            return FileType::kGlobalIndex;
        }
        return FileType::kFileIndex;
    }

    if (name.find(MANIFEST) != std::string::npos) {
        return FileType::kMeta;
    }

    if (StringUtils::StartsWith(name, INDEX_PREFIX)) {
        return FileType::kBucketIndex;
    }

    if (name == "EARLIEST" || name == "LATEST") {
        return FileType::kMeta;
    }

    if (StringUtils::EndsWith(name, "_SUCCESS")) {
        return FileType::kMeta;
    }

    const std::string parent = PathUtil::GetParentDirPath(file_path);
    if (StringUtils::StartsWith(name, CHANGELOG_PREFIX) &&
        PathUtil::GetName(parent) == CHANGELOG_DIR) {
        return FileType::kMeta;
    }

    return FileType::kData;
}

}  // namespace paimon
