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
#include <map>
#include <sstream>
#include <string>
#include <utility>

#include "paimon/common/utils/jsonizable.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

class PartitionStatistics : public Jsonizable<PartitionStatistics> {
 public:
    using SpecType = std::map<std::string, std::string>;
    PartitionStatistics(const SpecType& spec, int64_t record_count, int64_t file_size_in_bytes,
                        int64_t file_count, int64_t last_file_creation_time, int32_t total_buckets)
        : spec_(spec),
          record_count_(record_count),
          file_size_in_bytes_(file_size_in_bytes),
          file_count_(file_count),
          last_file_creation_time_(last_file_creation_time),
          total_buckets_(total_buckets) {}

    const SpecType& Spec() const {
        return spec_;
    }
    int64_t RecordCount() const {
        return record_count_;
    }
    int64_t FileSizeInBytes() const {
        return file_size_in_bytes_;
    }
    int64_t FileCount() const {
        return file_count_;
    }
    int64_t LastFileCreationTime() const {
        return last_file_creation_time_;
    }
    int32_t TotalBuckets() const {
        return total_buckets_;
    }

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember(rapidjson::StringRef(FIELD_SPEC),
                      RapidJsonUtil::SerializeValue(spec_, allocator).Move(), *allocator);
        obj.AddMember(rapidjson::StringRef(FIELD_RECORD_COUNT),
                      RapidJsonUtil::SerializeValue(record_count_, allocator).Move(), *allocator);
        obj.AddMember(rapidjson::StringRef(FIELD_FILE_SIZE_IN_BYTES),
                      RapidJsonUtil::SerializeValue(file_size_in_bytes_, allocator).Move(),
                      *allocator);
        obj.AddMember(rapidjson::StringRef(FIELD_FILE_COUNT),
                      RapidJsonUtil::SerializeValue(file_count_, allocator).Move(), *allocator);
        obj.AddMember(rapidjson::StringRef(FIELD_LAST_FILE_CREATION_TIME),
                      RapidJsonUtil::SerializeValue(last_file_creation_time_, allocator).Move(),
                      *allocator);
        obj.AddMember(rapidjson::StringRef(FIELD_TOTAL_BUCKETS),
                      RapidJsonUtil::SerializeValue(total_buckets_, allocator).Move(), *allocator);
        return obj;
    }

    void FromJson(const rapidjson::Value& obj) noexcept(false) override {
        spec_ = RapidJsonUtil::DeserializeKeyValue<SpecType>(obj, FIELD_SPEC);
        record_count_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, FIELD_RECORD_COUNT);
        file_size_in_bytes_ =
            RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, FIELD_FILE_SIZE_IN_BYTES);
        file_count_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, FIELD_FILE_COUNT);
        last_file_creation_time_ =
            RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, FIELD_LAST_FILE_CREATION_TIME);
        total_buckets_ = RapidJsonUtil::DeserializeKeyValue<int32_t>(obj, FIELD_TOTAL_BUCKETS);
    }

    bool operator==(const PartitionStatistics& rhs) const {
        return record_count_ == rhs.record_count_ &&
               file_size_in_bytes_ == rhs.file_size_in_bytes_ && file_count_ == rhs.file_count_ &&
               last_file_creation_time_ == rhs.last_file_creation_time_ &&
               total_buckets_ == rhs.total_buckets_ && spec_ == rhs.spec_;
    }

    std::string ToString() const {
        std::ostringstream oss;
        oss << "{spec={";
        bool first = true;
        for (const auto& kv : spec_) {
            if (!first) oss << ", ";
            first = false;
            oss << kv.first << ":" << kv.second;
        }
        oss << "}, recordCount=" << record_count_ << ", fileSizeInBytes=" << file_size_in_bytes_
            << ", fileCount=" << file_count_
            << ", lastFileCreationTime=" << last_file_creation_time_
            << ", totalBuckets=" << total_buckets_ << "}";
        return oss.str();
    }

 private:
    JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(PartitionStatistics);

 private:
    static constexpr const char* FIELD_SPEC = "spec";
    static constexpr const char* FIELD_RECORD_COUNT = "recordCount";
    static constexpr const char* FIELD_FILE_SIZE_IN_BYTES = "fileSizeInBytes";
    static constexpr const char* FIELD_FILE_COUNT = "fileCount";
    static constexpr const char* FIELD_LAST_FILE_CREATION_TIME = "lastFileCreationTime";
    static constexpr const char* FIELD_TOTAL_BUCKETS = "totalBuckets";

    SpecType spec_;
    int64_t record_count_ = 0;
    int64_t file_size_in_bytes_ = 0;
    int64_t file_count_ = 0;
    int64_t last_file_creation_time_ = 0;
    int32_t total_buckets_ = 0;
};

}  // namespace paimon
