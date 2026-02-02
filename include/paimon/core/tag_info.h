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
#include "paimon/type_fwd.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

class TagInfo : public Jsonizable<TagInfo> {
 public:
    static constexpr char FIELD_TAG_NAME[] = "tagName";
    static constexpr char FIELD_SNAPSHOT[] = "snapshot";
    static constexpr char FIELD_TAG_CREATE_TIME[] = "tagCreateTime";
    static constexpr char FIELD_TAG_TIME_RETAINED[] = "tagTimeRetained";

    JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(TagInfo);

    TagInfo(const std::string& tag_name, const std::shared_ptr<Snapshot>& snapshot,
            const std::optional<int64_t>& tag_create_time,
            const std::optional<std::string>& tag_time_retained)
        : tag_name_(tag_name),
          snapshot_(snapshot),
          tag_create_time_(tag_create_time),
          tag_time_retained_(tag_time_retained) {}

    const std::string& TagName() const {
        return tag_name_;
    }
    void SetTagName(const std::string& tag_name) {
        tag_name_ = tag_name;
    }

    const std::shared_ptr<Snapshot>& GetSnapshot() const {
        return snapshot_;
    }
    void SetSnapshot(const std::shared_ptr<Snapshot>& snapshot) {
        snapshot_ = snapshot;
    }

    const std::optional<int64_t>& TagCreateTime() const {
        return tag_create_time_;
    }
    void SetTagCreateTime(const std::optional<int64_t>& tag_create_time) {
        tag_create_time_ = tag_create_time;
    }

    const std::optional<std::string>& TagTimeRetained() const {
        return tag_time_retained_;
    }
    void SetTagTimeRetained(const std::optional<std::string>& tag_time_retained) {
        tag_time_retained_ = tag_time_retained;
    }

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;

    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

 private:
    std::string tag_name_;
    std::shared_ptr<Snapshot> snapshot_;
    std::optional<int64_t> tag_create_time_;
    std::optional<std::string> tag_time_retained_;
};

}  // namespace paimon
