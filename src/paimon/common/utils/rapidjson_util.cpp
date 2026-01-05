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

#include "paimon/common/utils/rapidjson_util.h"

namespace paimon {

std::string RapidJsonUtil::MapToJsonString(const std::map<std::string, std::string>& map) {
    rapidjson::Document d;
    d.SetObject();
    rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

    for (const auto& kv : map) {
        d.AddMember(rapidjson::Value(kv.first.c_str(), allocator),
                    rapidjson::Value(kv.second.c_str(), allocator), allocator);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}

Result<std::map<std::string, std::string>> RapidJsonUtil::MapFromJsonString(
    const std::string& json_str) {
    rapidjson::Document doc;
    doc.Parse(json_str.c_str());
    if (doc.HasParseError() || !doc.IsObject()) {
        return Status::Invalid("deserialize failed: parse error or not JSON object: ", json_str);
    }

    std::map<std::string, std::string> result;
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
        if (it->name.IsString() && it->value.IsString()) {
            result[it->name.GetString()] = it->value.GetString();
        }
    }
    return result;
}

}  // namespace paimon
