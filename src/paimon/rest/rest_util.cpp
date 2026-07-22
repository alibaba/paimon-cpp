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

#include "paimon/rest/rest_util.h"

#include <stdexcept>

#include "fmt/format.h"
#include "rapidjson/error/en.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon {

std::map<std::string, std::string> RestUtil::ExtractPrefixMap(
    const std::map<std::string, std::string>& options, const std::string& prefix) {
    std::map<std::string, std::string> result;
    for (const auto& [key, value] : options) {
        if (key.size() > prefix.size() && key.compare(0, prefix.size(), prefix) == 0) {
            result[key.substr(prefix.size())] = value;
        }
    }
    return result;
}

std::string RestUtil::ExtractRequestId(const std::map<std::string, std::string>& headers) {
    auto iter = headers.find(kRequestIdHeader);
    if (iter != headers.end() && !iter->second.empty()) {
        return iter->second;
    }
    for (const auto& [name, value] : headers) {
        if (!value.empty() && name.find("request-id") != std::string::npos) {
            return value;
        }
    }
    return kUnknownRequestId;
}

std::string RestUtil::JsonToString(const rapidjson::Value& value) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return buffer.GetString();
}

rapidjson::Value RestUtil::ParseToValue(const std::string& json,
                                        rapidjson::Document::AllocatorType* allocator) {
    rapidjson::Document doc;
    doc.Parse(json.c_str());
    if (doc.HasParseError()) {
        // The payload may carry credentials and be arbitrarily large; report only the error.
        throw std::invalid_argument(fmt::format("invalid json: {} (at offset {})",
                                                rapidjson::GetParseError_En(doc.GetParseError()),
                                                doc.GetErrorOffset()));
    }
    rapidjson::Value value;
    value.CopyFrom(doc, *allocator);
    return value;
}

}  // namespace paimon
