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

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/commit_message.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon {

// Manifest commit message.
class ManifestCommittable {
 public:
    explicit ManifestCommittable(int64_t identifier)
        : ManifestCommittable(identifier, std::nullopt) {}

    ManifestCommittable(int64_t identifier, std::optional<int64_t> watermark)
        : ManifestCommittable(identifier, watermark, {}, {}) {}

    ManifestCommittable(int64_t identifier, std::optional<int64_t> watermark,
                        const std::map<std::string, std::string>& properties,
                        const std::vector<std::shared_ptr<CommitMessage>>& commit_messages)
        : identifier_(identifier),
          watermark_(watermark),
          properties_(properties),
          commit_messages_(commit_messages) {}

    int64_t Identifier() const {
        return identifier_;
    }

    std::optional<int64_t> Watermark() const {
        return watermark_;
    }

    const std::map<std::string, std::string>& Properties() const {
        return properties_;
    }

    const std::vector<std::shared_ptr<CommitMessage>>& FileCommittables() const {
        return commit_messages_;
    }

    void AddFileCommittable(const std::shared_ptr<CommitMessage>& commit_messages) {
        commit_messages_.push_back(commit_messages);
    }

    Result<std::string> ToString() const {
        std::vector<std::string> commit_messages_str;
        commit_messages_str.reserve(commit_messages_.size());
        for (const auto& message : commit_messages_) {
            PAIMON_ASSIGN_OR_RAISE(std::string message_str, CommitMessage::ToDebugString(message));
            commit_messages_str.emplace_back(message_str);
        }
        std::string watermark_str =
            watermark_ == std::nullopt ? "null" : std::to_string(watermark_.value());

        std::vector<std::string> properties_str;
        properties_str.reserve(properties_.size());
        for (const auto& [key, value] : properties_) {
            properties_str.emplace_back(fmt::format("{}: {}", key, value));
        }

        return fmt::format(
            "ManifestCommittable {{identifier = {}, watermark = {}, "
            "commitMessages = {}, properties = {}}}",
            identifier_, watermark_str, fmt::join(commit_messages_str, ", "),
            fmt::join(properties_str, ", "));
    }

 private:
    int64_t identifier_;
    std::optional<int64_t> watermark_;
    std::map<std::string, std::string> properties_;
    std::vector<std::shared_ptr<CommitMessage>> commit_messages_;
};

}  // namespace paimon
