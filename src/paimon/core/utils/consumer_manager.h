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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/result.h"

namespace paimon {

class FileSystem;

/// Manager for table streaming consumer metadata files.
///
/// Consumers are stored under the selected table branch as `consumer/consumer-*` files.
class ConsumerManager {
 public:
    /// File name prefix for persisted consumer state files.
    static constexpr char kConsumerPrefix[] = "consumer-";

    ConsumerManager(std::shared_ptr<FileSystem> fs, std::string table_path, std::string branch);

    /// Returns the consumer metadata directory for the selected table branch.
    std::string ConsumerDirectory() const;

    /// Returns the metadata file path for a specific consumer id.
    std::string ConsumerPath(const std::string& consumer_id) const;

    /// Lists consumer ids found in the consumer metadata directory.
    Result<std::vector<std::string>> ListConsumers() const;

    /// Reads the next snapshot id persisted for the given consumer id.
    Result<std::optional<int64_t>> GetNextSnapshotId(const std::string& consumer_id) const;

    /// Reads all consumers and their next snapshot ids.
    Result<std::map<std::string, int64_t>> Consumers() const;

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string table_path_;
    std::string branch_;
};

}  // namespace paimon
