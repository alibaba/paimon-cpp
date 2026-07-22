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

#include <string>

namespace paimon {

/// Builds resource paths of the REST catalog server. All path segments are
/// url-encoded; `prefix` (usually pushed down by the server through `/v1/config`) may
/// be empty, in which case it is skipped.
class ResourcePaths {
 public:
    explicit ResourcePaths(const std::string& prefix);

    /// "/v1/config", the only path that does not carry the prefix.
    static std::string Config();

    std::string Databases() const;
    std::string Database(const std::string& database_name) const;
    std::string Tables(const std::string& database_name) const;
    std::string Table(const std::string& database_name, const std::string& table_name) const;
    std::string RenameTable() const;
    std::string Snapshots(const std::string& database_name, const std::string& table_name) const;

 private:
    /// "/v1" or "/v1/{encoded prefix}".
    std::string base_;
};

}  // namespace paimon
