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

#include "paimon/rest/resource_paths.h"

#include "paimon/common/utils/url_utils.h"

namespace paimon {

namespace {
constexpr const char kV1[] = "/v1";
}  // namespace

ResourcePaths::ResourcePaths(const std::string& prefix) {
    base_ = kV1;
    if (!prefix.empty()) {
        base_ += "/" + UrlUtils::EncodeString(prefix);
    }
}

std::string ResourcePaths::Config() {
    return std::string(kV1) + "/config";
}

std::string ResourcePaths::Databases() const {
    return base_ + "/databases";
}

std::string ResourcePaths::Database(const std::string& database_name) const {
    return Databases() + "/" + UrlUtils::EncodeString(database_name);
}

std::string ResourcePaths::Tables(const std::string& database_name) const {
    return Database(database_name) + "/tables";
}

std::string ResourcePaths::Table(const std::string& database_name,
                                 const std::string& table_name) const {
    return Tables(database_name) + "/" + UrlUtils::EncodeString(table_name);
}

std::string ResourcePaths::RenameTable() const {
    return base_ + "/tables/rename";
}

std::string ResourcePaths::Snapshots(const std::string& database_name,
                                     const std::string& table_name) const {
    return Table(database_name, table_name) + "/snapshots";
}

}  // namespace paimon
