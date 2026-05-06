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

#include <optional>
#include <string>

#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {

/// An identifier for a table containing database and table name.
class PAIMON_EXPORT Identifier {
 public:
    static const char kUnknownDatabase[];
    static const char kSystemTableSplitter[];
    static const char kSystemBranchPrefix[];
    static const char kDefaultMainBranch[];

    explicit Identifier(const std::string& table);
    Identifier(const std::string& database, const std::string& table);

    bool operator==(const Identifier& other);
    const std::string& GetDatabaseName() const;
    const std::string& GetTableName() const;
    Result<std::string> GetDataTableName() const;
    Result<std::optional<std::string>> GetBranchName() const;
    Result<std::string> GetBranchNameOrDefault() const;
    Result<std::optional<std::string>> GetSystemTableName() const;
    Result<bool> IsSystemTable() const;
    std::string ToString() const;

 private:
    Status SplitTableName() const;

    const std::string database_;
    const std::string table_;
    mutable bool parsed_ = false;
    mutable std::string data_table_;
    mutable std::optional<std::string> branch_;
    mutable std::optional<std::string> system_table_;
};

}  // namespace paimon
