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

#include "paimon/catalog/identifier.h"

#include <cstring>
#include <optional>
#include <stdexcept>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon {

const char Identifier::kUnknownDatabase[] = "unknown";
const char Identifier::kSystemTableSplitter[] = "$";
const char Identifier::kSystemBranchPrefix[] = "branch_";
const char Identifier::kDefaultMainBranch[] = "main";

Identifier::Identifier(const std::string& table)
    : Identifier(std::string(kUnknownDatabase), table) {}

Identifier::Identifier(const std::string& database, const std::string& table)
    : database_(database), table_(table) {}

bool Identifier::operator==(const Identifier& other) {
    return (database_ == other.database_ && table_ == other.table_);
}

const std::string& Identifier::GetDatabaseName() const {
    return database_;
}

const std::string& Identifier::GetTableName() const {
    return table_;
}

const std::string& Identifier::GetDataTableName() const {
    SplitTableName();
    return data_table_;
}

const std::optional<std::string>& Identifier::GetBranchName() const {
    SplitTableName();
    return branch_;
}

std::string Identifier::GetBranchNameOrDefault() const {
    const auto& branch = GetBranchName();
    return branch ? branch.value() : std::string(kDefaultMainBranch);
}

const std::optional<std::string>& Identifier::GetSystemTableName() const {
    SplitTableName();
    return system_table_;
}

bool Identifier::IsSystemTable() const {
    return GetSystemTableName().has_value();
}

std::string Identifier::ToString() const {
    return fmt::format("Identifier{{database='{}', table='{}'}}", database_, table_);
}

void Identifier::SplitTableName() const {
    if (parsed_) {
        return;
    }
    std::string data_table;
    std::optional<std::string> branch;
    std::optional<std::string> system_table;
    std::vector<std::string> splits =
        StringUtils::Split(table_, kSystemTableSplitter, /*ignore_empty=*/false);
    if (splits.size() == 1) {
        data_table = table_;
    } else if (splits.size() == 2) {
        data_table = splits[0];
        if (StringUtils::StartsWith(splits[1], kSystemBranchPrefix, /*start_pos=*/0)) {
            branch = splits[1].substr(std::strlen(kSystemBranchPrefix));
        } else {
            system_table = splits[1];
        }
    } else if (splits.size() == 3) {
        if (!StringUtils::StartsWith(splits[1], kSystemBranchPrefix, /*start_pos=*/0)) {
            throw std::invalid_argument(fmt::format(
                "System table can only contain one '$' separator, but this is: {}", table_));
        }
        data_table = splits[0];
        branch = splits[1].substr(std::strlen(kSystemBranchPrefix));
        system_table = splits[2];
    } else {
        throw std::invalid_argument(fmt::format("Invalid table name: {}", table_));
    }
    if (data_table.empty() || (branch && branch->empty()) ||
        (system_table && system_table->empty())) {
        throw std::invalid_argument(fmt::format("Invalid table name: {}", table_));
    }
    data_table_ = std::move(data_table);
    branch_ = std::move(branch);
    system_table_ = std::move(system_table);
    parsed_ = true;
}

}  // namespace paimon
