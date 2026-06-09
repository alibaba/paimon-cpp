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
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/murmurhash_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

const char Identifier::kUnknownDatabase[] = "unknown";
const char Identifier::kSystemTableSplitter[] = "$";
const char Identifier::kSystemBranchPrefix[] = "branch_";
const char Identifier::kDefaultMainBranch[] = "main";

Identifier::Identifier(const std::string& table)
    : Identifier(std::string(kUnknownDatabase), table) {}

Identifier::Identifier(const std::string& database, const std::string& table)
    : database_(database), table_(table) {}

bool Identifier::operator==(const Identifier& other) const {
    if (this == &other) {
        return true;
    }
    return database_ == other.database_ && table_ == other.table_;
}

const std::string& Identifier::GetDatabaseName() const {
    return database_;
}

const std::string& Identifier::GetTableName() const {
    return table_;
}

Result<std::string> Identifier::GetDataTableName() const {
    PAIMON_RETURN_NOT_OK(SplitTableName());
    return data_table_;
}

Result<std::optional<std::string>> Identifier::GetBranchName() const {
    PAIMON_RETURN_NOT_OK(SplitTableName());
    return branch_;
}

Result<std::string> Identifier::GetBranchNameOrDefault() const {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> branch, GetBranchName());
    return branch.value_or(kDefaultMainBranch);
}

Result<std::optional<std::string>> Identifier::GetSystemTableName() const {
    PAIMON_RETURN_NOT_OK(SplitTableName());
    return system_table_;
}

Result<bool> Identifier::IsSystemTable() const {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> system_table, GetSystemTableName());
    return system_table.has_value();
}

std::string Identifier::ToString() const {
    return fmt::format("Identifier{{database='{}', table='{}'}}", database_, table_);
}

int32_t Identifier::HashCode() const {
    int32_t hash = MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<const void*>(database_.data()),
                                                    /*offset=*/0, database_.size());
    return MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<const void*>(table_.data()),
                                            /*offset=*/0, table_.size(), hash);
}

std::string Identifier::GetFullName() const {
    if (database_ == kUnknownDatabase) {
        return table_;
    }
    return fmt::format("{}.{}", database_, table_);
}

Result<Identifier> Identifier::FromString(const std::string& full_name) {
    if (StringUtils::IsNullOrWhitespaceOnly(full_name)) {
        return Status::Invalid("full name cannot be empty or whitespace only");
    }
    // TODO(lisizhuo.lsz): deal with kUnknownDatabase to be done
    const auto dot_pos = full_name.find('.');
    if (dot_pos == std::string::npos || dot_pos == 0 || dot_pos == full_name.size() - 1) {
        return Status::Invalid(
            fmt::format("cannot get splits from '{}' to get database and table", full_name));
    }
    std::string database = full_name.substr(0, dot_pos);
    std::string table = full_name.substr(dot_pos + 1);
    return Identifier(database, table);
}

Status Identifier::SplitTableName() const {
    if (parsed_) {
        return Status::OK();
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
            return Status::Invalid(fmt::format(
                "System table can only contain one '$' separator, but this is: {}", table_));
        }
        data_table = splits[0];
        branch = splits[1].substr(std::strlen(kSystemBranchPrefix));
        system_table = splits[2];
    } else {
        return Status::Invalid(fmt::format("Invalid table name: {}", table_));
    }
    if (data_table.empty() || (branch && branch->empty()) ||
        (system_table && system_table->empty())) {
        return Status::Invalid(fmt::format("Invalid table name: {}", table_));
    }
    data_table_ = std::move(data_table);
    branch_ = std::move(branch);
    system_table_ = std::move(system_table);
    parsed_ = true;
    return Status::OK();
}

}  // namespace paimon
