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

#include "paimon/core/table/system/branches_system_table.h"

#include <utility>

#include "arrow/api.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/file_system.h"

namespace paimon {

BranchesSystemTable::BranchesSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                         std::string branch)
    : InMemorySystemTable(table_path),
      context_(SystemTableUtils::CreateContext(std::move(fs), std::move(table_path),
                                               std::move(branch))) {}

std::string BranchesSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> BranchesSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("branch_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("create_time", arrow::timestamp(arrow::TimeUnit::MILLI),
                     /*nullable=*/false),
    });
}

Result<std::vector<GenericRow>> BranchesSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> branches,
                           BranchManager::ListBranches(context_.fs, context_.table_path));
    std::vector<GenericRow> rows;
    rows.reserve(branches.size());

    for (const auto& name : branches) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<FileStatus> branch_status,
            context_.fs->GetFileStatus(BranchManager::BranchPath(context_.table_path, name)));
        GenericRow row(schema->num_fields());
        row.SetField(0, SystemTableUtils::StringValue(name));
        PAIMON_ASSIGN_OR_RAISE(VariantType create_time, SystemTableUtils::LocalTimestampMillisValue(
                                                            branch_status->GetModificationTime()));
        row.SetField(1, create_time);
        rows.push_back(std::move(row));
    }

    return rows;
}

}  // namespace paimon
