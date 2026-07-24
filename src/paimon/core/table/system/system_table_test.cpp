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

#include "paimon/core/table/system/system_table.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/system/audit_log_system_table.h"
#include "paimon/core/table/system/binlog_system_table.h"
#include "paimon/core/table/system/read_optimized_system_table.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

Result<std::shared_ptr<TableSchema>> CreateTableSchemaForTest(
    const std::map<std::string, std::string>& options) {
    std::shared_ptr<arrow::Schema> arrow_schema = arrow::schema({
        arrow::field("pk", arrow::utf8()),
        arrow::field("v", arrow::int32(), true),
    });
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow_schema,
                            /*partition_keys=*/{}, /*primary_keys=*/{"pk"}, options));
    return std::shared_ptr<TableSchema>(std::move(table_schema));
}

}  // namespace

TEST(SystemTableTest, TestChangelogArrowSchemaReturnsInvalidOptions) {
    std::map<std::string, std::string> options = {
        {Options::TABLE_READ_SEQUENCE_NUMBER_ENABLED, "invalid"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                         CreateTableSchemaForTest(options));

    AuditLogSystemTable audit_log(/*fs=*/nullptr, "/tmp/table", table_schema, options);
    ASSERT_NOK_WITH_MSG(audit_log.ArrowSchema(),
                        "Invalid Config [table-read.sequence-number.enabled: invalid]");

    BinlogSystemTable binlog(/*fs=*/nullptr, "/tmp/table", table_schema, options);
    ASSERT_NOK_WITH_MSG(binlog.ArrowSchema(),
                        "Invalid Config [table-read.sequence-number.enabled: invalid]");
}

TEST(SystemTableTest, TestReadOptimizedSystemTableRegistration) {
    ASSERT_TRUE(SystemTableLoader::IsSupported(ReadOptimizedSystemTable::kName));

    std::map<std::string, std::string> options = {{Options::FILE_SYSTEM, "local"},
                                                  {Options::FILE_FORMAT, "orc"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                         CreateTableSchemaForTest(options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<SystemTable> system_table,
                         SystemTableLoader::Load(ReadOptimizedSystemTable::kName, /*fs=*/nullptr,
                                                 "/tmp/table", table_schema,
                                                 /*dynamic_options=*/{}));
    ASSERT_EQ(system_table->Name(), ReadOptimizedSystemTable::kName);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Schema> arrow_schema, system_table->ArrowSchema());
    ASSERT_EQ(arrow_schema->field_names(), (std::vector<std::string>{"pk", "v"}));
    ASSERT_EQ(arrow_schema->field(0)->type()->id(), arrow::Type::STRING);
    ASSERT_EQ(arrow_schema->field(1)->type()->id(), arrow::Type::INT32);
}

TEST(SystemTableTest, TestReadOptimizedSystemTablePathParsing) {
    ASSERT_OK_AND_ASSIGN(std::optional<SystemTablePath> parsed,
                         SystemTableLoader::TryParsePath("/tmp/db.db/t$branch_audit$ro"));
    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->table_path, "/tmp/db.db/t");
    ASSERT_TRUE(parsed->branch.has_value());
    ASSERT_EQ(parsed->branch.value(), "audit");
    ASSERT_EQ(parsed->system_table_name, ReadOptimizedSystemTable::kName);
}

TEST(SystemTableTest, TestGlobalSystemTableWithoutCatalogReturnsNotImplemented) {
    ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", "/tmp", {}));
    std::shared_ptr<FileSystem> shared_fs(std::move(fs));
    ASSERT_NOK_WITH_MSG(SystemTableLoader::LoadFromPath(shared_fs, "/tmp/warehouse/sys/tables", {}),
                        "global system table requires catalog context: tables");
}

}  // namespace paimon::test
