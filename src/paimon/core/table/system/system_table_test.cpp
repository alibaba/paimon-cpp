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

#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/system/audit_log_system_table.h"
#include "paimon/core/table/system/binlog_system_table.h"
#include "paimon/core/table/system/read_optimized_system_table.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/reader/batch_reader.h"
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

class ArrayBatchReader : public BatchReader {
 public:
    explicit ArrayBatchReader(arrow::ArrayVector batches) : batches_(std::move(batches)) {}

    Result<ReadBatch> NextBatch() override {
        if (next_ == batches_.size()) {
            return BatchReader::MakeEofBatch();
        }
        auto c_array = std::make_unique<ArrowArray>();
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportArray(*batches_[next_++], c_array.get(), c_schema.get()));
        return std::make_pair(std::move(c_array), std::move(c_schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return nullptr;
    }

    void Close() override {}

 private:
    arrow::ArrayVector batches_;
    size_t next_ = 0;
};

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

TEST(SystemTableTest, TestBinlogArrowSchemaWithSequenceNumber) {
    std::map<std::string, std::string> options = {
        {Options::TABLE_READ_SEQUENCE_NUMBER_ENABLED, "true"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                         CreateTableSchemaForTest(options));

    BinlogSystemTable binlog(/*fs=*/nullptr, "/tmp/table", table_schema, options);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Schema> schema, binlog.ArrowSchema());

    ASSERT_EQ(schema->field_names(),
              (std::vector<std::string>{"rowkind", "_SEQUENCE_NUMBER", "pk", "v"}));
    ASSERT_EQ(schema->field(0)->type()->id(), arrow::Type::STRING);
    ASSERT_FALSE(schema->field(0)->nullable());
    ASSERT_EQ(schema->field(1)->type()->id(), arrow::Type::INT64);
    ASSERT_EQ(schema->field(2)->type()->id(), arrow::Type::LIST);
    ASSERT_EQ(schema->field(3)->type()->id(), arrow::Type::LIST);
}

TEST(SystemTableTest, TestStreamingBinlogPacksUpdateAcrossBatches) {
    std::shared_ptr<arrow::DataType> input_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("pk", arrow::utf8()),
        arrow::field("v", arrow::int32()),
    });
    arrow::ArrayVector input_batches = {
        arrow::ipc::internal::json::ArrayFromJSON(input_type,
                                                  R"([[0, 10, "a", 1], [1, 11, "b", 2]])")
            .ValueOrDie(),
        arrow::ipc::internal::json::ArrayFromJSON(input_type,
                                                  R"([[2, 12, "b", 3], [3, 13, "d", 4]])")
            .ValueOrDie(),
    };
    std::shared_ptr<arrow::Schema> output_schema = arrow::schema({
        arrow::field("rowkind", arrow::utf8(), /*nullable=*/false),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("pk", arrow::list(arrow::utf8())),
        arrow::field("v", arrow::list(arrow::int32())),
    });
    std::unique_ptr<BatchReader> reader = CreateChangelogBatchReader(
        std::make_unique<ArrayBatchReader>(std::move(input_batches)), output_schema,
        /*include_sequence_number=*/true, CreateBinlogBatchConverter(),
        /*pack_update_before_after=*/true, GetDefaultPool());

    arrow::ArrayVector output_batches;
    while (true) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [c_array, c_schema] = batch;
        arrow::Result<std::shared_ptr<arrow::Array>> output_result =
            arrow::ImportArray(c_array.get(), c_schema.get());
        ASSERT_TRUE(output_result.ok()) << output_result.status().ToString();
        output_batches.push_back(std::move(output_result).ValueOrDie());
    }
    std::shared_ptr<arrow::Array> actual = arrow::Concatenate(output_batches).ValueOrDie();
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(actual->type(), R"([
                ["+I", 10, ["a"], [1]],
                ["+U", 11, ["b", "b"], [2, 3]],
                ["-D", 13, ["d"], [4]]
            ])")
            .ValueOrDie();
    ASSERT_TRUE(actual->Equals(expected))
        << "expected: " << expected->ToString() << "\nactual: " << actual->ToString();
}

TEST(SystemTableTest, TestStreamingBinlogRejectsUnmatchedUpdateBefore) {
    std::shared_ptr<arrow::DataType> input_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("pk", arrow::utf8()),
        arrow::field("v", arrow::int32()),
    });
    arrow::ArrayVector input_batches = {
        arrow::ipc::internal::json::ArrayFromJSON(input_type, R"([[1, "b", 2]])").ValueOrDie(),
    };
    std::shared_ptr<arrow::Schema> output_schema = arrow::schema({
        arrow::field("rowkind", arrow::utf8(), /*nullable=*/false),
        arrow::field("pk", arrow::list(arrow::utf8())),
        arrow::field("v", arrow::list(arrow::int32())),
    });
    std::unique_ptr<BatchReader> reader = CreateChangelogBatchReader(
        std::make_unique<ArrayBatchReader>(std::move(input_batches)), output_schema,
        /*include_sequence_number=*/false, CreateBinlogBatchConverter(),
        /*pack_update_before_after=*/true, GetDefaultPool());

    ASSERT_NOK_WITH_MSG(reader->NextBatch(),
                        "UPDATE_BEFORE has no matching UPDATE_AFTER in binlog reader");
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
