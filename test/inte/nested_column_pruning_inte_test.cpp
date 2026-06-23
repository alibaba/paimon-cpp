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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/dict_array_converter.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class DataSplit;
class RecordBatch;
}  // namespace paimon

namespace paimon::test {

class NestedColumnPruningInteTest : public ::testing::Test,
                                    public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        file_format_ = GetParam();
        dir_ = UniqueTestDirectory::Create("local");
        test_dir_ = dir_->Str();
        table_path_ = PathUtil::JoinPath(test_dir_, "foo.db/bar");
    }
    void TearDown() override {
        dir_.reset();
    }

 protected:
    std::string file_format_;
    std::string test_dir_;
    std::string table_path_;
    std::unique_ptr<UniqueTestDirectory> dir_;
};

// Test: Table has struct field with 3 sub-fields, read only 1 sub-field via SetReadSchema.
TEST_P(NestedColumnPruningInteTest, PruneStructSubFields) {
    // Table schema: f0 (int32), f1 (struct{a: int32, b: utf8, c: float64})
    auto struct_type = arrow::struct_({
        arrow::field("a", arrow::int32()),
        arrow::field("b", arrow::utf8()),
        arrow::field("c", arrow::float64()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", struct_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    // Write data
    std::string data = R"([
        [1, [10, "hello", 1.1]],
        [2, [20, "world", 2.2]],
        [3, [30, "foo", 3.3]],
        [4, [40, "bar", 4.4]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // Scan to get splits
    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    // Build projected schema: only read f0 (full) and f1.a (sub-field of struct)
    auto pruned_struct_type = arrow::struct_({
        arrow::field("a", arrow::int32()),
    });
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", pruned_struct_type),
    };
    auto projected_schema = arrow::schema(projected_fields);

    // Export to C ArrowSchema
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    // Read with projected schema
    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // Expected: struct with _VALUE_KIND, f0, f1{a}
    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::struct_({arrow::field("a", arrow::int32())})),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 1, [10]],
        [0, 2, [20]],
        [0, 3, [30]],
        [0, 4, [40]]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: Two top-level struct columns have the same nested field name; projection should
// distinguish by parent column.
TEST_P(NestedColumnPruningInteTest, PruneSameNestedFieldNameFromDifferentStructColumns) {
    // Table schema: f0 (int32), s0 (struct{f1: int32, a: utf8}), s1 (struct{f1: int32, b: utf8})
    auto s0_type = arrow::struct_({
        arrow::field("f1", arrow::int32()),
        arrow::field("a", arrow::utf8()),
    });
    auto s1_type = arrow::struct_({
        arrow::field("f1", arrow::int32()),
        arrow::field("b", arrow::utf8()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("s0", s0_type),
        arrow::field("s1", s1_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [11, "left-1"], [101, "right-1"]],
        [2, [22, "left-2"], [202, "right-2"]],
        [3, [33, "left-3"], [303, "right-3"]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    // Project only s0.f1 and s1.f1; both nested field names are identical.
    auto projected_s0 = arrow::struct_({arrow::field("f1", arrow::int32())});
    auto projected_s1 = arrow::struct_({arrow::field("f1", arrow::int32())});
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("s0", projected_s0),
        arrow::field("s1", projected_s1),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("s0", arrow::struct_({arrow::field("f1", arrow::int32())})),
        arrow::field("s1", arrow::struct_({arrow::field("f1", arrow::int32())})),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 1, [11], [101]],
        [0, 2, [22], [202]],
        [0, 3, [33], [303]]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: Querying only non-existent struct sub-fields should fail fast.
TEST_P(NestedColumnPruningInteTest, QueryStructSubFieldsAllNonExistentReturnsNullStructColumn) {
    // Table schema: f0 (int32), f1 (struct{f1: int32, f2: utf8, f3: float64})
    auto struct_type = arrow::struct_({
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()),
        arrow::field("f3", arrow::float64()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", struct_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [11, "a", 1.1]],
        [2, [22, "b", 2.2]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    // Query struct sub-fields that do not exist in table schema.
    auto projected_struct_type = arrow::struct_({
        arrow::field("f4", arrow::int32()),
        arrow::field("f5", arrow::int64()),
    });
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", projected_struct_type),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto batch_reader_result = table_read->CreateReader(data_splits);
    if (!batch_reader_result.ok()) {
        auto message = batch_reader_result.status().ToString();
        ASSERT_TRUE(message.find("does not support schema evolution inside struct") !=
                        std::string::npos ||
                    message.find("requires paimon.id for nested struct field") !=
                        std::string::npos)
            << "unexpected error: " << message;
        return;
    }

    auto read_result_result = ReadResultCollector::CollectResult(batch_reader_result.value().get());
    ASSERT_FALSE(read_result_result.ok());
    auto message = read_result_result.status().ToString();
    ASSERT_TRUE(message.find("does not support schema evolution inside struct") !=
                    std::string::npos ||
                message.find("requires paimon.id for nested struct field") != std::string::npos)
        << "unexpected error: " << message;
}

// Test: Querying struct sub-fields with partial non-existent nested fields keeps
// existing nested fields and materializes missing ones as null.
TEST_P(NestedColumnPruningInteTest, QueryStructSubFieldsWithNonExistentField) {
    // Table schema: f0 (int32), f1 (struct{f1: int32, f2: utf8, f3: float64})
    auto struct_type = arrow::struct_({
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()),
        arrow::field("f3", arrow::float64()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", struct_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [11, "a", 1.1]],
        [2, [22, "b", 2.2]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    // Query struct sub-fields f2,f3,f4 where f4 does not exist in table schema.
    auto projected_struct_type = arrow::struct_({
        arrow::field("f2", arrow::utf8()),
        arrow::field("f3", arrow::float64()),
        arrow::field("f4", arrow::int32()),
    });
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", projected_struct_type),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto batch_reader_result = table_read->CreateReader(data_splits);
    if (!batch_reader_result.ok()) {
        auto message = batch_reader_result.status().ToString();
        ASSERT_TRUE(message.find("does not support schema evolution inside struct") !=
                        std::string::npos ||
                    message.find("requires paimon.id for nested struct field") !=
                        std::string::npos)
            << "unexpected error: " << message;
        return;
    }

    auto read_result_result = ReadResultCollector::CollectResult(batch_reader_result.value().get());
    ASSERT_FALSE(read_result_result.ok());
    auto message = read_result_result.status().ToString();
    ASSERT_TRUE(message.find("does not support schema evolution inside struct") !=
                    std::string::npos ||
                message.find("requires paimon.id for nested struct field") != std::string::npos)
        << "unexpected error: " << message;
}

// Test: Nested schema divergence (simulated evolution mismatch) must fail fast
// instead of silently skipping nested fields.
TEST_P(NestedColumnPruningInteTest, QueryStructSubFieldsWithTypeMismatchShouldFail) {
    // File schema (old): f1.a is INT32.
    auto struct_type = arrow::struct_({
        arrow::field("a", arrow::int32()),
        arrow::field("b", arrow::utf8()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", struct_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [11, "x"]],
        [2, [22, "y"]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // Persist schema-2 on disk so table latest schema diverges from old data files.
    std::string file_system_identifier = "local";
    auto fs_iter = options.find(Options::FILE_SYSTEM);
    if (fs_iter != options.end()) {
        file_system_identifier = StringUtils::ToLowerCase(fs_iter->second);
    }
    ASSERT_OK_AND_ASSIGN(auto file_system,
                         FileSystemFactory::Get(file_system_identifier, table_path_, options));
    std::shared_ptr<FileSystem> schema_fs(std::move(file_system));

    SchemaManager schema_manager(schema_fs, table_path_);
    ASSERT_OK_AND_ASSIGN(auto latest_schema_opt, schema_manager.Latest());
    ASSERT_TRUE(latest_schema_opt.has_value());
    auto latest_schema = latest_schema_opt.value();

    auto schema_v2_arrow = arrow::schema({
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::struct_({
                               arrow::field("a", arrow::utf8()),
                               arrow::field("b", arrow::utf8()),
                           })),
    });
    ASSERT_OK_AND_ASSIGN(
        auto schema_v2,
        TableSchema::Create(/*schema_id=*/latest_schema->Id() + 1, schema_v2_arrow,
                            latest_schema->PartitionKeys(), latest_schema->PrimaryKeys(),
                            latest_schema->Options()));
    ASSERT_OK_AND_ASSIGN(auto schema_v2_json, schema_v2->ToJsonString());
    auto schema_v2_path = PathUtil::JoinPath(schema_manager.SchemaDirectory(),
                                             "schema-" + std::to_string(schema_v2->Id()));
    ASSERT_OK(schema_fs->AtomicStore(schema_v2_path, schema_v2_json));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_NOK_WITH_MSG(table_read->CreateReader(data_splits),
                        "PruneDataType nested field type mismatch for 'a': read string vs "
                        "data int32");
}

// Test: With SetReadSchema using the new schema, context build should pass,
// and mismatch against old file type should be rejected in reader creation.
TEST_P(NestedColumnPruningInteTest, QueryStructSubFieldsWithTypeMismatchAndSetReadSchemaFailAtContext) {
    // File schema (old): f1.a is INT32.
    auto struct_type = arrow::struct_({
        arrow::field("a", arrow::int32()),
        arrow::field("b", arrow::utf8()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", struct_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [11, "x"]],
        [2, [22, "y"]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // Persist schema-2 on disk so table latest schema diverges from old data files.
    std::string file_system_identifier = "local";
    auto fs_iter = options.find(Options::FILE_SYSTEM);
    if (fs_iter != options.end()) {
        file_system_identifier = StringUtils::ToLowerCase(fs_iter->second);
    }
    ASSERT_OK_AND_ASSIGN(auto file_system,
                         FileSystemFactory::Get(file_system_identifier, table_path_, options));
    std::shared_ptr<FileSystem> schema_fs(std::move(file_system));

    SchemaManager schema_manager(schema_fs, table_path_);
    ASSERT_OK_AND_ASSIGN(auto latest_schema_opt, schema_manager.Latest());
    ASSERT_TRUE(latest_schema_opt.has_value());
    auto latest_schema = latest_schema_opt.value();

    auto schema_v2_arrow = arrow::schema({
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::struct_({
                               arrow::field("a", arrow::utf8()),
                               arrow::field("b", arrow::utf8()),
                           })),
    });
    ASSERT_OK_AND_ASSIGN(
        auto schema_v2,
        TableSchema::Create(/*schema_id=*/latest_schema->Id() + 1, schema_v2_arrow,
                            latest_schema->PartitionKeys(), latest_schema->PrimaryKeys(),
                            latest_schema->Options()));
    ASSERT_OK_AND_ASSIGN(auto schema_v2_json, schema_v2->ToJsonString());
    auto schema_v2_path = PathUtil::JoinPath(schema_manager.SchemaDirectory(),
                                             "schema-" + std::to_string(schema_v2->Id()));
    ASSERT_OK(schema_fs->AtomicStore(schema_v2_path, schema_v2_json));

    // User-provided read schema uses latest nested type (a:string).
    auto projected_schema = arrow::schema({
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::struct_({
                               arrow::field("a", arrow::utf8()),
                               arrow::field("b", arrow::utf8()),
                           })),
    });
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    ASSERT_NOK_WITH_MSG(table_read->CreateReader(data_splits),
                        "PruneDataType nested field type mismatch for 'a': read string vs "
                        "data int32");
}

// Test: Read only top-level fields, skip struct entirely.
TEST_P(NestedColumnPruningInteTest, PruneEntireStructField) {
    auto struct_type = arrow::struct_({
        arrow::field("x", arrow::int64()),
        arrow::field("y", arrow::utf8()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", struct_type),
        arrow::field("f2", arrow::float64()),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [100, [1, "aa"], 0.1],
        [200, [2, "bb"], 0.2],
        [300, [3, "cc"], 0.3]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));

    // Only read f0 and f2, skip f1 entirely.
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f2", arrow::float64()),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f2", arrow::float64()),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 100, 0.1],
        [0, 200, 0.2],
        [0, 300, 0.3]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: Nested struct — prune sub-fields of a struct inside another struct.
TEST_P(NestedColumnPruningInteTest, PruneDeepNestedStruct) {
    // Table schema: f0 (int32), f1 (struct{a: int32, inner: struct{x: int64, y: utf8}})
    auto inner_struct = arrow::struct_({
        arrow::field("x", arrow::int64()),
        arrow::field("y", arrow::utf8()),
    });
    auto outer_struct = arrow::struct_({
        arrow::field("a", arrow::int32()),
        arrow::field("inner", inner_struct),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", outer_struct),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [10, [100, "aaa"]]],
        [2, [20, [200, "bbb"]]],
        [3, [30, [300, "ccc"]]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));

    // Projected: f0, f1{inner{x}} — skip f1.a and f1.inner.y
    auto pruned_inner = arrow::struct_({
        arrow::field("x", arrow::int64()),
    });
    auto pruned_outer = arrow::struct_({
        arrow::field("inner", pruned_inner),
    });
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", pruned_outer),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::struct_({
                               arrow::field("inner", arrow::struct_({
                                                         arrow::field("x", arrow::int64()),
                                                     })),
                           })),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 1, [[100]]],
        [0, 2, [[200]]],
        [0, 3, [[300]]]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: Nested projected schema with special fields under row tracking.
TEST_P(NestedColumnPruningInteTest, PruneNestedStructWithSpecialFields) {
    // Table schema: f0 (int32), f1 (struct{a: int32, inner: struct{x: int64, y: utf8}})
    auto inner_struct = arrow::struct_({
        arrow::field("x", arrow::int64()),
        arrow::field("y", arrow::utf8()),
    });
    auto outer_struct = arrow::struct_({
        arrow::field("a", arrow::int32()),
        arrow::field("inner", inner_struct),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", outer_struct),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [10, [100, "aaa"]]],
        [2, [20, [200, "bbb"]]],
        [3, [30, [300, "ccc"]]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));

    // Projected: f0, f1{inner{x}}, _SEQUENCE_NUMBER, _ROW_ID
    auto pruned_inner = arrow::struct_({
        arrow::field("x", arrow::int64()),
    });
    auto pruned_outer = arrow::struct_({
        arrow::field("inner", pruned_inner),
    });
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", pruned_outer),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("_ROW_ID", arrow::int64()),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    ASSERT_EQ(read_result->num_chunks(), 1);
    auto result_array = std::dynamic_pointer_cast<arrow::StructArray>(read_result->chunk(0));
    ASSERT_TRUE(result_array);

    ASSERT_TRUE(result_array->GetFieldByName("_SEQUENCE_NUMBER"));
    ASSERT_TRUE(result_array->GetFieldByName("_ROW_ID"));
    auto nested_col = result_array->GetFieldByName("f1");
    ASSERT_TRUE(nested_col);

    auto expected_nested_type = arrow::struct_({
        arrow::field("inner", arrow::struct_({arrow::field("x", arrow::int64())})),
    });
    ASSERT_TRUE(nested_col->type()->Equals(expected_nested_type));

    auto expected_nested_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_nested_type, R"([
            [[100]],
            [[200]],
            [[300]]
        ])")
            .ValueOrDie();
    ASSERT_TRUE(nested_col->Equals(expected_nested_array));
}

// Test: Table has MAP<STRING, INT32> field, read with selected keys filter.
TEST_P(NestedColumnPruningInteTest, MapSelectedKeys) {
    // Table schema: f0 (int32), f1 (map<string, int32>)
    auto map_type = arrow::map(arrow::utf8(), arrow::int32());
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", map_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    // Write data: each row has a map with keys "a", "b", "c"
    std::string data = R"([
        [1, [["a", 10], ["b", 20], ["c", 30]]],
        [2, [["a", 100], ["c", 300]]],
        [3, [["b", 200], ["c", 400], ["d", 500]]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // Scan to get splits
    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    // Build projected schema: read f0 and f1 with selected keys "a,c"
    auto selected_keys_metadata =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a,c"});
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", map_type)->WithMetadata(selected_keys_metadata),
    };
    auto projected_schema = arrow::schema(projected_fields);

    // Export to C ArrowSchema
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    // Read with projected schema
    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // Expected: only keys "a" and "c" remain in each map
    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int32())),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 1, [["a", 10], ["c", 30]]],
        [0, 2, [["a", 100], ["c", 300]]],
        [0, 3, [["c", 400]]]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: Selected-keys metadata on MAP nested inside STRUCT should be applied.
TEST_P(NestedColumnPruningInteTest, NestedMapSelectedKeysInStruct) {
    auto map_type = arrow::map(arrow::utf8(), arrow::int32());
    auto struct_type = arrow::struct_({
        arrow::field("m", map_type),
        arrow::field("tag", arrow::utf8()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", struct_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [[ ["a", 10], ["b", 20], ["c", 30] ], "r1"]],
        [2, [[ ["a", 100], ["c", 300] ], "r2"]],
        [3, [[ ["b", 200], ["c", 400], ["d", 500] ], "r3"]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    auto selected_keys_metadata =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a,c"});
    auto projected_struct_type = arrow::struct_({
        arrow::field("m", map_type)->WithMetadata(selected_keys_metadata),
    });
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", projected_struct_type),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::struct_({
                               arrow::field("m", arrow::map(arrow::utf8(), arrow::int32())),
                           })),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 1, [[ ["a", 10], ["c", 30] ]]],
        [0, 2, [[ ["a", 100], ["c", 300] ]]],
        [0, 3, [[ ["c", 400] ]]]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: Partial STRUCT sub-field recall where one recalled child is MAP with selected keys.
TEST_P(NestedColumnPruningInteTest, PruneStructSubFieldsWithNestedMapSelectedKeys) {
    auto map_type = arrow::map(arrow::utf8(), arrow::int32());
    auto struct_type = arrow::struct_({
        arrow::field("m", map_type),
        arrow::field("keep", arrow::int64()),
        arrow::field("drop", arrow::utf8()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", struct_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [[ ["a", 10], ["b", 20], ["c", 30] ], 1001, "x1"]],
        [2, [[ ["a", 100], ["c", 300] ], 1002, "x2"]],
        [3, [[ ["b", 200], ["c", 400], ["d", 500] ], 1003, "x3"]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    auto selected_keys_metadata =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a,c"});
    auto projected_struct_type = arrow::struct_({
        arrow::field("m", map_type)->WithMetadata(selected_keys_metadata),
        arrow::field("keep", arrow::int64()),
    });
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", projected_struct_type),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::struct_({
                               arrow::field("m", arrow::map(arrow::utf8(), arrow::int32())),
                               arrow::field("keep", arrow::int64()),
                           })),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 1, [[ ["a", 10], ["c", 30] ], 1001]],
        [0, 2, [[ ["a", 100], ["c", 300] ], 1002]],
        [0, 3, [[ ["c", 400] ], 1003]]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: Null semantics should be preserved when pruning STRUCT sub-fields and
// applying selected-keys filtering on nested MAP.
TEST_P(NestedColumnPruningInteTest, PruneStructSubFieldsWithNestedMapSelectedKeysAndNulls) {
    auto map_type = arrow::map(arrow::utf8(), arrow::int32());
    auto struct_type = arrow::struct_({
        arrow::field("m", map_type),
        arrow::field("keep", arrow::int64()),
        arrow::field("drop", arrow::utf8()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", struct_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [[ ["a", 10], ["b", 20], ["c", 30] ], 1001, "x1"]],
        [2, null],
        [3, [null, 1003, "x3"]],
        [4, [[ ["b", 200], ["c", 400], ["d", 500] ], null, "x4"]],
        [5, [[ ["a", 500], ["c", null] ], 1005, "x5"]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    auto selected_keys_metadata =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a,c"});
    auto projected_struct_type = arrow::struct_({
        arrow::field("m", map_type)->WithMetadata(selected_keys_metadata),
        arrow::field("keep", arrow::int64()),
    });
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", projected_struct_type),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::struct_({
                               arrow::field("m", arrow::map(arrow::utf8(), arrow::int32())),
                               arrow::field("keep", arrow::int64()),
                           })),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 1, [[ ["a", 10], ["c", 30] ], 1001]],
        [0, 2, null],
        [0, 3, [null, 1003]],
        [0, 4, [[ ["c", 400] ], null]],
        [0, 5, [[ ["a", 500], ["c", null] ], 1005]]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: MAP_SELECTED_KEYS metadata value is empty string, select empty-string map key.
TEST_P(NestedColumnPruningInteTest, MapSelectedKeysEmptyStringKey) {
    // Table schema: f0 (int32), f1 (map<string, int32>)
    auto map_type = arrow::map(arrow::utf8(), arrow::int32());
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", map_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    // Write data: each row has a map that may contain empty-string key.
    std::string data = R"([
        [1, [["", 9], ["a", 10], ["c", 30]]],
        [2, [["a", 100], ["", 99], ["c", 300]]],
        [3, [["b", 200], ["c", 400], ["d", 500]]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // Scan to get splits
    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    // Build projected schema: read f0 and f1 with selected keys metadata set to empty string.
    auto selected_keys_metadata =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {""});
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", map_type)->WithMetadata(selected_keys_metadata),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    // Read with projected schema
    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // Expected: only empty-string key remains.
    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int32())),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 1, [["", 9]]],
        [0, 2, [["", 99]]],
        [0, 3, []]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: MAP_SELECTED_KEYS output map entry order should follow selected key order.
TEST_P(NestedColumnPruningInteTest, MapSelectedKeysPreserveOrder) {
    auto map_type = arrow::map(arrow::utf8(), arrow::int32());
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", map_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    // Write data with map key order different from selected key order.
    std::string data = R"([
        [1, [["a", 10], ["b", 20], ["c", 30]]],
        [2, [["a", 100], ["c", 300]]],
        [3, [["c", 400], ["a", 500], ["d", 600]]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    // Query key order is c,a and output should follow this order.
    auto selected_keys_metadata =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"c,a"});
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", map_type)->WithMetadata(selected_keys_metadata),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int32())),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 1, [["c", 30], ["a", 10]]],
        [0, 2, [["c", 300], ["a", 100]]],
        [0, 3, [["c", 400], ["a", 500]]]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: ORC dictionary-encoded map key/value should work with MAP_SELECTED_KEYS.
TEST_P(NestedColumnPruningInteTest, MapSelectedKeysWithOrcDictionaryEncodedMap) {
    if (file_format_ != "orc") {
        GTEST_SKIP() << "ORC-only dictionary encoding case";
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::utf8());
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", map_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {"orc.read.enable-lazy-decoding", "true"},
        {"orc.dictionary-key-size-threshold", "1.0"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    // Low-cardinality map keys/values increase dictionary-encoding probability for ORC.
    std::string data = R"([
        [1, [["a", "v1"], ["b", "v2"], ["c", "v3"]]],
        [2, [["a", "v1"], ["c", "v3"], ["d", "v4"]]],
        [3, [["a", "v1"], ["b", "v2"], ["e", "v5"]]],
        [4, [["a", "v1"], ["c", "v3"], ["e", "v5"]]],
        [5, [["a", "v1"], ["b", "v2"], ["c", "v3"]]],
        [6, [["a", "v1"], ["c", "v3"], ["d", "v4"]]],
        [7, [["a", "v1"], ["b", "v2"], ["e", "v5"]]],
        [8, [["a", "v1"], ["c", "v3"], ["e", "v5"]]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    auto selected_keys_metadata =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a,c"});
    auto projected_schema = arrow::schema({
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", map_type)->WithMetadata(selected_keys_metadata),
    });

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    ASSERT_OK_AND_ASSIGN(
        auto decoded_result,
        DictArrayConverter::ConvertDictArray(read_result->chunk(0), arrow::default_memory_pool()));
    auto actual_chunked = std::make_shared<arrow::ChunkedArray>(decoded_result);

    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::utf8())),
    });
    auto expected_array = arrow::ipc::internal::json::ArrayFromJSON(expected_type, R"([
        [0, 1, [["a", "v1"], ["c", "v3"]]],
        [0, 2, [["a", "v1"], ["c", "v3"]]],
        [0, 3, [["a", "v1"]]],
        [0, 4, [["a", "v1"], ["c", "v3"]]],
        [0, 5, [["a", "v1"], ["c", "v3"]]],
        [0, 6, [["a", "v1"], ["c", "v3"]]],
        [0, 7, [["a", "v1"]]],
        [0, 8, [["a", "v1"], ["c", "v3"]]]
    ])")
                              .ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(actual_chunked, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << actual_chunked->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << actual_chunked->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: Deeper nested struct — prune sub-fields of a struct inside a struct inside another struct.
TEST_P(NestedColumnPruningInteTest, PruneDeeperNestedStruct) {
    // Table schema: f0 (int32), f1 (struct{a: int32, inner1: struct{x: int64, inner2: struct{p:
    // utf8, q: float64}}})
    auto inner2_struct = arrow::struct_({
        arrow::field("p", arrow::utf8()),
        arrow::field("q", arrow::float64()),
    });
    auto inner1_struct = arrow::struct_({
        arrow::field("x", arrow::int64()),
        arrow::field("inner2", inner2_struct),
    });
    auto outer_struct = arrow::struct_({
        arrow::field("a", arrow::int32()),
        arrow::field("inner1", inner1_struct),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", outer_struct),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [10, [100, ["ppp", 1.1]]]],
        [2, [20, [200, ["qqq", 2.2]]]],
        [3, [30, [300, ["rrr", 3.3]]]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));

    // Projected: f0, f1{inner1{inner2{p}}}
    auto pruned_inner2 = arrow::struct_({
        arrow::field("p", arrow::utf8()),
    });
    auto pruned_inner1 = arrow::struct_({
        arrow::field("inner2", pruned_inner2),
    });
    auto pruned_outer = arrow::struct_({
        arrow::field("inner1", pruned_inner1),
    });
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", pruned_outer),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field(
            "f1", arrow::struct_({
                      arrow::field("inner1",
                                   arrow::struct_({
                                       arrow::field("inner2", arrow::struct_({
                                                                  arrow::field("p", arrow::utf8()),
                                                              })),
                                   })),
                  })),
    };
    auto expected_type = arrow::struct_(expected_fields);
    std::string expected_data = R"([
        [0, 1, [[[ "ppp" ]]]],
        [0, 2, [[[ "qqq" ]]]],
        [0, 3, [[[ "rrr" ]]]]
    ])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data).ValueOrDie();
    auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected_array);

    arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
    bool is_equal = expected_chunked->Equals(read_result, equal_options.diff_sink(&std::cout));
    if (!is_equal) {
        std::cout << "[expected_type] " << expected_chunked->type()->ToString() << std::endl;
        std::cout << "[actual_type]   " << read_result->type()->ToString() << std::endl;
        std::cout << "[expected] " << expected_chunked->ToString() << std::endl;
        std::cout << "[actual]   " << read_result->ToString() << std::endl;
    }
    ASSERT_TRUE(is_equal);
}

// Test: Nested pruning for LIST<STRUCT<...>> in integration path.
TEST_P(NestedColumnPruningInteTest, PruneListStructSubFields) {
    auto list_elem_struct = arrow::struct_({
        arrow::field("x", arrow::int64()),
        arrow::field("y", arrow::utf8()),
        arrow::field("z", arrow::float64()),
    });
    auto list_struct_type = arrow::list(arrow::field("item", list_elem_struct));
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", list_struct_type),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format_)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [[100, "a", 1.1], [200, "b", 2.2]]],
        [2, [[300, "c", 3.3]]],
        [3, []]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    auto pruned_list_elem_struct = arrow::struct_({arrow::field("x", arrow::int64())});
    auto pruned_list_type = arrow::list(arrow::field("item", pruned_list_elem_struct));
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", pruned_list_type),
    };
    auto projected_schema = arrow::schema(projected_fields);

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto batch_reader_result = table_read->CreateReader(data_splits);
    if (!batch_reader_result.ok()) {
        auto message = batch_reader_result.status().ToString();
        ASSERT_TRUE(message.find("partial projection inside list/map") != std::string::npos ||
                    message.find("partial projection inside list") != std::string::npos ||
                    message.find("type mismatch") != std::string::npos)
            << "unexpected error: " << message;
        return;
    }

    auto read_result_result = ReadResultCollector::CollectResult(batch_reader_result.value().get());
    ASSERT_FALSE(read_result_result.ok());
    auto message = read_result_result.status().ToString();
    ASSERT_TRUE(message.find("partial projection inside list/map") != std::string::npos ||
            message.find("partial projection inside list") != std::string::npos ||
                message.find("type mismatch") != std::string::npos)
        << "unexpected error: " << message;
}

INSTANTIATE_TEST_SUITE_P(FileFormats, NestedColumnPruningInteTest,
                         ::testing::Values("parquet", "orc"));

}  // namespace paimon::test
