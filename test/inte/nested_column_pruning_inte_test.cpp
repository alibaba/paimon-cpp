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
#include "paimon/defs.h"
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
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    // Read with projected schema
    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
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

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
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

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
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

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
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
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    // Read with projected schema
    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
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

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    // Read with projected schema
    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
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

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
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

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
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

// Test: Parquet page-level filtering should work together with nested pruning.
TEST_P(NestedColumnPruningInteTest, ParquetPageIndexFilterWithNestedPruning) {
    if (file_format_ != "parquet") {
        GTEST_SKIP() << "Parquet-only page-level filtering case";
    }

    auto nested_struct = arrow::struct_({
        arrow::field("x", arrow::int64()),
        arrow::field("y", arrow::utf8()),
    });
    arrow::FieldVector table_fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", nested_struct),
    };
    auto table_schema = arrow::schema(table_fields);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, "PARQUET"},
        {Options::TARGET_FILE_SIZE, "1048576"},
        {Options::BUCKET, "-1"},
        {Options::WRITE_BATCH_SIZE, "1"},
        {"parquet.page.size", "1"},
        {"parquet.enable-dictionary", "false"},
        {"parquet.write.enable-page-index", "true"},
        {"parquet.read.enable-page-index-filter", "true"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        ["Alice", [100, "a"]],
        ["Bob", [200, "b"]],
        ["Cathy", [300, "c"]],
        ["David", [400, "d"]]
    ])";
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    std::string literal_str = "Alice";
    auto predicate = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, literal_str.data(), literal_str.size()));

    ScanContextBuilder scan_context_builder(table_path_);
    scan_context_builder.WithStreamingMode(true)
        .SetOptions(options)
        .AddOption(Options::SCAN_MODE, StartupMode::LatestFull().ToString())
        .SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_FALSE(result_plan->Splits().empty());

    auto pruned_nested_struct = arrow::struct_({arrow::field("x", arrow::int64())});
    arrow::FieldVector projected_fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", pruned_nested_struct),
    };
    auto projected_schema = arrow::schema(projected_fields);
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetPredicate(predicate).SetReadSchema(&c_schema);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto batch_reader_result = table_read->CreateReader(result_plan->Splits());
    if (!batch_reader_result.ok()) {
        ASSERT_NE(batch_reader_result.status().ToString().find("has no matching Arrow field"),
                  std::string::npos);
        return;
    }

    auto read_result_result = ReadResultCollector::CollectResult(batch_reader_result.value().get());
    if (!read_result_result.ok()) {
        ASSERT_NE(read_result_result.status().ToString().find("has no matching Arrow field"),
                  std::string::npos);
        return;
    }
    auto read_result = std::move(read_result_result.value());

    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::struct_({arrow::field("x", arrow::int64())})),
    };
    auto expected_type = arrow::struct_(expected_fields);
    auto expected_array = arrow::ipc::internal::json::ArrayFromJSON(expected_type, R"([
        [0, "Alice", [100]]
    ])")
                              .ValueOrDie();
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

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto batch_reader_result = table_read->CreateReader(data_splits);
    if (!batch_reader_result.ok()) {
        auto message = batch_reader_result.status().ToString();
        ASSERT_TRUE(message.find("partial projection inside list/map") != std::string::npos ||
                    message.find("type mismatch") != std::string::npos)
            << "unexpected error: " << message;
        return;
    }

    auto read_result_result = ReadResultCollector::CollectResult(batch_reader_result.value().get());
    ASSERT_FALSE(read_result_result.ok());
    auto message = read_result_result.status().ToString();
    ASSERT_TRUE(message.find("partial projection inside list/map") != std::string::npos ||
                message.find("type mismatch") != std::string::npos)
        << "unexpected error: " << message;
}

INSTANTIATE_TEST_SUITE_P(FileFormats, NestedColumnPruningInteTest,
                         ::testing::Values("parquet", "orc"));

}  // namespace paimon::test
