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

class NestedColumnPruningInteTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        file_format_ = GetParam();
        dir_ = UniqueTestDirectory::Create("local");
        test_dir_ = dir_->Str();
        table_path_ = PathUtil::JoinPath(test_dir_, "foo.db/bar");
    }
    void TearDown() override { dir_.reset(); }

 protected:
    static std::shared_ptr<arrow::Field> AnnotateField(
        const std::shared_ptr<arrow::Field>& field, int32_t paimon_id) {
        auto metadata = arrow::KeyValueMetadata::Make(
            {DataField::FIELD_ID}, {std::to_string(paimon_id)});
        if (field->metadata()) {
            auto merged = field->metadata()->Merge(*metadata);
            return field->WithMetadata(merged);
        }
        return field->WithMetadata(metadata);
    }

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
        auto helper,
        TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                           /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    // Write data
    std::string data = R"([
        [1, [10, "hello", 1.1]],
        [2, [20, "world", 2.2]],
        [3, [30, "foo", 3.3]],
        [4, [40, "bar", 4.4]]
    ])";
    ASSERT_OK_AND_ASSIGN(
        auto batch,
        TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                    /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batch), commit_identifier++,
                               /*expected_commit_messages=*/std::nullopt));

    // Scan to get splits
    ASSERT_OK_AND_ASSIGN(
        auto data_splits,
        helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    // Build projected schema: only read f0 (full) and f1.a (sub-field of struct)
    // Catalog assigns IDs: f0->0, f1->1, f1.a->2, f1.b->3, f1.c->4
    auto pruned_struct_type = arrow::struct_({
        AnnotateField(arrow::field("a", arrow::int32()), 2),
    });
    arrow::FieldVector projected_fields = {
        AnnotateField(arrow::field("f0", arrow::int32()), 0),
        AnnotateField(arrow::field("f1", pruned_struct_type), 1),
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
    ASSERT_OK_AND_ASSIGN(auto read_result,
                         ReadResultCollector::CollectResult(batch_reader.get()));

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
        auto helper,
        TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                           /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [100, [1, "aa"], 0.1],
        [200, [2, "bb"], 0.2],
        [300, [3, "cc"], 0.3]
    ])";
    ASSERT_OK_AND_ASSIGN(
        auto batch,
        TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                    /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batch), commit_identifier++,
                               /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(
        auto data_splits,
        helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));

    // Only read f0 and f2, skip f1 entirely.
    // IDs: f0->0, f1->1, f1.x->2, f1.y->3, f2->4
    arrow::FieldVector projected_fields = {
        AnnotateField(arrow::field("f0", arrow::int32()), 0),
        AnnotateField(arrow::field("f2", arrow::float64()), 4),
    };
    auto projected_schema = arrow::schema(projected_fields);

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result,
                         ReadResultCollector::CollectResult(batch_reader.get()));

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
        auto helper,
        TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                           /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [10, [100, "aaa"]]],
        [2, [20, [200, "bbb"]]],
        [3, [30, [300, "ccc"]]]
    ])";
    ASSERT_OK_AND_ASSIGN(
        auto batch,
        TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                    /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batch), commit_identifier++,
                               /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(
        auto data_splits,
        helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));

    // Field IDs (assigned sequentially by catalog):
    // f0->0, f1->1, f1.a->2, f1.inner->3, f1.inner.x->4, f1.inner.y->5
    //
    // Projected: f0, f1{inner{x}} — skip f1.a and f1.inner.y
    auto pruned_inner = arrow::struct_({
        AnnotateField(arrow::field("x", arrow::int64()), 4),
    });
    auto pruned_outer = arrow::struct_({
        AnnotateField(arrow::field("inner", pruned_inner), 3),
    });
    arrow::FieldVector projected_fields = {
        AnnotateField(arrow::field("f0", arrow::int32()), 0),
        AnnotateField(arrow::field("f1", pruned_outer), 1),
    };
    auto projected_schema = arrow::schema(projected_fields);

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result,
                         ReadResultCollector::CollectResult(batch_reader.get()));

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
        auto helper,
        TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                           /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    // Write data: each row has a map with keys "a", "b", "c"
    std::string data = R"([
        [1, [["a", 10], ["b", 20], ["c", 30]]],
        [2, [["a", 100], ["c", 300]]],
        [3, [["b", 200], ["c", 400], ["d", 500]]]
    ])";
    ASSERT_OK_AND_ASSIGN(
        auto batch,
        TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                    /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batch), commit_identifier++,
                               /*expected_commit_messages=*/std::nullopt));

    // Scan to get splits
    ASSERT_OK_AND_ASSIGN(
        auto data_splits,
        helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_FALSE(data_splits.empty());

    // Build projected schema: read f0 and f1 with selected keys "a,c"
    auto selected_keys_metadata = arrow::KeyValueMetadata::Make(
        {DataField::MAP_SELECTED_KEYS}, {"a,c"});
    arrow::FieldVector projected_fields = {
        AnnotateField(arrow::field("f0", arrow::int32()), 0),
        AnnotateField(arrow::field("f1", map_type), 1)->WithMetadata(
            AnnotateField(arrow::field("f1", map_type), 1)
                ->metadata()->Merge(*selected_keys_metadata)),
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
    ASSERT_OK_AND_ASSIGN(auto read_result,
                         ReadResultCollector::CollectResult(batch_reader.get()));

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

// Test: Deeper nested struct — prune sub-fields of a struct inside a struct inside another struct.
TEST_P(NestedColumnPruningInteTest, PruneDeeperNestedStruct) {
    // Table schema: f0 (int32), f1 (struct{a: int32, inner1: struct{x: int64, inner2: struct{p: utf8, q: float64}}})
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
        auto helper,
        TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                           /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [10, [100, ["ppp", 1.1]]]],
        [2, [20, [200, ["qqq", 2.2]]]],
        [3, [30, [300, ["rrr", 3.3]]]]
    ])";
    ASSERT_OK_AND_ASSIGN(
        auto batch,
        TestHelper::MakeRecordBatch(arrow::struct_(table_fields), data,
                                    /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batch), commit_identifier++,
                               /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(
        auto data_splits,
        helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));

    // Field IDs (assigned sequentially by catalog):
    // f0->0, f1->1, f1.a->2, f1.inner1->3, f1.inner1.x->4, f1.inner1.inner2->5, f1.inner1.inner2.p->6, f1.inner1.inner2.q->7
    //
    // Projected: f0, f1{inner1{inner2{p}}}
    auto pruned_inner2 = arrow::struct_({
        AnnotateField(arrow::field("p", arrow::utf8()), 6),
    });
    auto pruned_inner1 = arrow::struct_({
        AnnotateField(arrow::field("inner2", pruned_inner2), 5),
    });
    auto pruned_outer = arrow::struct_({
        AnnotateField(arrow::field("inner1", pruned_inner1), 3),
    });
    arrow::FieldVector projected_fields = {
        AnnotateField(arrow::field("f0", arrow::int32()), 0),
        AnnotateField(arrow::field("f1", pruned_outer), 1),
    };
    auto projected_schema = arrow::schema(projected_fields);

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder read_context_builder(table_path_);
    read_context_builder.SetOptions(options).SetReadSchema(&c_schema);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto read_result,
                         ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector expected_fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::struct_({
            arrow::field("inner1", arrow::struct_({
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

INSTANTIATE_TEST_SUITE_P(FileFormats, NestedColumnPruningInteTest,
                         ::testing::Values("parquet", "orc"));

}  // namespace paimon::test
