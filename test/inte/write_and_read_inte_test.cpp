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

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/counting_cache_test_utils.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"
#include "paimon/write_context.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon::test {
// This is a sdk end-to-end test demo that supports write, commit, scan, and read operations.
class WriteAndReadInteTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<std::pair<std::string, std::string>> {
    void SetUp() override {
        auto [file_format, file_system] = GetParam();
        dir_ = UniqueTestDirectory::Create(file_system);
        test_dir_ = dir_->Str();
    }
    void TearDown() override {
        dir_.reset();
    }

    std::map<std::string, std::string> AddOptionsForJindo(
        const std::map<std::string, std::string>& options) const {
        std::map<std::string, std::string> jindo_options = GetJindoTestOptions();
        auto new_options = options;
        new_options.insert(jindo_options.begin(), jindo_options.end());
        return new_options;
    }

    Status WriteNextSchema(const std::vector<DataField>& fields, int32_t highest_field_id,
                           const std::map<std::string, std::string>& options) const {
        auto file_system = dir_->GetFileSystem();
        std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
        SchemaManager schema_manager(file_system, table_path);
        PAIMON_ASSIGN_OR_RAISE(auto latest_schema_opt, schema_manager.Latest());
        if (!latest_schema_opt) {
            return Status::Invalid("table schema does not exist");
        }
        auto next_schema = std::make_shared<TableSchema>(*latest_schema_opt.value());
        next_schema->id_ = latest_schema_opt.value()->Id() + 1;
        next_schema->fields_ = fields;
        next_schema->highest_field_id_ = highest_field_id;
        next_schema->options_ = options;
        PAIMON_ASSIGN_OR_RAISE(std::string schema_content, next_schema->ToJsonString());
        std::string schema_path = PathUtil::JoinPath(schema_manager.SchemaDirectory(),
                                                     "schema-" + std::to_string(next_schema->Id()));
        return file_system->AtomicStore(schema_path, schema_content);
    }

    Status WriteNextSchemaWithRawFieldTypes(const std::string& fields_json,
                                            int32_t highest_field_id) const {
        auto file_system = dir_->GetFileSystem();
        std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
        SchemaManager schema_manager(file_system, table_path);
        PAIMON_ASSIGN_OR_RAISE(auto latest_schema_opt, schema_manager.Latest());
        if (!latest_schema_opt) {
            return Status::Invalid("table schema does not exist");
        }
        auto next_schema = std::make_shared<TableSchema>(*latest_schema_opt.value());
        next_schema->id_ = latest_schema_opt.value()->Id() + 1;
        next_schema->highest_field_id_ = highest_field_id;
        PAIMON_ASSIGN_OR_RAISE(std::string schema_content, next_schema->ToJsonString());

        rapidjson::Document schema_doc;
        schema_doc.Parse(schema_content.c_str());
        rapidjson::Document fields_doc;
        fields_doc.Parse(fields_json.c_str());
        if (schema_doc.HasParseError() || fields_doc.HasParseError() ||
            !schema_doc.HasMember("fields")) {
            return Status::Invalid("failed to assemble schema json with raw field types");
        }
        schema_doc["fields"].CopyFrom(fields_doc, schema_doc.GetAllocator());
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        schema_doc.Accept(writer);

        std::string schema_path = PathUtil::JoinPath(schema_manager.SchemaDirectory(),
                                                     "schema-" + std::to_string(next_schema->Id()));
        return file_system->AtomicStore(schema_path, std::string(buffer.GetString()));
    }

    Result<bool> ReadAndCheckProjectedResult(const std::map<std::string, std::string>& options,
                                             const std::vector<std::string>& read_fields,
                                             const std::shared_ptr<arrow::DataType>& expected_type,
                                             const std::string& expected_data) const {
        std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
        PAIMON_ASSIGN_OR_RAISE(auto plan, InnerScan(options));

        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetOptions(options).SetReadFieldNames(read_fields);
        PAIMON_ASSIGN_OR_RAISE(auto read_context, read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(auto actual, ReadResultCollector::CollectResult(batch_reader.get()));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto expected, arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data));
        return std::make_shared<arrow::ChunkedArray>(expected)->Equals(actual);
    }

    /// Read with a custom Arrow read schema (supports nested column pruning and
    /// paimon.map.selected-keys metadata for shared-shredding partial key recall).
    Result<bool> ReadAndCheckWithReadSchema(const std::map<std::string, std::string>& options,
                                            const std::shared_ptr<arrow::Schema>& read_schema,
                                            const std::shared_ptr<arrow::DataType>& expected_type,
                                            const std::string& expected_data) const {
        std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
        PAIMON_ASSIGN_OR_RAISE(auto plan, InnerScan(options));

        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, c_schema.get()));
        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
        PAIMON_ASSIGN_OR_RAISE(auto read_context, read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(auto actual, ReadResultCollector::CollectResult(batch_reader.get()));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto expected, arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_data));
        return std::make_shared<arrow::ChunkedArray>(expected)->Equals(actual);
    }

    Result<std::shared_ptr<Plan>> InnerScan(
        const std::map<std::string, std::string>& options) const {
        std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetOptions(options).AddOption(Options::SCAN_MODE,
                                                           StartupMode::LatestFull().ToString());
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        return table_scan->CreatePlan();
    }

    Result<std::vector<std::pair<std::string, std::shared_ptr<DataFileMeta>>>> CurrentDataFiles(
        const std::map<std::string, std::string>& options) const {
        PAIMON_ASSIGN_OR_RAISE(auto plan, InnerScan(options));
        std::vector<std::pair<std::string, std::shared_ptr<DataFileMeta>>> files;
        for (const auto& split : plan->Splits()) {
            auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
            if (!data_split) {
                return Status::Invalid("split cannot be cast to DataSplitImpl");
            }
            for (const auto& data_file : data_split->DataFiles()) {
                files.emplace_back(data_split->BucketPath(), data_file);
            }
        }
        std::sort(files.begin(), files.end(), [](const auto& left, const auto& right) {
            return left.second->min_sequence_number < right.second->min_sequence_number;
        });
        return files;
    }

    Result<std::shared_ptr<arrow::Schema>> ReadDataFileSchema(
        const std::string& bucket_path, const std::shared_ptr<DataFileMeta>& file,
        const std::map<std::string, std::string>& options) const {
        std::string file_path = PathUtil::JoinPath(bucket_path, file->file_name);
        PAIMON_ASSIGN_OR_RAISE(auto unique_input_stream, dir_->GetFileSystem()->Open(file_path));
        std::shared_ptr<InputStream> input_stream(std::move(unique_input_stream));
        PAIMON_ASSIGN_OR_RAISE(std::string format_str, file->FileFormat());
        PAIMON_ASSIGN_OR_RAISE(auto file_format, FileFormatFactory::Get(format_str, options));
        PAIMON_ASSIGN_OR_RAISE(auto reader_builder, file_format->CreateReaderBuilder(10));
        PAIMON_ASSIGN_OR_RAISE(auto reader, reader_builder->Build(input_stream));
        PAIMON_ASSIGN_OR_RAISE(auto c_file_schema, reader->GetFileSchema());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto file_schema,
                                          arrow::ImportSchema(c_file_schema.get()));
        return file_schema;
    }

    Result<MapSharedShreddingFieldMeta> ReadShreddingMeta(
        const std::pair<std::string, std::shared_ptr<DataFileMeta>>& file,
        const std::string& field_name, const std::map<std::string, std::string>& options) const {
        PAIMON_ASSIGN_OR_RAISE(auto file_schema,
                               ReadDataFileSchema(file.first, file.second, options));
        std::shared_ptr<arrow::Field> field = file_schema->GetFieldByName(field_name);
        if (!field) {
            return Status::Invalid(
                fmt::format("field {} not found in data file schema", field_name));
        }
        std::shared_ptr<const arrow::KeyValueMetadata> metadata = field->metadata();
        if (!metadata) {
            return Status::Invalid(
                fmt::format("field {} has no shared-shredding metadata", field_name));
        }
        std::shared_ptr<arrow::KeyValueMetadata> metadata_copy = metadata->Copy();
        if (!MapSharedShreddingUtils::HasShreddingMetadata(metadata_copy)) {
            return Status::Invalid(
                fmt::format("field {} has no shared-shredding metadata", field_name));
        }
        return MapSharedShreddingUtils::DeserializeMetadata(metadata_copy);
    }

 private:
    std::string test_dir_;
    std::unique_ptr<UniqueTestDirectory> dir_;
};

TEST_P(WriteAndReadInteTest, TestAppendSimple) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32())};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    // manifest and file format are upper case
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "AVRO"},
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, StringUtils::ToUpperCase(file_system)},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;
    std::string data = R"([
            ["banana", 2],
            ["dog", 1],
            ["lucy", 14],
            ["mouse", 100]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
            [0, "banana", 2],
            [0, "dog", 1],
            [0, "lucy", 14],
            [0, "mouse", 100]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestPKSimple) {
    arrow::FieldVector fields = {
        arrow::field("pk", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::float64()),
    };
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},         {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},        {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, file_system},        {"orc.read.enable-lazy-decoding", "true"},
        {"orc.dictionary-key-size-threshold", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{"pk"}, options,
                                                         /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    std::string data_1 = R"([
            ["lucy", 14, 5.2],
            ["dog", 1, 4.1],
            ["banana", 2, 3.0],
            ["mouse", 100, 10.3]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    std::string data_2 = R"([
            ["apple", 20, 23.0],
            ["mouse", 200, 20.3],
            ["dog", 21, 24.1]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_2,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(commit_msgs,
                         helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string data = R"([
            [0, "apple", 20, 23.0],
            [0, "banana", 2, 3.0],
            [0, "dog", 21, 24.1],
            [0, "lucy", 14, 5.2],
            [0, "mouse", 200, 20.3]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type, data_splits, data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestNestedType) {
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::map(arrow::int8(), arrow::int16())),
        arrow::field("f2", arrow::list(arrow::float32())),
        arrow::field("f3", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                           arrow::field("f1", arrow::int64())})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f5", arrow::date32()),
        arrow::field("f6", arrow::decimal128(2, 2))};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;
    std::string data = R"([
        [[[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999999", 24, "0.28"],
        [[[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123123", 245, "0.12"],
        [[[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [[[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123123", 24, "0.78"]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));

    std::string expected_data = R"([
        [0, [[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [0, [[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999999", 24, "0.28"],
        [0, [[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [0, [[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123123", 245, "0.12"],
        [0, [[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [0, [[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123123", 24, "0.78"]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestSchemaEvolutionAddFieldInsideListAndMap) {
    auto [file_format, file_system] = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    auto list_struct =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    auto map_value = arrow::struct_({arrow::field("m1", arrow::int32())});
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("items", arrow::list(arrow::field("item", list_struct))),
        arrow::field("props", arrow::map(arrow::utf8(), map_value)),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        // Exercise ORC lazy decoding, which returns nested strings as dictionaries.
        {"orc.read.enable-lazy-decoding", "true"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(test_dir_, arrow::schema(fields),
                                                         /*partition_keys=*/{}, /*primary_keys=*/{},
                                                         options, /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch, TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                                 R"([
                [1, [[10, "x"], [20, "y"]], [["k1", [100]]]],
                [2, [[30, "z"]], [["k2", [200]]]]
            ])",
                                                                 /*partition_map=*/{},
                                                                 /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
    SchemaManager schema_manager(dir_->GetFileSystem(), table_path);
    ASSERT_OK_AND_ASSIGN(auto schema_v0, schema_manager.ReadSchema(0));
    std::vector<DataField> fields_v0 = schema_v0->Fields();
    int32_t next_id = schema_v0->HighestFieldId();

    auto items_field = fields_v0[1].ArrowField();
    auto items_list = arrow::internal::checked_pointer_cast<arrow::ListType>(items_field->type());
    auto items_struct =
        arrow::internal::checked_pointer_cast<arrow::StructType>(items_list->value_type());
    auto c_field = DataField::ConvertDataFieldToArrowField(
        DataField(++next_id, arrow::field("c", arrow::int32())));
    auto new_items_type = arrow::list(items_list->value_field()->WithType(
        arrow::struct_({items_struct->field(0), items_struct->field(1), c_field})));

    auto props_field = fields_v0[2].ArrowField();
    auto props_map = arrow::internal::checked_pointer_cast<arrow::MapType>(props_field->type());
    auto props_value =
        arrow::internal::checked_pointer_cast<arrow::StructType>(props_map->item_type());
    auto m2_field = DataField::ConvertDataFieldToArrowField(
        DataField(++next_id, arrow::field("m2", arrow::int32())));
    auto new_props_type = arrow::map(
        props_map->key_type(),
        props_map->item_field()->WithType(arrow::struct_({props_value->field(0), m2_field})));

    std::vector<DataField> fields_v1 = fields_v0;
    fields_v1[1] = DataField(fields_v0[1].Id(), items_field->WithType(new_items_type));
    fields_v1[2] = DataField(fields_v0[2].Id(), props_field->WithType(new_props_type));
    ASSERT_OK(WriteNextSchema(fields_v1, next_id, options));

    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("items", arrow::list(arrow::field(
                                  "item", arrow::struct_({arrow::field("a", arrow::int32()),
                                                          arrow::field("b", arrow::utf8()),
                                                          arrow::field("c", arrow::int32())})))),
        arrow::field("props", arrow::map(arrow::utf8(),
                                         arrow::struct_({arrow::field("m1", arrow::int32()),
                                                         arrow::field("m2", arrow::int32())}))),
    });
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(expected_type, splits,
                                                                  R"([
                [0, 1, [[10, "x", null], [20, "y", null]], [["k1", [100, null]]]],
                [0, 2, [[30, "z", null]], [["k2", [200, null]]]]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestAppendExternalPath) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    // create external path dir
    auto external_dir1 = UniqueTestDirectory::Create(file_system);
    ASSERT_TRUE(external_dir1);
    std::string external_test_dir1 = external_dir1->Str();
    auto external_dir2 = UniqueTestDirectory::Create(file_system);
    ASSERT_TRUE(external_dir2);
    std::string external_test_dir2 = external_dir2->Str();

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, file_system},
        {Options::DATA_FILE_PREFIX, "test-data-"},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
        options[Options::DATA_FILE_EXTERNAL_PATHS] = external_test_dir1 + "," + external_test_dir2;
    } else {
        options[Options::DATA_FILE_EXTERNAL_PATHS] =
            "FILE://" + external_test_dir1 + ",FILE://" + external_test_dir2;
    }
    // create table
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{"f1"},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));

    int64_t commit_identifier = 0;
    // write snapshot1
    std::string data1 = R"([
        ["Alice", 10, 0, 11.1],
        ["Bob", 10, 1, 12.1],
        ["Cathy", 10, 0, 13.1],
        ["Emily", 10, 0, 14.1]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch1,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data1,
                                    /*partition_map=*/{{"f1", "10"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    // write snapshot2
    std::string data2 = R"([
        ["Alex", 10, 1, 21.1],
        ["Lucy", 10, 0, 22.1],
        ["Tom", 10, 1, 23.1],
        ["John", 10, 1, 24.1]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch2,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data2,
                                    /*partition_map=*/{{"f1", "10"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // check external paths have data file & prefix with "test-data-"
    {
        auto get_file_list_in_external_path = [&](const UniqueTestDirectory* external_dir) {
            auto fs = external_dir->GetFileSystem();
            auto bucket_dir = external_dir->Str() + "/f1=10/bucket-0/";
            std::vector<std::unique_ptr<BasicFileStatus>> all_file_status;
            ASSERT_OK(fs->ListDir(bucket_dir, &all_file_status));
            ASSERT_FALSE(all_file_status.empty());
            for (const auto& file_status : all_file_status) {
                ASSERT_TRUE(PathUtil::GetName(file_status->GetPath()).find("test-data-") !=
                            std::string::npos);
            }
        };
        get_file_list_in_external_path(external_dir1.get());
        get_file_list_in_external_path(external_dir2.get());
    }

    // read
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
        [0, "Alice", 10, 0, 11.1],
        [0, "Bob", 10, 1, 12.1],
        [0, "Cathy", 10, 0, 13.1],
        [0, "Emily", 10, 0, 14.1],
        [0, "Alex", 10, 1, 21.1],
        [0, "Lucy", 10, 0, 22.1],
        [0, "Tom", 10, 1, 23.1],
        [0, "John", 10, 1, 24.1]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestAppendExternalPathAndNoneExternalPathStrategy) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    // create external path dir
    auto external_dir = UniqueTestDirectory::Create(file_system);
    ASSERT_TRUE(external_dir);
    std::string external_test_dir = external_dir->Str();

    // external path will not take effective as DATA_FILE_EXTERNAL_PATHS_STRATEGY default is None
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, file_format},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::FILE_SYSTEM, file_system},
                                                  {Options::DATA_FILE_PREFIX, "test-data-"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
        options[Options::DATA_FILE_EXTERNAL_PATHS] = external_test_dir;
    } else {
        options[Options::DATA_FILE_EXTERNAL_PATHS] = "FILE://" + external_test_dir;
    }
    // create table
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{"f1"},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));

    int64_t commit_identifier = 0;
    // write snapshot1
    std::string data1 = R"([
        ["Alice", 10, 0, 11.1],
        ["Bob", 10, 1, 12.1],
        ["Cathy", 10, 0, 13.1],
        ["Emily", 10, 0, 14.1]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch1,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data1,
                                    /*partition_map=*/{{"f1", "10"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    // write snapshot2
    std::string data2 = R"([
        ["Alex", 10, 1, 21.1],
        ["Lucy", 10, 0, 22.1],
        ["Tom", 10, 1, 23.1],
        ["John", 10, 1, 24.1]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch2,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data2,
                                    /*partition_map=*/{{"f1", "10"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // check external path does not have any data file
    {
        auto fs = external_dir->GetFileSystem();
        std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
        ASSERT_OK(fs->ListDir(external_test_dir, &file_status_list));
        ASSERT_TRUE(file_status_list.empty());
    }
    // read
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
        [0, "Alice", 10, 0, 11.1],
        [0, "Bob", 10, 1, 12.1],
        [0, "Cathy", 10, 0, 13.1],
        [0, "Emily", 10, 0, 14.1],
        [0, "Alex", 10, 1, 21.1],
        [0, "Lucy", 10, 0, 22.1],
        [0, "Tom", 10, 1, 23.1],
        [0, "John", 10, 1, 24.1]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestAppendTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system}, {"orc.timestamp-ltz.legacy.type", "false"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;
    std::string data = R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
["1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
["1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
[0, "1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
[0, "1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
[0, "1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestPkTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
        arrow::field("value", arrow::int32()),
    };
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, file_system}, {"orc.timestamp-ltz.legacy.type", "false"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/
                                        {"ts_sec", "ts_milli", "ts_micro", "ts_nano", "ts_tz_sec",
                                         "ts_tz_milli", "ts_tz_micro", "ts_tz_nano"},
                                        options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;
    std::string data = R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1969-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002", 2],
["1970-01-01 00:00:01", "1969-01-01 00:00:00.003", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004", 0],
["1970-01-01 00:00:01", "1969-01-01 00:00:00.003", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000005", 1]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
[0, "1970-01-01 00:00:01", "1969-01-01 00:00:00.003", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004", 0],
[0, "1970-01-01 00:00:01", "1969-01-01 00:00:00.003", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000005", 1],
[0, "1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1969-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002", 2]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestPKWithSequenceFieldInPKField) {
    arrow::FieldVector fields = {
        arrow::field("p1", arrow::utf8()),
        arrow::field("p2", arrow::int32()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::float64()),
    };
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},       {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, file_system},       {Options::SEQUENCE_FIELD, "p2"},
        {"orc.read.enable-lazy-decoding", "true"}, {"orc.dictionary-key-size-threshold", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{"p1", "p2"}, options,
                                                         /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    std::string data_1 = R"([
            ["banana", 2, 12, 13.0],
            ["lucy", 0, 14, 5.2],
            ["dog", 1, 1, 4.1],
            ["banana", 2, 2, 3.0],
            ["mouse", 3, 100, 10.3]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    std::string data_2 = R"([
            ["apple", 0, 20, 23.0],
            ["banana", 1, 200, 20.3],
            ["dog", 1, 21, 24.1],
            ["mouse", 3, 200, 20.3]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_2,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(commit_msgs,
                         helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string data = R"([
            [0, "apple", 0, 20, 23.0],
            [0, "banana", 1, 200, 20.3],
            [0, "banana", 2, 2, 3.0],
            [0, "dog", 1, 21, 24.1],
            [0, "lucy", 0, 14, 5.2],
            [0, "mouse", 3, 200, 20.3]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type, data_splits, data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestPKWithSequenceFieldPartialInPKField) {
    arrow::FieldVector fields = {
        arrow::field("p1", arrow::utf8()),
        arrow::field("p2", arrow::int32()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::float64()),
    };
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},       {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, file_system},       {Options::SEQUENCE_FIELD, "p2,f1"},
        {"orc.read.enable-lazy-decoding", "true"}, {"orc.dictionary-key-size-threshold", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{"p1", "p2"}, options,
                                                         /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    std::string data_1 = R"([
            ["banana", 2, 12, 13.0],
            ["lucy", 0, 14, 5.2],
            ["dog", 1, 1, 4.1],
            ["banana", 2, 2, 3.0],
            ["mouse", 3, 100, 10.3]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    std::string data_2 = R"([
            ["apple", 0, 20, 23.0],
            ["banana", 1, 200, 20.3],
            ["dog", 1, 21, 24.1],
            ["mouse", 3, 10, 20.3]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_2,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(commit_msgs,
                         helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string data = R"([
            [0, "apple", 0, 20, 23.0],
            [0, "banana", 1, 200, 20.3],
            [0, "banana", 2, 12, 13.0],
            [0, "dog", 1, 21, 24.1],
            [0, "lucy", 0, 14, 5.2],
            [0, "mouse", 3, 100, 10.3]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type, data_splits, data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestWriteSamePartitionTwiceWithAllBasicTypesForAppend) {
    arrow::FieldVector fields = {
        arrow::field("f_bool", arrow::boolean()), arrow::field("f_int8", arrow::int8()),
        arrow::field("f_int16", arrow::int16()),  arrow::field("f_int32", arrow::int32()),
        arrow::field("f_int64", arrow::int64()),  arrow::field("f_string", arrow::utf8()),
        arrow::field("f_date", arrow::date32()),  arrow::field("f_value", arrow::int32())};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "f_value"},
        {Options::FILE_SYSTEM, file_system},
        {Options::PARTITION_GENERATE_LEGACY_NAME, "false"},
        {Options::PARTITION_DEFAULT_NAME, "null"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema,
                                        /*partition_keys=*/
                                        {"f_bool", "f_int8", "f_int16", "f_int32", "f_int64",
                                         "f_string", "f_date"},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;

    {
        std::map<std::string, std::string> partition_map = {
            {"f_bool", "true"},      {"f_int8", "1"},       {"f_int16", "100"},
            {"f_int32", "10000"},    {"f_int64", "100000"}, {"f_string", "hello"},
            {"f_date", "1970-01-02"}};

        // First write to the same partition
        std::string data1 = R"([
            [true, 1, 100, 10000, 100000, "hello", 1, 10],
            [true, 1, 100, 10000, 100000, "hello", 1, 20]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch1,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data1, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                             helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));

        // Second write to the same partition
        std::string data2 = R"([
            [true, 1, 100, 10000, 100000, "hello", 1, 30],
            [true, 1, 100, 10000, 100000, "hello", 1, 40]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch2,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data2, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(commit_msgs,
                             helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));
    }
    {
        // write all null for partition fields
        std::map<std::string, std::string> partition_map = {
            {"f_bool", "null"},  {"f_int8", "null"},   {"f_int16", "null"}, {"f_int32", "null"},
            {"f_int64", "null"}, {"f_string", "null"}, {"f_date", "null"}};

        // First write to the same partition
        std::string data1 = R"([
            [null, null, null, null, null, null, null, 50]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch1,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data1, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                             helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));

        // Second write to the same partition
        std::string data2 = R"([
            [null, null, null, null, null, null, null, 60]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch2,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data2, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(commit_msgs,
                             helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));
    }
    // Read and verify
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
            [0, true, 1, 100, 10000, 100000, "hello", 1, 10],
            [0, true, 1, 100, 10000, 100000, "hello", 1, 20],
            [0, true, 1, 100, 10000, 100000, "hello", 1, 30],
            [0, true, 1, 100, 10000, 100000, "hello", 1, 40],
            [0, null, null, null, null, null, null, null, 50],
            [0, null, null, null, null, null, null, null, 60]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestWriteSamePartitionTwiceWithAllBasicTypesForPk) {
    arrow::FieldVector fields = {
        arrow::field("f_bool", arrow::boolean()), arrow::field("f_int8", arrow::int8()),
        arrow::field("f_int16", arrow::int16()),  arrow::field("f_int32", arrow::int32()),
        arrow::field("f_int64", arrow::int64()),  arrow::field("f_string", arrow::utf8()),
        arrow::field("f_date", arrow::date32()),  arrow::field("f_value", arrow::int32()),
        arrow::field("pk", arrow::utf8())};
    auto schema = arrow::schema(fields);
    auto [file_format, file_system] = GetParam();
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, file_format},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::FILE_SYSTEM, file_system},
                                                  {Options::PARTITION_GENERATE_LEGACY_NAME, "true"},
                                                  {Options::PARTITION_DEFAULT_NAME, "null"}};
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper,
        TestHelper::Create(
            test_dir_, schema,
            /*partition_keys=*/
            {"f_bool", "f_int8", "f_int16", "f_int32", "f_int64", "f_string", "f_date"},
            /*primary_keys=*/
            {"pk", "f_bool", "f_int8", "f_int16", "f_int32", "f_int64", "f_string", "f_date"},
            options, /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;

    {
        std::map<std::string, std::string> partition_map = {
            {"f_bool", "true"},      {"f_int8", "1"},       {"f_int16", "100"},
            {"f_int32", "10000"},    {"f_int64", "100000"}, {"f_string", "hello"},
            {"f_date", "1970-01-02"}};

        // First write to the same partition
        std::string data1 = R"([
            [true, 1, 100, 10000, 100000,"hello", 1, 10, "pk1"],
            [true, 1, 100, 10000, 100000,"hello", 1, 20, "pk2"]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch1,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data1, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                             helper->WriteAndCommit(std::move(batch1), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));

        // Second write to the same partition
        std::string data2 = R"([
            [true, 1, 100, 10000, 100000,"hello", 1, 30, "pk1"],
            [true, 1, 100, 10000, 100000,"hello", 1, 40, "pk3"]
    ])";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch2,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), data2, partition_map,
                                        /*bucket=*/0, {}));
        ASSERT_OK_AND_ASSIGN(commit_msgs,
                             helper->WriteAndCommit(std::move(batch2), commit_identifier++,
                                                    /*expected_commit_messages=*/std::nullopt));
    }
    // Read and verify
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
            [0, true, 1, 100, 10000, 100000, "hello", 1, 30, "pk1"],
            [0, true, 1, 100, 10000, 100000, "hello", 1, 20, "pk2"],
            [0, true, 1, 100, 10000, 100000, "hello", 1, 40, "pk3"]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestCharVarcharBinaryVarbinaryTypes) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }
    arrow::FieldVector fields = {
        arrow::field("c", arrow::utf8()),
        arrow::field("vc", arrow::utf8()),
        arrow::field("b", arrow::binary()),
        arrow::field("vb", arrow::binary()),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, arrow::schema(fields), /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    ASSERT_OK(WriteNextSchemaWithRawFieldTypes(R"json([
        { "id" : 0, "name" : "c", "type" : "CHAR(10)" },
        { "id" : 1, "name" : "vc", "type" : "VARCHAR(20)" },
        { "id" : 2, "name" : "b", "type" : "BINARY(10)" },
        { "id" : 3, "name" : "vb", "type" : "VARBINARY(20)" }
    ])json",
                                               /*highest_field_id=*/3));
    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options,
                                            /*is_streaming_mode=*/false));

    std::string data = R"([
        ["alice", "hello world", "abc", "xyz123"],
        [null, null, null, null]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
        [0, "alice", "hello world", "abc", "xyz123"],
        [0, null, null, null, null]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

std::vector<std::pair<std::string, std::string>> GetTestValuesForWriteAndReadInteTest() {
    std::vector<std::pair<std::string, std::string>> values = {{"parquet", "local"}};
#if defined(PAIMON_ENABLE_NETWORK_TESTS) && defined(PAIMON_ENABLE_JINDO)
    values.emplace_back("parquet", "jindo");
#endif
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc", "local");
#endif
#ifdef PAIMON_ENABLE_AVRO
    values.emplace_back("avro", "local");
#endif
    return values;
}

/// End-to-end test for parquet page-level filtering with a PK table.
/// Writes data with page index enabled and small page size so multiple pages are created,
/// then reads with a PK equality predicate and verifies only matching rows are returned.
TEST_P(WriteAndReadInteTest, TestPKWithParquetPageIndexFilter) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" || file_system != "local") {
        return;
    }

    auto test_dir = UniqueTestDirectory::Create("local");
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::TARGET_FILE_SIZE, "1048576"},
        {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, "local"},
        // Force exactly one row per parquet page. Parquet's writer checks the page
        // byte threshold only after every `write_batch_size` values, so the default
        // batch=1024 packs all rows into a single page regardless of page.size.
        // write.batch-size=1 + page.size=1 + no dictionary together guarantee that
        // every value triggers a page flush, giving ColumnIndexFilter pages whose
        // min == max == that row's value. With predicate f0="Alice", exactly one
        // page survives page pruning, so the reader emits exactly one row -- and
        // that result is attributable purely to page filtering (no row-level
        // filter is enabled below).
        {Options::WRITE_BATCH_SIZE, "1"},
        {"parquet.page.size", "1"},
        {"parquet.enable-dictionary", "false"},
        {"parquet.write.enable-page-index", "true"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir->Str(), schema, /*partition_keys=*/{"f1"},
                                            /*primary_keys=*/{"f0", "f1"}, options,
                                            /*is_streaming_mode=*/true));
    std::string table_path = test_dir->Str() + "/foo.db/bar";
    int64_t commit_identifier = 0;

    // Write data: 12 rows across 2 partitions
    std::string data_p1 = R"([
        ["Alice", "p1", 10, 1.1],
        ["Bob", "p1", 20, 2.2],
        ["Cathy", "p1", 30, 3.3],
        ["David", "p1", 40, 4.4],
        ["Emily", "p1", 50, 5.5],
        ["Frank", "p1", 60, 6.6]
    ])";
    std::string data_p2 = R"([
        ["Grace", "p2", 70, 7.7],
        ["Helen", "p2", 80, 8.8],
        ["Ivan", "p2", 90, 9.9],
        ["Jack", "p2", 100, 10.1],
        ["Kate", "p2", 110, 11.2],
        ["Lucy", "p2", 120, 12.3]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_p1,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data_p1,
                                    /*partition_map=*/{{"f1", "p1"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_p2,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data_p2,
                                    /*partition_map=*/{{"f1", "p2"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_1,
                         helper->WriteAndCommit(std::move(batch_p1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batch_p2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // Scan with PK predicate: f0 = "Alice"
    std::string literal_str = "Alice";
    auto predicate = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, literal_str.data(), literal_str.size()));

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_MODE, StartupMode::LatestFull().ToString())
        .SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 2);
    ASSERT_FALSE(result_plan->Splits().empty());

    // Read with predicate but WITHOUT EnablePredicateFilter -- so any narrowing
    // of the result is attributable to split/file/RG/page pruning, not to a
    // post-read row-level filter. This is what makes the exact assertion below
    // meaningful as a check that page-index filtering is wired and working.
    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // Expected: p2 file is pruned by file-level min/max key stats (f0 range
    // [Grace, Lucy] doesn't overlap "Alice"). Inside p1's file, write.batch-size=1
    // + page.size=1 produces one row per page, so page-index filter keeps only
    // the page whose min == max == "Alice" -- one row.
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto expected_data_type = arrow::struct_(fields_with_row_kind);
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(expected_data_type, R"([
[0, "Alice", "p1", 10, 1.1]
])")
            .ValueOrDie());
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

/// End-to-end test for parquet page-level filtering on an append-only table.
/// Append-only tables read parquet files directly without PK merge, so the result
/// reflects exactly what survives row-group and page-index pruning.
TEST_P(WriteAndReadInteTest, TestAppendWithParquetPageIndexFilter) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" || file_system != "local") {
        return;
    }

    auto test_dir = UniqueTestDirectory::Create("local");
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::TARGET_FILE_SIZE, "1048576"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
        // Force exactly one row per parquet page (see the PK variant for why these
        // three options together are required). With one row per page,
        // ColumnIndexFilter keeps only the page whose min == max == "Alice", and
        // without row-level filter the reader output is precisely that one row.
        {Options::WRITE_BATCH_SIZE, "1"},
        {"parquet.page.size", "1"},
        {"parquet.enable-dictionary", "false"},
        {"parquet.write.enable-page-index", "true"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir->Str(), schema, /*partition_keys=*/{"f1"},
                                            /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/true));
    std::string table_path = test_dir->Str() + "/foo.db/bar";
    int64_t commit_identifier = 0;

    // Write data: 12 rows across 2 partitions.
    std::string data_p1 = R"([
        ["Alice", "p1", 10, 1.1],
        ["Bob", "p1", 20, 2.2],
        ["Cathy", "p1", 30, 3.3],
        ["David", "p1", 40, 4.4],
        ["Emily", "p1", 50, 5.5],
        ["Frank", "p1", 60, 6.6]
    ])";
    std::string data_p2 = R"([
        ["Grace", "p2", 70, 7.7],
        ["Helen", "p2", 80, 8.8],
        ["Ivan", "p2", 90, 9.9],
        ["Jack", "p2", 100, 10.1],
        ["Kate", "p2", 110, 11.2],
        ["Lucy", "p2", 120, 12.3]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_p1,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data_p1,
                                    /*partition_map=*/{{"f1", "p1"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_p2,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data_p2,
                                    /*partition_map=*/{{"f1", "p2"}}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_1,
                         helper->WriteAndCommit(std::move(batch_p1), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batch_p2), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    // Predicate: f0 = "Alice"
    std::string literal_str = "Alice";
    auto predicate = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, literal_str.data(), literal_str.size()));

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::SCAN_MODE, StartupMode::LatestFull().ToString())
        .SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_EQ(result_plan->SnapshotId().value(), 2);
    ASSERT_FALSE(result_plan->Splits().empty());

    // Read with predicate but WITHOUT EnablePredicateFilter, so the narrowing
    // observed below is attributable to page-index filtering rather than a
    // post-read row-level filter.
    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    // Partition p2's row groups don't overlap "Alice" (min/max f0 in [Grace, Lucy]),
    // so the whole file is skipped. Within p1, page-index pruning narrows down to the
    // page containing "Alice". With no PK merge, the result is exactly that one row.
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto expected_data_type = arrow::struct_(fields_with_row_kind);
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(expected_data_type, R"([
[0, "Alice", "p1", 10, 1.1]
])")
            .ValueOrDie());
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(WriteAndReadInteTest, TestAppendWithParquetMetadataCache) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" || file_system != "local") {
        return;
    }

    auto test_dir = UniqueTestDirectory::Create("local");
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32())};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},      {Options::FILE_FORMAT, "parquet"},
        {Options::TARGET_FILE_SIZE, "1048576"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));
    std::string table_path = test_dir->Str() + "/foo.db/bar";

    std::string data = R"([
        ["banana", 2],
        ["dog", 1],
        ["lucy", 14],
        ["mouse", 100]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto expected_data_type = arrow::struct_(fields_with_row_kind);
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(expected_data_type, R"([
            [0, "banana", 2],
            [0, "dog", 1],
            [0, "lucy", 14],
            [0, "mouse", 100]
        ])")
            .ValueOrDie());

    auto cache =
        std::make_shared<CountingRoutingCache>(CacheKind::DATA_FILE_FOOTER, 128 * 1024 * 1024);
    auto read_once = [&]() -> Result<bool> {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::SCAN_MODE, StartupMode::LatestFull().ToString());
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, table_scan->CreatePlan());
        if (result_plan->SnapshotId() != std::optional<int64_t>(1)) {
            return Status::Invalid("unexpected snapshot id");
        }
        if (result_plan->Splits().empty()) {
            return Status::Invalid("no splits found");
        }

        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.WithCache(cache);
        PAIMON_ASSIGN_OR_RAISE(auto read_context, read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(auto read_result,
                               ReadResultCollector::CollectResult(batch_reader.get()));
        if (!read_result) {
            return Status::Invalid("read result is null");
        }
        return expected->Equals(read_result);
    };

    ASSERT_OK_AND_ASSIGN(bool first_success, read_once());
    ASSERT_TRUE(first_success);
    ASSERT_EQ(1, cache->GetCount());
    ASSERT_EQ(1, cache->SupplierCallCount());
    ASSERT_EQ(1, cache->Size());
    ASSERT_EQ(CacheKind::DATA_FILE_FOOTER, cache->LastKind());

    ASSERT_OK_AND_ASSIGN(bool second_success, read_once());
    ASSERT_TRUE(second_success);
    ASSERT_EQ(2, cache->GetCount());
    ASSERT_EQ(1, cache->SupplierCallCount());
    ASSERT_EQ(1, cache->Size());
    ASSERT_EQ(CacheKind::DATA_FILE_FOOTER, cache->LastKind());
}

TEST_P(WriteAndReadInteTest, TestAppendSharedShreddingMap) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "2"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data = R"([
        [1, [["a", 10], ["b", 20]]],
        [2, [["c", 30], ["a", 40], ["b", 50]]],
        [3, null],
        [4, [["d", 60], ["a", null], ["c", 70]]]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
        [0, 1, [["a", 10], ["b", 20]]],
        [0, 2, [["a", 40], ["b", 50], ["c", 30]]],
        [0, 3, null],
        [0, 4, [["a", null], ["c", 70], ["d", 60]]]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestMapSharedShreddingColumnPlacementPolicies) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("plain_metrics", map_type),
        arrow::field("sequential_metrics", map_type),
        arrow::field("lru_metrics", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.plain_metrics.map.storage-layout", "shared-shredding"},
        {"fields.plain_metrics.map.shared-shredding.max-columns", "3"},
        {"fields.plain_metrics.map.shared-shredding.column-placement-policy", "plain"},
        {"fields.sequential_metrics.map.storage-layout", "shared-shredding"},
        {"fields.sequential_metrics.map.shared-shredding.max-columns", "3"},
        {"fields.sequential_metrics.map.shared-shredding.column-placement-policy", "sequential"},
        {"fields.lru_metrics.map.storage-layout", "shared-shredding"},
        {"fields.lru_metrics.map.shared-shredding.max-columns", "3"},
        {"fields.lru_metrics.map.shared-shredding.column-placement-policy", "lru"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, arrow::schema(fields), /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", 10], ["b", 20], ["c", 30]], [["a", 10], ["c", 30], ["d", 60]], [["a", 10], ["b", 20], ["c", 30]]],
                [2, [["a", 40], ["b", 50]], [["a", 40], ["c", 50]], [["a", 40], ["b", 50]]],
                [3, [["d", 60]], [["b", 60]], [["d", 60]]],
                [4, [["a", 70], ["b", 80], ["c", 90], ["d", 100]], [["a", 70], ["b", 80], ["c", 90], ["d", 100]], [["a", 70], ["b", 80], ["c", 90], ["d", 100]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto files, CurrentDataFiles(options));
    ASSERT_EQ(1, files.size());
    auto check_meta = [&](const std::string& field_name,
                          const std::string& overflow_name) -> Status {
        PAIMON_ASSIGN_OR_RAISE(MapSharedShreddingFieldMeta meta,
                               ReadShreddingMeta(files[0], field_name, options));
        if (meta.name_to_id.size() != 4 || meta.num_columns != 3 || meta.max_row_width != 4 ||
            meta.overflow_field_set.size() != 1) {
            return Status::Invalid("unexpected shared-shredding metadata for ", field_name);
        }
        auto overflow_id = meta.name_to_id.find(overflow_name);
        if (overflow_id == meta.name_to_id.end() ||
            meta.overflow_field_set.count(overflow_id->second) != 1) {
            return Status::Invalid("unexpected overflow field for ", field_name);
        }
        return Status::OK();
    };
    ASSERT_OK(check_meta("plain_metrics", "d"));
    ASSERT_OK(check_meta("sequential_metrics", "b"));
    ASSERT_OK(check_meta("lru_metrics", "c"));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", 10], ["b", 20], ["c", 30]], [["a", 10], ["c", 30], ["d", 60]], [["a", 10], ["b", 20], ["c", 30]]],
                [0, 2, [["a", 40], ["b", 50]], [["a", 40], ["c", 50]], [["a", 40], ["b", 50]]],
                [0, 3, [["d", 60]], [["b", 60]], [["d", 60]]],
                [0, 4, [["a", 70], ["b", 80], ["c", 90], ["d", 100]], [["a", 70], ["b", 80], ["c", 90], ["d", 100]], [["a", 70], ["b", 80], ["c", 90], ["d", 100]]]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestAppendMapSharedShreddingWithPartitionAndBucket) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("dt", arrow::utf8()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "id"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "5"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, schema, /*partition_keys=*/{"dt"},
                                            /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));

    ASSERT_OK_AND_ASSIGN(
        auto p1_bucket0_first,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), R"([[1, "p1", [["a", 1]]]])",
                                    /*partition_map=*/{{"dt", "p1"}},
                                    /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(p1_bucket0_first), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(
        auto p1_bucket0_second,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), R"([[2, "p1", [["b", 2]]]])",
                                    /*partition_map=*/{{"dt", "p1"}},
                                    /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(p1_bucket0_second), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(
        auto p2_bucket1_first,
        TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                    R"([[3, "p2", [["x", 10], ["y", 20], ["z", 30], ["w", 40]]]])",
                                    /*partition_map=*/{{"dt", "p2"}}, /*bucket=*/1, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(p2_bucket1_first), /*commit_identifier=*/2,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_GE(data_splits.size(), 2);
    std::string expected_data = R"([
        [0, 1, "p1", [["a", 1]]],
        [0, 2, "p1", [["b", 2]]],
        [0, 3, "p2", [["w", 40], ["x", 10], ["y", 20], ["z", 30]]]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);

    ASSERT_OK_AND_ASSIGN(auto files, CurrentDataFiles(options));
    ASSERT_EQ(3, files.size());
    std::vector<std::pair<std::string, std::shared_ptr<DataFileMeta>>> p1_bucket0_files;
    std::vector<std::pair<std::string, std::shared_ptr<DataFileMeta>>> p2_bucket1_files;
    for (const auto& file : files) {
        if (file.first.find("dt=p1/bucket-0") != std::string::npos) {
            p1_bucket0_files.push_back(file);
        } else if (file.first.find("dt=p2/bucket-1") != std::string::npos) {
            p2_bucket1_files.push_back(file);
        }
    }
    ASSERT_EQ(2, p1_bucket0_files.size());
    ASSERT_EQ(1, p2_bucket1_files.size());

    ASSERT_OK_AND_ASSIGN(auto p1_first_meta,
                         ReadShreddingMeta(p1_bucket0_files[0], "tags", options));
    ASSERT_EQ(5, p1_first_meta.num_columns);
    ASSERT_EQ(1, p1_first_meta.max_row_width);

    ASSERT_OK_AND_ASSIGN(auto p1_second_meta,
                         ReadShreddingMeta(p1_bucket0_files[1], "tags", options));
    ASSERT_EQ(5, p1_second_meta.num_columns);
    ASSERT_EQ(1, p1_second_meta.max_row_width);

    ASSERT_OK_AND_ASSIGN(auto p2_first_meta,
                         ReadShreddingMeta(p2_bucket1_files[0], "tags", options));
    ASSERT_EQ(5, p2_first_meta.num_columns);
    ASSERT_EQ(4, p2_first_meta.max_row_width);
}

TEST_P(WriteAndReadInteTest, TestAppendMapSharedShreddingWithPredicate) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1048576"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {Options::WRITE_BATCH_SIZE, "1"},
        {"parquet.page.size", "1"},
        {"parquet.enable-dictionary", "false"},
        {"parquet.write.enable-page-index", "true"},
        {"parquet.write.max-row-group-length", "1"},
        {"parquet.read.enable-page-index-filter", "true"},
        {"orc.stripe.size", "1"},
        {"orc.row.index.stride", "1"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    (void)helper;

    std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
    WriteContextBuilder write_context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         write_context_builder.SetOptions(options).Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    auto write_one_row = [&](const std::string& data) -> Status {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> batch,
                               TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                           /*partition_map=*/{}, /*bucket=*/0, {}));
        return file_store_write->Write(std::move(batch));
    };

    ASSERT_OK(write_one_row(R"([[1, [["a", 10], ["b", 20]]]])"));
    ASSERT_OK(write_one_row(R"([[12, [["c", 31], ["d", 41]]]])"));
    ASSERT_OK(write_one_row(R"([[21, [["e", 50], ["f", 60]]]])"));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false,
                                                         /*commit_identifier=*/0));
    ASSERT_OK(file_store_write->Close());

    CommitContextBuilder commit_context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_context_builder.SetOptions(options).Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(commit_msgs, /*commit_identifier=*/0));

    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"id",
                                                   FieldType::INT, Literal(10));
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.SetOptions(options)
        .AddOption(Options::SCAN_MODE, StartupMode::LatestFull().ToString())
        .SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_FALSE(result_plan->Splits().empty());

    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.SetOptions(options).SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto expected_type = arrow::struct_(fields_with_row_kind);
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, R"([
        [0, 12, [["c", 31], ["d", 41]]],
        [0, 21, [["e", 50], ["f", 60]]]
    ])")
            .ValueOrDie());
    ASSERT_TRUE(expected->Equals(actual)) << actual->ToString();
}

TEST_P(WriteAndReadInteTest, TestMapSharedShreddingNewWriterStartsWithMaxColumnCount) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("metrics", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "id"},
        {Options::FILE_SYSTEM, file_system},
        {Options::WRITE_ONLY, "true"},
        {"fields.metrics.map.storage-layout", "shared-shredding"},
        {"fields.metrics.map.shared-shredding.max-columns", "8"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, arrow::schema(fields), /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    ASSERT_OK_AND_ASSIGN(
        auto batch_v0, TestHelper::MakeRecordBatch(arrow::struct_(fields), R"([[1, [["a", 11]]]])",
                                                   /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(
        auto batch_v1, TestHelper::MakeRecordBatch(arrow::struct_(fields), R"([[2, [["b", 22]]]])",
                                                   /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto files, CurrentDataFiles(options));
    ASSERT_EQ(2, files.size());
    ASSERT_OK_AND_ASSIGN(auto first_meta, ReadShreddingMeta(files[0], "metrics", options));
    ASSERT_EQ(8, first_meta.num_columns);
    ASSERT_EQ(1, first_meta.max_row_width);
    ASSERT_OK_AND_ASSIGN(auto second_meta, ReadShreddingMeta(files[1], "metrics", options));
    ASSERT_EQ(8, second_meta.num_columns);
    ASSERT_EQ(1, second_meta.max_row_width);

    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("metrics", map_type),
    });
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(expected_type, splits,
                                                                  R"([
        [0, 1, [["a", 11]]],
        [0, 2, [["b", 22]]]
    ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestMapSharedShreddingAdaptsAcrossRollingFiles) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("metrics", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1048576"},
        {Options::TARGET_FILE_ROW_NUM, "2"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "id"},
        {Options::FILE_SYSTEM, file_system},
        {Options::WRITE_ONLY, "true"},
        {"fields.metrics.map.storage-layout", "shared-shredding"},
        {"fields.metrics.map.shared-shredding.max-columns", "8"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, arrow::schema(fields), /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::vector<std::unique_ptr<RecordBatch>> batches;
    ASSERT_OK_AND_ASSIGN(
        auto first_batch,
        TestHelper::MakeRecordBatch(
            arrow::struct_(fields),
            R"([[1, [["a", 11], ["b", 12]]], [2, [["c", 21], ["d", 22], ["e", 23]]]])",
            /*partition_map=*/{}, /*bucket=*/0, {}));
    batches.push_back(std::move(first_batch));
    ASSERT_OK_AND_ASSIGN(
        auto second_batch,
        TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                    R"([[3, [["f", 31]]], [4, [["g", 41], ["h", 42]]]])",
                                    /*partition_map=*/{}, /*bucket=*/0, {}));
    batches.push_back(std::move(second_batch));
    ASSERT_OK(helper->WriteAndCommit(std::move(batches), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto files, CurrentDataFiles(options));
    ASSERT_EQ(2, files.size());
    ASSERT_OK_AND_ASSIGN(auto first_meta, ReadShreddingMeta(files[0], "metrics", options));
    ASSERT_EQ(8, first_meta.num_columns);
    ASSERT_EQ(3, first_meta.max_row_width);
    ASSERT_OK_AND_ASSIGN(auto second_meta, ReadShreddingMeta(files[1], "metrics", options));
    ASSERT_EQ(3, second_meta.num_columns);
    ASSERT_EQ(2, second_meta.max_row_width);

    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("metrics", map_type),
    });
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(expected_type, splits,
                                                                  R"([
        [0, 1, [["a", 11], ["b", 12]]],
        [0, 2, [["c", 21], ["d", 22], ["e", 23]]],
        [0, 3, [["f", 31]]],
        [0, 4, [["g", 41], ["h", 42]]]
    ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestMapSharedShreddingSwitchMapLayoutAndUseMaxColumnsWithoutMetadata) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("metrics", map_type),
        arrow::field("labels", map_type),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "id"},
        {Options::FILE_SYSTEM, file_system},
        {Options::WRITE_ONLY, "true"},
        {"fields.labels.map.storage-layout", "shared-shredding"},
        {"fields.labels.map.shared-shredding.max-columns", "4"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields), /*partition_keys=*/{},
                                            /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));

    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
        [1, [["a", 11], ["b", 12]], [["x", 21]]]
    ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.metrics.map.storage-layout"] = "shared-shredding";
    options_v1["fields.metrics.map.shared-shredding.max-columns"] = "3";
    options_v1["fields.labels.map.storage-layout"] = "default";
    options_v1.erase("fields.labels.map.shared-shredding.max-columns");
    ASSERT_OK(
        WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1]), DataField(2, fields[2])},
                        /*highest_field_id=*/2, options_v1));

    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper, TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"),
                                                    options_v1, /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
        [2, [["c", 31]], [["y", 41], ["z", 42]]]
    ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto files, CurrentDataFiles(options_v1));
    ASSERT_EQ(2, files.size());
    ASSERT_OK_AND_ASSIGN(auto metrics_meta, ReadShreddingMeta(files[1], "metrics", options_v1));
    ASSERT_EQ(3, metrics_meta.num_columns);
    ASSERT_EQ(1, metrics_meta.max_row_width);

    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("metrics", map_type),
        arrow::field("labels", map_type),
    });
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(expected_type, splits,
                                                                  R"([
        [0, 1, [["a", 11], ["b", 12]], [["x", 21]]],
        [0, 2, [["c", 31]], [["y", 41], ["z", 42]]]
    ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestMapSharedShreddingReadAfterRenameColumn) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields_v0 = {
        arrow::field("id", arrow::int32()),
        arrow::field("metrics", map_type),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.metrics.map.storage-layout", "shared-shredding"},
        {"fields.metrics.map.shared-shredding.max-columns", "2"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields_v0),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));

    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields_v0),
                                                     R"([
        [1, [["a", 11], ["b", 12]]],
        [2, [["c", 21]]]
    ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_v1 = {
        arrow::field("id", arrow::int32()),
        arrow::field("renamed_metrics", map_type),
    };
    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1.erase("fields.metrics.map.storage-layout");
    options_v1.erase("fields.metrics.map.shared-shredding.max-columns");
    options_v1["fields.renamed_metrics.map.storage-layout"] = "shared-shredding";
    options_v1["fields.renamed_metrics.map.shared-shredding.max-columns"] = "2";
    ASSERT_OK(WriteNextSchema({DataField(0, fields_v1[0]), DataField(1, fields_v1[1])},
                              /*highest_field_id=*/1, options_v1));

    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper, TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"),
                                                    options_v1, /*is_streaming_mode=*/false));

    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("renamed_metrics", map_type),
    });
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(expected_type, splits,
                                                                  R"([
        [0, 1, [["a", 11], ["b", 12]]],
        [0, 2, [["c", 21]]]
    ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingWithSchemaEvolution) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields_v0 = {
        arrow::field("f0", map_type),
        arrow::field("f1", map_type),
        arrow::field("k1", arrow::utf8()),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.f0.map.storage-layout", "shared-shredding"},
        {"fields.f0.map.shared-shredding.max-columns", "1"},
        {"fields.f1.map.storage-layout", "shared-shredding"},
        {"fields.f1.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields_v0),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));

    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields_v0),
                                                     R"([
                [[["a", 10], ["z", 11]], [["b", 20]], "old-1"],
                [[["a", 12]], [["b", 21], ["y", 22]], "old-2"]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_v1 = {
        arrow::field("f0", map_type),
        arrow::field("f1", map_type),
        arrow::field("k2", arrow::utf8()),
        arrow::field("f2", map_type),
    };
    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.f2.map.storage-layout"] = "shared-shredding";
    options_v1["fields.f2.map.shared-shredding.max-columns"] = "1";
    std::vector<DataField> data_fields_v1 = {
        DataField(0, fields_v1[0]),
        DataField(1, fields_v1[1]),
        DataField(2, fields_v1[2]),
        DataField(3, fields_v1[3]),
    };
    ASSERT_OK(WriteNextSchema(data_fields_v1, /*highest_field_id=*/3, options_v1));

    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options_v1,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields_v1),
                                                     R"([
                [[["a", 30], ["z", 31]], [["b", 40]], "new-1", [["c", 50], ["x", 51]]],
                [[["a", 32]], [["b", 41]], "new-2", [["c", 52]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("f0", map_type),
        arrow::field("f2", map_type),
        arrow::field("k2", arrow::utf8()),
    });
    ASSERT_OK_AND_ASSIGN(bool success,
                         ReadAndCheckProjectedResult(options_v1, {"f0", "f2", "k2"}, expected_type,
                                                     R"([
                [0, [["a", 10], ["z", 11]], null, "old-1"],
                [0, [["a", 12]], null, "old-2"],
                [0, [["a", 30], ["z", 31]], [["c", 50], ["x", 51]], "new-1"],
                [0, [["a", 32]], [["c", 52]], "new-2"]
            ])"));
    ASSERT_TRUE(success);
}

// Verify storage-layout evolution: default->shared-shredding.
TEST_P(WriteAndReadInteTest, TestMapStorageLayoutDefaultToSharedShredding) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system}, {"fields.tags.map.storage-layout", "default"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([[1, [["a", 10], ["z", 11]]], [2, null]])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.tags.map.storage-layout"] = "shared-shredding";
    options_v1["fields.tags.map.shared-shredding.max-columns"] = "1";
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));
    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options_v1,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1, TestHelper::MakeRecordBatch(
                                            arrow::struct_(fields),
                                            R"([[3, [["a", 30], ["z", 31]]], [4, [["a", 40]]]])",
                                            /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", 10], ["z", 11]]],
                [0, 2, null],
                [0, 3, [["a", 30], ["z", 31]]],
                [0, 4, [["a", 40]]]
            ])"));
    ASSERT_TRUE(success);
}

// Verify storage-layout evolution: shared-shredding->default.
TEST_P(WriteAndReadInteTest, TestMapStorageLayoutSharedShreddingToDefault) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v0, TestHelper::MakeRecordBatch(
                                            arrow::struct_(fields),
                                            R"([[1, [["a", 10], ["z", 11]]], [2, [["a", 20]]]])",
                                            /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.tags.map.storage-layout"] = "default";
    options_v1.erase("fields.tags.map.shared-shredding.max-columns");
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));
    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options_v1,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([[3, [["a", 30], ["z", 31]]], [4, null]])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", 10], ["z", 11]]],
                [0, 2, [["a", 20]]],
                [0, 3, [["a", 30], ["z", 31]]],
                [0, 4, null]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestAppendMapStorageLayoutSharedShreddingToDefaultCompaction) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "id"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/true));
    std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
    int64_t commit_identifier = 0;

    ASSERT_OK_AND_ASSIGN(auto batch_v0_file1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", 10], ["b", 11]]],
                [2, [["c", 20]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0_file1), commit_identifier++,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto batch_v0_file2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [3, [["d", 30], ["e", 31]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0_file2), commit_identifier++,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.tags.map.storage-layout"] = "default";
    options_v1.erase("fields.tags.map.shared-shredding.max-columns");
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));

    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper, TestHelper::Create(table_path, options_v1,
                                                    /*is_streaming_mode=*/true));
    ASSERT_OK_AND_ASSIGN(auto batch_v1_file3,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [4, [["a", 40], ["f", 41]]],
                [5, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1_file3), commit_identifier++,
                                     /*expected_commit_messages=*/std::nullopt));

    WriteContextBuilder write_context_builder(table_path, "commit_user");
    ASSERT_OK_AND_ASSIGN(
        auto write_context,
        write_context_builder.SetOptions(options_v1).WithStreamingMode(true).Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));
    ASSERT_OK(file_store_write->Compact(/*partition=*/{}, /*bucket=*/0,
                                        /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(auto compact_messages, file_store_write->PrepareCommit(
                                                    /*wait_compaction=*/true, commit_identifier));
    ASSERT_FALSE(compact_messages.empty());

    CommitContextBuilder commit_context_builder(table_path, "commit_user");
    ASSERT_OK_AND_ASSIGN(auto commit_context,
                         commit_context_builder.SetOptions(options_v1).Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(file_store_commit->Commit(compact_messages, commit_identifier));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(1, splits.size());
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", 10], ["b", 11]]],
                [0, 2, [["c", 20]]],
                [0, 3, [["d", 30], ["e", 31]]],
                [0, 4, [["a", 40], ["f", 41]]],
                [0, 5, null]
            ])"));
    ASSERT_TRUE(success);
}

// Nested map values through both selected physical columns and overflow.
TEST_P(WriteAndReadInteTest, TestSharedShreddingWithStructValue) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto value_type = arrow::struct_({
        arrow::field("name", arrow::utf8()),
        arrow::field("score", arrow::int32()),
    });
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), value_type)),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", ["alice", 10]], ["z", ["zoe", 11]]]],
                [2, [["a", ["amy", null]]]],
                [3, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", ["alice", 10]], ["z", ["zoe", 11]]]],
                [0, 2, [["a", ["amy", null]]]],
                [0, 3, null]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestMapSharedShreddingWithComplexValue) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto value_type = arrow::struct_({
        arrow::field("name", arrow::utf8()),
        arrow::field("scores", arrow::list(arrow::int32())),
        arrow::field("attrs", arrow::map(arrow::utf8(), arrow::int64())),
    });
    auto map_type = arrow::map(arrow::utf8(), value_type);
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [
                    ["a", ["alpha", [1, 2], [["ia", 10], ["ib", 20]]]],
                    ["z", ["zeta", [9], [["iz", 90]]]]
                ]],
                [2, [
                    ["a", ["amy", null, [["ia", 30]]]],
                    ["b", ["beta", [], []]]
                ]],
                [3, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool full_success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [
                    ["a", ["alpha", [1, 2], [["ia", 10], ["ib", 20]]]],
                    ["z", ["zeta", [9], [["iz", 90]]]]
                ]],
                [0, 2, [
                    ["a", ["amy", null, [["ia", 30]]]],
                    ["b", ["beta", [], []]]
                ]],
                [0, 3, null]
            ])"));
    ASSERT_TRUE(full_success);

    auto selected_keys_meta =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"z,a"});
    auto read_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
    });
    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    });
    ASSERT_OK_AND_ASSIGN(bool selected_success,
                         ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                    R"([
                [0, 1, [
                    ["z", ["zeta", [9], [["iz", 90]]]],
                    ["a", ["alpha", [1, 2], [["ia", 10], ["ib", 20]]]]
                ]],
                [0, 2, [
                    ["a", ["amy", null, [["ia", 30]]]]
                ]],
                [0, 3, null]
            ])"));
    ASSERT_TRUE(selected_success);
}

TEST_P(WriteAndReadInteTest, TestMapSharedShreddingWithAllSupportedComplexValueTypes) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    TimezoneGuard timezone_guard("Asia/Shanghai");
    // Nested TIMESTAMP_LTZ needs a timezone projection inside MAP. The Parquet reader currently
    // rejects that projection, so only ORC exercises LTZ here; Parquet still covers both timestamp
    // precisions as timezone-free timestamps.
    std::string timezone =
        file_format == "orc" ? DateTimeUtils::GetLocalTimezoneName() : std::string();
    auto value_type = arrow::struct_({
        arrow::field("bool", arrow::boolean()),
        arrow::field("tiny", arrow::int8()),
        arrow::field("small", arrow::int16()),
        arrow::field("i", arrow::int32()),
        arrow::field("big", arrow::int64()),
        arrow::field("f", arrow::float32()),
        arrow::field("d", arrow::float64()),
        arrow::field("s", arrow::utf8()),
        arrow::field("bin", arrow::binary()),
        arrow::field("compact_decimal", arrow::decimal128(10, 2)),
        arrow::field("large_decimal", arrow::decimal128(23, 5)),
        arrow::field("date", arrow::date32()),
        arrow::field("ts3", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts9", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_ltz3", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_ltz6", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ints", arrow::list(arrow::int32())),
        arrow::field("attrs", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("nested", arrow::struct_({
                                   arrow::field("name", arrow::utf8()),
                                   arrow::field("score", arrow::int32()),
                               })),
    });
    auto map_type = arrow::map(arrow::utf8(), value_type);
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("metrics", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {Options::WRITE_ONLY, "true"},
        {"orc.timestamp-ltz.legacy.type", "false"},
        {"fields.metrics.map.storage-layout", "shared-shredding"},
        {"fields.metrics.map.shared-shredding.max-columns", "2"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, arrow::schema(fields), /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [
                    ["fixed-a", [
                        true, 1, 2, 3, 4, 5.5, 6.25, "str", "bin",
                        "12345678.90", "123456789012345678.12345", 19500,
                        "2023-11-14 22:13:20.123", "2023-11-14 22:13:20.123456789",
                        "2023-11-14 22:13:20.123", "2023-11-14 22:13:20.123456",
                        [7, null, 8], [["m1", 10], ["m2", null]], ["nested", 99]
                    ]],
                    ["fixed-b", [
                        null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null
                    ]],
                    ["overflow", [
                        false, 9, 10, 11, 12, 13.5, 14.25, "overflow", "raw",
                        "-1.23", "-123456789012345678.12345", 19600,
                        "2027-01-15 08:00:00.321", "2027-01-15 08:00:00.321987654",
                        "2027-01-15 08:00:00.321", "2027-01-15 08:00:00.321987",
                        [42], [["om", 100]], ["overflow-nested", -7]
                    ]]
                ]],
                [2, null],
                [3, []]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto files, CurrentDataFiles(options));
    ASSERT_EQ(1, files.size());
    ASSERT_OK_AND_ASSIGN(auto meta, ReadShreddingMeta(files[0], "metrics", options));
    ASSERT_EQ(2, meta.num_columns);
    ASSERT_EQ(3, meta.max_row_width);

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [
                    ["fixed-a", [
                        true, 1, 2, 3, 4, 5.5, 6.25, "str", "bin",
                        "12345678.90", "123456789012345678.12345", 19500,
                        "2023-11-14 22:13:20.123", "2023-11-14 22:13:20.123456789",
                        "2023-11-14 22:13:20.123", "2023-11-14 22:13:20.123456",
                        [7, null, 8], [["m1", 10], ["m2", null]], ["nested", 99]
                    ]],
                    ["fixed-b", [
                        null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null
                    ]],
                    ["overflow", [
                        false, 9, 10, 11, 12, 13.5, 14.25, "overflow", "raw",
                        "-1.23", "-123456789012345678.12345", 19600,
                        "2027-01-15 08:00:00.321", "2027-01-15 08:00:00.321987654",
                        "2027-01-15 08:00:00.321", "2027-01-15 08:00:00.321987",
                        [42], [["om", 100]], ["overflow-nested", -7]
                    ]]
                ]],
                [0, 2, null],
                [0, 3, []]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestMapSharedShreddingStructValueSchemaEvolutionReadFails) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto tag_value_type = arrow::struct_({
        arrow::field("v", arrow::int64()),
        arrow::field("label", arrow::utf8()),
    });
    auto profile_type = arrow::struct_({
        arrow::field("name", arrow::utf8()),
        arrow::field("score", arrow::int64()),
    });
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), tag_value_type)),
        arrow::field("profile", profile_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", [10, "one"]], ["z", [11, "overflow"]]], ["alice", 100]],
                [2, [["a", [20, "two"]]], ["bob", 200]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", [10, "one"]], ["z", [11, "overflow"]]], ["alice", 100]],
                [0, 2, [["a", [20, "two"]]], ["bob", 200]]
            ])"));
    ASSERT_TRUE(success);

    std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
    SchemaManager schema_manager(dir_->GetFileSystem(), table_path);
    ASSERT_OK_AND_ASSIGN(auto schema_v0, schema_manager.ReadSchema(0));
    std::vector<DataField> fields_v0 = schema_v0->Fields();

    auto read_fields = [&](const std::vector<std::string>& field_names) -> Status {
        PAIMON_ASSIGN_OR_RAISE(auto plan, InnerScan(options));
        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetOptions(options).SetReadFieldNames(field_names);
        PAIMON_ASSIGN_OR_RAISE(auto read_context, read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(auto actual, ReadResultCollector::CollectResult(batch_reader.get()));
        (void)actual;
        return Status::OK();
    };

    auto tag_field = fields_v0[1].ArrowField();
    auto tag_map = arrow::internal::checked_pointer_cast<arrow::MapType>(tag_field->type());
    auto tag_value_struct =
        arrow::internal::checked_pointer_cast<arrow::StructType>(tag_map->item_type());

    // Simulate alter table changing the shared-shredding MAP value struct field type.
    auto changed_tag_value_type =
        arrow::map(tag_map->key_type(), tag_map->item_field()->WithType(arrow::struct_({
                                            tag_value_struct->field(0)->WithType(arrow::utf8()),
                                            tag_value_struct->field(1),
                                        })));
    std::vector<DataField> fields_with_changed_tag_value = fields_v0;
    fields_with_changed_tag_value[1] =
        DataField(fields_v0[1].Id(), tag_field->WithType(changed_tag_value_type));
    ASSERT_OK(WriteNextSchema(fields_with_changed_tag_value, schema_v0->HighestFieldId(), options));
    ASSERT_NOK_WITH_MSG(
        read_fields({"tags"}),
        "PruneDataType nested item type mismatch inside map: read string vs data int64");

    auto profile_field = fields_v0[2].ArrowField();
    auto profile_struct =
        arrow::internal::checked_pointer_cast<arrow::StructType>(profile_field->type());

    // Simulate alter table renaming a nested field inside a STRUCT column.
    std::vector<DataField> fields_with_renamed_profile_child = fields_v0;
    auto renamed_profile_type = arrow::struct_({
        profile_struct->field(0)->WithName("renamed_name"),
        profile_struct->field(1),
    });
    fields_with_renamed_profile_child[2] =
        DataField(fields_v0[2].Id(), profile_field->WithType(renamed_profile_type));
    ASSERT_OK(
        WriteNextSchema(fields_with_renamed_profile_child, schema_v0->HighestFieldId(), options));
    ASSERT_NOK_WITH_MSG(read_fields({"profile"}),
                        "name mismatch: read 'renamed_name' vs data 'name'");

    // Simulate alter table changing a nested field type inside a STRUCT column.
    std::vector<DataField> fields_with_changed_profile_child_type = fields_v0;
    auto changed_profile_type = arrow::struct_({
        profile_struct->field(0),
        profile_struct->field(1)->WithType(arrow::utf8()),
    });
    fields_with_changed_profile_child_type[2] =
        DataField(fields_v0[2].Id(), profile_field->WithType(changed_profile_type));
    ASSERT_OK(WriteNextSchema(fields_with_changed_profile_child_type, schema_v0->HighestFieldId(),
                              options));
    ASSERT_NOK_WITH_MSG(read_fields({"profile"}),
                        "PruneDataType nested field type mismatch for 'score': read string vs "
                        "data int64");
}

// Keep ORC lazy dictionary decoding enabled across a default -> shared-shredding schema change.
// The test inspects every user-visible batch directly, because ReadResultCollector would otherwise
// decode dictionary arrays and hide a type mismatch between old and new files.
TEST_P(WriteAndReadInteTest, TestOrcDictionaryLazyDecodingWithSharedShredding) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},       {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},       {"fields.tags.map.storage-layout", "default"},
        {"orc.read.enable-lazy-decoding", "true"}, {"orc.dictionary-key-size-threshold", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", "red"], ["z", "blue"]]],
                [2, [["a", "red"], ["z", "green"]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options;
    options_v1["fields.tags.map.storage-layout"] = "shared-shredding";
    options_v1["fields.tags.map.shared-shredding.max-columns"] = "1";
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));

    helper.reset();
    std::string table_path = PathUtil::JoinPath(test_dir_, "foo.db/bar");
    ASSERT_OK_AND_ASSIGN(helper, TestHelper::Create(table_path, options_v1,
                                                    /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [3, [["a", "red"], ["z", "yellow"]]],
                [4, [["a", "red"]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", "red"], ["z", "blue"]]],
                [0, 2, [["a", "red"], ["z", "green"]]],
                [0, 3, [["a", "red"], ["z", "yellow"]]],
                [0, 4, [["a", "red"]]]
            ])"));
    ASSERT_TRUE(success);
}

// Verify shared-shredding in the PK read path.
TEST_P(WriteAndReadInteTest, TestPkSharedShreddingMap) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    arrow::FieldVector fields = {
        arrow::field("pk", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{"pk"}, options,
                                            /*is_streaming_mode=*/true));

    ASSERT_OK_AND_ASSIGN(auto batch_0, TestHelper::MakeRecordBatch(
                                           arrow::struct_(fields),
                                           R"([[1, [["a", 10], ["z", 11]]], [2, [["b", 20]]]])",
                                           /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto batch_1, TestHelper::MakeRecordBatch(
                                           arrow::struct_(fields),
                                           R"([[1, [["a", 100], ["z", 101]]], [3, [["c", 30]]]])",
                                           /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, [["a", 100], ["z", 101]]],
                [0, 2, [["b", 20]]],
                [0, 3, [["c", 30]]]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingPartialKeyRecallWithOverflow) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "2"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));

    // max-columns=2, so keys are allocated in insertion order:
    // Row 1: {a:1, b:2, c:3, d:4} -> physical cols: a, b; overflow: c, d
    // Row 2: {a:10, b:20}          -> physical cols: a, b; no overflow
    // Row 3: null
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", 1], ["b", 2], ["c", 3], ["d", 4]]],
                [2, [["a", 10], ["b", 20]]],
                [3, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    // Sub-case 1: read only key "a" (in physical column)
    {
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"a"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, [["a", 1]]],
                    [0, 2, [["a", 10]]],
                    [0, 3, null]
                ])"));
        ASSERT_TRUE(success);
    }

    // Sub-case 2: read only key "c" (in overflow)
    {
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"c"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, [["c", 3]]],
                    [0, 2, []],
                    [0, 3, null]
                ])"));
        ASSERT_TRUE(success);
    }

    // Sub-case 3: read keys "c" and "a" (cross physical + overflow), order follows user request
    {
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"c,a"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, [["c", 3], ["a", 1]]],
                    [0, 2, [["a", 10]]],
                    [0, 3, null]
                ])"));
        ASSERT_TRUE(success);
    }
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingPartialKeyRecallWithNullOrMissingKey) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));

    // Row 1: tags=null
    // Row 2: tags={b:2} (no key "a")
    // Row 3: tags={a:30, b:40}
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, null],
                [2, [["b", 2]]],
                [3, [["a", 30], ["b", 40]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    // Sub-case 1: row with null tags, request key "a" -> tags should be null
    // Sub-case 2: row without key "a", request key "a" -> key "a" maps to null value
    // Sub-case 3: row with key "a", request key "a" -> normal value
    {
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"a"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, null],
                    [0, 2, []],
                    [0, 3, [["a", 30]]]
                ])"));
        ASSERT_TRUE(success);
    }

    // Sub-case 4: request a key that was never written ("nonexistent") -> all rows have null value
    {
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"nonexistent"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, null],
                    [0, 2, []],
                    [0, 3, []]
                ])"));
        ASSERT_TRUE(success);
    }
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingPartialKeyRecallMultipleColumns) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
        arrow::field("metrics", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
        {"fields.metrics.map.storage-layout", "shared-shredding"},
        {"fields.metrics.map.shared-shredding.max-columns", "3"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));

    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", 1], ["b", 2]], [["x", 100], ["y", 200]]],
                [2, [["a", 10], ["c", 30]], [["x", 1000], ["z", 3000]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    // Sub-case 1: partial key recall on both columns independently
    {
        auto tags_meta = arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"a"});
        auto metrics_meta = arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"x"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(tags_meta),
            arrow::field("metrics", map_type)->WithMetadata(metrics_meta),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
            arrow::field("metrics", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, [["a", 1]], [["x", 100]]],
                    [0, 2, [["a", 10]], [["x", 1000]]]
                ])"));
        ASSERT_TRUE(success);
    }

    // Sub-case 2: partial key recall on tags + full recall on metrics
    {
        auto tags_meta = arrow::KeyValueMetadata::Make({"paimon.map.selected-keys"}, {"a,b"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(tags_meta),
            arrow::field("metrics", map_type),
        });
        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
            arrow::field("metrics", map_type),
        });
        ASSERT_OK_AND_ASSIGN(bool success,
                             ReadAndCheckWithReadSchema(options, read_schema, expected_type,
                                                        R"([
                    [0, 1, [["a", 1], ["b", 2]], [["x", 100], ["y", 200]]],
                    [0, 2, [["a", 10]], [["x", 1000], ["z", 3000]]]
                ])"));
        ASSERT_TRUE(success);
    }
}

TEST_P(WriteAndReadInteTest, TestMapStorageLayoutDefaultToSharedShreddingPartialKeyRecall) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},  {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system}, {"fields.tags.map.storage-layout", "default"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", 10], ["b", 20]]],
                [2, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.tags.map.storage-layout"] = "shared-shredding";
    options_v1["fields.tags.map.shared-shredding.max-columns"] = "1";
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));
    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options_v1,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [3, [["a", 30], ["z", 31]]],
                [4, [["z", 41]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    auto selected_keys_meta = arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a"});
    auto read_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
    });
    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    });
    ASSERT_OK_AND_ASSIGN(bool success,
                         ReadAndCheckWithReadSchema(options_v1, read_schema, expected_type,
                                                    R"([
                [0, 1, [["a", 10]]],
                [0, 2, null],
                [0, 3, [["a", 30]]],
                [0, 4, []]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestMapStorageLayoutSharedShreddingToDefaultPartialKeyRecall) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options_v0 = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options_v0 = AddOptionsForJindo(options_v0);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options_v0,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, [["a", 10], ["z", 11]]],
                [2, [["z", 21]]]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v0), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    std::map<std::string, std::string> options_v1 = options_v0;
    options_v1["fields.tags.map.storage-layout"] = "default";
    options_v1.erase("fields.tags.map.shared-shredding.max-columns");
    ASSERT_OK(WriteNextSchema({DataField(0, fields[0]), DataField(1, fields[1])},
                              /*highest_field_id=*/1, options_v1));
    helper.reset();
    ASSERT_OK_AND_ASSIGN(helper,
                         TestHelper::Create(PathUtil::JoinPath(test_dir_, "foo.db/bar"), options_v1,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch_v1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [3, [["a", 30], ["z", 31]]],
                [4, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_v1), /*commit_identifier=*/1,
                                     /*expected_commit_messages=*/std::nullopt));

    auto selected_keys_meta = arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a"});
    auto read_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
    });
    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    });
    ASSERT_OK_AND_ASSIGN(bool success,
                         ReadAndCheckWithReadSchema(options_v1, read_schema, expected_type,
                                                    R"([
                [0, 1, [["a", 10]]],
                [0, 2, []],
                [0, 3, [["a", 30]]],
                [0, 4, null]
            ])"));
    ASSERT_TRUE(success);
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingDuplicateSelectedKeys) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch, TestHelper::MakeRecordBatch(
                                         arrow::struct_(fields), R"([[1, [["a", 10], ["b", 20]]]])",
                                         /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    auto selected_keys_meta =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a,a"});
    auto read_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
    });
    auto expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    });
    ASSERT_NOK_WITH_MSG(ReadAndCheckWithReadSchema(options, read_schema, expected_type, "[]"),
                        "Duplicate selected key 'a'");
}

TEST_P(WriteAndReadInteTest, TestSharedShreddingAllNullMapColumn) {
    auto [file_format, file_system] = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, file_system},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    if (file_system == "jindo") {
        options = AddOptionsForJindo(options);
    }
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(auto batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
                [1, null],
                [2, null],
                [3, null]
            ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                     /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector expected_fields = fields;
    expected_fields.insert(expected_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(auto splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(arrow::struct_(expected_fields), splits,
                                                    R"([
                [0, 1, null],
                [0, 2, null],
                [0, 3, null]
            ])"));
    ASSERT_TRUE(success);
}

INSTANTIATE_TEST_SUITE_P(FileFormatAndFileSystem, WriteAndReadInteTest,
                         ::testing::ValuesIn(GetTestValuesForWriteAndReadInteTest()));

}  // namespace paimon::test
