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

#include "paimon/core/postpone/postpone_bucket_writer.h"

#include <cstddef>
#include <map>
#include <optional>
#include <variant>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/fs/external_path_provider.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::test {
class PostponeBucketWriterTest : public ::testing::Test,
                                 public ::testing::WithParamInterface<std::string> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        file_system_ = std::make_shared<LocalFileSystem>();
        value_fields_ = {DataField(0, arrow::field("f0", arrow::utf8())),
                         DataField(1, arrow::field("f1", arrow::int32())),
                         DataField(2, arrow::field("f2", arrow::int32())),
                         DataField(3, arrow::field("f3", arrow::float64()))};
        value_schema_ = DataField::ConvertDataFieldsToArrowSchema(value_fields_);
        value_type_ = DataField::ConvertDataFieldsToArrowStructType(value_fields_);
        primary_keys_ = {"f0"};
        std::vector<DataField> write_fields = {SpecialFields::SequenceNumber(),
                                               SpecialFields::ValueKind()};
        write_fields.insert(write_fields.end(), value_fields_.begin(), value_fields_.end());
        write_type_ = DataField::ConvertDataFieldsToArrowStructType(write_fields);
    }
    void TearDown() override {}

    void WriteBatch(const std::shared_ptr<arrow::Array>& array,
                    const std::vector<RecordBatch::RowKind>& row_kinds,
                    PostponeBucketWriter* writer) const {
        ::ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        RecordBatchBuilder batch_builder(&c_array);
        batch_builder.SetRowKinds(row_kinds);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.Finish());
        ASSERT_OK(writer->Write(std::move(batch)));
    }

    void CheckFileContent(const std::string& file_format_str, const std::string& data_file_name,
                          const std::shared_ptr<arrow::DataType>& file_schema,
                          const std::shared_ptr<arrow::ChunkedArray>& expected_array) const {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(data_file_name));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileFormat> file_format,
                             FileFormatFactory::Get(file_format_str, {}));
        ASSERT_OK_AND_ASSIGN(auto reader_builder,
                             file_format->CreateReaderBuilder(/*batch_size=*/10));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, reader_builder->Build(input_stream));
        auto c_schema = std::make_unique<::ArrowSchema>();
        ASSERT_TRUE(arrow::ExportType(*file_schema, c_schema.get()).ok());
        ASSERT_OK(batch_reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                              /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                             ReadResultCollector::CollectResult(batch_reader.get()));
        ASSERT_TRUE(expected_array->Equals(result_array)) << result_array->ToString() << "\n != \n"
                                                          << expected_array->ToString();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> file_system_;
    std::vector<DataField> value_fields_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<arrow::DataType> value_type_;
    std::vector<std::string> primary_keys_;
    std::shared_ptr<arrow::DataType> write_type_;
};

std::vector<std::string> GetTestValuesForPostponeBucketWriterTest() {
    std::vector<std::string> values = {"parquet"};
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc");
#endif
#ifdef PAIMON_ENABLE_AVRO
    values.emplace_back("avro");
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormat, PostponeBucketWriterTest,
                         ::testing::ValuesIn(GetTestValuesForPostponeBucketWriterTest()));

TEST_P(PostponeBucketWriterTest, TestSimple) {
    auto file_format = GetParam();
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, file_format}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), file_format, options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto postpone_bucket_writer = std::make_shared<PostponeBucketWriter>(
        primary_keys_, path_factory, /*schema_id=*/1, value_schema_, options, pool_);

    // write batch
    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 14.1],
      ["Paul", 40, 2, null],
      ["Lucy", 30, 3, 15.1],
      ["Alice", 10, 0, 13.1]
    ])")
            .ValueOrDie();
    WriteBatch(array1, /*row_kinds=*/{}, postpone_bucket_writer.get());

    // prepare commit
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment,
                         postpone_bucket_writer->PrepareCommit(/*wait_compaction=*/false));
    ASSERT_OK(postpone_bucket_writer->Close());

    // check data file exist and read ok
    std::string expected_data_file_name = "data-" + uuid + "-0." + file_format;
    std::string expected_data_file_path = dir->Str() + "/" + expected_data_file_name;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> data_file_status,
                         options.GetFileSystem()->GetFileStatus(expected_data_file_path));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [-1, 0, "Lucy", 20, 1, 14.1],
      [-1, 0, "Paul", 40, 2, null],
      [-1, 0, "Lucy", 30, 3, 15.1],
      [-1, 0, "Alice", 10, 0, 13.1]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(file_format, expected_data_file_path, write_type_, expected_array);

    // check data file meta
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(1, commit_increment.GetNewFilesIncrement().NewFiles().size());
    auto expected_data_file_meta = std::make_shared<DataFileMeta>(
        expected_data_file_name, /*file_size=*/data_file_status->GetLen(), /*row_count=*/4,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Lucy")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Alice")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, {-1}, pool_.get()),
        /*value_stats=*/SimpleStats::EmptyStats(),
        /*min_sequence_number=*/-1, /*max_sequence_number=*/-1, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/commit_increment.GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement expected_data_increment({expected_data_file_meta}, /*deleted_files=*/{},
                                          /*changelog_files=*/{});
    ASSERT_EQ(expected_data_increment, commit_increment.GetNewFilesIncrement());
}

TEST_P(PostponeBucketWriterTest, TestNestedType) {
    auto file_format = GetParam();
    arrow::FieldVector fields = {
        arrow::field("key", arrow::utf8()),
        arrow::field("f0", arrow::list(arrow::struct_(
                               {field("a", arrow::int64()), field("b", arrow::boolean())}))),
        arrow::field("f1", arrow::map(arrow::struct_({field("a", arrow::int64()),
                                                      field("b", arrow::boolean())}),
                                      arrow::boolean()))};
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, file_format}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), file_format, options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto postpone_bucket_writer = std::make_shared<PostponeBucketWriter>(
        std::vector<std::string>{"key"}, path_factory, /*schema_id=*/1, arrow::schema(fields),
        options, pool_);

    // write batch
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        ["Lucy", [null, [1, true], null], [[[1, true], true]]],
        ["Bob", [[2, false], null], null],
        ["David", [[2, false], [3, true], [4, null]], [[[1, true], true], [[5, false], null]]],
        ["Alice", null, null]
    ])")
                      .ValueOrDie();
    ASSERT_TRUE(array1);
    WriteBatch(array1, /*row_kinds=*/{}, postpone_bucket_writer.get());

    // prepare commit
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment,
                         postpone_bucket_writer->PrepareCommit(/*wait_compaction=*/false));
    ASSERT_OK(postpone_bucket_writer->Close());

    // check data file exist and read ok
    std::string expected_data_file_name = "data-" + uuid + "-0." + file_format;
    std::string expected_data_file_path = dir->Str() + "/" + expected_data_file_name;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> data_file_status,
                         options.GetFileSystem()->GetFileStatus(expected_data_file_path));

    arrow::FieldVector write_fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                       arrow::field("_VALUE_KIND", arrow::int8())};
    write_fields.insert(write_fields.end(), fields.begin(), fields.end());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto write_type = arrow::struct_(write_fields);
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type, {R"([
        [-1, 0, "Lucy", [null, [1, true], null], [[[1, true], true]]],
        [-1, 0, "Bob", [[2, false], null], null],
        [-1, 0, "David", [[2, false], [3, true], [4, null]], [[[1, true], true], [[5, false], null]]],
        [-1, 0, "Alice", null, null]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(file_format, expected_data_file_path, write_type, expected_array);

    // check data file meta
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(1, commit_increment.GetNewFilesIncrement().NewFiles().size());
    auto expected_data_file_meta = std::make_shared<DataFileMeta>(
        expected_data_file_name, /*file_size=*/data_file_status->GetLen(), /*row_count=*/4,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Lucy")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Alice")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, {-1}, pool_.get()),
        /*value_stats=*/SimpleStats::EmptyStats(),
        /*min_sequence_number=*/-1, /*max_sequence_number=*/-1, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/commit_increment.GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement expected_data_increment({expected_data_file_meta}, /*deleted_files=*/{},
                                          /*changelog_files=*/{});
    ASSERT_EQ(expected_data_increment, commit_increment.GetNewFilesIncrement());
}

TEST_P(PostponeBucketWriterTest, TestWriteMultiBatch) {
    auto file_format = GetParam();
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, file_format}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), file_format, options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto postpone_bucket_writer = std::make_shared<PostponeBucketWriter>(
        primary_keys_, path_factory, /*schema_id=*/1, value_schema_, options, pool_);

    // write batch 1, batch size = 3
    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["David", 120, 11, null],
      ["Bob", 140, 12, null],
      ["Alex", 110, 10, null]
    ])")
            .ValueOrDie();
    WriteBatch(array1, /*row_kinds=*/{}, postpone_bucket_writer.get());

    // write batch 2, batch size = 4, with retracted row
    std::shared_ptr<arrow::Array> array2 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 14.1],
      ["Paul", 40, 2, null],
      ["Lucy", 30, 3, 15.1],
      ["Alice", 10, 0, 13.1]
    ])")
            .ValueOrDie();
    WriteBatch(array2, /*row_kinds=*/
               {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::UPDATE_BEFORE,
                RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE},
               postpone_bucket_writer.get());

    // write batch 3, batch size = 2
    std::shared_ptr<arrow::Array> array3 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Judy", 220, 21, 24.1],
      ["Tom", 210, 20, 23.1]
    ])")
            .ValueOrDie();
    WriteBatch(array3, /*row_kinds=*/{}, postpone_bucket_writer.get());

    // prepare commit
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment,
                         postpone_bucket_writer->PrepareCommit(/*wait_compaction=*/false));
    ASSERT_OK(postpone_bucket_writer->Close());

    // check data file exist and read ok
    std::string expected_data_file_name = "data-" + uuid + "-0." + file_format;
    std::string expected_data_file_path = dir->Str() + "/" + expected_data_file_name;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> data_file_status,
                         options.GetFileSystem()->GetFileStatus(expected_data_file_path));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [-1, 0, "David", 120, 11, null],
      [-1, 0, "Bob", 140, 12, null],
      [-1, 0, "Alex", 110, 10, null],
      [-1, 0, "Lucy", 20, 1, 14.1],
      [-1, 1, "Paul", 40, 2, null],
      [-1, 2, "Lucy", 30, 3, 15.1],
      [-1, 3, "Alice", 10, 0, 13.1],
      [-1, 0, "Judy", 220, 21, 24.1],
      [-1, 0, "Tom", 210, 20, 23.1]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(file_format, expected_data_file_path, write_type_, expected_array);

    // check data file meta
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(1, commit_increment.GetNewFilesIncrement().NewFiles().size());
    auto expected_data_file_meta = std::make_shared<DataFileMeta>(
        expected_data_file_name, /*file_size=*/data_file_status->GetLen(), /*row_count=*/9,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Tom")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, {-1}, pool_.get()),
        /*value_stats=*/SimpleStats::EmptyStats(),
        /*min_sequence_number=*/-1, /*max_sequence_number=*/-1, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/commit_increment.GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement expected_data_increment({expected_data_file_meta}, /*deleted_files=*/{},
                                          /*changelog_files=*/{});
    ASSERT_EQ(expected_data_increment, commit_increment.GetNewFilesIncrement());
}

TEST_P(PostponeBucketWriterTest, TestMultiplePrepareCommit) {
    auto file_format = GetParam();
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, file_format},
                                               {"orc.write.enable-metrics", "true"}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), file_format, options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto postpone_bucket_writer = std::make_shared<PostponeBucketWriter>(
        primary_keys_, path_factory, /*schema_id=*/1, value_schema_, options, pool_);

    // write batch 1, batch size = 3
    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["David", 120, 11, null],
      ["Bob", 140, 12, null],
      ["Alex", 110, 10, null]
    ])")
            .ValueOrDie();
    WriteBatch(array1, /*row_kinds=*/{}, postpone_bucket_writer.get());
    // prepare commit 1
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment1,
                         postpone_bucket_writer->PrepareCommit(/*wait_compaction=*/false));
    // check metrics
    auto metrics = postpone_bucket_writer->GetMetrics();
    uint64_t write_io_count = 0;
    if (file_format == "orc") {
        ASSERT_OK_AND_ASSIGN(write_io_count, metrics->GetCounter("orc.write.io.count"));
        ASSERT_GT(write_io_count, 0);
    }

    // write batch 2, batch size = 2
    std::shared_ptr<arrow::Array> array2 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Judy", 220, 21, 24.1],
      ["Tom", 210, 20, 23.1]
    ])")
            .ValueOrDie();
    WriteBatch(array2, /*row_kinds=*/{}, postpone_bucket_writer.get());
    // prepare commit 2
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment2,
                         postpone_bucket_writer->PrepareCommit(/*wait_compaction=*/false));
    // check metrics
    metrics = postpone_bucket_writer->GetMetrics();
    if (file_format == "orc") {
        ASSERT_OK_AND_ASSIGN(uint64_t write_io_count2, metrics->GetCounter("orc.write.io.count"));
        ASSERT_GT(write_io_count2, write_io_count);
    }

    ASSERT_OK(postpone_bucket_writer->Close());

    // check data file exist and read ok
    std::string expected_data_file_name1 = "data-" + uuid + "-0." + file_format;
    std::string expected_data_file_name2 = "data-" + uuid + "-1." + file_format;

    std::string expected_data_file_dir = dir->Str() + "/";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileStatus> data_file_status1,
        options.GetFileSystem()->GetFileStatus(expected_data_file_dir + expected_data_file_name1));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileStatus> data_file_status2,
        options.GetFileSystem()->GetFileStatus(expected_data_file_dir + expected_data_file_name2));

    std::shared_ptr<arrow::ChunkedArray> expected_array1;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [-1, 0, "David", 120, 11, null],
      [-1, 0, "Bob", 140, 12, null],
      [-1, 0, "Alex", 110, 10, null]
    ])"},
                                                                         &expected_array1);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(file_format, expected_data_file_dir + expected_data_file_name1, write_type_,
                     expected_array1);

    std::shared_ptr<arrow::ChunkedArray> expected_array2;
    array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [-1, 0, "Judy", 220, 21, 24.1],
      [-1, 0, "Tom", 210, 20, 23.1]
    ])"},
                                                                    &expected_array2);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(file_format, expected_data_file_dir + expected_data_file_name2, write_type_,
                     expected_array2);

    // check data file meta
    ASSERT_TRUE(commit_increment1.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(1, commit_increment1.GetNewFilesIncrement().NewFiles().size());
    auto expected_data_file_meta1 = std::make_shared<DataFileMeta>(
        expected_data_file_name1, /*file_size=*/data_file_status1->GetLen(), /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Alex")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, {-1}, pool_.get()),
        /*value_stats=*/SimpleStats::EmptyStats(),
        /*min_sequence_number=*/-1, /*max_sequence_number=*/-1, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/commit_increment1.GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement expected_data_increment1({expected_data_file_meta1}, /*deleted_files=*/{},
                                           /*changelog_files=*/{});
    ASSERT_EQ(expected_data_increment1, commit_increment1.GetNewFilesIncrement());

    ASSERT_TRUE(commit_increment2.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(1, commit_increment2.GetNewFilesIncrement().NewFiles().size());
    auto expected_data_file_meta2 = std::make_shared<DataFileMeta>(
        expected_data_file_name2, /*file_size=*/data_file_status2->GetLen(), /*row_count=*/2,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Judy")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Tom")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, {-1}, pool_.get()),
        /*value_stats=*/SimpleStats::EmptyStats(),
        /*min_sequence_number=*/-1, /*max_sequence_number=*/-1, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/commit_increment2.GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement expected_data_increment2({expected_data_file_meta2}, /*deleted_files=*/{},
                                           /*changelog_files=*/{});
    ASSERT_EQ(expected_data_increment2, commit_increment2.GetNewFilesIncrement());
}

TEST_P(PostponeBucketWriterTest, TestPrepareCommitForEmptyData) {
    auto file_format = GetParam();
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, file_format}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), file_format, options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto postpone_bucket_writer = std::make_shared<PostponeBucketWriter>(
        primary_keys_, path_factory, /*schema_id=*/1, value_schema_, options, pool_);

    // prepare commit, without write
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment,
                         postpone_bucket_writer->PrepareCommit(/*wait_compaction=*/false));
    // check data file meta empty
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_TRUE(commit_increment.GetNewFilesIncrement().NewFiles().empty());

    // write empty batch
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([])").ValueOrDie();
    WriteBatch(array, /*row_kinds=*/{}, postpone_bucket_writer.get());
    // prepare commit, without write
    ASSERT_OK_AND_ASSIGN(commit_increment,
                         postpone_bucket_writer->PrepareCommit(/*wait_compaction=*/false));
    // check data file meta empty
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_TRUE(commit_increment.GetNewFilesIncrement().NewFiles().empty());

    ASSERT_OK(postpone_bucket_writer->Close());

    // check data file not exist
    std::string expected_data_file_name = "data-" + uuid + "-0." + file_format;
    std::string expected_data_file_path = dir->Str() + "/" + expected_data_file_name;
    ASSERT_FALSE(options.GetFileSystem()->Exists(expected_data_file_path).value());
}

TEST_P(PostponeBucketWriterTest, TestCloseBeforePrepareCommit) {
    auto file_format = GetParam();
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, file_format}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), file_format, options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto postpone_bucket_writer = std::make_shared<PostponeBucketWriter>(
        primary_keys_, path_factory, /*schema_id=*/1, value_schema_, options, pool_);

    // write batch
    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 14.1],
      ["Paul", 40, 2, null],
      ["Lucy", 30, 3, 15.1],
      ["Alice", 10, 0, 13.1]
    ])")
            .ValueOrDie();
    WriteBatch(array1, /*row_kinds=*/{}, postpone_bucket_writer.get());
    ASSERT_OK(postpone_bucket_writer->Close());
}

TEST_P(PostponeBucketWriterTest, TestIOException) {
    auto file_format = GetParam();
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, file_format}}));

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 200; i++) {
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        auto path_factory = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory->Init(dir->Str(), file_format, options.DataFilePrefix(), nullptr));
        std::string uuid = path_factory->uuid_;

        auto postpone_bucket_writer = std::make_shared<PostponeBucketWriter>(
            primary_keys_, path_factory, /*schema_id=*/1, value_schema_, options, pool_);

        // write batch
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 14.1],
      ["Paul", 40, 2, null],
      ["Lucy", 30, 3, 15.1],
      ["Alice", 10, 0, 13.1]
    ])")
                .ValueOrDie();
        ::ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        RecordBatchBuilder batch_builder(&c_array);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.Finish());
        CHECK_HOOK_STATUS(postpone_bucket_writer->Write(std::move(batch)), i);
        auto commit_increment = postpone_bucket_writer->PrepareCommit(/*wait_compaction=*/false);
        CHECK_HOOK_STATUS(commit_increment.status(), i);
        ASSERT_FALSE(commit_increment.value().GetNewFilesIncrement().NewFiles().empty());
        ASSERT_OK(postpone_bucket_writer->Close());
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

}  // namespace paimon::test
