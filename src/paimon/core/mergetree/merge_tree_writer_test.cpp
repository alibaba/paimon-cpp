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

#include "paimon/core/mergetree/merge_tree_writer.h"

#include <cassert>
#include <cstddef>
#include <map>
#include <optional>
#include <utility>
#include <variant>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/fs/external_path_provider.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
template <typename T>
class MergeFunctionWrapper;
}  // namespace paimon

namespace paimon::test {
class MergeTreeWriterTest : public ::testing::Test {
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
        ASSERT_OK_AND_ASSIGN(key_comparator_, FieldsComparator::Create({value_fields_[0]},
                                                                       /*is_ascending_order=*/true,
                                                                       /*use_view=*/true));
        std::vector<DataField> write_fields = {SpecialFields::SequenceNumber(),
                                               SpecialFields::ValueKind()};
        write_fields.insert(write_fields.end(), value_fields_.begin(), value_fields_.end());
        write_type_ = DataField::ConvertDataFieldsToArrowStructType(write_fields);

        auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        merge_function_wrapper_ = std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));
    }
    void TearDown() override {}

    void WriteBatch(const std::shared_ptr<arrow::Array>& array,
                    const std::vector<RecordBatch::RowKind>& row_kinds,
                    MergeTreeWriter* writer) const {
        ::ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        RecordBatchBuilder batch_builder(&c_array);
        batch_builder.SetRowKinds(row_kinds);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.Finish());
        ASSERT_OK(writer->Write(std::move(batch)));
    }

    void CheckFileContent(const std::string& data_file_name,
                          const std::shared_ptr<arrow::ChunkedArray>& expected_array) const {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(data_file_name));
        ASSERT_TRUE(input_stream);
        ASSERT_OK_AND_ASSIGN(auto file_format, FileFormatFactory::Get("orc", /*options=*/{}));
        ASSERT_OK_AND_ASSIGN(auto reader_builder,
                             file_format->CreateReaderBuilder(/*batch_size=*/10));
        ASSERT_OK_AND_ASSIGN(auto orc_batch_reader, reader_builder->Build(input_stream));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                             ReadResultCollector::CollectResult(orc_batch_reader.get()));
        ASSERT_TRUE(expected_array->Equals(result_array)) << result_array->ToString();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> file_system_;
    std::vector<DataField> value_fields_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<arrow::DataType> value_type_;
    std::vector<std::string> primary_keys_;
    std::shared_ptr<arrow::DataType> write_type_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
};

TEST_F(MergeTreeWriterTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto merge_writer = std::make_shared<MergeTreeWriter>(
        /*last_sequence_number=*/-1, primary_keys_, path_factory, key_comparator_,
        /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper_, /*schema_id=*/1,
        value_schema_, options, pool_);

    // write batch
    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 14.1],
      ["Paul", 20, 1, null],
      ["Alice", 10, 0, 13.1]
    ])")
            .ValueOrDie();
    WriteBatch(array1, /*row_kinds=*/{}, merge_writer.get());

    // prepare commit
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment,
                         merge_writer->PrepareCommit(/*wait_compaction=*/false));
    ASSERT_OK(merge_writer->Close());

    // check data file exist and read ok
    std::string expected_data_file_name = "data-" + uuid + "-0.orc";
    std::string expected_data_file_path = dir->Str() + "/" + expected_data_file_name;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> data_file_status,
                         options.GetFileSystem()->GetFileStatus(expected_data_file_path));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [2, 0, "Alice", 10, 0, 13.1],
      [0, 0, "Lucy", 20, 1, 14.1],
      [1, 0, "Paul", 20, 1, null]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(expected_data_file_path, expected_array);

    // check data file meta
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(1, commit_increment.GetNewFilesIncrement().NewFiles().size());
    auto expected_data_file_meta = std::make_shared<DataFileMeta>(
        expected_data_file_name, /*file_size=*/data_file_status->GetLen(), /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice"}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Paul"}, pool_.get()),
        /*key_stats=*/BinaryRowGenerator::GenerateStats({"Alice"}, {"Paul"}, {0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 0, 13.1}, {"Paul", 20, 1, 14.1},
                                          {0, 0, 0, 1}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/1,
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

TEST_F(MergeTreeWriterTest, TestWriteMultiBatch) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto merge_writer = std::make_shared<MergeTreeWriter>(
        /*last_sequence_number=*/9, primary_keys_, path_factory, key_comparator_,
        /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper_, /*schema_id=*/0,
        value_schema_, options, pool_);
    // batch1
    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 14.1],
      ["Paul", 20, 1, null],
      ["Alice", 10, 0, 13.1],
      ["Paul", 20, 1, 15.1]
    ])")
            .ValueOrDie();
    WriteBatch(array1, /*row_kinds=*/{}, merge_writer.get());
    // batch2
    std::shared_ptr<arrow::Array> array2 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 114.1],
      ["Skye", 10, 0, 118.1],
      ["Alice", 10, 0, 113.1]
    ])")
            .ValueOrDie();
    WriteBatch(array2, /*row_kinds=*/{}, merge_writer.get());

    // prepare commit
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment,
                         merge_writer->PrepareCommit(/*wait_compaction=*/false));
    ASSERT_OK(merge_writer->Close());

    // check data file exist and read ok
    std::string expected_data_file_name = "data-" + uuid + "-0.orc";
    std::string expected_data_file_path = dir->Str() + "/" + expected_data_file_name;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> data_file_status,
                         options.GetFileSystem()->GetFileStatus(expected_data_file_path));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [16, 0, "Alice", 10, 0, 113.1],
      [14, 0, "Lucy", 20, 1, 114.1],
      [13, 0, "Paul", 20, 1, 15.1],
      [15, 0, "Skye", 10, 0, 118.1]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(expected_data_file_path, expected_array);

    // check data file meta
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(1, commit_increment.GetNewFilesIncrement().NewFiles().size());
    auto expected_data_file_meta = std::make_shared<DataFileMeta>(
        expected_data_file_name, /*file_size=*/data_file_status->GetLen(), /*row_count=*/4,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice"}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Skye"}, pool_.get()),
        /*key_stats=*/BinaryRowGenerator::GenerateStats({"Alice"}, {"Skye"}, {0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 0, 15.1}, {"Skye", 20, 1, 118.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/13, /*max_sequence_number=*/16, /*schema_id=*/0,
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

TEST_F(MergeTreeWriterTest, TestWriteWithDeleteRow) {
    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"}, {Options::SEQUENCE_FIELD, "f1"}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_defined_seq_comparator,
                         FieldsComparator::Create({value_fields_[1]},
                                                  /*is_ascending_order=*/true,
                                                  /*use_view=*/false));
    assert(user_defined_seq_comparator);
    auto merge_writer = std::make_shared<MergeTreeWriter>(
        /*last_sequence_number=*/9, primary_keys_, path_factory, key_comparator_,
        user_defined_seq_comparator, merge_function_wrapper_, /*schema_id=*/0, value_schema_,
        options, pool_);
    // batch1
    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 14.1],
      ["Paul", 20, 1, null],
      ["Alice", 10, 0, 13.1],
      ["Paul", 10, 1, 15.1]
    ])")
            .ValueOrDie();
    WriteBatch(array1,
               {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                RecordBatch::RowKind::DELETE, RecordBatch::RowKind::INSERT},
               merge_writer.get());

    // prepare commit
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment,
                         merge_writer->PrepareCommit(/*wait_compaction=*/false));
    ASSERT_OK(merge_writer->Close());

    // check data file exist and read ok
    std::string expected_data_file_name = "data-" + uuid + "-0.orc";
    std::string expected_data_file_path = dir->Str() + "/" + expected_data_file_name;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> data_file_status,
                         options.GetFileSystem()->GetFileStatus(expected_data_file_path));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [12, 3, "Alice", 10, 0, 13.1],
      [10, 0, "Lucy", 20, 1, 14.1],
      [11, 0, "Paul", 20, 1, null]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(expected_data_file_path, expected_array);

    // check data file meta
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(1, commit_increment.GetNewFilesIncrement().NewFiles().size());
    auto expected_data_file_meta = std::make_shared<DataFileMeta>(
        expected_data_file_name, /*file_size=*/data_file_status->GetLen(), /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice"}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Paul"}, pool_.get()),
        /*key_stats=*/BinaryRowGenerator::GenerateStats({"Alice"}, {"Paul"}, {0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 0, 13.1}, {"Paul", 20, 1, 14.1},
                                          {0, 0, 0, 1}, pool_.get()),
        /*min_sequence_number=*/10, /*max_sequence_number=*/12, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/commit_increment.GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/1, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement expected_data_increment({expected_data_file_meta}, /*deleted_files=*/{},
                                          /*changelog_files=*/{});
    ASSERT_EQ(expected_data_increment, commit_increment.GetNewFilesIncrement());
}

TEST_F(MergeTreeWriterTest, TestMultiplePrepareCommit) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"},
                                               {"orc.write.enable-metrics", "true"}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto merge_writer = std::make_shared<MergeTreeWriter>(
        /*last_sequence_number=*/9, primary_keys_, path_factory, key_comparator_,
        /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper_, /*schema_id=*/0,
        value_schema_, options, pool_);
    // batch1
    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 14.1],
      ["Paul", 20, 1, null],
      ["Alice", 10, 0, 13.1],
      ["Paul", 20, 1, 15.1]
    ])")
            .ValueOrDie();
    WriteBatch(array1, /*row_kinds=*/{}, merge_writer.get());
    // prepare commit1
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment1,
                         merge_writer->PrepareCommit(/*wait_compaction=*/false));
    // check metrics
    auto metrics = merge_writer->GetMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t write_io_count, metrics->GetCounter("orc.write.io.count"));
    ASSERT_GT(write_io_count, 0);

    // batch2
    std::shared_ptr<arrow::Array> array2 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 114.1],
      ["Skye", 10, 0, 118.1],
      ["Alice", 10, 0, 113.1]
    ])")
            .ValueOrDie();
    WriteBatch(array2, /*row_kinds=*/{}, merge_writer.get());
    // prepare commit2
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment2,
                         merge_writer->PrepareCommit(/*wait_compaction=*/false));
    // check metrics
    metrics = merge_writer->GetMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t write_io_count2, metrics->GetCounter("orc.write.io.count"));
    ASSERT_TRUE(write_io_count2 > write_io_count);

    ASSERT_OK(merge_writer->Close());

    // check data file exist and read ok
    std::string expected_data_file_name1 = "data-" + uuid + "-0.orc";
    std::string expected_data_file_name2 = "data-" + uuid + "-1.orc";

    std::string expected_data_file_dir = dir->Str() + "/";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileStatus> data_file_status1,
        options.GetFileSystem()->GetFileStatus(expected_data_file_dir + expected_data_file_name1));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileStatus> data_file_status2,
        options.GetFileSystem()->GetFileStatus(expected_data_file_dir + expected_data_file_name2));

    std::shared_ptr<arrow::ChunkedArray> expected_array1;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [12, 0, "Alice", 10, 0, 13.1],
      [10, 0, "Lucy", 20, 1, 14.1],
      [13, 0, "Paul", 20, 1, 15.1]
    ])"},
                                                                         &expected_array1);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(expected_data_file_dir + expected_data_file_name1, expected_array1);

    std::shared_ptr<arrow::ChunkedArray> expected_array2;
    array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [16, 0, "Alice", 10, 0, 113.1],
      [14, 0, "Lucy", 20, 1, 114.1],
      [15, 0, "Skye", 10, 0, 118.1]
    ])"},
                                                                    &expected_array2);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(expected_data_file_dir + expected_data_file_name2, expected_array2);

    // check data file meta
    ASSERT_TRUE(commit_increment1.GetCompactIncrement().IsEmpty());
    ASSERT_TRUE(commit_increment2.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(1, commit_increment1.GetNewFilesIncrement().NewFiles().size());
    ASSERT_EQ(1, commit_increment2.GetNewFilesIncrement().NewFiles().size());
    auto expected_data_file_meta1 = std::make_shared<DataFileMeta>(
        expected_data_file_name1, /*file_size=*/data_file_status1->GetLen(), /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice"}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Paul"}, pool_.get()),
        /*key_stats=*/BinaryRowGenerator::GenerateStats({"Alice"}, {"Paul"}, {0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 0, 13.1}, {"Paul", 20, 1, 15.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/10, /*max_sequence_number=*/13, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/commit_increment1.GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    auto expected_data_file_meta2 = std::make_shared<DataFileMeta>(
        expected_data_file_name2, /*file_size=*/data_file_status2->GetLen(), /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice"}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Skye"}, pool_.get()),
        /*key_stats=*/BinaryRowGenerator::GenerateStats({"Alice"}, {"Skye"}, {0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 0, 113.1}, {"Skye", 20, 1, 118.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/14, /*max_sequence_number=*/16, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/commit_increment2.GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement expected_data_increment1({expected_data_file_meta1},
                                           /*deleted_files=*/{},
                                           /*changelog_files=*/{});
    ASSERT_EQ(expected_data_increment1, commit_increment1.GetNewFilesIncrement());

    DataIncrement expected_data_increment2({expected_data_file_meta2},
                                           /*deleted_files=*/{},
                                           /*changelog_files=*/{});
    ASSERT_EQ(expected_data_increment2, commit_increment2.GetNewFilesIncrement());
}

TEST_F(MergeTreeWriterTest, TestPrepareCommitForEmptyData) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto merge_writer = std::make_shared<MergeTreeWriter>(
        /*last_sequence_number=*/-1, primary_keys_, path_factory, key_comparator_,
        /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper_, /*schema_id=*/0,
        value_schema_, options, pool_);

    // prepare commit, without write
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment,
                         merge_writer->PrepareCommit(/*wait_compaction=*/false));
    // check data file meta empty
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_TRUE(commit_increment.GetNewFilesIncrement().NewFiles().empty());

    // write empty batch
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([])").ValueOrDie();
    WriteBatch(array, /*row_kinds=*/{}, merge_writer.get());
    // prepare commit, without write
    ASSERT_OK_AND_ASSIGN(commit_increment, merge_writer->PrepareCommit(/*wait_compaction=*/false));
    // check data file meta empty
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_TRUE(commit_increment.GetNewFilesIncrement().NewFiles().empty());

    ASSERT_OK(merge_writer->Close());

    // check data file not exist
    std::string expected_data_file_name = "data-" + uuid + "-0.orc";
    std::string expected_data_file_path = dir->Str() + "/" + expected_data_file_name;
    ASSERT_FALSE(options.GetFileSystem()->Exists(expected_data_file_path).value());
}

TEST_F(MergeTreeWriterTest, TestCloseBeforePrepareCommit) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto merge_writer = std::make_shared<MergeTreeWriter>(
        /*last_sequence_number=*/-1, primary_keys_, path_factory, key_comparator_,
        /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper_, /*schema_id=*/0,
        value_schema_, options, pool_);

    // write batch
    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 14.1],
      ["Paul", 20, 1, null],
      ["Alice", 10, 0, 13.1]
    ])")
            .ValueOrDie();
    WriteBatch(array1, /*row_kinds=*/{}, merge_writer.get());
    ASSERT_OK(merge_writer->Close());
}

TEST_F(MergeTreeWriterTest, TestAutoFlush) {
    // each batch is a file due to WRITE_BUFFER_SIZE
    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"}, {Options::WRITE_BUFFER_SIZE, "1"}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto merge_writer = std::make_shared<MergeTreeWriter>(
        /*last_sequence_number=*/9, primary_keys_, path_factory, key_comparator_,
        /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper_, /*schema_id=*/0,
        value_schema_, options, pool_);
    // batch1
    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 14.1],
      ["Paul", 20, 1, null],
      ["Alice", 10, 0, 13.1],
      ["Paul", 20, 1, 15.1]
    ])")
            .ValueOrDie();
    WriteBatch(array1, /*row_kinds=*/{}, merge_writer.get());

    // batch2
    std::shared_ptr<arrow::Array> array2 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Lucy", 20, 1, 114.1],
      ["Skye", 10, 0, 118.1],
      ["Alice", 10, 0, 113.1]
    ])")
            .ValueOrDie();
    WriteBatch(array2, /*row_kinds=*/{}, merge_writer.get());
    // prepare commit
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment,
                         merge_writer->PrepareCommit(/*wait_compaction=*/false));
    ASSERT_OK(merge_writer->Close());

    // check data file exist and read ok
    std::string expected_data_file_name1 = "data-" + uuid + "-0.orc";
    std::string expected_data_file_name2 = "data-" + uuid + "-1.orc";

    std::string expected_data_file_dir = dir->Str() + "/";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileStatus> data_file_status1,
        options.GetFileSystem()->GetFileStatus(expected_data_file_dir + expected_data_file_name1));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileStatus> data_file_status2,
        options.GetFileSystem()->GetFileStatus(expected_data_file_dir + expected_data_file_name2));

    std::shared_ptr<arrow::ChunkedArray> expected_array1;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [12, 0, "Alice", 10, 0, 13.1],
      [10, 0, "Lucy", 20, 1, 14.1],
      [13, 0, "Paul", 20, 1, 15.1]
    ])"},
                                                                         &expected_array1);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(expected_data_file_dir + expected_data_file_name1, expected_array1);

    std::shared_ptr<arrow::ChunkedArray> expected_array2;
    array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(write_type_, {R"([
      [16, 0, "Alice", 10, 0, 113.1],
      [14, 0, "Lucy", 20, 1, 114.1],
      [15, 0, "Skye", 10, 0, 118.1]
    ])"},
                                                                    &expected_array2);
    ASSERT_TRUE(array_status.ok());
    CheckFileContent(expected_data_file_dir + expected_data_file_name2, expected_array2);

    // check data file meta
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(2, commit_increment.GetNewFilesIncrement().NewFiles().size());
    auto expected_data_file_meta1 = std::make_shared<DataFileMeta>(
        expected_data_file_name1, /*file_size=*/data_file_status1->GetLen(), /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice"}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Paul"}, pool_.get()),
        /*key_stats=*/BinaryRowGenerator::GenerateStats({"Alice"}, {"Paul"}, {0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 0, 13.1}, {"Paul", 20, 1, 15.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/10, /*max_sequence_number=*/13, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/commit_increment.GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    auto expected_data_file_meta2 = std::make_shared<DataFileMeta>(
        expected_data_file_name2, /*file_size=*/data_file_status2->GetLen(), /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice"}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({"Skye"}, pool_.get()),
        /*key_stats=*/BinaryRowGenerator::GenerateStats({"Alice"}, {"Skye"}, {0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({"Alice", 10, 0, 113.1}, {"Skye", 20, 1, 118.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/14, /*max_sequence_number=*/16, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/commit_increment.GetNewFilesIncrement().NewFiles()[1]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement expected_data_increment({expected_data_file_meta1, expected_data_file_meta2},
                                          /*deleted_files=*/{},
                                          /*changelog_files=*/{});
    ASSERT_EQ(expected_data_increment, commit_increment.GetNewFilesIncrement());
}

TEST_F(MergeTreeWriterTest, TestIOException) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"}}));

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 200; i++) {
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        auto path_factory = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
        std::string uuid = path_factory->uuid_;

        auto merge_writer = std::make_shared<MergeTreeWriter>(
            /*last_sequence_number=*/-1, primary_keys_, path_factory, key_comparator_,
            /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper_, /*schema_id=*/0,
            value_schema_, options, pool_);

        // write batch
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
          ["Lucy", 20, 1, 14.1],
          ["Paul", 20, 1, null],
          ["Alice", 10, 0, 13.1]
        ])")
                .ValueOrDie();

        ::ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        RecordBatchBuilder batch_builder(&c_array);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.Finish());
        CHECK_HOOK_STATUS(merge_writer->Write(std::move(batch)), i);
        auto commit_increment = merge_writer->PrepareCommit(/*wait_compaction=*/false);
        CHECK_HOOK_STATUS(commit_increment.status(), i);
        ASSERT_FALSE(commit_increment.value().GetNewFilesIncrement().NewFiles().empty());
        ASSERT_OK(merge_writer->Close());
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_F(MergeTreeWriterTest, TestEstimateMemoryUse) {
    {
        // test simple
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
          ["Lucy", 20, 1, 14.1],
          ["Paul", 20, 1, null],
          ["Alice", 10, 0, 13.1]
        ])")
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(int64_t memory_use, MergeTreeWriter::EstimateMemoryUse(array));
        int64_t expected_memory_use =
            1 + (13 + 3 * 4 + 1) + (3 * 4 + 1) + (3 * 4 + 1) + (3 * 8 + 1);
        ASSERT_EQ(memory_use, expected_memory_use);
    }
    {
        // test primitive type
        arrow::FieldVector fields = {arrow::field("v0", arrow::boolean()),
                                     arrow::field("v1", arrow::int8()),
                                     arrow::field("v2", arrow::int16()),
                                     arrow::field("v3", arrow::int32()),
                                     arrow::field("v4", arrow::int64()),
                                     arrow::field("v5", arrow::float32()),
                                     arrow::field("v6", arrow::float64()),
                                     arrow::field("v7", arrow::date32()),
                                     arrow::field("v8", arrow::timestamp(arrow::TimeUnit::NANO)),
                                     arrow::field("v9", arrow::decimal128(30, 20)),
                                     arrow::field("v10", arrow::utf8()),
                                     arrow::field("v11", arrow::binary())};

        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [true, 10, 200, 65536, 123456789, 0.0, 0.0, 2000, -86399999999500, "2134.48690000000000000009", "difference", "Alice"],
        [false, -128, -32768, -2147483648, -9223372036854775808, -3.4028235E38, -1.7976931348623157E308, -719528, -9223372036854775808, "-999999999999999999.99999999999999999999", "Alice", "Two"],
        [true, 127, 32767, 2147483647, 9223372036854775807, 3.4028235E38, 1.7976931348623157E308, 2932896, 9223372036854775807, "999999999999999999.99999999999999999999", "Alice", "made"],
        [true, 0, 0, 0, 0, 1.4E-45, 4.9E-324, 0, 0, "0.00000000000000000000", "Alice", "wood"]
])")
                .ValueOrDie());
        ASSERT_OK_AND_ASSIGN(int64_t memory_use, MergeTreeWriter::EstimateMemoryUse(array));
        int64_t expected_memory_use = 1 + (4 + 1) + (4 + 1) + (2 * 4 + 1) + (4 * 4 + 1) +
                                      (8 * 4 + 1) + (4 * 4 + 1) + (8 * 4 + 1) + (4 * 4 + 1) +
                                      (8 * 4 + 1) + (4 * 16 + 1) + (25 + 4 * 4 + 1) +
                                      (16 + 4 * 4 + 1);
        ASSERT_EQ(memory_use, expected_memory_use);
    }
    {
        // test nested type
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::list(arrow::int32())),
            arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
            arrow::field("f2", arrow::struct_({arrow::field("sub1", arrow::int64()),
                                               arrow::field("sub2", arrow::float64()),
                                               arrow::field("sub3", arrow::boolean())})),
        };
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false]],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true]],
        [[6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true]]
    ])")
                .ValueOrDie());
        ASSERT_OK_AND_ASSIGN(int64_t memory_use, MergeTreeWriter::EstimateMemoryUse(array));
        int64_t list_mem = 1 + (4 * 6 + 1);
        int64_t map_mem = 1 + (33 + 4 * 7 + 1) + (8 * 7 + 1);
        int64_t struct_mem = 1 + (8 * 3 + 1) + (8 * 3 + 1) + (1 * 3 + 1);
        int64_t expected_memory_use = 1 + list_mem + map_mem + struct_mem;
        ASSERT_EQ(memory_use, expected_memory_use);
    }
}

TEST_F(MergeTreeWriterTest, TestBulkData) {
    // each batch is a file due to WRITE_BUFFER_SIZE
    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"}, {Options::WRITE_BUFFER_SIZE, "1"}}));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    std::string uuid = path_factory->uuid_;

    auto merge_writer = std::make_shared<MergeTreeWriter>(
        /*last_sequence_number=*/-1, primary_keys_, path_factory, key_comparator_,
        /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper_, /*schema_id=*/0,
        value_schema_, options, pool_);
    // multi batch
    size_t batch_size = 500;
    for (size_t i = 0; i < batch_size; ++i) {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
          ["Lucy", 20, 1, 14.1],
          ["Paul", 20, 1, null],
          ["Alice", 10, 0, 13.1],
          ["Paul", 20, 1, 15.1]
        ])")
                .ValueOrDie();
        WriteBatch(array, /*row_kinds=*/{}, merge_writer.get());
    }

    // prepare commit
    ASSERT_OK_AND_ASSIGN(CommitIncrement commit_increment,
                         merge_writer->PrepareCommit(/*wait_compaction=*/false));
    ASSERT_OK(merge_writer->Close());

    std::string expected_data_file_dir = dir->Str() + "/";
    ASSERT_TRUE(commit_increment.GetCompactIncrement().IsEmpty());
    ASSERT_EQ(batch_size, commit_increment.GetNewFilesIncrement().NewFiles().size());

    for (size_t i = 0; i < batch_size; ++i) {
        std::string expected_data_file_name = "data-" + uuid + "-" + std::to_string(i) + ".orc";
        // check data file exist and read ok
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> data_file_status,
                             options.GetFileSystem()->GetFileStatus(expected_data_file_dir +
                                                                    expected_data_file_name));
        // check data file meta
        auto expected_data_file_meta = std::make_shared<DataFileMeta>(
            expected_data_file_name, /*file_size=*/data_file_status->GetLen(), /*row_count=*/3,
            /*min_key=*/BinaryRowGenerator::GenerateRow({"Alice"}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({"Paul"}, pool_.get()),
            /*key_stats=*/BinaryRowGenerator::GenerateStats({"Alice"}, {"Paul"}, {0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats({"Alice", 10, 0, 13.1}, {"Paul", 20, 1, 15.1},
                                              {0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/i * 4, /*max_sequence_number=*/i * 4 + 3, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/commit_increment.GetNewFilesIncrement().NewFiles()[i]->creation_time,
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        ASSERT_EQ(*commit_increment.GetNewFilesIncrement().NewFiles()[i], *expected_data_file_meta);
    }
}

}  // namespace paimon::test
