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

#include "paimon/core/operation/append_only_file_store_write.h"

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/record_batch.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class AppendOnlyFileStoreWriteTest : public testing::Test {
 public:
    void SetUp() override {
        fields_ = {arrow::field("f0", arrow::boolean()),
                   arrow::field("f1", arrow::int8()),
                   arrow::field("f2", arrow::int8()),
                   arrow::field("f3", arrow::int16()),
                   arrow::field("f4", arrow::int16()),
                   arrow::field("f5", arrow::int32()),
                   arrow::field("f6", arrow::int32()),
                   arrow::field("f7", arrow::int64()),
                   arrow::field("f8", arrow::int64()),
                   arrow::field("f9", arrow::float32()),
                   arrow::field("f10", arrow::float64()),
                   arrow::field("f11", arrow::utf8()),
                   arrow::field("f12", arrow::binary()),
                   arrow::field("non-partition-field", arrow::int32())};
        commit_user_ = "test_commit_user";
    }

    void CreateTable(const std::string& warehouse, const std::shared_ptr<arrow::Schema>& schema,
                     const std::map<std::string, std::string>& options) const {
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(warehouse, options));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema,
                                       /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                       /*ignore_if_exists=*/false));
    }

    std::unique_ptr<RecordBatch> MakeBatch(const std::shared_ptr<arrow::Schema>& schema,
                                           const std::string& json) const {
        auto struct_type = arrow::struct_(schema->fields());
        auto array = arrow::ipc::internal::json::ArrayFromJSON(struct_type, json).ValueOrDie();
        ::ArrowArray arrow_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        return batch_builder.SetBucket(0).Finish().value();
    }

    std::vector<std::shared_ptr<CommitMessage>> WriteAndPrepare(
        const std::string& table_path, const std::shared_ptr<arrow::Schema>& schema,
        const std::map<std::string, std::string>& options, const std::string& json,
        int64_t commit_identifier) const {
        WriteContextBuilder builder(table_path, commit_user_);
        builder.SetOptions(options);
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
        EXPECT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        EXPECT_OK(file_store_write->Write(MakeBatch(schema, json)));
        EXPECT_OK_AND_ASSIGN(auto commit_msgs, file_store_write->PrepareCommit(
                                                   /*wait_compaction=*/false, commit_identifier));
        EXPECT_OK(file_store_write->Close());
        return commit_msgs;
    }

    std::vector<std::shared_ptr<CommitMessage>> WriteAndPrepareWithWriteSchema(
        const std::string& table_path, const std::shared_ptr<arrow::Schema>& schema,
        const std::map<std::string, std::string>& options,
        const std::vector<std::string>& write_schema, const std::string& json,
        int64_t commit_identifier) const {
        WriteContextBuilder builder(table_path, commit_user_);
        builder.SetOptions(options).WithWriteSchema(write_schema);
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
        EXPECT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        EXPECT_OK(file_store_write->Write(MakeBatch(schema, json)));
        EXPECT_OK_AND_ASSIGN(auto commit_msgs, file_store_write->PrepareCommit(
                                                   /*wait_compaction=*/false, commit_identifier));
        EXPECT_OK(file_store_write->Close());
        return commit_msgs;
    }

    void Commit(const std::string& table_path, const std::map<std::string, std::string>& options,
                const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        CommitContextBuilder builder(table_path, commit_user_);
        builder.SetOptions(options);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_commit,
                             FileStoreCommit::Create(std::move(commit_context)));
        ASSERT_OK(file_store_commit->Commit(commit_msgs));
    }

    std::shared_ptr<DataFileMeta> OnlyNewFile(
        const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        EXPECT_EQ(1, commit_msgs.size());
        auto msg = std::dynamic_pointer_cast<CommitMessageImpl>(commit_msgs[0]);
        EXPECT_NE(nullptr, msg);
        EXPECT_EQ(1, msg->GetNewFilesIncrement().NewFiles().size());
        return msg->GetNewFilesIncrement().NewFiles()[0];
    }

    std::shared_ptr<arrow::Schema> ReadDataFileSchema(
        const std::string& table_path, const std::shared_ptr<DataFileMeta>& file,
        const std::map<std::string, std::string>& options) const {
        std::string file_path =
            PathUtil::JoinPath(PathUtil::JoinPath(table_path, "bucket-0"), file->file_name);
        auto fs = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, fs->Open(file_path));
        EXPECT_OK_AND_ASSIGN(auto format_str, file->FileFormat());
        EXPECT_OK_AND_ASSIGN(auto file_format, FileFormatFactory::Get(format_str, options));
        EXPECT_OK_AND_ASSIGN(auto reader_builder, file_format->CreateReaderBuilder(10));
        EXPECT_OK_AND_ASSIGN(auto reader, reader_builder->Build(input_stream));
        EXPECT_OK_AND_ASSIGN(auto c_file_schema, reader->GetFileSchema());
        return arrow::ImportSchema(c_file_schema.get()).ValueOrDie();
    }

    MapSharedShreddingFieldMeta ShreddingMeta(const std::shared_ptr<arrow::Schema>& file_schema,
                                              int32_t field_index) const {
        auto metadata = file_schema->field(field_index)->metadata();
        EXPECT_NE(nullptr, metadata);
        return MapSharedShreddingUtils::DeserializeMetadata(
                   metadata->Copy(), MapSharedShreddingDefine::kDefaultDictCompression)
            .value();
    }

 private:
    arrow::FieldVector fields_;
    std::string commit_user_;
};

TEST_F(AppendOnlyFileStoreWriteTest, TestWriteWithInvalidBatch) {
    {
        arrow::Schema typed_schema(fields_);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);

        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{}, /*options=*/{},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), commit_user_);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        ASSERT_NOK_WITH_MSG(file_store_write->Write(nullptr), "batch is null pointer");
    }
    {
        arrow::Schema typed_schema(fields_);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{}, /*options=*/{},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                            commit_user_);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        auto array = std::make_shared<arrow::Array>();
        arrow::StringBuilder builder;
        for (size_t j = 0; j < 100; j++) {
            ASSERT_TRUE(builder.Append(std::to_string(j)).ok());
        }
        ASSERT_TRUE(builder.Finish(&array).ok());
        ::ArrowArray arrow_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch,
            batch_builder.SetBucket(1).SetPartition({{"f0", "true"}, {"f3", "1"}}).Finish());
        ASSERT_NOK_WITH_MSG(file_store_write->Write(std::move(batch)),
                            "batch bucket is 1 while options bucket is -1");
        ArrowArrayRelease(&arrow_array);
    }
}

TEST_F(AppendOnlyFileStoreWriteTest, TestGetMaxSequenceNumberFromMultiPartition) {
    WriteContextBuilder builder(
        paimon::test::GetDataDir() +
            "/orc/multi_partition_append_table.db/multi_partition_append_table/",
        commit_user_);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<WriteContext> write_context,
        builder.AddOption("file.format", "orc").AddOption("manifest.format", "orc").Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));
    auto write = dynamic_cast<AppendOnlyFileStoreWrite*>(file_store_write.get());
    auto pool = GetDefaultPool();
    {
        BinaryRow partition(2);
        BinaryRowWriter writer(&partition, 20, pool.get());
        writer.WriteInt(0, 20);
        writer.WriteInt(1, 1);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RestoreFiles> restore_files,
                             write->ScanExistingFileMetas(partition,
                                                          /*bucket=*/0));
        ASSERT_EQ(-1, restore_files->TotalBuckets().value());
        ASSERT_EQ(0, DataFileMeta::GetMaxSequenceNumber(restore_files->DataFiles()));
    }
    {
        BinaryRow partition(2);
        BinaryRowWriter writer(&partition, 20, pool.get());
        writer.WriteInt(0, 10);
        writer.WriteInt(1, 0);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RestoreFiles> restore_files,
                             write->ScanExistingFileMetas(partition,
                                                          /*bucket=*/0));
        ASSERT_EQ(-1, restore_files->TotalBuckets().value());
        ASSERT_EQ(2, DataFileMeta::GetMaxSequenceNumber(restore_files->DataFiles()));
    }
    {
        BinaryRow partition(2);
        BinaryRowWriter writer(&partition, 20, pool.get());
        writer.WriteInt(0, 10);
        writer.WriteInt(1, 0);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RestoreFiles> restore_files,
                             write->ScanExistingFileMetas(partition,
                                                          /*bucket=*/1));
        ASSERT_EQ(std::nullopt, restore_files->TotalBuckets());
        ASSERT_EQ(-1, DataFileMeta::GetMaxSequenceNumber(restore_files->DataFiles()));
    }
}

TEST_F(AppendOnlyFileStoreWriteTest, TestSharedShreddingMapRestoreInitializesNextWriter) {
    std::map<std::string, std::string> options = {
        {"file.format", "parquet"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "10"},
        {"write-only", "true"},
        {"bucket", "1"},
        {"bucket-key", "id"},
    };
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    CreateTable(dir->Str(), logical_schema, options);

    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    auto first_commit_msgs = WriteAndPrepare(table_path, logical_schema, options, R"([
        [1, [["a", 1], ["b", 2]]]
    ])",
                                             /*commit_identifier=*/0);
    auto first_file_schema =
        ReadDataFileSchema(table_path, OnlyNewFile(first_commit_msgs), options);
    auto first_meta = ShreddingMeta(first_file_schema, /*field_index=*/1);
    ASSERT_EQ(10, first_meta.num_columns);
    ASSERT_EQ(2, first_meta.max_row_width);
    Commit(table_path, options, first_commit_msgs);

    auto second_commit_msgs = WriteAndPrepare(table_path, logical_schema, options, R"([
        [2, [["c", 3], ["d", 4], ["e", 5]]]
    ])",
                                              /*commit_identifier=*/1);
    auto second_file_schema =
        ReadDataFileSchema(table_path, OnlyNewFile(second_commit_msgs), options);
    auto second_meta = ShreddingMeta(second_file_schema, /*field_index=*/1);

    ASSERT_OK_AND_ASSIGN(
        auto expected_second_schema,
        MapSharedShreddingUtils::LogicalToPhysicalSchema(logical_schema, {{"tags", 2}}));
    ASSERT_TRUE(second_file_schema->Equals(*expected_second_schema, /*check_metadata=*/false));
    ASSERT_EQ(2, second_meta.num_columns);
    ASSERT_EQ(3, second_meta.max_row_width);
}

TEST_F(AppendOnlyFileStoreWriteTest, TestSharedShreddingRestoreIgnoresAvroFileWithoutMetadata) {
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    std::map<std::string, std::string> avro_options = {
        {"file.format", "avro"},
        {"write-only", "true"},
        {"bucket", "1"},
        {"bucket-key", "id"},
    };
    std::map<std::string, std::string> shredding_options = avro_options;
    shredding_options["file.format"] = "parquet";
    shredding_options["fields.tags.map.storage-layout"] = "shared-shredding";
    shredding_options["fields.tags.map.shared-shredding.max-columns"] = "10";

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    CreateTable(dir->Str(), logical_schema, avro_options);
    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    auto avro_commit_msgs = WriteAndPrepare(table_path, logical_schema, avro_options, R"([
        [1, [["a", 1], ["b", 2]]]
    ])",
                                            /*commit_identifier=*/0);
    Commit(table_path, avro_options, avro_commit_msgs);

    auto second_commit_msgs = WriteAndPrepare(table_path, logical_schema, shredding_options, R"([
        [2, [["c", 3], ["d", 4], ["e", 5]]]
    ])",
                                              /*commit_identifier=*/1);
    auto second_file_schema =
        ReadDataFileSchema(table_path, OnlyNewFile(second_commit_msgs), shredding_options);
    auto second_meta = ShreddingMeta(second_file_schema, /*field_index=*/1);

    ASSERT_OK_AND_ASSIGN(auto expected_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                   logical_schema, {{"tags", 10}}));
    ASSERT_TRUE(second_file_schema->Equals(*expected_schema, /*check_metadata=*/false));
    ASSERT_EQ(10, second_meta.num_columns);
    ASSERT_EQ(3, second_meta.max_row_width);
}

TEST_F(AppendOnlyFileStoreWriteTest, TestSharedShreddingRestoreMultipleMapColumns) {
    std::map<std::string, std::string> options = {
        {"file.format", "parquet"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "10"},
        {"fields.attrs.map.storage-layout", "shared-shredding"},
        {"fields.attrs.map.shared-shredding.max-columns", "10"},
        {"write-only", "true"},
        {"bucket", "1"},
        {"bucket-key", "id"},
    };
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("attrs", arrow::map(arrow::utf8(), arrow::int64())),
    });
    auto tags_schema = arrow::schema({
        logical_schema->field(0),
        logical_schema->field(1),
    });
    auto attrs_schema = arrow::schema({
        logical_schema->field(0),
        logical_schema->field(2),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    CreateTable(dir->Str(), logical_schema, options);
    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    auto tags_commit_msgs =
        WriteAndPrepareWithWriteSchema(table_path, tags_schema, options, {"id", "tags"}, R"([
            [1, [["a", 1], ["b", 2]]]
        ])",
                                       /*commit_identifier=*/0);
    Commit(table_path, options, tags_commit_msgs);

    auto attrs_commit_msgs =
        WriteAndPrepareWithWriteSchema(table_path, attrs_schema, options, {"id", "attrs"}, R"([
            [2, [["c", 3], ["d", 4], ["e", 5], ["f", 6]]]
        ])",
                                       /*commit_identifier=*/1);
    Commit(table_path, options, attrs_commit_msgs);

    auto full_commit_msgs = WriteAndPrepare(table_path, logical_schema, options, R"([
        [3, [["g", 7], ["h", 8], ["i", 9]], [["j", 10], ["k", 11], ["l", 12], ["m", 13], ["n", 14]]]
    ])",
                                            /*commit_identifier=*/2);
    auto full_file_schema = ReadDataFileSchema(table_path, OnlyNewFile(full_commit_msgs), options);
    auto tags_meta = ShreddingMeta(full_file_schema, /*field_index=*/1);
    auto attrs_meta = ShreddingMeta(full_file_schema, /*field_index=*/2);

    ASSERT_OK_AND_ASSIGN(auto expected_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                   logical_schema, {{"tags", 2}, {"attrs", 4}}));
    ASSERT_TRUE(full_file_schema->Equals(*expected_schema, /*check_metadata=*/false));
    ASSERT_EQ(2, tags_meta.num_columns);
    ASSERT_EQ(3, tags_meta.max_row_width);
    ASSERT_EQ(4, attrs_meta.num_columns);
    ASSERT_EQ(5, attrs_meta.max_row_width);
}

TEST_F(AppendOnlyFileStoreWriteTest, TestSharedShreddingRestoreUsesDefaultForMissingMap) {
    std::map<std::string, std::string> options = {
        {"file.format", "parquet"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "10"},
        {"fields.attrs.map.storage-layout", "shared-shredding"},
        {"fields.attrs.map.shared-shredding.max-columns", "10"},
        {"write-only", "true"},
        {"bucket", "1"},
        {"bucket-key", "id"},
    };
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("attrs", arrow::map(arrow::utf8(), arrow::int64())),
    });
    auto tags_schema = arrow::schema({
        logical_schema->field(0),
        logical_schema->field(1),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    CreateTable(dir->Str(), logical_schema, options);
    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    auto tags_commit_msgs =
        WriteAndPrepareWithWriteSchema(table_path, tags_schema, options, {"id", "tags"}, R"([
            [1, [["a", 1], ["b", 2]]]
        ])",
                                       /*commit_identifier=*/0);
    Commit(table_path, options, tags_commit_msgs);

    auto full_commit_msgs = WriteAndPrepare(table_path, logical_schema, options, R"([
        [2, [["c", 3], ["d", 4], ["e", 5]], [["f", 6], ["g", 7], ["h", 8], ["i", 9]]]
    ])",
                                            /*commit_identifier=*/1);
    auto full_file_schema = ReadDataFileSchema(table_path, OnlyNewFile(full_commit_msgs), options);
    auto tags_meta = ShreddingMeta(full_file_schema, /*field_index=*/1);
    auto attrs_meta = ShreddingMeta(full_file_schema, /*field_index=*/2);

    ASSERT_OK_AND_ASSIGN(auto expected_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                   logical_schema, {{"tags", 2}, {"attrs", 10}}));
    ASSERT_TRUE(full_file_schema->Equals(*expected_schema, /*check_metadata=*/false));
    ASSERT_EQ(2, tags_meta.num_columns);
    ASSERT_EQ(3, tags_meta.max_row_width);
    ASSERT_EQ(10, attrs_meta.num_columns);
    ASSERT_EQ(4, attrs_meta.max_row_width);
}

TEST_F(AppendOnlyFileStoreWriteTest, TestSharedShreddingPartialWriteSkipsMissingMapColumn) {
    std::map<std::string, std::string> options = {
        {"file.format", "parquet"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "10"},
        {"write-only", "true"},
        {"bucket", "1"},
        {"bucket-key", "id"},
    };
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });
    auto id_only_schema = arrow::schema({
        logical_schema->field(0),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    CreateTable(dir->Str(), logical_schema, options);
    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    auto first_commit_msgs = WriteAndPrepare(table_path, logical_schema, options, R"([
        [1, [["a", 1], ["b", 2]]]
    ])",
                                             /*commit_identifier=*/0);
    Commit(table_path, options, first_commit_msgs);

    auto id_only_commit_msgs =
        WriteAndPrepareWithWriteSchema(table_path, id_only_schema, options, {"id"}, R"([
            [2]
        ])",
                                       /*commit_identifier=*/1);
    auto id_only_file_schema =
        ReadDataFileSchema(table_path, OnlyNewFile(id_only_commit_msgs), options);

    ASSERT_TRUE(id_only_file_schema->Equals(*id_only_schema, /*check_metadata=*/false));
    for (const auto& field : id_only_file_schema->fields()) {
        auto metadata = field->metadata() ? field->metadata()->Copy() : nullptr;
        ASSERT_FALSE(MapSharedShreddingUtils::HasShreddingMetadata(metadata));
    }
}

}  // namespace paimon::test
