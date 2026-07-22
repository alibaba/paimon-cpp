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

#include "paimon/core/operation/key_value_file_store_write.h"

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/record_batch.h"
#include "paimon/status.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class KeyValueFileStoreWriteTest : public ::testing::Test {
 protected:
    Result<std::unique_ptr<FileStoreWrite>> CreateSingleStringFileStoreWrite(
        const std::map<std::string, std::string>& table_options, bool with_temp_directory) {
        auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(typed_schema, &schema));

        auto dir = UniqueTestDirectory::Create();
        if (!dir) {
            return Status::Invalid("failed to create test directory");
        }
        PAIMON_ASSIGN_OR_RAISE(auto catalog, Catalog::Create(dir->Str(), {}));
        PAIMON_RETURN_NOT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        PAIMON_RETURN_NOT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                                  /*partition_keys=*/{},
                                                  /*primary_keys=*/{"f0"}, table_options,
                                                  /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
        if (with_temp_directory) {
            context_builder.WithTempDirectory(dir->Str());
        }

        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context,
                               context_builder.Finish());
        return FileStoreWrite::Create(std::move(write_context));
    }

    Status WriteSingleStringRow(FileStoreWrite* file_store_write, int32_t bucket,
                                const std::string& value) {
        auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
        auto struct_type = arrow::struct_(fields);
        arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::StringBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Append());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(string_builder->Append(value));

        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Finish(&array));
        ::ArrowArray arrow_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &arrow_array));

        RecordBatchBuilder batch_builder(&arrow_array);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> batch,
                               batch_builder.SetBucket(bucket).Finish());
        Status write_status = file_store_write->Write(std::move(batch));
        if (!ArrowArrayIsReleased(&arrow_array)) {
            ArrowArrayRelease(&arrow_array);
        }
        return write_status;
    }

    void CreateTable(const std::string& warehouse, const std::shared_ptr<arrow::Schema>& schema,
                     const std::map<std::string, std::string>& options) const {
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(warehouse, options));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema,
                                       /*partition_keys=*/{}, /*primary_keys=*/{"id"}, options,
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
        WriteContextBuilder builder(table_path, "test");
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

    std::shared_ptr<DataFileMeta> OnlyNewFile(
        const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        EXPECT_EQ(1, commit_msgs.size());
        auto msg = std::dynamic_pointer_cast<CommitMessageImpl>(commit_msgs[0]);
        EXPECT_NE(nullptr, msg);
        EXPECT_EQ(1, msg->GetNewFilesIncrement().NewFiles().size());
        return msg->GetNewFilesIncrement().NewFiles()[0];
    }

    void Commit(const std::string& table_path, const std::map<std::string, std::string>& options,
                const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        CommitContextBuilder builder(table_path, "test");
        builder.SetOptions(options);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_commit,
                             FileStoreCommit::Create(std::move(commit_context)));
        ASSERT_OK(file_store_commit->Commit(commit_msgs));
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
};

TEST_F(KeyValueFileStoreWriteTest, TestWriteWithInvalidBatch) {
    auto fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),     arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),    arrow::field("f5", arrow::int32()),
        arrow::field("f6", arrow::int32()),    arrow::field("f7", arrow::int64()),
        arrow::field("f8", arrow::int64()),    arrow::field("f9", arrow::float32()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};
    std::string commit_user = "test";
    {
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);

        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{"f1"}, /*options=*/{{"bucket", "1"}},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), commit_user);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        ASSERT_NOK_WITH_MSG(file_store_write->Write(nullptr), "batch is null pointer");
    }
    {
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{"f1"}, /*options=*/{{"bucket", "-2"}},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                            commit_user);
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
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             batch_builder.SetBucket(1).Finish());
        ASSERT_NOK_WITH_MSG(file_store_write->Write(std::move(batch)),
                            "batch bucket is 1 while options bucket is -2");
        ArrowArrayRelease(&arrow_array);
    }
    {
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{"f1"}, /*options=*/{{"bucket", "2"}},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                            commit_user);
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
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             batch_builder.SetBucket(3).Finish());
        ASSERT_NOK_WITH_MSG(
            file_store_write->Write(std::move(batch)),
            "fixed bucketed mode must specify a bucket which in [0, 2) in RecordBatch");
        ArrowArrayRelease(&arrow_array);
    }
}

TEST_F(KeyValueFileStoreWriteTest, TestPrepareCommitShouldSucceedWhenLookupEnabledWithIOManager) {
    ASSERT_OK_AND_ASSIGN(
        auto file_store_write,
        CreateSingleStringFileStoreWrite({{"bucket", "1"}, {Options::FORCE_LOOKUP, "true"}},
                                         /*with_temp_directory=*/true));

    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "k1"));
    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(commit_messages.size(), 1);
}

TEST_F(KeyValueFileStoreWriteTest,
       TestPrepareCommitShouldSucceedWhenDefaultCompactRewriterPathEnabled) {
    ASSERT_OK_AND_ASSIGN(
        auto file_store_write,
        CreateSingleStringFileStoreWrite({{"bucket", "1"}}, /*with_temp_directory=*/false));

    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "k1"));
    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(commit_messages.size(), 1);
}

TEST_F(KeyValueFileStoreWriteTest, TestSharedShreddingMapRestoreInitializesNextWriter) {
    std::map<std::string, std::string> options = {
        {"file.format", "parquet"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "10"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {"write-only", "true"},
        {"bucket", "1"},
    };
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32(), /*nullable=*/false),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });
    auto write_schema = SpecialFields::CompleteSequenceAndValueKindField(logical_schema);

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
    auto first_meta = ShreddingMeta(first_file_schema, /*field_index=*/3);
    ASSERT_EQ(10, first_meta.num_columns);
    ASSERT_EQ(2, first_meta.max_row_width);
    Commit(table_path, options, first_commit_msgs);

    auto second_commit_msgs = WriteAndPrepare(table_path, logical_schema, options, R"([
        [2, [["c", 3], ["d", 4], ["e", 5]]]
    ])",
                                              /*commit_identifier=*/1);
    auto second_file_schema =
        ReadDataFileSchema(table_path, OnlyNewFile(second_commit_msgs), options);
    auto second_meta = ShreddingMeta(second_file_schema, /*field_index=*/3);

    ASSERT_OK_AND_ASSIGN(
        auto expected_second_schema,
        MapSharedShreddingUtils::LogicalToPhysicalSchema(write_schema, {{"tags", 2}}));
    ASSERT_TRUE(second_file_schema->Equals(*expected_second_schema, /*check_metadata=*/false));
    ASSERT_EQ(2, second_meta.num_columns);
    ASSERT_EQ(3, second_meta.max_row_width);
}

TEST_F(KeyValueFileStoreWriteTest, TestSpillSimple) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{}, /*primary_keys=*/{"f0"},
                                   {{Options::BUCKET, "2"},
                                    {Options::WRITE_BUFFER_SIZE, "64"},
                                    {Options::WRITE_BUFFER_SPILLABLE, "true"}},
                                   /*ignore_if_exists=*/false));

    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    context_builder.WithTempDirectory(dir->Str());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));
    auto key_value_file_store_write = dynamic_cast<KeyValueFileStoreWrite*>(file_store_write.get());
    auto get_writer = [&](int32_t bucket) -> std::shared_ptr<paimon::BatchWriter> {
        auto partition_iter = key_value_file_store_write->writers_.find(BinaryRow::EmptyRow());
        if (partition_iter != key_value_file_store_write->writers_.end()) {
            auto& buckets = partition_iter->second;
            auto bucket_iter = buckets.find(bucket);
            if (PAIMON_LIKELY(bucket_iter != buckets.end())) {
                return bucket_iter->second.writer;
            }
        }
        assert(false);
        return nullptr;
    };

    // write bucket 0, not trigger spill
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, std::string(48, 'a')));
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);
    ASSERT_GT(get_writer(0)->GetMemoryUsage(), 0);

    // write bucket 1, spill bucket 0 (pick largest writer)
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/1, std::string(32, 'b')));
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 1);
    ASSERT_EQ(get_writer(0)->GetMemoryUsage(), 0);
    ASSERT_GT(get_writer(1)->GetMemoryUsage(), 0);

    // prepare commit, clean all spill files and memory buffers
    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(commit_messages.size(), 2);
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);
    ASSERT_EQ(get_writer(0)->GetMemoryUsage(), 0);
    ASSERT_EQ(get_writer(1)->GetMemoryUsage(), 0);
}

TEST_F(KeyValueFileStoreWriteTest, TestSpillDiskQuotaExhaustedFallsBackToFlushDataFile) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{}, /*primary_keys=*/{"f0"},
                                   {{Options::BUCKET, "1"},
                                    {Options::WRITE_BUFFER_SIZE, "1"},
                                    {Options::WRITE_BUFFER_SPILLABLE, "true"},
                                    {Options::WRITE_BUFFER_SPILL_MAX_DISK_SIZE, "1b"}},
                                   /*ignore_if_exists=*/false));
    ArrowSchemaRelease(&schema);
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    context_builder.WithTempDirectory(dir->Str());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    // Disk quota is 1 byte, so spill will exhaust quota immediately and fall back to
    // FlushWriteBuffer (writing data files directly instead of spill temp files).
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "alice"));
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);

    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "bob"));
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);

    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(commit_messages.size(), 1);

    // Verify all rows are committed correctly despite disk quota exhaustion.
    auto* commit_impl = dynamic_cast<CommitMessageImpl*>(commit_messages[0].get());
    ASSERT_NE(commit_impl, nullptr);
    const auto& new_files = commit_impl->GetNewFilesIncrement().NewFiles();
    ASSERT_FALSE(new_files.empty());

    int64_t total_row_count = 0;
    for (const auto& file : new_files) {
        total_row_count += file->row_count;
    }
    ASSERT_EQ(total_row_count, 2);
}

TEST_F(KeyValueFileStoreWriteTest, TestMultiRoundSpillWithSameKeyDeduplication) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{}, /*primary_keys=*/{"f0"},
                                   {{Options::BUCKET, "1"},
                                    {Options::WRITE_BUFFER_SIZE, "1"},
                                    {Options::WRITE_BUFFER_SPILLABLE, "true"}},
                                   /*ignore_if_exists=*/false));
    ArrowSchemaRelease(&schema);
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    context_builder.WithTempDirectory(dir->Str()).WithStreamingMode(true);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    // Round 1: alice, bob, alice (duplicate key) → after dedup: alice + bob = 2 rows
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "alice"));
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "bob"));
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "alice"));

    ASSERT_OK_AND_ASSIGN(auto commit_messages_1,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true, 0));
    ASSERT_EQ(commit_messages_1.size(), 1);
    {
        auto* commit_impl = dynamic_cast<CommitMessageImpl*>(commit_messages_1[0].get());
        ASSERT_NE(commit_impl, nullptr);
        int64_t total_row_count = 0;
        for (const auto& file : commit_impl->GetNewFilesIncrement().NewFiles()) {
            total_row_count += file->row_count;
        }
        ASSERT_EQ(total_row_count, 2);
    }
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);

    // Round 2: bob, charlie, charlie (duplicate key) → after dedup: bob + charlie = 2 rows
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "bob"));
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "charlie"));
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "charlie"));

    ASSERT_OK_AND_ASSIGN(auto commit_messages_2,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true, 1));
    ASSERT_EQ(commit_messages_2.size(), 1);
    {
        auto* commit_impl = dynamic_cast<CommitMessageImpl*>(commit_messages_2[0].get());
        ASSERT_NE(commit_impl, nullptr);
        int64_t total_row_count = 0;
        for (const auto& file : commit_impl->GetNewFilesIncrement().NewFiles()) {
            total_row_count += file->row_count;
        }
        ASSERT_EQ(total_row_count, 2);
    }
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);
}

}  // namespace paimon::test
