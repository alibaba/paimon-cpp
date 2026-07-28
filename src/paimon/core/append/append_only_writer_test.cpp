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

#include "paimon/core/append/append_only_writer.h"

#include <cstddef>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/data/blob_view_struct.h"
#include "paimon/common/data/shredding/map_shared_shredding_context.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/common/fs/external_path_provider.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/core/compact/compact_deletion_file.h"
#include "paimon/core/compact/compact_result.h"
#include "paimon/core/compact/noop_compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/defs.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::test {

namespace {

class FakeCompactDeletionFile : public CompactDeletionFile,
                                public std::enable_shared_from_this<FakeCompactDeletionFile> {
 public:
    explicit FakeCompactDeletionFile(std::string id) : id_(std::move(id)) {}

    Result<std::optional<std::shared_ptr<IndexFileMeta>>> GetOrCompute() override {
        return std::optional<std::shared_ptr<IndexFileMeta>>();
    }

    Result<std::shared_ptr<CompactDeletionFile>> MergeOldFile(
        const std::shared_ptr<CompactDeletionFile>& old) override {
        merged_old_ = old;
        return shared_from_this();
    }

    void Clean() override {
        cleaned_ = true;
    }

    const std::string& Id() const {
        return id_;
    }

    bool Cleaned() const {
        return cleaned_;
    }

    std::shared_ptr<CompactDeletionFile> MergedOld() const {
        return merged_old_;
    }

 private:
    std::string id_;
    bool cleaned_ = false;
    std::shared_ptr<CompactDeletionFile> merged_old_;
};

class FakeCompactManager : public CompactManager {
 public:
    Status AddNewFile(const std::shared_ptr<DataFileMeta>& file) override {
        added_files.push_back(file);
        return Status::OK();
    }

    std::vector<std::shared_ptr<DataFileMeta>> AllFiles() const override {
        return all_files;
    }

    Status TriggerCompaction(bool full_compaction) override {
        trigger_calls.push_back(full_compaction);
        return Status::OK();
    }

    Result<std::optional<std::shared_ptr<CompactResult>>> GetCompactionResult(
        bool blocking) override {
        get_result_blocking_calls.push_back(blocking);
        if (queued_results.empty()) {
            return std::optional<std::shared_ptr<CompactResult>>();
        }
        auto result = queued_results.front();
        queued_results.pop_front();
        return result;
    }

    void RequestCancelCompaction() override {
        request_cancel_called = true;
    }

    void WaitForCompactionToExit() override {
        wait_called = true;
    }

    bool CompactNotCompleted() const override {
        return compact_not_completed;
    }

    bool ShouldWaitForLatestCompaction() const override {
        return should_wait_latest;
    }

    bool ShouldWaitForPreparingCheckpoint() const override {
        return should_wait_prepare;
    }

    Status Close() override {
        close_called = true;
        return Status::OK();
    }

    std::vector<std::shared_ptr<DataFileMeta>> added_files;
    std::vector<std::shared_ptr<DataFileMeta>> all_files;
    std::vector<bool> trigger_calls;
    std::vector<bool> get_result_blocking_calls;
    std::deque<Result<std::optional<std::shared_ptr<CompactResult>>>> queued_results;
    bool compact_not_completed = false;
    bool should_wait_latest = false;
    bool should_wait_prepare = false;
    bool request_cancel_called = false;
    bool wait_called = false;
    bool close_called = false;
};

}  // namespace

class AppendOnlyWriterTest : public testing::Test {
 public:
    void SetUp() override {
        memory_pool_ = GetDefaultPool();
        compact_manager_ = std::make_shared<NoopCompactManager>();
    }

    CoreOptions CreateOptions(const std::map<std::string, std::string>& overrides = {}) const {
        std::map<std::string, std::string> raw_options = {
            {Options::FILE_SYSTEM, "local"},
            {Options::FILE_FORMAT, "mock_format"},
            {Options::MANIFEST_FORMAT, "mock_format"},
        };
        for (const auto& [key, value] : overrides) {
            raw_options[key] = value;
        }
        return CoreOptions::FromMap(raw_options).value();
    }

    std::shared_ptr<DataFilePathFactory> CreatePathFactory(const std::string& dir,
                                                           const std::string& format,
                                                           const CoreOptions& options) const {
        auto path_factory = std::make_shared<DataFilePathFactory>();
        EXPECT_TRUE(path_factory->Init(dir, format, options.DataFilePrefix(), nullptr).ok());
        return path_factory;
    }

    std::shared_ptr<DataFileMeta> NewAppendFile(const std::string& file_name, int64_t row_count,
                                                int64_t min_sequence_number,
                                                int64_t max_sequence_number) const {
        return DataFileMeta::ForAppend(file_name, /*file_size=*/row_count, row_count,
                                       SimpleStats::EmptyStats(), min_sequence_number,
                                       max_sequence_number, /*schema_id=*/0, FileSource::Append(),
                                       std::nullopt, std::nullopt, std::nullopt, std::nullopt)
            .value();
    }

    std::unique_ptr<RecordBatch> CreateSingleStringBatch(
        const std::vector<std::string>& values,
        const std::optional<std::vector<RecordBatch::RowKind>>& row_kinds = std::nullopt) const {
        arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
        auto struct_type = arrow::struct_(fields);
        arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::StringBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        for (const auto& value : values) {
            EXPECT_TRUE(struct_builder.Append().ok());
            EXPECT_TRUE(string_builder->Append(value).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());

        ::ArrowArray arrow_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        if (row_kinds.has_value()) {
            batch_builder.SetRowKinds(row_kinds.value());
        }
        return batch_builder.Finish().value();
    }

    std::unique_ptr<RecordBatch> CreateStructBatch(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::shared_ptr<arrow::Array>>& columns) const {
        auto raw_struct_array = arrow::StructArray::Make(columns, schema->fields()).ValueOrDie();
        ::ArrowArray arrow_array;
        EXPECT_TRUE(arrow::ExportArray(*raw_struct_array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        return batch_builder.Finish().value();
    }

    /// Creates a RecordBatch from a JSON string matching the given schema.
    std::unique_ptr<RecordBatch> CreateBatch(const std::shared_ptr<arrow::Schema>& schema,
                                             const std::string& json) const {
        auto struct_type = arrow::struct_(schema->fields());
        auto array = arrow::ipc::internal::json::ArrayFromJSON(struct_type, json).ValueOrDie();
        ::ArrowArray arrow_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        return batch_builder.Finish().value();
    }

    /// Opens a file using the specified format and returns a reader.
    std::unique_ptr<FileBatchReader> OpenFormatReader(const std::string& file_path,
                                                      const std::string& format) const {
        auto fs = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, fs->Open(file_path));
        EXPECT_TRUE(input_stream);
        EXPECT_OK_AND_ASSIGN(auto file_format, FileFormatFactory::Get(format, /*options=*/{}));
        EXPECT_OK_AND_ASSIGN(auto reader_builder,
                             file_format->CreateReaderBuilder(/*batch_size=*/10));
        return reader_builder->Build(input_stream).value();
    }

    /// Reads a file's content and compares it to expected_array.
    void CheckFileContent(const std::string& file_path, const std::string& format,
                          const std::shared_ptr<arrow::ChunkedArray>& expected_array) const {
        auto reader = OpenFormatReader(file_path, format);
        auto c_file_schema = reader->GetFileSchema().value();
        ASSERT_OK(reader->SetReadSchema(c_file_schema.get(), /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(reader.get()));
        ASSERT_TRUE(expected_array->Equals(result_array))
            << "Expected:\n"
            << expected_array->ToString() << "\nActual:\n"
            << result_array->ToString();
    }

    /// Reads a file's schema, compares structure against expected physical schema
    /// (ignoring metadata), then verifies shared-shredding map metadata on the given field.
    void CheckShreddingFileSchema(const std::string& file_path, const std::string& format,
                                  const std::shared_ptr<arrow::Schema>& expected_physical_schema,
                                  int32_t field_index,
                                  const MapSharedShreddingFieldMeta& expected_meta,
                                  const std::string& compression) const {
        auto reader = OpenFormatReader(file_path, format);
        auto c_file_schema = reader->GetFileSchema().value();
        auto file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();

        // Compare schema structure (types + field names), ignoring metadata.
        ASSERT_TRUE(file_schema->Equals(*expected_physical_schema, /*check_metadata=*/false))
            << "Expected schema:\n"
            << expected_physical_schema->ToString() << "\nActual schema:\n"
            << file_schema->ToString();

        // Deserialize and compare the per-field shared-shredding map metadata.
        auto metadata = file_schema->field(field_index)->metadata();
        ASSERT_NE(nullptr, metadata);
        ASSERT_OK_AND_ASSIGN(
            auto deserialized_meta,
            MapSharedShreddingUtils::DeserializeMetadata(
                metadata->Copy(), MapSharedShreddingDefine::kDefaultDictCompression));
        ASSERT_EQ(expected_meta, deserialized_meta);
    }

    Result<std::unique_ptr<AppendOnlyWriter>> CreateAppendOnlyWriter(
        const CoreOptions& options, int64_t schema_id,
        const std::shared_ptr<arrow::Schema>& write_schema,
        const std::optional<std::vector<std::string>>& write_cols, int64_t max_sequence_number,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<CompactManager>& compact_manager,
        const std::shared_ptr<MemoryPool>& memory_pool) const {
        PAIMON_ASSIGN_OR_RAISE(
            auto shredding_context,
            MapSharedShreddingUtils::CreateShreddingContext(write_schema, options));
        return std::make_unique<AppendOnlyWriter>(options, schema_id, write_schema, write_cols,
                                                  max_sequence_number, path_factory,
                                                  compact_manager, shredding_context, memory_pool);
    }

 protected:
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<CompactManager> compact_manager_;
};

TEST_F(AppendOnlyWriterTest, TestEmptyCommits) {
    std::map<std::string, std::string> raw_options;
    raw_options[Options::FILE_FORMAT] = "mock_format";
    raw_options[Options::FILE_SYSTEM] = "local";
    raw_options[Options::MANIFEST_FORMAT] = "mock_format";
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::uint8()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};

    auto schema = arrow::schema(fields);

    auto path_factory = std::make_shared<DataFilePathFactory>();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK(path_factory->Init(dir->Str(), "mock_format", options.DataFilePrefix(), nullptr));

    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager_, memory_pool_));
    for (int32_t i = 0; i < 3; i++) {
        ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(true));
        ASSERT_TRUE(inc.GetNewFilesIncrement().IsEmpty());
        ASSERT_TRUE(inc.GetCompactIncrement().IsEmpty());
    }
}

TEST_F(AppendOnlyWriterTest, TestWriteAndPrepareCommit) {
    std::map<std::string, std::string> raw_options;
    raw_options[Options::FILE_FORMAT] = "mock_format";
    raw_options[Options::FILE_SYSTEM] = "local";
    raw_options[Options::MANIFEST_FORMAT] = "mock_format";
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::uint8()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};

    auto schema = arrow::schema(fields);

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "mock_format", options.DataFilePrefix(), nullptr));
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/2, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager_, memory_pool_));
    arrow::StringBuilder builder;
    for (size_t j = 0; j < 100; j++) {
        ASSERT_TRUE(builder.Append(std::to_string(j)).ok());
    }
    std::shared_ptr<arrow::Array> array = builder.Finish().ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(auto record_batch, batch_builder.Finish());
    ASSERT_OK(writer->Write(std::move(record_batch)));
    ASSERT_TRUE(ArrowArrayIsReleased(&arrow_array));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(true));
    ASSERT_FALSE(inc.GetNewFilesIncrement().IsEmpty());
    const auto& data_increment = inc.GetNewFilesIncrement();
    const auto& data_file_metas = data_increment.NewFiles();
    ASSERT_EQ(1, data_file_metas.size());
    ASSERT_EQ(2, data_file_metas[0]->schema_id);
    ASSERT_TRUE(inc.GetCompactIncrement().IsEmpty());
    std::string path = path_factory->ToPath(inc.GetNewFilesIncrement().NewFiles()[0]->file_name);
    ASSERT_OK_AND_ASSIGN(bool exist, options.GetFileSystem()->Exists(path));
    ASSERT_TRUE(exist);
    ASSERT_OK(writer->Close());
}

TEST_F(AppendOnlyWriterTest, TestWriteAndClose) {
    std::map<std::string, std::string> raw_options;
    raw_options[Options::FILE_FORMAT] = "orc";
    raw_options[Options::FILE_SYSTEM] = "local";
    raw_options[Options::MANIFEST_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/1, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager_, memory_pool_));
    auto struct_type = arrow::struct_(fields);
    arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::StringBuilder>()});
    auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
    for (size_t j = 0; j < 100; j++) {
        ASSERT_TRUE(struct_builder.Append().ok());
        ASSERT_TRUE(string_builder->Append(std::to_string(j)).ok());
    }
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(struct_builder.Finish(&array).ok());
    ASSERT_TRUE(array);
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());

    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(auto record_batch, batch_builder.Finish());
    ASSERT_OK(writer->Write(std::move(record_batch)));
    ASSERT_TRUE(ArrowArrayIsReleased(&arrow_array));
    ASSERT_OK(writer->Close());

    auto file_system = std::make_shared<LocalFileSystem>();
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    ASSERT_OK(file_system->ListDir(dir->Str(), &file_status_list));
    ASSERT_TRUE(file_status_list.empty());
}

TEST_F(AppendOnlyWriterTest, TestInvalidRowKind) {
    std::map<std::string, std::string> raw_options;
    raw_options[Options::FILE_FORMAT] = "orc";
    raw_options[Options::FILE_SYSTEM] = "local";
    raw_options[Options::MANIFEST_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/1, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager_, memory_pool_));
    auto struct_type = arrow::struct_(fields);
    arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::StringBuilder>()});
    auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
    ASSERT_TRUE(struct_builder.Append().ok());
    ASSERT_TRUE(string_builder->Append("row0").ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(struct_builder.Finish(&array).ok());
    ASSERT_TRUE(array);
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());

    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(auto record_batch,
                         batch_builder.SetRowKinds({RecordBatch::RowKind::DELETE}).Finish());
    ASSERT_NOK_WITH_MSG(writer->Write(std::move(record_batch)),
                        "Append only writer can not accept record batch with RowKind DELETE");
    ASSERT_TRUE(ArrowArrayIsReleased(&arrow_array));
    ASSERT_OK(writer->Close());

    auto file_system = std::make_shared<LocalFileSystem>();
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    ASSERT_OK(file_system->ListDir(dir->Str(), &file_status_list));
    ASSERT_TRUE(file_status_list.empty());
}

TEST_F(AppendOnlyWriterTest, TestPrepareCommitWaitCompactionUsesBlockingGetResult) {
    auto options = CreateOptions();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "mock_format", options);
    auto compact_manager = std::make_shared<FakeCompactManager>();

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager, memory_pool_));

    ASSERT_OK(writer->Write(CreateSingleStringBatch({"a", "b"})));
    ASSERT_OK(writer->PrepareCommit(/*wait_compaction=*/true).status());

    ASSERT_EQ(compact_manager->get_result_blocking_calls.size(), 2);
    ASSERT_FALSE(compact_manager->get_result_blocking_calls[0]);
    ASSERT_TRUE(compact_manager->get_result_blocking_calls[1]);
    ASSERT_OK(writer->Close());
}

TEST_F(AppendOnlyWriterTest, TestPrepareCommitForceCompactUsesBlockingGetResult) {
    auto options = CreateOptions({{Options::COMMIT_FORCE_COMPACT, "true"}});
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "mock_format", options);
    auto compact_manager = std::make_shared<FakeCompactManager>();

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager, memory_pool_));

    ASSERT_OK(writer->Write(CreateSingleStringBatch({"a"})));
    ASSERT_OK(writer->PrepareCommit(/*wait_compaction=*/false).status());

    ASSERT_EQ(compact_manager->get_result_blocking_calls.size(), 2);
    ASSERT_FALSE(compact_manager->get_result_blocking_calls[0]);
    ASSERT_TRUE(compact_manager->get_result_blocking_calls[1]);
    ASSERT_OK(writer->Close());
}

TEST_F(AppendOnlyWriterTest,
       TestSyncAndPrepareCommitConsumeCompactionResultsAndMergeDeletionFiles) {
    auto options = CreateOptions();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "mock_format", options);
    auto compact_manager = std::make_shared<FakeCompactManager>();

    auto before1 = NewAppendFile("before-1", 10, 0, 9);
    auto after1 = NewAppendFile("after-1", 10, 10, 19);
    auto before2 = NewAppendFile("before-2", 10, 20, 29);
    auto after2 = NewAppendFile("after-2", 10, 30, 39);
    auto deletion_file1 = std::make_shared<FakeCompactDeletionFile>("d1");
    auto deletion_file2 = std::make_shared<FakeCompactDeletionFile>("d2");

    auto result1 =
        std::make_shared<CompactResult>(std::vector<std::shared_ptr<DataFileMeta>>{before1},
                                        std::vector<std::shared_ptr<DataFileMeta>>{after1});
    result1->SetDeletionFile(deletion_file1);
    auto result2 =
        std::make_shared<CompactResult>(std::vector<std::shared_ptr<DataFileMeta>>{before2},
                                        std::vector<std::shared_ptr<DataFileMeta>>{after2});
    result2->SetDeletionFile(deletion_file2);
    compact_manager->queued_results.push_back(
        std::optional<std::shared_ptr<CompactResult>>(result1));
    compact_manager->queued_results.push_back(
        std::optional<std::shared_ptr<CompactResult>>(result2));

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager, memory_pool_));

    ASSERT_OK(writer->Sync());
    ASSERT_OK(writer->Sync());
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/false));

    ASSERT_EQ(inc.GetCompactIncrement().CompactBefore().size(), 2);
    ASSERT_EQ(inc.GetCompactIncrement().CompactAfter().size(), 2);
    ASSERT_EQ(*inc.GetCompactIncrement().CompactBefore()[0], *before1);
    ASSERT_EQ(*inc.GetCompactIncrement().CompactBefore()[1], *before2);
    ASSERT_EQ(*inc.GetCompactIncrement().CompactAfter()[0], *after1);
    ASSERT_EQ(*inc.GetCompactIncrement().CompactAfter()[1], *after2);

    auto merged = std::dynamic_pointer_cast<FakeCompactDeletionFile>(inc.GetCompactDeletionFile());
    ASSERT_TRUE(merged);
    ASSERT_EQ(merged->Id(), "d2");
    ASSERT_EQ(merged->MergedOld(), deletion_file1);
    ASSERT_OK(writer->Close());
}

TEST_F(AppendOnlyWriterTest, TestCloseDeletesCompactAfterFiles) {
    auto options =
        CreateOptions({{Options::FILE_FORMAT, "orc"}, {Options::MANIFEST_FORMAT, "orc"}});
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "orc", options);
    auto compact_manager = std::make_shared<FakeCompactManager>();

    auto compact_after = NewAppendFile("compact-after.orc", 1, 0, 0);
    auto compact_after_path = path_factory->ToPath(compact_after->file_name);
    ASSERT_OK_AND_ASSIGN(auto output, options.GetFileSystem()->Create(compact_after_path, true));
    ASSERT_OK(output->Close());

    auto result =
        std::make_shared<CompactResult>(std::vector<std::shared_ptr<DataFileMeta>>{},
                                        std::vector<std::shared_ptr<DataFileMeta>>{compact_after});
    compact_manager->queued_results.push_back(
        std::optional<std::shared_ptr<CompactResult>>(result));

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager, memory_pool_));

    ASSERT_OK(writer->Sync());
    ASSERT_TRUE(options.GetFileSystem()->Exists(compact_after_path).value());
    ASSERT_OK(writer->Close());
    ASSERT_FALSE(options.GetFileSystem()->Exists(compact_after_path).value());
    ASSERT_TRUE(compact_manager->request_cancel_called);
    ASSERT_TRUE(compact_manager->wait_called);
    ASSERT_TRUE(compact_manager->close_called);
}

TEST_F(AppendOnlyWriterTest, TestCloseCleansDeletionFile) {
    auto options = CreateOptions();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "mock_format", options);
    auto compact_manager = std::make_shared<FakeCompactManager>();

    auto deletion_file = std::make_shared<FakeCompactDeletionFile>("del-close");
    auto before = NewAppendFile("before-close", 5, 0, 4);
    auto after = NewAppendFile("after-close", 5, 5, 9);
    auto result =
        std::make_shared<CompactResult>(std::vector<std::shared_ptr<DataFileMeta>>{before},
                                        std::vector<std::shared_ptr<DataFileMeta>>{after});
    result->SetDeletionFile(deletion_file);
    compact_manager->queued_results.push_back(
        std::optional<std::shared_ptr<CompactResult>>(result));

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager, memory_pool_));

    // Sync to consume the compaction result and populate compact_deletion_file_.
    ASSERT_OK(writer->Sync());
    ASSERT_FALSE(deletion_file->Cleaned());

    ASSERT_OK(writer->Close());
    ASSERT_TRUE(deletion_file->Cleaned());
}

TEST_F(AppendOnlyWriterTest, TestCompactNotCompletedTriggersCompaction) {
    auto options = CreateOptions();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "mock_format", options);
    auto compact_manager = std::make_shared<FakeCompactManager>();
    compact_manager->compact_not_completed = true;

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager, memory_pool_));

    ASSERT_OK_AND_ASSIGN(bool not_completed, writer->CompactNotCompleted());
    ASSERT_TRUE(not_completed);
    ASSERT_EQ(compact_manager->trigger_calls, std::vector<bool>({false}));
    ASSERT_OK(writer->Close());
}

TEST_F(AppendOnlyWriterTest, TestCompactPassesFullCompactionFlag) {
    auto options = CreateOptions();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "mock_format", options);
    auto compact_manager = std::make_shared<FakeCompactManager>();

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager, memory_pool_));

    ASSERT_OK(writer->Compact(/*full_compaction=*/true));
    ASSERT_OK(writer->Compact(/*full_compaction=*/false));
    ASSERT_EQ(compact_manager->trigger_calls, std::vector<bool>({true, false}));
    ASSERT_OK(writer->Close());
}

TEST_F(AppendOnlyWriterTest, TestWriteWithSingleBlobField) {
    auto options =
        CreateOptions({{Options::FILE_FORMAT, "orc"}, {Options::MANIFEST_FORMAT, "orc"}});
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "orc", options);

    auto int_field = arrow::field("id", arrow::int32());
    auto blob_field = BlobUtils::ToArrowField("blob", false);
    auto schema = arrow::schema({int_field, blob_field});

    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager_, memory_pool_));

    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.AppendValues({1, 2}).ok());
    auto int_array = int_builder.Finish().ValueOrDie();
    arrow::LargeBinaryBuilder blob_builder;
    ASSERT_TRUE(blob_builder.Append("a", 1).ok());
    ASSERT_TRUE(blob_builder.Append("bb", 2).ok());
    auto blob_array = blob_builder.Finish().ValueOrDie();

    ASSERT_OK(writer->Write(CreateStructBatch(schema, {int_array, blob_array})));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/true));

    ASSERT_EQ(inc.GetNewFilesIncrement().NewFiles().size(), 2);
    const auto& main_file = inc.GetNewFilesIncrement().NewFiles()[0];
    const auto& blob_file = inc.GetNewFilesIncrement().NewFiles()[1];
    ASSERT_TRUE(
        options.GetFileSystem()->Exists(path_factory->ToPath(main_file->file_name)).value());
    ASSERT_TRUE(
        options.GetFileSystem()->Exists(path_factory->ToPath(blob_file->file_name)).value());
    ASSERT_OK(writer->Close());
}

TEST_F(AppendOnlyWriterTest, TestWriteWithOnlyBlobField) {
    auto options =
        CreateOptions({{Options::FILE_FORMAT, "orc"}, {Options::MANIFEST_FORMAT, "orc"}});
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "orc", options);

    auto blob_field = BlobUtils::ToArrowField("blob", false);
    auto schema = arrow::schema({blob_field});
    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/0, schema,
                                                /*write_cols=*/std::vector<std::string>{"blob"},
                                                /*max_sequence_number=*/-1, path_factory,
                                                compact_manager_, memory_pool_));

    arrow::LargeBinaryBuilder blob_builder;
    ASSERT_TRUE(blob_builder.Append("a", 1).ok());
    ASSERT_TRUE(blob_builder.Append("bb", 2).ok());
    auto blob_array = blob_builder.Finish().ValueOrDie();

    ASSERT_OK(writer->Write(CreateStructBatch(schema, {blob_array})));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_OK(writer->Close());

    const auto& new_files = inc.GetNewFilesIncrement().NewFiles();
    ASSERT_EQ(new_files.size(), 1);
    ASSERT_TRUE(BlobUtils::IsBlobFile(new_files[0]->file_name));
    ASSERT_EQ(new_files[0]->row_count, 2);
    ASSERT_TRUE(new_files[0]->write_cols.has_value());
    ASSERT_EQ(new_files[0]->write_cols.value(), std::vector<std::string>({"blob"}));
    std::string blob_file_path = path_factory->ToPath(new_files[0]->file_name);
    ASSERT_TRUE(options.GetFileSystem()->Exists(blob_file_path).value());

    auto blob_reader = OpenFormatReader(blob_file_path, "blob");
    ::ArrowSchema c_blob_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_blob_schema).ok());
    ASSERT_OK(blob_reader->SetReadSchema(&c_blob_schema, /*predicate=*/nullptr,
                                         /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto actual_array, ReadResultCollector::CollectResult(blob_reader.get()));
    auto expected_struct_array =
        arrow::StructArray::Make({blob_array}, {blob_field->name()}).ValueOrDie();
    auto expected_array = std::make_shared<arrow::ChunkedArray>(expected_struct_array);
    ASSERT_TRUE(expected_array->Equals(actual_array)) << "Expected:\n"
                                                      << expected_array->ToString() << "\nActual:\n"
                                                      << actual_array->ToString();
}

TEST_F(AppendOnlyWriterTest, TestWriteWithMultipleBlobFields) {
    auto options =
        CreateOptions({{Options::FILE_FORMAT, "orc"}, {Options::MANIFEST_FORMAT, "orc"}});
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "orc", options);

    auto schema =
        arrow::schema({arrow::field("id", arrow::int32()), BlobUtils::ToArrowField("blob1", false),
                       BlobUtils::ToArrowField("blob2", false)});
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager_, memory_pool_));

    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.AppendValues({1}).ok());
    auto int_array = int_builder.Finish().ValueOrDie();
    arrow::LargeBinaryBuilder blob_builder1;
    ASSERT_TRUE(blob_builder1.Append("a", 1).ok());
    auto blob_array1 = blob_builder1.Finish().ValueOrDie();
    arrow::LargeBinaryBuilder blob_builder2;
    ASSERT_TRUE(blob_builder2.Append("b", 1).ok());
    auto blob_array2 = blob_builder2.Finish().ValueOrDie();

    ASSERT_OK(writer->Write(CreateStructBatch(schema, {int_array, blob_array1, blob_array2})));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/true));

    ASSERT_EQ(inc.GetNewFilesIncrement().NewFiles().size(), 3);
    const auto& main_file = inc.GetNewFilesIncrement().NewFiles()[0];
    const auto& blob_file1 = inc.GetNewFilesIncrement().NewFiles()[1];
    const auto& blob_file2 = inc.GetNewFilesIncrement().NewFiles()[2];
    ASSERT_TRUE(
        options.GetFileSystem()->Exists(path_factory->ToPath(main_file->file_name)).value());
    ASSERT_TRUE(
        options.GetFileSystem()->Exists(path_factory->ToPath(blob_file1->file_name)).value());
    ASSERT_TRUE(
        options.GetFileSystem()->Exists(path_factory->ToPath(blob_file2->file_name)).value());
    ASSERT_OK(writer->Close());
}

TEST_F(AppendOnlyWriterTest, TestMultiplePrepareCommitSequenceContinuity) {
    auto options = CreateOptions();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "mock_format", options);

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager_, memory_pool_));

    ASSERT_OK(writer->Write(CreateSingleStringBatch({"a", "b", "c"})));
    ASSERT_OK_AND_ASSIGN(CommitIncrement first, writer->PrepareCommit(/*wait_compaction=*/false));
    ASSERT_OK(writer->Write(CreateSingleStringBatch({"d", "e"})));
    ASSERT_OK_AND_ASSIGN(CommitIncrement second, writer->PrepareCommit(/*wait_compaction=*/false));

    ASSERT_EQ(first.GetNewFilesIncrement().NewFiles().size(), 1);
    ASSERT_EQ(second.GetNewFilesIncrement().NewFiles().size(), 1);
    ASSERT_EQ(first.GetNewFilesIncrement().NewFiles()[0]->min_sequence_number, 0);
    ASSERT_EQ(first.GetNewFilesIncrement().NewFiles()[0]->max_sequence_number, 2);
    ASSERT_EQ(second.GetNewFilesIncrement().NewFiles()[0]->min_sequence_number, 3);
    ASSERT_EQ(second.GetNewFilesIncrement().NewFiles()[0]->max_sequence_number, 4);
    ASSERT_OK(writer->Close());
}

TEST_F(AppendOnlyWriterTest, TestWriteValidBlobViewField) {
    auto options = CreateOptions({{Options::FILE_FORMAT, "orc"},
                                  {Options::MANIFEST_FORMAT, "orc"},
                                  {Options::BLOB_VIEW_FIELD, "view"}});
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "orc", options);

    auto schema =
        arrow::schema({arrow::field("f0", arrow::int32()), BlobUtils::ToArrowField("view", true)});
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager_, memory_pool_));

    // Build f0 column
    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.AppendValues({1, 2}).ok());
    auto int_array = int_builder.Finish().ValueOrDie();

    // Build view column with valid BlobViewStruct values
    arrow::LargeBinaryBuilder view_builder;
    BlobViewStruct view_struct_0(Identifier("db", "tbl"), /*field_id=*/1, /*row_id=*/0);
    auto view_bytes_0 = view_struct_0.Serialize(memory_pool_);
    ASSERT_TRUE(view_builder.Append(view_bytes_0->data(), view_bytes_0->size()).ok());

    BlobViewStruct view_struct_1(Identifier("db", "tbl"), /*field_id=*/1, /*row_id=*/1);
    auto view_bytes_1 = view_struct_1.Serialize(memory_pool_);
    ASSERT_TRUE(view_builder.Append(view_bytes_1->data(), view_bytes_1->size()).ok());

    auto view_array = view_builder.Finish().ValueOrDie();
    ASSERT_OK(writer->Write(CreateStructBatch(schema, {int_array, view_array})));
    ASSERT_OK_AND_ASSIGN(auto inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_FALSE(inc.GetNewFilesIncrement().NewFiles().empty());
    ASSERT_OK(writer->Close());
}

TEST_F(AppendOnlyWriterTest, TestWriteInvalidBlobViewFieldRejected) {
    auto options = CreateOptions({{Options::FILE_FORMAT, "orc"},
                                  {Options::MANIFEST_FORMAT, "orc"},
                                  {Options::BLOB_VIEW_FIELD, "view"}});
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "orc", options);

    auto schema =
        arrow::schema({arrow::field("f0", arrow::int32()), BlobUtils::ToArrowField("view", true)});
    ASSERT_OK_AND_ASSIGN(
        auto writer, CreateAppendOnlyWriter(
                         options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                         /*max_sequence_number=*/-1, path_factory, compact_manager_, memory_pool_));

    // Build f0 column
    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.Append(1).ok());
    auto int_array = int_builder.Finish().ValueOrDie();

    // Build view column with raw bytes
    arrow::LargeBinaryBuilder view_builder;
    ASSERT_TRUE(view_builder.Append("not_a_valid_blob_view_or_descriptor").ok());
    auto view_array = view_builder.Finish().ValueOrDie();

    ASSERT_NOK_WITH_MSG(writer->Write(CreateStructBatch(schema, {int_array, view_array})),
                        "BLOB inline field view require values to be set as corresponding type.");
    ASSERT_OK(writer->Close());
}

/// Parameterized test class for shared-shredding tests, parameterized by file format.
class AppendOnlyWriterShreddingTest : public AppendOnlyWriterTest,
                                      public ::testing::WithParamInterface<std::string> {
 public:
    std::string GetFormat() const {
        return GetParam();
    }
};

INSTANTIATE_TEST_SUITE_P(FileFormats, AppendOnlyWriterShreddingTest,
                         ::testing::Values("parquet", "orc"));

TEST_F(AppendOnlyWriterTest, TestSharedShreddingMapRejectsAvroFormatOnCommit) {
    auto options = CreateOptions({
        {Options::FILE_FORMAT, "avro"},
        {Options::MANIFEST_FORMAT, "avro"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    });

    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "avro", options);

    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/0, logical_schema,
                                                /*write_cols=*/std::nullopt,
                                                /*max_sequence_number=*/-1, path_factory,
                                                compact_manager_, memory_pool_));

    auto batch = CreateBatch(logical_schema, R"([
        [1, [["a", 10]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_NOK_WITH_MSG(writer->PrepareCommit(/*wait_compaction=*/true),
                        "AddMetadata is not supported by avro format writer.");
    ASSERT_OK(writer->Close());
}

TEST_P(AppendOnlyWriterShreddingTest, TestWriteSharedShreddingMapFieldContent) {
    std::string format = GetFormat();
    // Configure with shared-shredding map on "tags" field, K=3.
    auto options = CreateOptions({
        {Options::FILE_FORMAT, format},
        {Options::MANIFEST_FORMAT, format},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    });

    // Logical schema: id(INT32), tags(MAP<STRING, INT64>)
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), format, options);

    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/0, logical_schema,
                                                /*write_cols=*/std::nullopt,
                                                /*max_sequence_number=*/-1, path_factory,
                                                compact_manager_, memory_pool_));

    // Write a batch with MAP data using the logical schema.
    // Row0: id=1, tags={a:10, b:20}          → fits K=3
    // Row1: id=2, tags={c:30, a:40, b:50}    → fits K=3
    // Row2: id=3, tags={a:60}                → fits K=3
    auto batch = CreateBatch(logical_schema, R"([
        [1, [["a", 10], ["b", 20]]],
        [2, [["c", 30], ["a", 40], ["b", 50]]],
        [3, [["a", 60]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch)));

    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_OK(writer->Close());

    // Verify we got one data file.
    ASSERT_EQ(1, inc.GetNewFilesIncrement().NewFiles().size());
    std::string data_file_path =
        path_factory->ToPath(inc.GetNewFilesIncrement().NewFiles()[0]->file_name);

    // Check shared-shredding map metadata: a=0, b=1, c=2; K=3, max_row_width=3, no overflow.
    std::map<std::string, int32_t> column_to_k = {{"tags", 3}};
    ASSERT_OK_AND_ASSIGN(
        auto expected_physical_schema,
        MapSharedShreddingUtils::LogicalToPhysicalSchema(logical_schema, column_to_k));

    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    expected_meta.field_to_columns = {{0, {0, 1}}, {1, {1, 2}}, {2, {0}}};
    expected_meta.num_columns = 3;
    expected_meta.max_row_width = 3;
    std::string compression = options.GetFileCompression();
    CheckShreddingFileSchema(data_file_path, format, expected_physical_schema, /*field_index=*/1,
                             expected_meta, compression);

    auto physical_type = arrow::struct_(expected_physical_schema->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(physical_type, {R"([
        [1, [[0, 1, -1], 10, 20, null, null]],
        [2, [[2, 0, 1],  30, 40, 50,   null]],
        [3, [[0, -1, -1], 60, null, null, null]]
    ])"},
                                                                 &expected_array)
                    .ok());
    CheckFileContent(data_file_path, format, expected_array);
}

TEST_P(AppendOnlyWriterShreddingTest, TestSharedShreddingMapAllEmptyFirstFile) {
    std::string format = GetFormat();
    auto options = CreateOptions({
        {Options::FILE_FORMAT, format},
        {Options::MANIFEST_FORMAT, format},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    });

    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), format, options);

    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/0, logical_schema,
                                                /*write_cols=*/std::nullopt,
                                                /*max_sequence_number=*/-1, path_factory,
                                                compact_manager_, memory_pool_));

    auto batch = CreateBatch(logical_schema, R"([
        [1, []],
        [2, []]
    ])");
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_OK(writer->Close());

    ASSERT_EQ(1, inc.GetNewFilesIncrement().NewFiles().size());
    std::string data_file_path =
        path_factory->ToPath(inc.GetNewFilesIncrement().NewFiles()[0]->file_name);

    std::map<std::string, int32_t> first_file_k = {{"tags", 3}};
    ASSERT_OK_AND_ASSIGN(auto first_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                logical_schema, first_file_k));
    MapSharedShreddingFieldMeta empty_meta;
    empty_meta.num_columns = 3;
    empty_meta.max_row_width = 0;
    CheckShreddingFileSchema(data_file_path, format, first_schema, /*field_index=*/1, empty_meta,
                             options.GetFileCompression());

    auto physical_type = arrow::struct_(first_schema->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(physical_type, {R"([
        [1, [[-1, -1, -1], null, null, null, null]],
        [2, [[-1, -1, -1], null, null, null, null]]
    ])"},
                                                                 &expected_array)
                    .ok());
    CheckFileContent(data_file_path, format, expected_array);
}

TEST_P(AppendOnlyWriterShreddingTest, TestSharedShreddingMapAllNullThenAllEmptyFiles) {
    std::string format = GetFormat();
    auto options = CreateOptions({
        {Options::FILE_FORMAT, format},
        {Options::MANIFEST_FORMAT, format},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    });

    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), format, options);

    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/0, logical_schema,
                                                /*write_cols=*/std::nullopt,
                                                /*max_sequence_number=*/-1, path_factory,
                                                compact_manager_, memory_pool_));

    auto null_batch = CreateBatch(logical_schema, R"([
        [1, null],
        [2, null]
    ])");
    ASSERT_OK(writer->Write(std::move(null_batch)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement null_inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, null_inc.GetNewFilesIncrement().NewFiles().size());
    std::string null_file_path =
        path_factory->ToPath(null_inc.GetNewFilesIncrement().NewFiles()[0]->file_name);

    std::map<std::string, int32_t> first_file_k = {{"tags", 3}};
    ASSERT_OK_AND_ASSIGN(auto first_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                logical_schema, first_file_k));
    MapSharedShreddingFieldMeta empty_meta;
    empty_meta.num_columns = 3;
    empty_meta.max_row_width = 0;
    CheckShreddingFileSchema(null_file_path, format, first_schema, /*field_index=*/1, empty_meta,
                             options.GetFileCompression());

    auto first_physical_type = arrow::struct_(first_schema->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_null_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(first_physical_type, {R"([
        [1, null],
        [2, null]
    ])"},
                                                                 &expected_null_array)
                    .ok());
    CheckFileContent(null_file_path, format, expected_null_array);

    auto empty_batch = CreateBatch(logical_schema, R"([
        [3, []],
        [4, []]
    ])");
    ASSERT_OK(writer->Write(std::move(empty_batch)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement empty_inc,
                         writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, empty_inc.GetNewFilesIncrement().NewFiles().size());
    std::string empty_file_path =
        path_factory->ToPath(empty_inc.GetNewFilesIncrement().NewFiles()[0]->file_name);

    // Previous file observed max_row_width=0, but the next file must still keep at least one
    // physical value column so shared-shredding never produces a K=0 schema.
    std::map<std::string, int32_t> second_file_k = {{"tags", 1}};
    ASSERT_OK_AND_ASSIGN(auto second_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                 logical_schema, second_file_k));
    empty_meta.num_columns = 1;
    CheckShreddingFileSchema(empty_file_path, format, second_schema, /*field_index=*/1, empty_meta,
                             options.GetFileCompression());

    auto second_physical_type = arrow::struct_(second_schema->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_empty_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(second_physical_type, {R"([
        [3, [[-1], null, null]],
        [4, [[-1], null, null]]
    ])"},
                                                                 &expected_empty_array)
                    .ok());
    CheckFileContent(empty_file_path, format, expected_empty_array);

    auto null_value_batch = CreateBatch(logical_schema, R"([
        [5, [["a", null]]],
        [6, [["b", null]]],
        [7, [["c", 7], ["d", null]]]
    ])");
    ASSERT_OK(writer->Write(std::move(null_value_batch)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement null_value_inc,
                         writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, null_value_inc.GetNewFilesIncrement().NewFiles().size());
    std::string null_value_file_path =
        path_factory->ToPath(null_value_inc.GetNewFilesIncrement().NewFiles()[0]->file_name);

    MapSharedShreddingFieldMeta null_value_meta;
    null_value_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}, {"d", 3}};
    null_value_meta.field_to_columns = {{0, {0}}, {1, {0}}, {2, {0}}};
    null_value_meta.overflow_field_set = {3};
    null_value_meta.num_columns = 1;
    null_value_meta.max_row_width = 2;
    CheckShreddingFileSchema(null_value_file_path, format, second_schema, /*field_index=*/1,
                             null_value_meta, options.GetFileCompression());

    std::shared_ptr<arrow::ChunkedArray> expected_null_value_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(second_physical_type, {R"([
        [5, [[0], null, null]],
        [6, [[1], null, null]],
        [7, [[2], 7, [[3, null]]]]
    ])"},
                                                                 &expected_null_value_array)
                    .ok());
    CheckFileContent(null_value_file_path, format, expected_null_value_array);

    ASSERT_OK(writer->Close());
}

TEST_P(AppendOnlyWriterShreddingTest, TestWriteSharedShreddingMapWithOverflow) {
    std::string format = GetFormat();
    // K=2, write rows with 3+ keys to trigger overflow.
    auto options = CreateOptions({
        {Options::FILE_FORMAT, format},
        {Options::MANIFEST_FORMAT, format},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "2"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    });

    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), format, options);

    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/0, logical_schema,
                                                /*write_cols=*/std::nullopt,
                                                /*max_sequence_number=*/-1, path_factory,
                                                compact_manager_, memory_pool_));

    // Row0: {a:1, b:2}           → fits K=2
    // Row1: {c:3, a:4, b:5}      → 3 keys, K=2: c→col0, a→col1, b→overflow
    // Row2: {d:6, e:7, f:8, a:9} → 4 keys, K=2: d→col0, e→col1, f+a→overflow
    auto batch = CreateBatch(logical_schema, R"([
        [1, [["a", 1], ["b", 2]]],
        [2, [["c", 3], ["a", 4], ["b", 5]]],
        [3, [["d", 6], ["e", 7], ["f", 8], ["a", 9]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_OK(writer->Close());

    ASSERT_EQ(1, inc.GetNewFilesIncrement().NewFiles().size());
    std::string data_file_path =
        path_factory->ToPath(inc.GetNewFilesIncrement().NewFiles()[0]->file_name);

    std::map<std::string, int32_t> column_to_k = {{"tags", 2}};
    ASSERT_OK_AND_ASSIGN(
        auto expected_physical_schema,
        MapSharedShreddingUtils::LogicalToPhysicalSchema(logical_schema, column_to_k));
    std::string compression = options.GetFileCompression();

    // Verify metadata: a=0,b=1,c=2,d=3,e=4,f=5; K=2, max_row_width=4
    // b overflows in row1, f and a overflow in row2
    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}, {"d", 3}, {"e", 4}, {"f", 5}};
    expected_meta.field_to_columns = {{0, {0, 1}}, {1, {1}}, {2, {0}}, {3, {0}}, {4, {1}}};
    expected_meta.overflow_field_set = {0, 1, 5};
    expected_meta.num_columns = 2;
    expected_meta.max_row_width = 4;
    CheckShreddingFileSchema(data_file_path, format, expected_physical_schema, /*field_index=*/1,
                             expected_meta, compression);

    // Verify data content.
    auto physical_type = arrow::struct_(expected_physical_schema->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(physical_type, {R"([
        [1, [[0, 1],  1, 2, null]],
        [2, [[2, 0],  3, 4, [[1, 5]]]],
        [3, [[3, 4],  6, 7, [[5, 8], [0, 9]]]]
    ])"},
                                                                 &expected_array)
                    .ok());
    CheckFileContent(data_file_path, format, expected_array);
}

TEST_P(AppendOnlyWriterShreddingTest, TestWriteSharedShreddingMapWithLruPlacement) {
    std::string format = GetFormat();
    auto options = CreateOptions({
        {Options::FILE_FORMAT, format},
        {Options::MANIFEST_FORMAT, format},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "lru"},
        {Options::WRITE_ONLY, "true"},
    });

    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), format, options);

    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/0, logical_schema,
                                                /*write_cols=*/std::nullopt,
                                                /*max_sequence_number=*/-1, path_factory,
                                                compact_manager_, memory_pool_));

    auto batch = CreateBatch(logical_schema, R"([
        [1, [["a", 10], ["b", 20], ["c", 30]]],
        [2, [["a", 40], ["b", 50]]],
        [3, [["d", 60]]],
        [4, [["a", 70], ["b", 80], ["c", 90], ["d", 100]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_OK(writer->Close());

    ASSERT_EQ(1, inc.GetNewFilesIncrement().NewFiles().size());
    std::string data_file_path =
        path_factory->ToPath(inc.GetNewFilesIncrement().NewFiles()[0]->file_name);

    std::map<std::string, int32_t> column_to_k = {{"tags", 3}};
    ASSERT_OK_AND_ASSIGN(
        auto expected_physical_schema,
        MapSharedShreddingUtils::LogicalToPhysicalSchema(logical_schema, column_to_k));

    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}, {"d", 3}};
    expected_meta.field_to_columns = {{0, {0}}, {1, {1}}, {2, {2}}, {3, {2}}};
    expected_meta.overflow_field_set = {2};
    expected_meta.num_columns = 3;
    expected_meta.max_row_width = 4;
    CheckShreddingFileSchema(data_file_path, format, expected_physical_schema, /*field_index=*/1,
                             expected_meta, options.GetFileCompression());

    auto physical_type = arrow::struct_(expected_physical_schema->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(physical_type, {R"([
        [1, [[0, 1, 2],  10,  20,  30, null]],
        [2, [[0, 1, -1], 40,  50, null, null]],
        [3, [[-1, -1, 3], null, null, 60, null]],
        [4, [[0, 1, 3],  70,  80, 100, [[2, 90]]]]
    ])"},
                                                                 &expected_array)
                    .ok());
    CheckFileContent(data_file_path, format, expected_array);
}

TEST_P(AppendOnlyWriterShreddingTest, TestSharedShreddingMapKAdaptationAcrossFiles) {
    std::string format = GetFormat();
    auto options = CreateOptions({
        {Options::FILE_FORMAT, format},
        {Options::MANIFEST_FORMAT, format},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "10"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    });

    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), format, options);

    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/0, logical_schema,
                                                /*write_cols=*/std::nullopt,
                                                /*max_sequence_number=*/-1, path_factory,
                                                compact_manager_, memory_pool_));

    // --- File 1: max_row_width = 3, K = K_max = 10 (first file, no history) ---
    auto batch1 = CreateBatch(logical_schema, R"([
        [1, [["a", 10], ["b", 20]]],
        [2, [["c", 30], ["a", 40], ["b", 50]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch1)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc1, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, inc1.GetNewFilesIncrement().NewFiles().size());

    std::string file1_path =
        path_factory->ToPath(inc1.GetNewFilesIncrement().NewFiles()[0]->file_name);

    // File 1 should have K=10 (first file uses K_max).
    std::map<std::string, int32_t> column_to_k_file1 = {{"tags", 10}};
    ASSERT_OK_AND_ASSIGN(auto phys_schema1, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                logical_schema, column_to_k_file1));
    // Verify file1 physical schema has 10 columns.
    auto struct_type1 = std::static_pointer_cast<arrow::StructType>(phys_schema1->field(1)->type());
    ASSERT_EQ(12, struct_type1->num_fields());  // mapping + 10 cols + overflow

    MapSharedShreddingFieldMeta meta1;
    meta1.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    meta1.field_to_columns = {{0, {0, 1}}, {1, {1, 2}}, {2, {0}}};
    meta1.num_columns = 10;
    meta1.max_row_width = 3;
    std::string compression = options.GetFileCompression();
    CheckShreddingFileSchema(file1_path, format, phys_schema1, /*field_index=*/1, meta1,
                             compression);

    // --- File 2: K should adapt to min(max_window=3, K_max=10) = 3 ---
    // Write 5 keys → 3 fit in columns, 2 overflow.
    auto batch2 = CreateBatch(logical_schema, R"([
        [3, [["x", 100], ["y", 200], ["z", 300], ["w", 400], ["v", 500]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch2)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc2, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, inc2.GetNewFilesIncrement().NewFiles().size());

    std::string file2_path =
        path_factory->ToPath(inc2.GetNewFilesIncrement().NewFiles()[0]->file_name);

    // File 2 should have K=3 (adapted from file1's max_row_width=3).
    std::map<std::string, int32_t> column_to_k_file2 = {{"tags", 3}};
    ASSERT_OK_AND_ASSIGN(auto phys_schema2, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                logical_schema, column_to_k_file2));
    auto struct_type2 = std::static_pointer_cast<arrow::StructType>(phys_schema2->field(1)->type());
    ASSERT_EQ(5, struct_type2->num_fields());  // mapping + 3 cols + overflow

    MapSharedShreddingFieldMeta meta2;
    meta2.name_to_id = {{"x", 0}, {"y", 1}, {"z", 2}, {"w", 3}, {"v", 4}};
    meta2.field_to_columns = {{0, {0}}, {1, {1}}, {2, {2}}};
    meta2.overflow_field_set = {3, 4};
    meta2.num_columns = 3;
    meta2.max_row_width = 5;
    CheckShreddingFileSchema(file2_path, format, phys_schema2, /*field_index=*/1, meta2,
                             compression);

    // Verify data: 5 keys, K=3, so w and v overflow.
    auto physical_type2 = arrow::struct_(phys_schema2->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array2;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(physical_type2, {R"([
        [3, [[0, 1, 2], 100, 200, 300, [[3, 400], [4, 500]]]]
    ])"},
                                                                 &expected_array2)
                    .ok());
    CheckFileContent(file2_path, format, expected_array2);

    // --- File 3: K should adapt to min(max_window=max(3,5)=5, K_max=10) = 5 ---
    // File2 reported max_row_width=5, so window now has [3, 5], max=5.
    // Write 4 keys → all fit in K=5, no overflow.
    auto batch3 = CreateBatch(logical_schema, R"([
        [4, [["p", 1000], ["q", 2000], ["r", 3000], ["s", 4000]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch3)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc3, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, inc3.GetNewFilesIncrement().NewFiles().size());

    std::string file3_path =
        path_factory->ToPath(inc3.GetNewFilesIncrement().NewFiles()[0]->file_name);

    // File 3 should have K=5 (window max grew from file2's max_row_width=5).
    std::map<std::string, int32_t> column_to_k_file3 = {{"tags", 5}};
    ASSERT_OK_AND_ASSIGN(auto phys_schema3, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                logical_schema, column_to_k_file3));
    auto struct_type3 = std::static_pointer_cast<arrow::StructType>(phys_schema3->field(1)->type());
    ASSERT_EQ(7, struct_type3->num_fields());  // mapping + 5 cols + overflow

    MapSharedShreddingFieldMeta meta3;
    meta3.name_to_id = {{"p", 0}, {"q", 1}, {"r", 2}, {"s", 3}};
    meta3.field_to_columns = {{0, {0}}, {1, {1}}, {2, {2}}, {3, {3}}};
    meta3.num_columns = 5;
    meta3.max_row_width = 4;
    CheckShreddingFileSchema(file3_path, format, phys_schema3, /*field_index=*/1, meta3,
                             compression);

    // Verify data: 4 keys fit in K=5, col4 unused, no overflow.
    auto physical_type3 = arrow::struct_(phys_schema3->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array3;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(physical_type3, {R"([
        [4, [[0, 1, 2, 3, -1], 1000, 2000, 3000, 4000, null, null]]
    ])"},
                                                                 &expected_array3)
                    .ok());
    CheckFileContent(file3_path, format, expected_array3);

    ASSERT_OK(writer->Close());
}

TEST_P(AppendOnlyWriterShreddingTest, TestSharedShreddingMapUsesInitialContextForFirstFile) {
    std::string format = GetFormat();
    auto options = CreateOptions({
        {Options::FILE_FORMAT, format},
        {Options::MANIFEST_FORMAT, format},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "10"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    });

    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), format, options);

    auto initial_context =
        std::make_shared<MapSharedShreddingContext>(std::map<std::string, int32_t>{{"tags", 10}});
    initial_context->ReportFileStats("tags", 2);
    auto writer = std::make_unique<AppendOnlyWriter>(
        options, /*schema_id=*/0, logical_schema, /*write_cols=*/std::nullopt,
        /*max_sequence_number=*/-1, path_factory, compact_manager_, initial_context, memory_pool_);

    auto batch = CreateBatch(logical_schema, R"([
        [1, [["a", 10], ["b", 20], ["c", 30]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, inc.GetNewFilesIncrement().NewFiles().size());

    std::string file_path =
        path_factory->ToPath(inc.GetNewFilesIncrement().NewFiles()[0]->file_name);

    std::map<std::string, int32_t> column_to_k = {{"tags", 2}};
    ASSERT_OK_AND_ASSIGN(auto physical_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                   logical_schema, column_to_k));
    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    expected_meta.field_to_columns = {{0, {0}}, {1, {1}}};
    expected_meta.overflow_field_set = {2};
    expected_meta.num_columns = 2;
    expected_meta.max_row_width = 3;
    CheckShreddingFileSchema(file_path, format, physical_schema, /*field_index=*/1, expected_meta,
                             options.GetFileCompression());

    auto physical_type = arrow::struct_(physical_schema->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(physical_type, {R"([
        [1, [[0, 1], 10, 20, [[2, 30]]]]
    ])"},
                                                                 &expected_array)
                    .ok());
    CheckFileContent(file_path, format, expected_array);

    ASSERT_OK(writer->Close());
}

TEST_P(AppendOnlyWriterShreddingTest, TestMultipleSharedShreddingMapFieldsWithKAdaptation) {
    std::string format = GetFormat();
    // Two shared-shredding MAP fields with different initial K: tags(K=8), attrs(K=4).
    auto options = CreateOptions({
        {Options::FILE_FORMAT, format},
        {Options::MANIFEST_FORMAT, format},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "8"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {"fields.attrs.map.storage-layout", "shared-shredding"},
        {"fields.attrs.map.shared-shredding.max-columns", "4"},
        {"fields.attrs.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    });

    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("attrs", arrow::map(arrow::utf8(), arrow::utf8())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), format, options);

    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/0, logical_schema,
                                                /*write_cols=*/std::nullopt,
                                                /*max_sequence_number=*/-1, path_factory,
                                                compact_manager_, memory_pool_));
    std::string compression = options.GetFileCompression();

    // --- File 1: first file, tags K=8, attrs K=4 ---
    // tags: max_row_width=2, attrs: max_row_width=1
    auto batch1 = CreateBatch(logical_schema, R"([
        [1, [["a", 10], ["b", 20]], [["x", "v1"]]],
        [2, [["a", 30]],            [["x", "v2"]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch1)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc1, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, inc1.GetNewFilesIncrement().NewFiles().size());

    std::string file1_path =
        path_factory->ToPath(inc1.GetNewFilesIncrement().NewFiles()[0]->file_name);

    // Verify file1: tags K=8, attrs K=4 (first file uses K_max).
    std::map<std::string, int32_t> col_to_k_file1 = {{"tags", 8}, {"attrs", 4}};
    ASSERT_OK_AND_ASSIGN(auto phys_schema1, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                logical_schema, col_to_k_file1));

    MapSharedShreddingFieldMeta meta1_tags;
    meta1_tags.name_to_id = {{"a", 0}, {"b", 1}};
    meta1_tags.field_to_columns = {{0, {0}}, {1, {1}}};
    meta1_tags.num_columns = 8;
    meta1_tags.max_row_width = 2;
    CheckShreddingFileSchema(file1_path, format, phys_schema1, /*field_index=*/1, meta1_tags,
                             compression);

    MapSharedShreddingFieldMeta meta1_attrs;
    meta1_attrs.name_to_id = {{"x", 0}};
    meta1_attrs.field_to_columns = {{0, {0}}};
    meta1_attrs.num_columns = 4;
    meta1_attrs.max_row_width = 1;
    CheckShreddingFileSchema(file1_path, format, phys_schema1, /*field_index=*/2, meta1_attrs,
                             compression);

    // --- File 2: tags K=min(2,8)=2, attrs K=min(1,4)=1 ---
    // tags: 3 keys → 1 overflow; attrs: 3 keys → 2 overflow
    auto batch2 = CreateBatch(logical_schema, R"([
        [3, [["c", 100], ["d", 200], ["e", 300]], [["p", "a1"], ["q", "a2"], ["r", "a3"]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch2)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc2, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, inc2.GetNewFilesIncrement().NewFiles().size());

    std::string file2_path =
        path_factory->ToPath(inc2.GetNewFilesIncrement().NewFiles()[0]->file_name);

    std::map<std::string, int32_t> col_to_k_file2 = {{"tags", 2}, {"attrs", 1}};
    ASSERT_OK_AND_ASSIGN(auto phys_schema2, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                logical_schema, col_to_k_file2));

    MapSharedShreddingFieldMeta meta2_tags;
    meta2_tags.name_to_id = {{"c", 0}, {"d", 1}, {"e", 2}};
    meta2_tags.field_to_columns = {{0, {0}}, {1, {1}}};
    meta2_tags.overflow_field_set = {2};
    meta2_tags.num_columns = 2;
    meta2_tags.max_row_width = 3;
    CheckShreddingFileSchema(file2_path, format, phys_schema2, /*field_index=*/1, meta2_tags,
                             compression);

    MapSharedShreddingFieldMeta meta2_attrs;
    meta2_attrs.name_to_id = {{"p", 0}, {"q", 1}, {"r", 2}};
    meta2_attrs.field_to_columns = {{0, {0}}};
    meta2_attrs.overflow_field_set = {1, 2};
    meta2_attrs.num_columns = 1;
    meta2_attrs.max_row_width = 3;
    CheckShreddingFileSchema(file2_path, format, phys_schema2, /*field_index=*/2, meta2_attrs,
                             compression);

    // --- File 3: tags K=min(max(2,3),8)=3, attrs K=min(max(1,3),4)=3 ---
    // tags: 2 keys, fits; attrs: 2 keys, fits.
    auto batch3 = CreateBatch(logical_schema, R"([
        [4, [["f", 400], ["g", 500]], [["s", "b1"], ["t", "b2"]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch3)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc3, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, inc3.GetNewFilesIncrement().NewFiles().size());

    std::string file3_path =
        path_factory->ToPath(inc3.GetNewFilesIncrement().NewFiles()[0]->file_name);

    std::map<std::string, int32_t> col_to_k_file3 = {{"tags", 3}, {"attrs", 3}};
    ASSERT_OK_AND_ASSIGN(auto phys_schema3, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                logical_schema, col_to_k_file3));

    MapSharedShreddingFieldMeta meta3_tags;
    meta3_tags.name_to_id = {{"f", 0}, {"g", 1}};
    meta3_tags.field_to_columns = {{0, {0}}, {1, {1}}};
    meta3_tags.num_columns = 3;
    meta3_tags.max_row_width = 2;
    CheckShreddingFileSchema(file3_path, format, phys_schema3, /*field_index=*/1, meta3_tags,
                             compression);

    MapSharedShreddingFieldMeta meta3_attrs;
    meta3_attrs.name_to_id = {{"s", 0}, {"t", 1}};
    meta3_attrs.field_to_columns = {{0, {0}}, {1, {1}}};
    meta3_attrs.num_columns = 3;
    meta3_attrs.max_row_width = 2;
    CheckShreddingFileSchema(file3_path, format, phys_schema3, /*field_index=*/2, meta3_attrs,
                             compression);

    ASSERT_OK(writer->Close());
}

TEST_P(AppendOnlyWriterShreddingTest, TestSharedShreddingMapDataFileMetaInfo) {
    std::string format = GetFormat();
    // Verify PrepareCommit returns correct DataFileMeta for shared-shredding map files.
    auto options = CreateOptions({
        {Options::FILE_FORMAT, format},
        {Options::MANIFEST_FORMAT, format},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    });

    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), format, options);

    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/5, logical_schema,
                                                /*write_cols=*/std::nullopt,
                                                /*max_sequence_number=*/9, path_factory,
                                                compact_manager_, memory_pool_));

    // Write 3 rows.
    auto batch = CreateBatch(logical_schema, R"([
        [1, [["a", 10], ["b", 20]]],
        [2, [["c", 30]]],
        [3, [["a", 40], ["b", 50], ["c", 60]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(1, inc.GetNewFilesIncrement().NewFiles().size());

    auto actual_meta = inc.GetNewFilesIncrement().NewFiles()[0];

    // Construct expected value_stats independently.
    // Physical schema has 2 top-level fields: id(INT32), tags(STRUCT).
    // id: min=1, max=3, null_count=0; tags is nested: NullType(), null_count=null.
    int32_t map_null_count = (format == "parquet" ? -1 : 0);
    auto expected_value_stats = BinaryRowGenerator::GenerateStats(
        {1, NullType()}, {3, NullType()}, {0, map_null_count}, memory_pool_.get());

    // Build expected DataFileMeta with fake file_name/file_size (TEST_Equal ignores them).
    auto expected_meta = DataFileMeta::ForAppend(
                             /*file_name=*/"fake-file.parquet", /*file_size=*/999, /*row_count=*/3,
                             expected_value_stats,
                             /*min_sequence_number=*/10, /*max_sequence_number=*/12,
                             /*schema_id=*/5, FileSource::Append(),
                             /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
                             /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt)
                             .value();

    ASSERT_TRUE(expected_meta->TEST_Equal(*actual_meta));

    // Verify the written file has correct shared-shredding map content.
    std::string file_path = path_factory->ToPath(actual_meta->file_name);
    std::map<std::string, int32_t> col_to_k = {{"tags", 3}};
    ASSERT_OK_AND_ASSIGN(auto phys_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                               logical_schema, col_to_k));
    auto physical_type = arrow::struct_(phys_schema->fields());

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(physical_type, {R"([
        [1, [[0, 1, -1], 10, 20, null, null]],
        [2, [[2, -1, -1], 30, null, null, null]],
        [3, [[0, 1, 2],  40, 50, 60, null]]
    ])"},
                                                                 &expected_array)
                    .ok());
    CheckFileContent(file_path, format, expected_array);

    ASSERT_OK(writer->Close());
}

TEST_P(AppendOnlyWriterShreddingTest, TestSharedShreddingMapWithBlobSeparation) {
    std::string format = GetFormat();
    // Schema: id(INT32), blob_data(BLOB), tags(MAP<STRING, INT64>)
    // BLOB field will be separated into a .blob file; MAP field uses shared-shredding.
    // This tests that blob separation + shredding works correctly together.
    auto options = CreateOptions({
        {Options::FILE_FORMAT, format},
        {Options::MANIFEST_FORMAT, format},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "3"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    });

    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        BlobUtils::ToArrowField("blob_data", false),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), format, options);

    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateAppendOnlyWriter(options, /*schema_id=*/0, logical_schema,
                                                /*write_cols=*/std::nullopt,
                                                /*max_sequence_number=*/-1, path_factory,
                                                compact_manager_, memory_pool_));

    // Write rows with id, blob_data, and tags.
    // Row0: id=1, blob="hello", tags={a:10, b:20}
    // Row1: id=2, blob="world", tags={c:30}
    auto batch = CreateBatch(logical_schema, R"([
        [1, "hello", [["a", 10], ["b", 20]]],
        [2, "world", [["c", 30]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch)));

    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_OK(writer->Close());

    // Verify: should produce 2 files — one main data file and one .blob file.
    const auto& new_files = inc.GetNewFilesIncrement().NewFiles();
    ASSERT_EQ(2, new_files.size());

    // Identify main vs blob file.
    std::string main_file_path, blob_file_path;
    for (const auto& file_meta : new_files) {
        std::string file_path = path_factory->ToPath(file_meta->file_name);
        if (BlobUtils::IsBlobFile(file_meta->file_name)) {
            blob_file_path = file_path;
        } else {
            main_file_path = file_path;
        }
    }
    ASSERT_FALSE(main_file_path.empty()) << "Main data file not found";
    ASSERT_FALSE(blob_file_path.empty()) << "Blob file not found";

    // Verify both files exist on disk.
    auto fs = options.GetFileSystem();
    ASSERT_TRUE(fs->Exists(main_file_path).value());
    ASSERT_TRUE(fs->Exists(blob_file_path).value());

    // Verify main file schema: should have id(INT32) + tags(shredded STRUCT), no blob_data.
    // Build expected physical schema for the main schema (id + tags).
    auto main_logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });
    std::map<std::string, int32_t> col_to_k = {{"tags", 3}};
    ASSERT_OK_AND_ASSIGN(
        auto expected_physical_schema,
        MapSharedShreddingUtils::LogicalToPhysicalSchema(main_logical_schema, col_to_k));

    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    expected_meta.field_to_columns = {{0, {0}}, {1, {1}}, {2, {0}}};
    expected_meta.num_columns = 3;
    expected_meta.max_row_width = 2;

    CheckShreddingFileSchema(main_file_path, format, expected_physical_schema,
                             /*field_index=*/1, expected_meta, options.GetFileCompression());

    // Verify main file content: id + shredded tags.
    auto physical_type = arrow::struct_(expected_physical_schema->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(physical_type, {R"([
        [1, [[0, 1, -1], 10, 20, null, null]],
        [2, [[2, -1, -1], 30, null, null, null]]
    ])"},
                                                                 &expected_array)
                    .ok());
    CheckFileContent(main_file_path, format, expected_array);

    // Verify blob file exists and can be read (blob_data column).
    auto blob_reader = OpenFormatReader(blob_file_path, "blob");
    ASSERT_NE(blob_reader, nullptr);
}

}  // namespace paimon::test
