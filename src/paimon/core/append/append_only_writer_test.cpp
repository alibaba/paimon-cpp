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
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/fs/external_path_provider.h"
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
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
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

 private:
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

    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);
    for (int32_t i = 0; i < 3; i++) {
        ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer.PrepareCommit(true));
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
    AppendOnlyWriter writer(options, /*schema_id=*/2, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);
    arrow::StringBuilder builder;
    for (size_t j = 0; j < 100; j++) {
        ASSERT_TRUE(builder.Append(std::to_string(j)).ok());
    }
    std::shared_ptr<arrow::Array> array = builder.Finish().ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(auto record_batch, batch_builder.Finish());
    ASSERT_OK(writer.Write(std::move(record_batch)));
    ASSERT_TRUE(ArrowArrayIsReleased(&arrow_array));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer.PrepareCommit(true));
    ASSERT_FALSE(inc.GetNewFilesIncrement().IsEmpty());
    const auto& data_increment = inc.GetNewFilesIncrement();
    const auto& data_file_metas = data_increment.NewFiles();
    ASSERT_EQ(1, data_file_metas.size());
    ASSERT_EQ(2, data_file_metas[0]->schema_id);
    ASSERT_TRUE(inc.GetCompactIncrement().IsEmpty());
    std::string path = path_factory->ToPath(inc.GetNewFilesIncrement().NewFiles()[0]->file_name);
    ASSERT_OK_AND_ASSIGN(bool exist, options.GetFileSystem()->Exists(path));
    ASSERT_TRUE(exist);
    ASSERT_OK(writer.Close());
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
    AppendOnlyWriter writer(options, /*schema_id=*/1, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);
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
    ASSERT_OK(writer.Write(std::move(record_batch)));
    ASSERT_TRUE(ArrowArrayIsReleased(&arrow_array));
    ASSERT_OK(writer.Close());

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
    AppendOnlyWriter writer(options, /*schema_id=*/1, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);
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
    ASSERT_NOK_WITH_MSG(writer.Write(std::move(record_batch)),
                        "Append only writer can not accept record batch with RowKind DELETE");
    ASSERT_TRUE(ArrowArrayIsReleased(&arrow_array));
    ASSERT_OK(writer.Close());

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
    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager,
                            memory_pool_);

    ASSERT_OK(writer.Write(CreateSingleStringBatch({"a", "b"})));
    ASSERT_OK(writer.PrepareCommit(/*wait_compaction=*/true).status());

    ASSERT_EQ(compact_manager->get_result_blocking_calls.size(), 2);
    ASSERT_FALSE(compact_manager->get_result_blocking_calls[0]);
    ASSERT_TRUE(compact_manager->get_result_blocking_calls[1]);
    ASSERT_OK(writer.Close());
}

TEST_F(AppendOnlyWriterTest, TestPrepareCommitForceCompactUsesBlockingGetResult) {
    auto options = CreateOptions({{Options::COMMIT_FORCE_COMPACT, "true"}});
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "mock_format", options);
    auto compact_manager = std::make_shared<FakeCompactManager>();

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager,
                            memory_pool_);

    ASSERT_OK(writer.Write(CreateSingleStringBatch({"a"})));
    ASSERT_OK(writer.PrepareCommit(/*wait_compaction=*/false).status());

    ASSERT_EQ(compact_manager->get_result_blocking_calls.size(), 2);
    ASSERT_FALSE(compact_manager->get_result_blocking_calls[0]);
    ASSERT_TRUE(compact_manager->get_result_blocking_calls[1]);
    ASSERT_OK(writer.Close());
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
    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager,
                            memory_pool_);

    ASSERT_OK(writer.Sync());
    ASSERT_OK(writer.Sync());
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer.PrepareCommit(/*wait_compaction=*/false));

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
    ASSERT_OK(writer.Close());
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
    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager,
                            memory_pool_);

    ASSERT_OK(writer.Sync());
    ASSERT_TRUE(options.GetFileSystem()->Exists(compact_after_path).value());
    ASSERT_OK(writer.Close());
    ASSERT_FALSE(options.GetFileSystem()->Exists(compact_after_path).value());
    ASSERT_TRUE(compact_manager->request_cancel_called);
    ASSERT_TRUE(compact_manager->wait_called);
    ASSERT_TRUE(compact_manager->close_called);
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
    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager,
                            memory_pool_);

    ASSERT_OK_AND_ASSIGN(bool not_completed, writer.CompactNotCompleted());
    ASSERT_TRUE(not_completed);
    ASSERT_EQ(compact_manager->trigger_calls, std::vector<bool>({false}));
    ASSERT_OK(writer.Close());
}

TEST_F(AppendOnlyWriterTest, TestCompactPassesFullCompactionFlag) {
    auto options = CreateOptions();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "mock_format", options);
    auto compact_manager = std::make_shared<FakeCompactManager>();

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager,
                            memory_pool_);

    ASSERT_OK(writer.Compact(/*full_compaction=*/true));
    ASSERT_OK(writer.Compact(/*full_compaction=*/false));
    ASSERT_EQ(compact_manager->trigger_calls, std::vector<bool>({true, false}));
    ASSERT_OK(writer.Close());
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

    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);

    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.AppendValues({1, 2}).ok());
    auto int_array = int_builder.Finish().ValueOrDie();
    arrow::LargeBinaryBuilder blob_builder;
    ASSERT_TRUE(blob_builder.Append("a", 1).ok());
    ASSERT_TRUE(blob_builder.Append("bb", 2).ok());
    auto blob_array = blob_builder.Finish().ValueOrDie();

    ASSERT_OK(writer.Write(CreateStructBatch(schema, {int_array, blob_array})));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer.PrepareCommit(/*wait_compaction=*/true));

    ASSERT_EQ(inc.GetNewFilesIncrement().NewFiles().size(), 2);
    const auto& main_file = inc.GetNewFilesIncrement().NewFiles()[0];
    const auto& blob_file = inc.GetNewFilesIncrement().NewFiles()[1];
    ASSERT_TRUE(
        options.GetFileSystem()->Exists(path_factory->ToPath(main_file->file_name)).value());
    ASSERT_TRUE(
        options.GetFileSystem()->Exists(path_factory->ToPath(blob_file->file_name)).value());
    ASSERT_OK(writer.Close());
}

TEST_F(AppendOnlyWriterTest, TestWriteWithMultipleBlobFieldsShouldFail) {
    auto options =
        CreateOptions({{Options::FILE_FORMAT, "orc"}, {Options::MANIFEST_FORMAT, "orc"}});
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "orc", options);

    auto schema =
        arrow::schema({arrow::field("id", arrow::int32()), BlobUtils::ToArrowField("blob1", false),
                       BlobUtils::ToArrowField("blob2", false)});
    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);

    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.AppendValues({1}).ok());
    auto int_array = int_builder.Finish().ValueOrDie();
    arrow::LargeBinaryBuilder blob_builder1;
    ASSERT_TRUE(blob_builder1.Append("a", 1).ok());
    auto blob_array1 = blob_builder1.Finish().ValueOrDie();
    arrow::LargeBinaryBuilder blob_builder2;
    ASSERT_TRUE(blob_builder2.Append("b", 1).ok());
    auto blob_array2 = blob_builder2.Finish().ValueOrDie();

    ASSERT_NOK_WITH_MSG(
        writer.Write(CreateStructBatch(schema, {int_array, blob_array1, blob_array2})),
        "Limit exactly one blob field in one paimon table yet.");
    ASSERT_OK(writer.Close());
}

TEST_F(AppendOnlyWriterTest, TestMultiplePrepareCommitSequenceContinuity) {
    auto options = CreateOptions();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path_factory = CreatePathFactory(dir->Str(), "mock_format", options);

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);
    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);

    ASSERT_OK(writer.Write(CreateSingleStringBatch({"a", "b", "c"})));
    ASSERT_OK_AND_ASSIGN(CommitIncrement first, writer.PrepareCommit(/*wait_compaction=*/false));
    ASSERT_OK(writer.Write(CreateSingleStringBatch({"d", "e"})));
    ASSERT_OK_AND_ASSIGN(CommitIncrement second, writer.PrepareCommit(/*wait_compaction=*/false));

    ASSERT_EQ(first.GetNewFilesIncrement().NewFiles().size(), 1);
    ASSERT_EQ(second.GetNewFilesIncrement().NewFiles().size(), 1);
    ASSERT_EQ(first.GetNewFilesIncrement().NewFiles()[0]->min_sequence_number, 0);
    ASSERT_EQ(first.GetNewFilesIncrement().NewFiles()[0]->max_sequence_number, 2);
    ASSERT_EQ(second.GetNewFilesIncrement().NewFiles()[0]->min_sequence_number, 3);
    ASSERT_EQ(second.GetNewFilesIncrement().NewFiles()[0]->max_sequence_number, 4);
    ASSERT_OK(writer.Close());
}

}  // namespace paimon::test
