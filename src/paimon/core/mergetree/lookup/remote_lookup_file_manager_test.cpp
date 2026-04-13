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

#include "paimon/core/mergetree/lookup/remote_lookup_file_manager.h"

#include <cstring>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/compact/noop_compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/mergetree/lookup/default_lookup_serializer_factory.h"
#include "paimon/core/mergetree/lookup/persist_value_and_pos_processor.h"
#include "paimon/core/mergetree/lookup/positioned_key_value.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class RemoteLookupFileManagerTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        arrow::FieldVector fields = {
            arrow::field("key", arrow::int32()),
            arrow::field("value", arrow::int32()),
        };
        arrow_schema_ = arrow::schema(fields);
        key_schema_ = arrow::schema({fields[0]});
        tmp_dir_ = UniqueTestDirectory::Create("local");
        dir_ = UniqueTestDirectory::Create("local");
        fs_ = dir_->GetFileSystem();
        noop_compact_manager_ = std::make_shared<NoopCompactManager>();
    }

    void TearDown() override {}

    Result<std::shared_ptr<DataFileMeta>> NewFiles(int32_t level, int64_t last_sequence_number,
                                                   const std::string& table_path,
                                                   const CoreOptions& options,
                                                   const std::string& src_array_str) const {
        std::shared_ptr<arrow::Array> src_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(arrow_schema_->fields()),
                                                      src_array_str)
                .ValueOrDie();

        PAIMON_ASSIGN_OR_RAISE(auto path_factory, CreateFileStorePathFactory(table_path, options));
        PAIMON_ASSIGN_OR_RAISE(auto data_path_factory, path_factory->CreateDataFilePathFactory(
                                                           BinaryRow::EmptyRow(), /*bucket=*/0));
        PAIMON_ASSIGN_OR_RAISE(auto key_comparator, CreateKeyComparator());
        auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        auto merge_function_wrapper =
            std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));

        auto writer = std::make_shared<MergeTreeWriter>(
            /*last_sequence_number=*/last_sequence_number, std::vector<std::string>({"key"}),
            data_path_factory, key_comparator,
            /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper, /*schema_id=*/0,
            arrow_schema_, options, noop_compact_manager_, pool_);

        ArrowArray c_src_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*src_array, &c_src_array));
        RecordBatchBuilder batch_builder(&c_src_array);
        batch_builder.SetBucket(0);
        PAIMON_ASSIGN_OR_RAISE(auto batch, batch_builder.Finish());
        PAIMON_RETURN_NOT_OK(writer->Write(std::move(batch)));
        PAIMON_ASSIGN_OR_RAISE(auto commit_increment,
                               writer->PrepareCommit(/*wait_compaction=*/false));
        const auto& file_metas = commit_increment.GetNewFilesIncrement().NewFiles();
        EXPECT_EQ(file_metas.size(), 1);
        auto file_meta = file_metas[0];
        file_meta->level = level;
        return file_meta;
    }

    Result<std::shared_ptr<FieldsComparator>> CreateKeyComparator() const {
        std::vector<DataField> key_fields = {DataField(0, key_schema_->field(0))};
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                               FieldsComparator::Create(key_fields,
                                                        /*is_ascending_order=*/true));
        return key_comparator;
    }

    Result<std::string> CreateTable(const std::map<std::string, std::string>& options) const {
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema_, &c_schema));

        PAIMON_ASSIGN_OR_RAISE(auto catalog, Catalog::Create(dir_->Str(), {}));
        PAIMON_RETURN_NOT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        PAIMON_RETURN_NOT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema,
                                                  /*partition_keys=*/{},
                                                  /*primary_keys=*/{"key"}, options,
                                                  /*ignore_if_exists=*/false));
        return PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    }

    Result<std::shared_ptr<FileStorePathFactory>> CreateFileStorePathFactory(
        const std::string& table_path, const CoreOptions& options) const {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                               options.CreateExternalPaths());
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                               options.CreateGlobalIndexExternalPath());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                table_path, arrow_schema_, /*partition_keys=*/{}, options.GetPartitionDefaultName(),
                options.GetFileFormat()->Identifier(), options.DataFilePrefix(),
                options.LegacyPartitionNameEnabled(), external_paths, global_index_external_path,
                options.IndexFileInDataFileDir(), pool_));
        return path_factory;
    }

    Result<std::unique_ptr<LookupLevels<PositionedKeyValue>>> CreateLookupLevels(
        const std::string& table_path, const std::shared_ptr<Levels>& levels,
        const std::shared_ptr<RemoteLookupFileManager>& remote_lookup_file_manager) const {
        auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
        PAIMON_ASSIGN_OR_RAISE(auto table_schema, schema_manager->ReadSchema(0));
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(table_schema->Options()));

        auto io_manager = IOManager::Create(tmp_dir_->Str());
        auto processor_factory =
            std::make_shared<PersistValueAndPosProcessor::Factory>(arrow_schema_);
        auto serializer_factory = std::make_shared<DefaultLookupSerializerFactory>();
        PAIMON_ASSIGN_OR_RAISE(auto key_comparator,
                               RowCompactedSerializer::CreateSliceComparator(key_schema_, pool_));
        PAIMON_ASSIGN_OR_RAISE(
            auto lookup_store_factory,
            LookupStoreFactory::Create(key_comparator,
                                       std::make_shared<CacheManager>(1024 * 1024, 0.0), options));
        PAIMON_ASSIGN_OR_RAISE(auto path_factory, CreateFileStorePathFactory(table_path, options));
        return LookupLevels<PositionedKeyValue>::Create(
            fs_, BinaryRow::EmptyRow(), /*bucket=*/0, options, schema_manager,
            std::move(io_manager), path_factory, table_schema, levels,
            /*dv_factory=*/{}, processor_factory, serializer_factory, lookup_store_factory,
            remote_lookup_file_manager, pool_);
    }

    Result<std::shared_ptr<RemoteLookupFileManager>> CreateRemoteLookupFileManager(
        const std::string& table_path, CoreOptions& core_options) const {
        PAIMON_ASSIGN_OR_RAISE(auto path_factory,
                               CreateFileStorePathFactory(table_path, core_options));
        PAIMON_ASSIGN_OR_RAISE(auto data_path_factory,
                               path_factory->CreateDataFilePathFactory(BinaryRow::EmptyRow(),
                                                                       /*bucket=*/0));

        return std::make_shared<RemoteLookupFileManager>(
            /*level_threshold=*/1, data_path_factory, fs_, pool_);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> arrow_schema_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::unique_ptr<UniqueTestDirectory> tmp_dir_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<NoopCompactManager> noop_compact_manager_;
};

TEST_F(RemoteLookupFileManagerTest, GenRemoteLookupFileSimple) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    // Create a data file at level1 and level0
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11], [3, 33]]"));
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/0, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[1, 111]]"));

    std::vector<std::shared_ptr<DataFileMeta>> files = {file1, file0};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));
    // level_threshold=1 means files at level >= 1 will be processed
    ASSERT_OK_AND_ASSIGN(auto manager, CreateRemoteLookupFileManager(table_path, core_options));
    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels, manager));

    // level0 < level_threshold and will not produce lookup file
    ASSERT_OK_AND_ASSIGN(auto result_file,
                         manager->GenRemoteLookupFile(file0, lookup_levels.get()));
    ASSERT_TRUE(result_file->extra_files.empty());

    // First call: file has no .lookup extra file, so GenRemoteLookupFile should do the upload
    ASSERT_OK_AND_ASSIGN(result_file, manager->GenRemoteLookupFile(file1, lookup_levels.get()));
    // After upload, the result should have an extra file ending with .lookup
    ASSERT_EQ(result_file->extra_files.size(), 1);
    ASSERT_TRUE(StringUtils::EndsWith(result_file->extra_files[0].value(), ".lookup"));

    // Second call with the result_file (which already has .lookup extra file):
    // GenRemoteLookupFile should short-circuit and return the same file
    ASSERT_OK_AND_ASSIGN(auto short_circuit_result,
                         manager->GenRemoteLookupFile(result_file, lookup_levels.get()));
    ASSERT_EQ(*short_circuit_result, *result_file);
    ASSERT_OK(lookup_levels->Close());
}

/// Test that TryToDownload returns false when the file system fails to open the remote file.
TEST_F(RemoteLookupFileManagerTest, TryToDownloadReturnsFalseOnFailure) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));
    ASSERT_OK_AND_ASSIGN(auto manager, CreateRemoteLookupFileManager(table_path, core_options));
    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels, manager));

    bool download_result = manager->TryToDownload(file0, "non-exist-remote_sst.lookup",
                                                  dir_->Str() + "/local_sst.lookup");
    ASSERT_FALSE(download_result);

    ASSERT_OK(lookup_levels->Close());
}

/// Test that TryToDownload correctly handles a 2.3MB file that spans multiple
/// internal buffers (kBufferSize = 1MB), requiring 3 read/write iterations.
TEST_F(RemoteLookupFileManagerTest, TryToDownloadLargeFileAcrossMultipleBuffers) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));
    ASSERT_OK_AND_ASSIGN(auto manager, CreateRemoteLookupFileManager(table_path, core_options));
    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels, manager));

    // Prepare a 2.3MB file (2.3 * 1024 * 1024 bytes) to test multi-buffer copy.
    // file_size is 1MB, so this requires 3 iterations: 1MB + 1MB + 0.3MB.
    auto file_size = static_cast<uint64_t>(2.3 * 1024 * 1024);

    std::string remote_sst_name = "test_large_file.lookup";
    std::string remote_file_path = manager->RemoteSstPath(file0, remote_sst_name);

    // Write 2.3MB of deterministic data to the remote file
    {
        ASSERT_OK_AND_ASSIGN(auto output_stream, fs_->Create(remote_file_path, /*overwrite=*/true));
        std::vector<char> write_buffer(file_size);
        for (uint64_t i = 0; i < file_size; ++i) {
            write_buffer[i] = static_cast<char>(i % 251);
        }
        ASSERT_OK_AND_ASSIGN(int32_t bytes_written,
                             output_stream->Write(write_buffer.data(), file_size));
        ASSERT_EQ(static_cast<uint64_t>(bytes_written), file_size);
        ASSERT_OK(output_stream->Flush());
        ASSERT_OK(output_stream->Close());
    }

    // Perform the download
    std::string local_file_path = PathUtil::JoinPath(tmp_dir_->Str(), "local_large_file.lookup");
    bool download_result = manager->TryToDownload(file0, remote_sst_name, local_file_path);
    ASSERT_TRUE(download_result);

    // Verify the downloaded file has the correct size
    ASSERT_OK_AND_ASSIGN(auto local_status, fs_->GetFileStatus(local_file_path));
    ASSERT_EQ(local_status->GetLen(), file_size);

    // Verify the downloaded file content matches the original
    {
        ASSERT_OK_AND_ASSIGN(auto input_stream, fs_->Open(local_file_path));
        std::vector<char> read_buffer(file_size);
        ASSERT_OK_AND_ASSIGN(int32_t bytes_read, input_stream->Read(read_buffer.data(), file_size));
        ASSERT_EQ(static_cast<uint64_t>(bytes_read), file_size);
        for (uint64_t i = 0; i < file_size; ++i) {
            ASSERT_EQ(read_buffer[i], static_cast<char>(i % 251))
                << "Data mismatch at byte offset " << i;
        }
        ASSERT_OK(input_stream->Close());
    }
    ASSERT_OK(lookup_levels->Close());
}

}  // namespace paimon::test
