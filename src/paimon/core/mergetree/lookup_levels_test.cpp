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

#include "paimon/core/mergetree/lookup_levels.h"

#include <chrono>
#include <thread>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/common/utils/fields_comparator.h"
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
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
class LookupLevelsTest : public testing::Test {
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

        // prepare writer
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

        // write data
        ArrowArray c_src_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*src_array, &c_src_array));
        RecordBatchBuilder batch_builder(&c_src_array);
        batch_builder.SetBucket(0);
        PAIMON_ASSIGN_OR_RAISE(auto batch, batch_builder.Finish());
        PAIMON_RETURN_NOT_OK(writer->Write(std::move(batch)));
        // get file meta
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
        std::shared_ptr<LookupFile::LookupFileCache> lookup_file_cache = nullptr) const {
        auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
        PAIMON_ASSIGN_OR_RAISE(auto table_schema, schema_manager->ReadSchema(0));
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(table_schema->Options()));

        auto io_manager = std::make_shared<IOManager>(tmp_dir_->Str(), tmp_dir_->GetFileSystem());
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
        if (!lookup_file_cache) {
            lookup_file_cache = LookupFile::CreateLookupFileCache(
                options.GetLookupCacheFileRetentionMs(), options.GetLookupCacheMaxDiskSize());
        }
        return LookupLevels<PositionedKeyValue>::Create(
            fs_, BinaryRow::EmptyRow(), /*bucket=*/0, options, schema_manager,
            std::move(io_manager), path_factory, table_schema, levels,
            /*dv_factory=*/{}, processor_factory, serializer_factory, lookup_store_factory,
            lookup_file_cache,
            /*remote_lookup_file_manager=*/nullptr, pool_);
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

TEST_F(LookupLevelsTest, TestMultiLevels) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/2, /*last_sequence_number=*/3, table_path,
                                              core_options, "[[2, 22], [5, 55]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels));

    // only in level 1
    ASSERT_OK_AND_ASSIGN(auto positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 1);
    ASSERT_EQ(positioned_kv.value().key_value.level, 1);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), 11);

    // only in level 2
    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({2}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 4);
    ASSERT_EQ(positioned_kv.value().key_value.level, 2);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), 22);

    // both in level 1 and level 2
    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({5}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 3);
    ASSERT_EQ(positioned_kv.value().key_value.level, 1);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), 5);

    // no exists
    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({4}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_FALSE(positioned_kv);

    ASSERT_EQ(lookup_levels->lookup_file_cache_->Size(), 2);
    ASSERT_EQ(lookup_levels->schema_id_and_ser_version_to_processors_.size(), 1);
    ASSERT_EQ(lookup_levels->GetLevels()->NonEmptyHighestLevel(), 2);

    // test lookup file in tmp dir
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    ASSERT_OK(fs_->ListDir(tmp_dir_->Str(), &file_status_list));
    ASSERT_EQ(file_status_list.size(), 2);
    ASSERT_EQ(levels->drop_file_callbacks_.size(), 1);

    // test close will rm local lookup file
    ASSERT_OK(lookup_levels->Close());
    file_status_list.clear();
    ASSERT_OK(fs_->ListDir(tmp_dir_->Str(), &file_status_list));
    ASSERT_TRUE(file_status_list.empty());
    ASSERT_TRUE(levels->drop_file_callbacks_.empty());
    ASSERT_EQ(lookup_levels->lookup_file_cache_->Size(), 0);
}

TEST_F(LookupLevelsTest, TestMultiFiles) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11], [2, 22]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/1, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[4, 44], [5, 55]]"));
    ASSERT_OK_AND_ASSIGN(auto file2, NewFiles(/*level=*/1, /*last_sequence_number=*/4, table_path,
                                              core_options, "[[7, 77], [8, 88]]"));
    ASSERT_OK_AND_ASSIGN(auto file3, NewFiles(/*level=*/1, /*last_sequence_number=*/6, table_path,
                                              core_options, "[[10, 1010], [11, 1111]]"));

    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1, file2, file3};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels));

    std::map<int32_t, int32_t> contains = {{1, 11}, {2, 22}, {4, 44},    {5, 55},
                                           {7, 77}, {8, 88}, {10, 1010}, {11, 1111}};
    for (const auto& [key, value] : contains) {
        ASSERT_OK_AND_ASSIGN(
            auto positioned_kv,
            lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({key}, pool_.get()),
                                  /*start_level=*/1));
        ASSERT_TRUE(positioned_kv);
        ASSERT_EQ(positioned_kv.value().key_value.level, 1);
        ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), value);
    }

    std::vector<int32_t> not_contains = {0, 3, 6, 9, 12};
    for (const auto& key : not_contains) {
        ASSERT_OK_AND_ASSIGN(
            auto positioned_kv,
            lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({key}, pool_.get()),
                                  /*start_level=*/1));
        ASSERT_FALSE(positioned_kv);
    }
}

TEST_F(LookupLevelsTest, TestLookupEmptyLevel) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/3, /*last_sequence_number=*/3, table_path,
                                              core_options, "[[2, 22], [5, 55]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels));

    ASSERT_OK_AND_ASSIGN(auto positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({2}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 4);
    ASSERT_EQ(positioned_kv.value().key_value.level, 3);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), 22);
}

TEST_F(LookupLevelsTest, TestLookupLevel0) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/0, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 0]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/1, /*last_sequence_number=*/1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file2, NewFiles(/*level=*/2, /*last_sequence_number=*/4, table_path,
                                              core_options, "[[2, 22], [5, 55]]"));

    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1, file2};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels));

    ASSERT_OK_AND_ASSIGN(auto positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/0));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 1);
    ASSERT_EQ(positioned_kv.value().key_value.level, 0);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), 0);
}

TEST_F(LookupLevelsTest, TestLookupLevel0NotInLevel0) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/2, /*last_sequence_number=*/3, table_path,
                                              core_options, "[[2, 22], [5, 55]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels));

    ASSERT_OK_AND_ASSIGN(auto positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/0));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 1);
    ASSERT_EQ(positioned_kv.value().key_value.level, 1);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), 11);
}

TEST_F(LookupLevelsTest, TestLookupLevel0WithMultipleFiles) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/0, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 0], [4, 44]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/0, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file2, NewFiles(/*level=*/2, /*last_sequence_number=*/5, table_path,
                                              core_options, "[[2, 22], [5, 55]]"));

    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1, file2};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels));

    ASSERT_OK_AND_ASSIGN(auto positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/0));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 3);
    ASSERT_EQ(positioned_kv.value().key_value.level, 0);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), 11);

    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({3}, pool_.get()),
                                               /*start_level=*/0));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 4);
    ASSERT_EQ(positioned_kv.value().key_value.level, 0);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), 33);

    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({4}, pool_.get()),
                                               /*start_level=*/0));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 2);
    ASSERT_EQ(positioned_kv.value().key_value.level, 0);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), 44);

    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({5}, pool_.get()),
                                               /*start_level=*/2));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 7);
    ASSERT_EQ(positioned_kv.value().key_value.level, 2);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(1), 55);

    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({4}, pool_.get()),
                                               /*start_level=*/2));
    ASSERT_FALSE(positioned_kv);
}

TEST_F(LookupLevelsTest, TestWithPosistion) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/2, /*last_sequence_number=*/3, table_path,
                                              core_options, "[[2, 22], [5, 55]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels));

    // only in level 1
    ASSERT_OK_AND_ASSIGN(auto positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().row_position, 0);

    // only in level 2
    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({2}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().row_position, 0);

    // both in level 1 and level 2
    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({5}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().row_position, 2);

    // no exists
    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({4}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_FALSE(positioned_kv);
}

TEST_F(LookupLevelsTest, TestLevelsWithValueFieldAppearBeforeKey) {
    arrow::FieldVector fields = {
        arrow::field("value", arrow::int32()),
        arrow::field("key", arrow::int32()),
    };
    arrow_schema_ = arrow::schema(fields);
    key_schema_ = arrow::schema({fields[1]});

    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[11, 1], [33, 3], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/2, /*last_sequence_number=*/3, table_path,
                                              core_options, "[[22, 2], [55, 5]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels));

    // only in level 1
    ASSERT_OK_AND_ASSIGN(auto positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 1);
    ASSERT_EQ(positioned_kv.value().key_value.level, 1);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(0), 11);

    // only in level 2
    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({2}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 4);
    ASSERT_EQ(positioned_kv.value().key_value.level, 2);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(0), 22);

    // both in level 1 and level 2
    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({5}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_EQ(positioned_kv.value().key_value.sequence_number, 3);
    ASSERT_EQ(positioned_kv.value().key_value.level, 1);
    ASSERT_EQ(positioned_kv.value().key_value.value->GetInt(0), 5);

    // no exists
    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({4}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_FALSE(positioned_kv);
}

TEST_F(LookupLevelsTest, TestDropFileCallbackOnUpdate) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11], [3, 33]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/2, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[2, 22], [5, 55]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels));

    // Trigger lookup to populate the cache for both files.
    ASSERT_OK_AND_ASSIGN(auto positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);
    ASSERT_OK_AND_ASSIGN(positioned_kv,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({2}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(positioned_kv);

    // Both files should be cached now.
    ASSERT_EQ(lookup_levels->lookup_file_cache_->Size(), 2);
    ASSERT_TRUE(lookup_levels->lookup_file_cache_->GetIfPresent(file0->file_name).has_value());
    ASSERT_TRUE(lookup_levels->lookup_file_cache_->GetIfPresent(file1->file_name).has_value());

    // Update: remove file0 from level1, add a new file to level1.
    ASSERT_OK_AND_ASSIGN(auto new_file, NewFiles(/*level=*/1, /*last_sequence_number=*/4,
                                                 table_path, core_options, "[[1, 111], [3, 333]]"));
    ASSERT_OK(levels->Update(/*before=*/{file0}, /*after=*/{new_file}));

    // file0 was dropped, so its cache entry should be invalidated.
    ASSERT_EQ(lookup_levels->lookup_file_cache_->Size(), 1);
    ASSERT_FALSE(lookup_levels->lookup_file_cache_->GetIfPresent(file0->file_name).has_value());
    // file1 was not dropped, so its cache entry should still exist.
    ASSERT_TRUE(lookup_levels->lookup_file_cache_->GetIfPresent(file1->file_name).has_value());
}

TEST_F(LookupLevelsTest, TestRemoteSst) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels));

    // The processor identifier for PositionedKeyValue is "position-and-value"
    // RemoteSst format: {fileName}.{length}.{processorId}.{serVersion}.lookup

    // 1. Empty extra_files: should return nullopt
    auto result = lookup_levels->RemoteSst(file0);
    ASSERT_FALSE(result.has_value());

    // 2. extra_files with only nullopt elements: should return nullopt
    auto meta_with_nullopt_extras = file0->CopyWithExtraFiles({std::nullopt, std::nullopt});
    result = lookup_levels->RemoteSst(meta_with_nullopt_extras);
    ASSERT_FALSE(result.has_value());

    // 3. extra_files with non-.lookup suffix: should return nullopt
    auto meta_with_non_lookup = file0->CopyWithExtraFiles({std::string("some_file.index")});
    result = lookup_levels->RemoteSst(meta_with_non_lookup);
    ASSERT_FALSE(result.has_value());

    // 4. .lookup file with fewer than 3 dot-separated parts (e.g. "x.lookup"):
    //    split("x.lookup", ".") = ["x", "lookup"], size=2 < 3, should return nullopt
    auto meta_with_short_lookup = file0->CopyWithExtraFiles({std::string("x.lookup")});
    result = lookup_levels->RemoteSst(meta_with_short_lookup);
    ASSERT_FALSE(result.has_value());

    // 5. .lookup file with mismatched processorId: should return nullopt
    //    Format: name.100.wrong_processor.v1.lookup
    //    parts = ["name", "100", "wrong_processor", "v1", "lookup"]
    //    processorId = parts[size-3] = "wrong_processor" != "position-and-value"
    auto meta_with_wrong_processor =
        file0->CopyWithExtraFiles({std::string("name.100.wrong_processor.v1.lookup")});
    result = lookup_levels->RemoteSst(meta_with_wrong_processor);
    ASSERT_FALSE(result.has_value());

    // 6. Valid .lookup file with matching processorId: should return RemoteSstFile
    //    Format: data.orc.1024.position-and-value.v1.lookup
    std::string valid_lookup_name = "data.orc.1024.position-and-value.v1.lookup";
    auto meta_with_valid_lookup = file0->CopyWithExtraFiles({std::string(valid_lookup_name)});
    result = lookup_levels->RemoteSst(meta_with_valid_lookup);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().sst_file_name, valid_lookup_name);
    ASSERT_EQ(result.value().ser_version, "v1");

    // 7. Multiple extra_files: first matching .lookup is used, non-matching ones are skipped
    auto meta_with_multiple_extras =
        file0->CopyWithExtraFiles({std::string("changelog.orc"), std::nullopt,
                                   std::string("first.100.position-and-value.ver1.lookup"),
                                   std::string("second.200.position-and-value.ver2.lookup")});
    result = lookup_levels->RemoteSst(meta_with_multiple_extras);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().sst_file_name, "first.100.position-and-value.ver1.lookup");
    ASSERT_EQ(result.value().ser_version, "ver1");

    // 8. Verify NewRemoteSst generates a name that RemoteSst can correctly parse
    std::string generated_name = lookup_levels->NewRemoteSst(file0, /*length=*/2048);
    auto meta_with_generated = file0->CopyWithExtraFiles({std::string(generated_name)});
    result = lookup_levels->RemoteSst(meta_with_generated);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().sst_file_name, generated_name);

    // 9. Exactly 3 parts with matching processorId: "position-and-value.v2.lookup"
    //    parts = ["position-and-value", "v2", "lookup"]
    //    processorId = parts[0] = "position-and-value" (matches)
    auto meta_with_minimal_parts =
        file0->CopyWithExtraFiles({std::string("position-and-value.v2.lookup")});
    result = lookup_levels->RemoteSst(meta_with_minimal_parts);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().sst_file_name, "position-and-value.v2.lookup");
    ASSERT_EQ(result.value().ser_version, "v2");

    ASSERT_OK(lookup_levels->Close());
}

TEST_F(LookupLevelsTest, TestLookupFileCacheIntegration) {
    // This single test covers multiple cache scenarios:
    // 1. Cache is populated on first lookup (cache miss -> create -> put)
    // 2. Subsequent lookups hit the cache (no new files created)
    // 3. Two LookupLevels instances share the same global cache
    // 4. Close() invalidates only own_cached_files_ from the shared cache
    // 5. NotifyDropFile triggers cache invalidation and file deletion
    // 6. Local lookup files are deleted when evicted from cache

    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    // Create two data files at different levels
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11], [2, 22]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/2, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[3, 33], [4, 44]]"));

    // Create a shared global cache
    auto shared_cache = LookupFile::CreateLookupFileCache(/*file_retention_ms=*/-1,
                                                          /*max_disk_size=*/INT64_MAX);
    ASSERT_EQ(shared_cache->Size(), 0);

    // --- Scenario 1: First lookup populates the cache ---
    // Instance 1: uses file0 and file1
    std::vector<std::shared_ptr<DataFileMeta>> files1 = {file0, file1};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels1,
                         Levels::Create(key_comparator, files1, /*num_levels=*/3));
    ASSERT_OK_AND_ASSIGN(auto lookup_levels1,
                         CreateLookupLevels(table_path, levels1, shared_cache));

    // Lookup key=1 -> triggers cache miss, creates lookup file for file0
    ASSERT_OK_AND_ASSIGN(
        auto result, lookup_levels1->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                            /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 11);
    ASSERT_EQ(shared_cache->Size(), 1);

    // Lookup key=3 -> triggers cache miss, creates lookup file for file1
    ASSERT_OK_AND_ASSIGN(
        result, lookup_levels1->Lookup(BinaryRowGenerator::GenerateRowPtr({3}, pool_.get()),
                                       /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 33);
    ASSERT_EQ(shared_cache->Size(), 2);

    // --- Scenario 2: Subsequent lookup hits the cache (no new file created) ---
    ASSERT_OK_AND_ASSIGN(
        result, lookup_levels1->Lookup(BinaryRowGenerator::GenerateRowPtr({2}, pool_.get()),
                                       /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 22);
    // Cache size should not increase (file0 was already cached)
    ASSERT_EQ(shared_cache->Size(), 2);

    // --- Scenario 3: Two LookupLevels share the same cache ---
    // Create a second data file set that has no overlap with lookup_levels1.
    ASSERT_OK_AND_ASSIGN(auto file2, NewFiles(/*level=*/1, /*last_sequence_number=*/4, table_path,
                                              core_options, "[[5, 55], [6, 66]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files2 = {file2};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels2,
                         Levels::Create(key_comparator, files2, /*num_levels=*/3));
    ASSERT_OK_AND_ASSIGN(auto lookup_levels2,
                         CreateLookupLevels(table_path, levels2, shared_cache));

    // Lookup key=5 in instance 2 -> cache miss, creates lookup file for file2
    ASSERT_OK_AND_ASSIGN(
        result, lookup_levels2->Lookup(BinaryRowGenerator::GenerateRowPtr({5}, pool_.get()),
                                       /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 55);
    ASSERT_EQ(shared_cache->Size(), 3);  // file0, file1, file2

    // Collect local file paths for later verification
    std::vector<std::unique_ptr<BasicFileStatus>> tmp_files;
    ASSERT_OK(fs_->ListDir(tmp_dir_->Str(), &tmp_files));
    ASSERT_EQ(tmp_files.size(), 3);

    // --- Scenario 4: Close instance 1 invalidates only its own files ---
    // Instance 1 owns file0 and file1 in the cache
    ASSERT_OK(lookup_levels1->Close());
    // file0 and file1 should be evicted (only owned by instance 1)
    ASSERT_FALSE(shared_cache->GetIfPresent(file0->file_name).has_value());
    ASSERT_FALSE(shared_cache->GetIfPresent(file1->file_name).has_value());
    // file2 should still be in cache (owned by instance 2, not invalidated)
    ASSERT_TRUE(shared_cache->GetIfPresent(file2->file_name).has_value());
    ASSERT_EQ(shared_cache->Size(), 1);

    // --- Scenario 5: Close instance 2 cleans up remaining files ---
    ASSERT_OK(lookup_levels2->Close());
    ASSERT_EQ(shared_cache->Size(), 0);

    // All local lookup files should be deleted
    tmp_files.clear();
    ASSERT_OK(fs_->ListDir(tmp_dir_->Str(), &tmp_files));
    ASSERT_TRUE(tmp_files.empty());
}

TEST_F(LookupLevelsTest, TestCacheEvictionBySmallMaxDiskSize) {
    // Verify that when max_disk_size is small enough to hold only 2 lookup files,
    // adding a 3rd triggers weight-based (SIZE) eviction of the LRU entry.
    // After eviction, subsequent lookups on the evicted file still work by
    // re-creating the lookup file on demand.

    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    // Create 3 data files at level 1.
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11], [2, 22]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/1, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[3, 33], [4, 44]]"));
    ASSERT_OK_AND_ASSIGN(auto file2, NewFiles(/*level=*/1, /*last_sequence_number=*/4, table_path,
                                              core_options, "[[5, 55], [6, 66]]"));

    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1, file2};

    // Phase 1: Probe with unlimited cache to measure per-file weights.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> probe_levels_obj,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));
    auto unlimited_cache = LookupFile::CreateLookupFileCache(/*file_retention_ms=*/-1,
                                                             /*max_disk_size=*/INT64_MAX);
    ASSERT_OK_AND_ASSIGN(auto probe_levels,
                         CreateLookupLevels(table_path, probe_levels_obj, unlimited_cache));

    // Trigger lookups to populate cache for all 3 files.
    ASSERT_OK_AND_ASSIGN(auto result,
                         probe_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                              /*start_level=*/1));
    ASSERT_TRUE(result);
    int64_t weight_after_file0 = unlimited_cache->GetCurrentWeight();

    ASSERT_OK_AND_ASSIGN(result,
                         probe_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({3}, pool_.get()),
                                              /*start_level=*/1));
    ASSERT_TRUE(result);
    int64_t weight_after_file1 = unlimited_cache->GetCurrentWeight();

    ASSERT_OK_AND_ASSIGN(result,
                         probe_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({5}, pool_.get()),
                                              /*start_level=*/1));
    ASSERT_TRUE(result);
    int64_t weight_after_file2 = unlimited_cache->GetCurrentWeight();

    ASSERT_EQ(unlimited_cache->Size(), 3);
    int64_t file0_weight = weight_after_file0;
    int64_t file1_weight = weight_after_file1 - weight_after_file0;
    int64_t file2_weight = weight_after_file2 - weight_after_file1;
    ASSERT_GT(file0_weight, 0);
    ASSERT_GT(file1_weight, 0);
    ASSERT_GT(file2_weight, 0);

    // Set max_disk_size to exactly hold file0 + file1 but not file2.
    int64_t max_disk_for_two = file0_weight + file1_weight + file2_weight - 1;
    ASSERT_OK(probe_levels->Close());

    // Phase 2: Create a new LookupLevels with the constrained cache.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels2,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));
    auto small_cache = LookupFile::CreateLookupFileCache(/*file_retention_ms=*/-1,
                                                         /*max_disk_size=*/max_disk_for_two);
    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels(table_path, levels2, small_cache));

    // Lookup file0: fits in cache.
    ASSERT_OK_AND_ASSIGN(result,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 11);
    ASSERT_EQ(small_cache->Size(), 1);

    // Lookup file1: still fits (file0 + file1 <= max).
    ASSERT_OK_AND_ASSIGN(result,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({3}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 33);
    ASSERT_EQ(small_cache->Size(), 2);
    ASSERT_TRUE(small_cache->GetIfPresent(file0->file_name).has_value());
    ASSERT_TRUE(small_cache->GetIfPresent(file1->file_name).has_value());

    // Lookup file2: total weight exceeds max, should evict file0 (LRU).
    ASSERT_OK_AND_ASSIGN(result,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({5}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 55);

    // file0 should have been evicted (LRU).
    ASSERT_FALSE(small_cache->GetIfPresent(file0->file_name).has_value());
    ASSERT_TRUE(small_cache->GetIfPresent(file1->file_name).has_value());
    ASSERT_TRUE(small_cache->GetIfPresent(file2->file_name).has_value());

    // Lookup key=1 again (file0 was evicted): should re-create the lookup file.
    ASSERT_OK_AND_ASSIGN(result,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 11);

    // file0 is back in cache; file1 should now be evicted (it was LRU).
    ASSERT_TRUE(small_cache->GetIfPresent(file0->file_name).has_value());
    ASSERT_FALSE(small_cache->GetIfPresent(file1->file_name).has_value());
    ASSERT_TRUE(small_cache->GetIfPresent(file2->file_name).has_value());

    ASSERT_OK(lookup_levels->Close());
}

TEST_F(LookupLevelsTest, TestCacheEvictionByExpiration) {
    // Verify that when expire_after_access_ms is very short, cached lookup files
    // expire and are evicted. Subsequent lookups re-create the files.

    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    ASSERT_OK_AND_ASSIGN(auto key_comparator, CreateKeyComparator());

    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/0, table_path,
                                              core_options, "[[1, 11], [2, 22]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/2, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[3, 33], [4, 44]]"));

    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels,
                         Levels::Create(key_comparator, files, /*num_levels=*/3));

    // Create a cache with a very short expiration (300ms).
    auto expiring_cache = LookupFile::CreateLookupFileCache(/*file_retention_ms=*/300,
                                                            /*max_disk_size=*/INT64_MAX);
    ASSERT_OK_AND_ASSIGN(auto lookup_levels,
                         CreateLookupLevels(table_path, levels, expiring_cache));

    // Lookup to populate cache.
    ASSERT_OK_AND_ASSIGN(auto result,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 11);

    ASSERT_OK_AND_ASSIGN(result,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({3}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 33);
    ASSERT_EQ(expiring_cache->Size(), 2);

    // Wait for entries to expire.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Entries should be expired now. GetIfPresent triggers expiration check.
    ASSERT_FALSE(expiring_cache->GetIfPresent(file0->file_name).has_value());
    ASSERT_FALSE(expiring_cache->GetIfPresent(file1->file_name).has_value());
    ASSERT_EQ(expiring_cache->Size(), 0);

    // Lookups should still work by re-creating the lookup files.
    ASSERT_OK_AND_ASSIGN(result,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({1}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 11);
    ASSERT_EQ(expiring_cache->Size(), 1);

    ASSERT_OK_AND_ASSIGN(result,
                         lookup_levels->Lookup(BinaryRowGenerator::GenerateRowPtr({3}, pool_.get()),
                                               /*start_level=*/1));
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().key_value.value->GetInt(1), 33);
    ASSERT_EQ(expiring_cache->Size(), 2);

    ASSERT_OK(lookup_levels->Close());
}
}  // namespace paimon::test
