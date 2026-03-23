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

#include "paimon/core/mergetree/compact/lookup_merge_tree_compact_rewriter.h"

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/mergetree/compact/aggregate/aggregate_merge_function.h"
#include "paimon/core/mergetree/compact/changelog_merge_tree_rewriter.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/interval_partition.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/mergetree/lookup/default_lookup_serializer_factory.h"
#include "paimon/core/mergetree/lookup/persist_empty_processor.h"
#include "paimon/core/mergetree/lookup/persist_position_processor.h"
#include "paimon/core/mergetree/lookup/persist_value_and_pos_processor.h"
#include "paimon/core/mergetree/lookup/persist_value_processor.h"
#include "paimon/core/mergetree/lookup/positioned_key_value.h"
#include "paimon/core/mergetree/lookup_levels.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
class LookupMergeTreeCompactRewriterTest : public testing::Test {
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
            arrow_schema_, options, pool_);

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
                                                        /*is_ascending_order=*/true,
                                                        /*use_view=*/false));
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

    Result<std::unique_ptr<LookupMergeTreeCompactRewriter<bool>>> CreateCompactRewriterForFirstRow(
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        const CoreOptions& options, std::unique_ptr<LookupLevels<bool>>&& lookup_levels) const {
        auto dv_factory = [](const std::string&) -> Result<std::shared_ptr<DeletionVector>> {
            return std::shared_ptr<DeletionVector>();
        };
        auto path_factory_cache =
            std::make_shared<FileStorePathFactoryCache>(table_path, table_schema, options, pool_);
        auto merge_function_wrapper_factory =
            [lookup_levels_ptr = lookup_levels.get()](
                int32_t output_level) -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
            std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper =
                LookupMergeTreeCompactRewriter<bool>::CreateFirstRowMergeFunctionWrapper(
                    std::make_unique<FirstRowMergeFunction>(/*ignore_delete=*/true), output_level,
                    lookup_levels_ptr);
            return merge_function_wrapper;
        };

        return LookupMergeTreeCompactRewriter<bool>::Create(
            /*max_level=*/5, std::move(lookup_levels), /*dv_maintainer=*/nullptr,
            std::move(merge_function_wrapper_factory),
            /*bucket=*/0,
            /*partition=*/BinaryRow::EmptyRow(), table_schema, std::move(dv_factory),
            path_factory_cache, options, pool_);
    }

    Result<std::unique_ptr<LookupMergeTreeCompactRewriter<KeyValue>>>
    CreateCompactRewriterForKeyValue(
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        const CoreOptions& options, std::unique_ptr<LookupLevels<KeyValue>>&& lookup_levels) const {
        auto dv_factory = [](const std::string&) -> Result<std::shared_ptr<DeletionVector>> {
            return std::shared_ptr<DeletionVector>();
        };
        auto path_factory_cache =
            std::make_shared<FileStorePathFactoryCache>(table_path, table_schema, options, pool_);
        auto merge_function_wrapper_factory =
            [this, table_schema, options, lookup_levels_ptr = lookup_levels.get(),
             lookup_strategy = options.GetLookupStrategy()](
                int32_t output_level) -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
            PAIMON_ASSIGN_OR_RAISE(
                auto merge_func,
                AggregateMergeFunction::Create(
                    arrow_schema_, table_schema->TrimmedPrimaryKeys().value(), options));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper,
                LookupMergeTreeCompactRewriter<KeyValue>::CreateLookupMergeFunctionWrapper(
                    std::make_unique<LookupMergeFunction>(std::move(merge_func)), output_level,
                    /*deletion_vectors_maintainer=*/nullptr, lookup_strategy,
                    /*user_defined_seq_comparator=*/nullptr, lookup_levels_ptr));
            return merge_function_wrapper;
        };

        return LookupMergeTreeCompactRewriter<KeyValue>::Create(
            /*max_level=*/5, std::move(lookup_levels), /*dv_maintainer=*/nullptr,
            std::move(merge_function_wrapper_factory), /*bucket=*/0,
            /*partition=*/BinaryRow::EmptyRow(), table_schema, std::move(dv_factory),
            path_factory_cache, options, pool_);
    }

    Result<std::unique_ptr<LookupMergeTreeCompactRewriter<FilePosition>>>
    CreateCompactRewriterForFilePosition(
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        const CoreOptions& options, std::unique_ptr<LookupLevels<FilePosition>>&& lookup_levels,
        DeletionVector::Factory dv_factory = nullptr) const {
        if (!dv_factory) {
            dv_factory = [](const std::string&) -> Result<std::shared_ptr<DeletionVector>> {
                return std::shared_ptr<DeletionVector>();
            };
        }
        auto path_factory_cache =
            std::make_shared<FileStorePathFactoryCache>(table_path, table_schema, options, pool_);

        auto path_factory = std::make_shared<MockIndexPathFactory>(table_path + "/index/");
        auto dv_index_file = std::make_shared<DeletionVectorsIndexFile>(fs_, path_factory,
                                                                        /*bitmap64=*/false, pool_);
        std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
        auto dv_maintainer =
            std::make_shared<BucketedDvMaintainer>(dv_index_file, deletion_vectors);

        auto merge_function_wrapper_factory =
            [lookup_levels_ptr = lookup_levels.get(), lookup_strategy = options.GetLookupStrategy(),
             dv_maintainer_ptr = dv_maintainer](
                int32_t output_level) -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
            auto merge_func = std::make_unique<DeduplicateMergeFunction>(false);
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper,
                LookupMergeTreeCompactRewriter<FilePosition>::CreateLookupMergeFunctionWrapper(
                    std::make_unique<LookupMergeFunction>(std::move(merge_func)), output_level,
                    dv_maintainer_ptr, lookup_strategy,
                    /*user_defined_seq_comparator=*/nullptr, lookup_levels_ptr));
            return merge_function_wrapper;
        };

        return LookupMergeTreeCompactRewriter<FilePosition>::Create(
            /*max_level=*/5, std::move(lookup_levels), dv_maintainer,
            std::move(merge_function_wrapper_factory), /*bucket=*/0,
            /*partition=*/BinaryRow::EmptyRow(), table_schema, std::move(dv_factory),
            path_factory_cache, options, pool_);
    }

    Result<std::unique_ptr<LookupMergeTreeCompactRewriter<PositionedKeyValue>>>
    CreateCompactRewriterForPositionedKeyValue(
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        const CoreOptions& options,
        std::unique_ptr<LookupLevels<PositionedKeyValue>>&& lookup_levels) const {
        auto dv_factory = [](const std::string&) -> Result<std::shared_ptr<DeletionVector>> {
            return std::shared_ptr<DeletionVector>();
        };

        auto path_factory_cache =
            std::make_shared<FileStorePathFactoryCache>(table_path, table_schema, options, pool_);

        auto path_factory = std::make_shared<MockIndexPathFactory>(table_path + "/index/");
        auto dv_index_file = std::make_shared<DeletionVectorsIndexFile>(fs_, path_factory,
                                                                        /*bitmap64=*/false, pool_);
        std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
        auto dv_maintainer =
            std::make_shared<BucketedDvMaintainer>(dv_index_file, deletion_vectors);

        auto merge_function_wrapper_factory =
            [this, table_schema, options, lookup_levels_ptr = lookup_levels.get(),
             lookup_strategy = options.GetLookupStrategy(), dv_maintainer_ptr = dv_maintainer](
                int32_t output_level) -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
            PAIMON_ASSIGN_OR_RAISE(
                auto merge_func,
                AggregateMergeFunction::Create(
                    arrow_schema_, table_schema->TrimmedPrimaryKeys().value(), options));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper,
                LookupMergeTreeCompactRewriter<PositionedKeyValue>::
                    CreateLookupMergeFunctionWrapper(
                        std::make_unique<LookupMergeFunction>(std::move(merge_func)), output_level,
                        dv_maintainer_ptr, lookup_strategy,
                        /*user_defined_seq_comparator=*/nullptr, lookup_levels_ptr));
            return merge_function_wrapper;
        };

        return LookupMergeTreeCompactRewriter<PositionedKeyValue>::Create(
            /*max_level=*/5, std::move(lookup_levels), dv_maintainer,
            std::move(merge_function_wrapper_factory), /*bucket=*/0,
            /*partition=*/BinaryRow::EmptyRow(), table_schema, std::move(dv_factory),
            path_factory_cache, options, pool_);
    }

    void CheckResult(const std::string& compact_file_name,
                     const std::shared_ptr<TableSchema>& table_schema,
                     const std::string& file_format_str,
                     const std::shared_ptr<arrow::ChunkedArray>& expected_array) const {
        ASSERT_OK_AND_ASSIGN(auto file_format,
                             FileFormatFactory::Get(file_format_str, table_schema->Options()));
        ASSERT_OK_AND_ASSIGN(auto reader_builder,
                             file_format->CreateReaderBuilder(/*batch_size=*/10));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             fs_->Open(compact_file_name));
        ASSERT_OK_AND_ASSIGN(auto file_batch_reader, reader_builder->Build(input_stream));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(file_batch_reader.get()));
        // handle type nullable, as result_array does not have not null flag
        result_array = result_array->View(expected_array->type()).ValueOrDie();

        ASSERT_TRUE(expected_array->type()->Equals(result_array->type()))
            << "result=" << result_array->type()->ToString()
            << ", expected=" << expected_array->type()->ToString() << std::endl;
        ASSERT_TRUE(expected_array->Equals(*result_array)) << result_array->ToString();
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

    template <typename T>
    Result<std::unique_ptr<LookupLevels<T>>> CreateLookupLevels(
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<typename PersistProcessor<T>::Factory>& processor_factory,
        const std::vector<std::shared_ptr<DataFileMeta>>& files) const {
        auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
        PAIMON_ASSIGN_OR_RAISE(auto key_comparator, CreateKeyComparator());
        PAIMON_ASSIGN_OR_RAISE(auto levels,
                               Levels::Create(key_comparator, files, /*num_levels=*/5));
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(table_schema->Options()));

        auto io_manager = IOManager::Create(tmp_dir_->Str());
        auto serializer_factory = std::make_shared<DefaultLookupSerializerFactory>();
        PAIMON_ASSIGN_OR_RAISE(auto lookup_key_comparator,
                               RowCompactedSerializer::CreateSliceComparator(key_schema_, pool_));
        PAIMON_ASSIGN_OR_RAISE(auto lookup_store_factory,
                               LookupStoreFactory::Create(lookup_key_comparator, options));
        PAIMON_ASSIGN_OR_RAISE(auto path_factory, CreateFileStorePathFactory(table_path, options));
        return LookupLevels<T>::Create(fs_, BinaryRow::EmptyRow(), /*bucket=*/0, options,
                                       schema_manager, std::move(io_manager), path_factory,
                                       table_schema, std::move(levels),
                                       /*deletion_file_map=*/{}, processor_factory,
                                       serializer_factory, lookup_store_factory, pool_);
    }

    Result<std::vector<std::vector<SortedRun>>> GenerateSortedRuns(
        const std::vector<std::shared_ptr<DataFileMeta>>& files) const {
        PAIMON_ASSIGN_OR_RAISE(auto key_comparator, CreateKeyComparator());
        IntervalPartition interval_partition(files, key_comparator);
        return interval_partition.Partition();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> arrow_schema_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::unique_ptr<UniqueTestDirectory> tmp_dir_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
};

TEST_F(LookupMergeTreeCompactRewriterTest, TestFirstRowRewrite) {
    std::map<std::string, std::string> options = {{Options::MERGE_ENGINE, "first-row"},
                                                  {Options::FILE_FORMAT, "orc"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 2 files
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/0, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/0, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[2, 22], [5, 55]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    auto processor_factory = std::make_shared<PersistEmptyProcessor::Factory>();
    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels<bool>(table_path, table_schema,
                                                                      processor_factory, files));

    // compact and rewrite
    ASSERT_OK_AND_ASSIGN(auto rewriter,
                         CreateCompactRewriterForFirstRow(table_path, table_schema, core_options,
                                                          std::move(lookup_levels)));
    ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns(files));
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Rewrite(
                                                  /*output_level=*/5, /*drop_delete=*/true, runs));
    ASSERT_EQ(2, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());

    // check compact result
    const auto& compact_file_meta = compact_result.After()[0];
    auto expected_file_meta = std::make_shared<DataFileMeta>(
        "file.orc", 100l, /*row_count=*/4,
        /*min_key=*/BinaryRowGenerator::GenerateRow({1}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({5}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({1}, {5}, {0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({1, 5}, {5, 33}, {0, 0}, pool_.get()),
        /*min_sequence_number=*/0l, /*max_sequence_number=*/3l, /*schema_id=*/0, /*level=*/5,
        std::vector<std::optional<std::string>>(), Timestamp(0l, 0), /*delete_row_count=*/0,
        nullptr, FileSource::Compact(), std::nullopt, std::nullopt, std::nullopt, std::nullopt);
    ASSERT_TRUE(expected_file_meta->TEST_Equal(*compact_file_meta));

    // check compact file exist
    std::string compact_file_name = table_path + "/bucket-0/" + compact_file_meta->file_name;
    ASSERT_OK_AND_ASSIGN(bool exist, fs_->Exists(compact_file_name));
    ASSERT_TRUE(exist);

    // check file content
    auto type_with_special_fields =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(arrow_schema_)->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[0,  0,  1, 11],
[3,  0,  2, 22],
[1,  0,  3, 33],
[2,  0,  5, 5]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(compact_file_name, table_schema, "orc", expected_array);
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestFirstRowUpgrade) {
    std::map<std::string, std::string> options = {{Options::MERGE_ENGINE, "first-row"},
                                                  {Options::FILE_FORMAT, "orc"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 1 files
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/0, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 5]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0};
    auto processor_factory = std::make_shared<PersistEmptyProcessor::Factory>();
    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels<bool>(table_path, table_schema,
                                                                      processor_factory, files));

    // upgrade and rewrite
    ASSERT_OK_AND_ASSIGN(auto rewriter,
                         CreateCompactRewriterForFirstRow(table_path, table_schema, core_options,
                                                          std::move(lookup_levels)));
    // output_level = max_level, with ChangelogNoRewrite UpgradeStrategy
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Upgrade(
                                                  /*output_level=*/5, file0));
    ASSERT_EQ(1, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());

    // check compact result
    const auto& compact_file_meta = compact_result.After()[0];
    // ChangelogNoRewrite only upgrade file level without rewrite
    ASSERT_EQ(compact_file_meta->file_name, file0->file_name);

    auto expected_file_meta = std::make_shared<DataFileMeta>(
        "file.orc", 100l, /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({1}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({5}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({1}, {5}, {0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({1, 5}, {5, 33}, {0, 0}, pool_.get()),
        /*min_sequence_number=*/0l, /*max_sequence_number=*/2l, /*schema_id=*/0, /*level=*/5,
        std::vector<std::optional<std::string>>(), Timestamp(0l, 0), /*delete_row_count=*/0,
        nullptr, FileSource::Append(), std::nullopt, std::nullopt, std::nullopt, std::nullopt);
    ASSERT_TRUE(expected_file_meta->TEST_Equal(*compact_file_meta))
        << compact_file_meta->ToString();
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestWithFileFormatPerLevel) {
    std::map<std::string, std::string> options = {{Options::MERGE_ENGINE, "first-row"},
                                                  {Options::FILE_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT_PER_LEVEL, "5:parquet"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 2 files
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/0, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/0, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[2, 22], [5, 55]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    auto processor_factory = std::make_shared<PersistEmptyProcessor::Factory>();
    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels<bool>(table_path, table_schema,
                                                                      processor_factory, files));

    // compact and rewrite
    ASSERT_OK_AND_ASSIGN(auto rewriter,
                         CreateCompactRewriterForFirstRow(table_path, table_schema, core_options,
                                                          std::move(lookup_levels)));
    ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns(files));
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Rewrite(
                                                  /*output_level=*/5, /*drop_delete=*/true, runs));
    ASSERT_EQ(2, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());

    const auto& compact_file_meta = compact_result.After()[0];
    ASSERT_TRUE(StringUtils::EndsWith(compact_file_meta->file_name, ".parquet"));
    // check compact file exist
    std::string compact_file_name = table_path + "/bucket-0/" + compact_file_meta->file_name;
    ASSERT_OK_AND_ASSIGN(bool exist, fs_->Exists(compact_file_name));
    ASSERT_TRUE(exist);

    // check file content
    auto type_with_special_fields =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(arrow_schema_)->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[0,  0,  1, 11],
[3,  0,  2, 22],
[1,  0,  3, 33],
[2,  0,  5, 5]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(compact_file_name, table_schema, "parquet", expected_array);
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestRewriteWithAllHighLevel) {
    // When all input files are high level, rewrite will call RewriteCompaction
    std::map<std::string, std::string> options = {{Options::MERGE_ENGINE, "first-row"},
                                                  {Options::FILE_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT_PER_LEVEL, "5:parquet"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 2 files
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/1, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/1, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[8, 88], [9, 99]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    auto processor_factory = std::make_shared<PersistEmptyProcessor::Factory>();
    ASSERT_OK_AND_ASSIGN(auto lookup_levels, CreateLookupLevels<bool>(table_path, table_schema,
                                                                      processor_factory, files));

    // compact and rewrite
    ASSERT_OK_AND_ASSIGN(auto rewriter,
                         CreateCompactRewriterForFirstRow(table_path, table_schema, core_options,
                                                          std::move(lookup_levels)));
    ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns(files));
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Rewrite(
                                                  /*output_level=*/5, /*drop_delete=*/true, runs));
    ASSERT_EQ(2, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());

    const auto& compact_file_meta = compact_result.After()[0];
    ASSERT_TRUE(StringUtils::EndsWith(compact_file_meta->file_name, ".parquet"));
    // check compact file exist
    std::string compact_file_name = table_path + "/bucket-0/" + compact_file_meta->file_name;
    ASSERT_OK_AND_ASSIGN(bool exist, fs_->Exists(compact_file_name));
    ASSERT_TRUE(exist);

    // check file content
    auto type_with_special_fields =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(arrow_schema_)->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[0,  0,  1, 11],
[1,  0,  3, 33],
[2,  0,  5, 5],
[3,  0,  8, 88],
[4,  0,  9, 99]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(compact_file_name, table_schema, "parquet", expected_array);
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestRewriteWithForceLookupAndSumAgg) {
    std::map<std::string, std::string> options = {{Options::MERGE_ENGINE, "aggregation"},
                                                  {Options::FILE_FORMAT, "orc"},
                                                  {Options::FORCE_LOOKUP, "true"},
                                                  {Options::FIELDS_DEFAULT_AGG_FUNC, "sum"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 3 files
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/5, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 55]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/0, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[2, 22], [4, 44], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file2, NewFiles(/*level=*/0, /*last_sequence_number=*/5, table_path,
                                              core_options, "[[2, 222], [5, 555]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1, file2};
    auto processor_factory = std::make_shared<PersistValueProcessor::Factory>(arrow_schema_);
    ASSERT_OK_AND_ASSIGN(
        auto lookup_levels,
        CreateLookupLevels<KeyValue>(table_path, table_schema, processor_factory, files));

    // compact and rewrite
    ASSERT_OK_AND_ASSIGN(auto rewriter,
                         CreateCompactRewriterForKeyValue(table_path, table_schema, core_options,
                                                          std::move(lookup_levels)));
    ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns({file1, file2}));
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Rewrite(
                                                  /*output_level=*/4, /*drop_delete=*/true, runs));
    ASSERT_EQ(2, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());

    const auto& compact_file_meta = compact_result.After()[0];
    // check compact file exist
    std::string compact_file_name = table_path + "/bucket-0/" + compact_file_meta->file_name;
    ASSERT_OK_AND_ASSIGN(bool exist, fs_->Exists(compact_file_name));
    ASSERT_TRUE(exist);

    // check file content
    auto type_with_special_fields =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(arrow_schema_)->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[6,  0,  2, 244],
[4,  0,  4, 44],
[7,  0,  5, 615]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(compact_file_name, table_schema, "orc", expected_array);
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestRewriteWithDvAndDeduplicate) {
    std::map<std::string, std::string> options = {
        {Options::MERGE_ENGINE, "deduplicate"},
        {Options::FILE_FORMAT, "orc"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 3 files
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/5, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 55]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/0, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[2, 22], [4, 44], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file2, NewFiles(/*level=*/0, /*last_sequence_number=*/5, table_path,
                                              core_options, "[[2, 222], [5, 555]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1, file2};
    auto processor_factory = std::make_shared<PersistPositionProcessor::Factory>();
    ASSERT_OK_AND_ASSIGN(
        auto lookup_levels,
        CreateLookupLevels<FilePosition>(table_path, table_schema, processor_factory, files));

    // compact and rewrite
    ASSERT_OK_AND_ASSIGN(
        auto rewriter, CreateCompactRewriterForFilePosition(table_path, table_schema, core_options,
                                                            std::move(lookup_levels)));
    ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns({file1, file2}));
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Rewrite(
                                                  /*output_level=*/4, /*drop_delete=*/true, runs));
    ASSERT_EQ(2, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());

    const auto& compact_file_meta = compact_result.After()[0];
    // check compact file exist
    std::string compact_file_name = table_path + "/bucket-0/" + compact_file_meta->file_name;
    ASSERT_OK_AND_ASSIGN(bool exist, fs_->Exists(compact_file_name));
    ASSERT_TRUE(exist);

    // check file content
    auto type_with_special_fields =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(arrow_schema_)->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[6,  0,  2, 222],
[4,  0,  4, 44],
[7,  0,  5, 555]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(compact_file_name, table_schema, "orc", expected_array);

    // test dv
    auto dv_maintainer = rewriter->dv_maintainer_;
    ASSERT_TRUE(dv_maintainer);
    auto dv = dv_maintainer->DeletionVectorOf(file0->file_name);
    ASSERT_TRUE(dv);
    ASSERT_FALSE(dv.value()->IsDeleted(0).value());
    ASSERT_TRUE(dv.value()->IsDeleted(2).value());
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestRewriteWithDvAndAgg) {
    std::map<std::string, std::string> options = {
        {Options::MERGE_ENGINE, "aggregation"},
        {Options::FIELDS_DEFAULT_AGG_FUNC, "sum"},
        {Options::FILE_FORMAT, "orc"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 3 files
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/5, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 55]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/0, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[2, 22], [4, 44], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file2, NewFiles(/*level=*/0, /*last_sequence_number=*/5, table_path,
                                              core_options, "[[2, 222], [5, 555]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1, file2};
    auto processor_factory = std::make_shared<PersistValueAndPosProcessor::Factory>(arrow_schema_);
    ASSERT_OK_AND_ASSIGN(
        auto lookup_levels,
        CreateLookupLevels<PositionedKeyValue>(table_path, table_schema, processor_factory, files));

    // compact and rewrite
    ASSERT_OK_AND_ASSIGN(auto rewriter,
                         CreateCompactRewriterForPositionedKeyValue(
                             table_path, table_schema, core_options, std::move(lookup_levels)));
    ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns({file1, file2}));
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Rewrite(
                                                  /*output_level=*/4, /*drop_delete=*/true, runs));
    ASSERT_EQ(2, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());

    const auto& compact_file_meta = compact_result.After()[0];
    // check compact file exist
    std::string compact_file_name = table_path + "/bucket-0/" + compact_file_meta->file_name;
    ASSERT_OK_AND_ASSIGN(bool exist, fs_->Exists(compact_file_name));
    ASSERT_TRUE(exist);

    // check file content
    auto type_with_special_fields =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(arrow_schema_)->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[6,  0,  2, 244],
[4,  0,  4, 44],
[7,  0,  5, 615]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(compact_file_name, table_schema, "orc", expected_array);

    // test dv
    auto dv_maintainer = rewriter->dv_maintainer_;
    ASSERT_TRUE(dv_maintainer);
    auto dv = dv_maintainer->DeletionVectorOf(file0->file_name);
    ASSERT_TRUE(dv);
    ASSERT_FALSE(dv.value()->IsDeleted(0).value());
    ASSERT_TRUE(dv.value()->IsDeleted(2).value());
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestDvUpgradeWithLookup) {
    std::map<std::string, std::string> options = {
        {Options::MERGE_ENGINE, "deduplicate"},
        {Options::FILE_FORMAT, "orc"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 2 files
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/5, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 55]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/0, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[2, 22], [4, 44], [5, 5]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    auto processor_factory = std::make_shared<PersistPositionProcessor::Factory>();
    ASSERT_OK_AND_ASSIGN(
        auto lookup_levels,
        CreateLookupLevels<FilePosition>(table_path, table_schema, processor_factory, files));

    // compact and rewrite
    ASSERT_OK_AND_ASSIGN(
        auto rewriter, CreateCompactRewriterForFilePosition(table_path, table_schema, core_options,
                                                            std::move(lookup_levels)));
    // goto ChangelogNoRewrite UpgradeStrategy
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Upgrade(
                                                  /*output_level=*/4, file1));
    ASSERT_EQ(1, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());

    const auto& compact_file_meta = compact_result.After()[0];
    ASSERT_EQ(compact_file_meta->file_name, file1->file_name);
    ASSERT_EQ(compact_file_meta->level, 4);

    // test dv
    auto dv_maintainer = rewriter->dv_maintainer_;
    ASSERT_TRUE(dv_maintainer);
    auto dv = dv_maintainer->DeletionVectorOf(file0->file_name);
    ASSERT_TRUE(dv);
    ASSERT_FALSE(dv.value()->IsDeleted(0).value());
    ASSERT_TRUE(dv.value()->IsDeleted(2).value());
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestDvUpgradeWithoutLookup) {
    std::map<std::string, std::string> options = {
        {Options::MERGE_ENGINE, "deduplicate"},
        {Options::FILE_FORMAT, "orc"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 2 files
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/5, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 55]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/1, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[2, 22], [4, 44], [5, 5]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    auto processor_factory = std::make_shared<PersistPositionProcessor::Factory>();
    ASSERT_OK_AND_ASSIGN(
        auto lookup_levels,
        CreateLookupLevels<FilePosition>(table_path, table_schema, processor_factory, files));

    // compact and rewrite
    ASSERT_OK_AND_ASSIGN(
        auto rewriter, CreateCompactRewriterForFilePosition(table_path, table_schema, core_options,
                                                            std::move(lookup_levels)));
    // goto NoChangelogNoRewrite UpgradeStrategy
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Upgrade(
                                                  /*output_level=*/4, file1));
    ASSERT_EQ(1, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());

    const auto& compact_file_meta = compact_result.After()[0];
    ASSERT_EQ(compact_file_meta->file_name, file1->file_name);
    ASSERT_EQ(compact_file_meta->level, 4);

    // test dv
    auto dv_maintainer = rewriter->dv_maintainer_;
    ASSERT_TRUE(dv_maintainer);
    auto dv = dv_maintainer->DeletionVectorOf(file0->file_name);
    ASSERT_FALSE(dv);
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestRewriteWithDvFactory) {
    std::map<std::string, std::string> options = {
        {Options::MERGE_ENGINE, "deduplicate"},
        {Options::FILE_FORMAT, "orc"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 3 files
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/5, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33], [5, 55]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/1, /*last_sequence_number=*/2, table_path,
                                              core_options, "[[2, 22], [4, 44], [5, 5]]"));
    ASSERT_OK_AND_ASSIGN(auto file2, NewFiles(/*level=*/0, /*last_sequence_number=*/5, table_path,
                                              core_options, "[[2, 222], [5, 555]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1, file2};
    auto processor_factory = std::make_shared<PersistPositionProcessor::Factory>();
    ASSERT_OK_AND_ASSIGN(
        auto lookup_levels,
        CreateLookupLevels<FilePosition>(table_path, table_schema, processor_factory, files));
    auto dv_factory = [file_name = file1->file_name](
                          const std::string& name) -> Result<std::shared_ptr<DeletionVector>> {
        if (name == file_name) {
            // rm the second row for file1
            auto dv = std::make_shared<BitmapDeletionVector>(RoaringBitmap32());
            PAIMON_RETURN_NOT_OK(dv->Delete(1));
            return dv;
        }
        return std::shared_ptr<DeletionVector>();
    };

    // compact and rewrite
    ASSERT_OK_AND_ASSIGN(auto rewriter, CreateCompactRewriterForFilePosition(
                                            table_path, table_schema, core_options,
                                            std::move(lookup_levels), std::move(dv_factory)));
    ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns({file1, file2}));

    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Rewrite(
                                                  /*output_level=*/4, /*drop_delete=*/true, runs));
    ASSERT_EQ(2, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());

    const auto& compact_file_meta = compact_result.After()[0];
    // check compact file exist
    std::string compact_file_name = table_path + "/bucket-0/" + compact_file_meta->file_name;
    ASSERT_OK_AND_ASSIGN(bool exist, fs_->Exists(compact_file_name));
    ASSERT_TRUE(exist);

    // check file content
    auto type_with_special_fields =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(arrow_schema_)->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[6,  0,  2, 222],
[4,  0,  4, 44],
[7,  0,  5, 555]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(compact_file_name, table_schema, "orc", expected_array);

    // test dv
    auto dv_maintainer = rewriter->dv_maintainer_;
    ASSERT_TRUE(dv_maintainer);
    auto dv = dv_maintainer->DeletionVectorOf(file0->file_name);
    ASSERT_FALSE(dv);
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestIOException) {
    std::map<std::string, std::string> options = {
        {Options::MERGE_ENGINE, "aggregation"},
        {Options::FIELDS_DEFAULT_AGG_FUNC, "sum"},
        {Options::FILE_FORMAT, "orc"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 300; i++) {
        tmp_dir_ = UniqueTestDirectory::Create("local");
        dir_ = UniqueTestDirectory::Create("local");
        ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
        auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

        // write 3 files
        ASSERT_OK_AND_ASSIGN(
            auto file0, NewFiles(/*level=*/5, /*last_sequence_number=*/-1, table_path, core_options,
                                 "[[1, 11], [3, 33], [5, 55]]"));
        ASSERT_OK_AND_ASSIGN(
            auto file1, NewFiles(/*level=*/0, /*last_sequence_number=*/2, table_path, core_options,
                                 "[[2, 22], [4, 44], [5, 5]]"));
        ASSERT_OK_AND_ASSIGN(
            auto file2, NewFiles(/*level=*/0, /*last_sequence_number=*/5, table_path, core_options,
                                 "[[2, 222], [5, 555]]"));
        std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1, file2};
        auto processor_factory =
            std::make_shared<PersistValueAndPosProcessor::Factory>(arrow_schema_);
        ASSERT_OK_AND_ASSIGN(auto lookup_levels,
                             CreateLookupLevels<PositionedKeyValue>(table_path, table_schema,
                                                                    processor_factory, files));

        // compact and rewrite
        ASSERT_OK_AND_ASSIGN(auto rewriter,
                             CreateCompactRewriterForPositionedKeyValue(
                                 table_path, table_schema, core_options, std::move(lookup_levels)));
        ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns({file1, file2}));

        // rewrite may trigger I/O exception
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        auto compact_result = rewriter->Rewrite(
            /*output_level=*/4, /*drop_delete=*/true, runs);
        CHECK_HOOK_STATUS_WITHOUT_MESSAGE_CHECK(compact_result.status());
        io_hook->Clear();

        ASSERT_EQ(2, compact_result.value().Before().size());
        ASSERT_EQ(1, compact_result.value().After().size());

        const auto& compact_file_meta = compact_result.value().After()[0];
        // check compact file exist
        std::string compact_file_name = table_path + "/bucket-0/" + compact_file_meta->file_name;
        ASSERT_OK_AND_ASSIGN(bool exist, fs_->Exists(compact_file_name));
        ASSERT_TRUE(exist);

        // check file content
        auto type_with_special_fields = arrow::struct_(
            SpecialFields::CompleteSequenceAndValueKindField(arrow_schema_)->fields());
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status =
            arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[6,  0,  2, 244],
[4,  0,  4, 44],
[7,  0,  5, 615]
])"},
                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(compact_file_name, table_schema, "orc", expected_array);

        // test dv
        auto dv_maintainer = rewriter->dv_maintainer_;
        ASSERT_TRUE(dv_maintainer);
        auto dv = dv_maintainer->DeletionVectorOf(file0->file_name);
        ASSERT_TRUE(dv);
        ASSERT_FALSE(dv.value()->IsDeleted(0).value());
        ASSERT_TRUE(dv.value()->IsDeleted(2).value());
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestGenerateUpgradeStrategy) {
    auto create_meta = [this](int32_t level, std::optional<int64_t> delete_row_count) {
        return std::make_shared<DataFileMeta>(
            "file.orc", 100l, /*row_count=*/4,
            /*min_key=*/BinaryRowGenerator::GenerateRow({1}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({5}, pool_.get()),
            /*key_stats=*/
            BinaryRowGenerator::GenerateStats({1}, {5}, {0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats({1, 5}, {5, 33}, {0, 0}, pool_.get()),
            /*min_sequence_number=*/0l, /*max_sequence_number=*/3l, /*schema_id=*/0, level,
            std::vector<std::optional<std::string>>(), Timestamp(0l, 0), delete_row_count, nullptr,
            FileSource::Compact(), std::nullopt, std::nullopt, std::nullopt, std::nullopt);
    };
    {
        std::map<std::string, std::string> options = {};
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));

        LookupMergeTreeCompactRewriter<bool> rewriter(
            /*lookup_levels=*/nullptr, /*dv_maintainer=*/nullptr,
            /*max_level=*/5, BinaryRow::EmptyRow(), /*bucket=*/0, /*schema_id=*/0,
            /*trimmed_primary_keys=*/{"key"}, core_options, /*data_schema=*/nullptr,
            /*write_schema=*/nullptr, /*dv_factory*/ nullptr, /*path_factory_cache=*/nullptr,
            /*merge_file_split_read=*/nullptr, /*merge_function_wrapper_factory=*/nullptr, pool_);
        auto file = create_meta(/*level=*/1, /*delete_row_count=*/std::nullopt);
        ASSERT_EQ(ChangelogMergeTreeRewriter::UpgradeStrategy::NoChangelogNoRewrite(),
                  rewriter.GenerateUpgradeStrategy(/*output_level=*/2, file));
    }
    {
        std::map<std::string, std::string> options = {
            {Options::FILE_FORMAT, "orc"}, {Options::FILE_FORMAT_PER_LEVEL, "5:parquet"}};
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));

        LookupMergeTreeCompactRewriter<bool> rewriter(
            /*lookup_levels=*/nullptr, /*dv_maintainer=*/nullptr,
            /*max_level=*/5, BinaryRow::EmptyRow(), /*bucket=*/0, /*schema_id=*/0,
            /*trimmed_primary_keys=*/{"key"}, core_options, /*data_schema=*/nullptr,
            /*write_schema=*/nullptr, /*dv_factory*/ nullptr, /*path_factory_cache=*/nullptr,
            /*merge_file_split_read=*/nullptr, /*merge_function_wrapper_factory=*/nullptr, pool_);
        auto file = create_meta(/*level=*/0, /*delete_row_count=*/std::nullopt);
        ASSERT_EQ(ChangelogMergeTreeRewriter::UpgradeStrategy::ChangelogWithRewrite(),
                  rewriter.GenerateUpgradeStrategy(/*output_level=*/5, file));
    }
    {
        std::map<std::string, std::string> options = {};
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));

        auto dv_maintainer = std::make_shared<BucketedDvMaintainer>(
            std::make_shared<DeletionVectorsIndexFile>(/*fs=*/nullptr, /*path_factory=*/nullptr,
                                                       /*bitmap64=*/false, pool_),
            std::map<std::string, std::shared_ptr<DeletionVector>>());
        LookupMergeTreeCompactRewriter<bool> rewriter(
            /*lookup_levels=*/nullptr, dv_maintainer,
            /*max_level=*/5, BinaryRow::EmptyRow(), /*bucket=*/0, /*schema_id=*/0,
            /*trimmed_primary_keys=*/{"key"}, core_options, /*data_schema=*/nullptr,
            /*write_schema=*/nullptr, /*dv_factory*/ nullptr, /*path_factory_cache=*/nullptr,
            /*merge_file_split_read=*/nullptr, /*merge_function_wrapper_factory=*/nullptr, pool_);
        auto file = create_meta(/*level=*/0, /*delete_row_count=*/1);
        ASSERT_EQ(ChangelogMergeTreeRewriter::UpgradeStrategy::ChangelogWithRewrite(),
                  rewriter.GenerateUpgradeStrategy(/*output_level=*/2, file));
    }
    {
        std::map<std::string, std::string> options = {};
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));

        LookupMergeTreeCompactRewriter<bool> rewriter(
            /*lookup_levels=*/nullptr, /*dv_maintainer=*/nullptr,
            /*max_level=*/5, BinaryRow::EmptyRow(), /*bucket=*/0, /*schema_id=*/0,
            /*trimmed_primary_keys=*/{"key"}, core_options, /*data_schema=*/nullptr,
            /*write_schema=*/nullptr, /*dv_factory*/ nullptr, /*path_factory_cache=*/nullptr,
            /*merge_file_split_read=*/nullptr, /*merge_function_wrapper_factory=*/nullptr, pool_);
        auto file = create_meta(/*level=*/0, /*delete_row_count=*/std::nullopt);
        ASSERT_EQ(ChangelogMergeTreeRewriter::UpgradeStrategy::ChangelogNoRewrite(),
                  rewriter.GenerateUpgradeStrategy(/*output_level=*/5, file));
    }
    {
        std::map<std::string, std::string> options = {};
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));

        LookupMergeTreeCompactRewriter<bool> rewriter(
            /*lookup_levels=*/nullptr, /*dv_maintainer=*/nullptr,
            /*max_level=*/5, BinaryRow::EmptyRow(), /*bucket=*/0, /*schema_id=*/0,
            /*trimmed_primary_keys=*/{"key"}, core_options, /*data_schema=*/nullptr,
            /*write_schema=*/nullptr, /*dv_factory*/ nullptr, /*path_factory_cache=*/nullptr,
            /*merge_file_split_read=*/nullptr, /*merge_function_wrapper_factory=*/nullptr, pool_);
        auto file = create_meta(/*level=*/0, /*delete_row_count=*/std::nullopt);
        ASSERT_EQ(ChangelogMergeTreeRewriter::UpgradeStrategy::ChangelogNoRewrite(),
                  rewriter.GenerateUpgradeStrategy(/*output_level=*/2, file));
    }
    {
        std::map<std::string, std::string> options = {{Options::MERGE_ENGINE, "aggregation"},
                                                      {Options::FIELDS_DEFAULT_AGG_FUNC, "sum"}};
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));

        LookupMergeTreeCompactRewriter<bool> rewriter(
            /*lookup_levels=*/nullptr, /*dv_maintainer=*/nullptr,
            /*max_level=*/5, BinaryRow::EmptyRow(), /*bucket=*/0, /*schema_id=*/0,
            /*trimmed_primary_keys=*/{"key"}, core_options, /*data_schema=*/nullptr,
            /*write_schema=*/nullptr, /*dv_factory*/ nullptr, /*path_factory_cache=*/nullptr,
            /*merge_file_split_read=*/nullptr, /*merge_function_wrapper_factory=*/nullptr, pool_);
        auto file = create_meta(/*level=*/0, /*delete_row_count=*/std::nullopt);
        ASSERT_EQ(ChangelogMergeTreeRewriter::UpgradeStrategy::ChangelogWithRewrite(),
                  rewriter.GenerateUpgradeStrategy(/*output_level=*/2, file));
    }
}

TEST_F(LookupMergeTreeCompactRewriterTest, TestRewriteLookupChangelogWithOutputLevelZero) {
    std::map<std::string, std::string> options = {
        {Options::MERGE_ENGINE, "deduplicate"},
        {Options::FILE_FORMAT, "orc"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_OK_AND_ASSIGN(auto table_path, CreateTable(options));
    auto schema_manager = std::make_shared<SchemaManager>(fs_, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager->ReadSchema(0));

    // write 2 files with level 0
    ASSERT_OK_AND_ASSIGN(auto file0, NewFiles(/*level=*/0, /*last_sequence_number=*/-1, table_path,
                                              core_options, "[[1, 11], [3, 33]]"));
    ASSERT_OK_AND_ASSIGN(auto file1, NewFiles(/*level=*/0, /*last_sequence_number=*/1, table_path,
                                              core_options, "[[2, 22], [4, 44]]"));
    std::vector<std::shared_ptr<DataFileMeta>> files = {file0, file1};
    auto processor_factory = std::make_shared<PersistPositionProcessor::Factory>();
    ASSERT_OK_AND_ASSIGN(
        auto lookup_levels,
        CreateLookupLevels<FilePosition>(table_path, table_schema, processor_factory, files));

    // compact and rewrite with output_level=0
    ASSERT_OK_AND_ASSIGN(
        auto rewriter, CreateCompactRewriterForFilePosition(table_path, table_schema, core_options,
                                                            std::move(lookup_levels)));
    ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns({file0, file1}));

    // When output_level is 0, RewriteLookupChangelog should return false
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Rewrite(
                                                  /*output_level=*/0, /*drop_delete=*/true, runs));
    ASSERT_EQ(2, compact_result.Before().size());
    // When output_level is 0, rewrite should still produce valid result
    ASSERT_GE(compact_result.After().size(), 1);

    const auto& compact_file_meta = compact_result.After()[0];
    // check compact file exist
    std::string compact_file_name = table_path + "/bucket-0/" + compact_file_meta->file_name;
    ASSERT_OK_AND_ASSIGN(bool exist, fs_->Exists(compact_file_name));
    ASSERT_TRUE(exist);

    // check file content
    auto type_with_special_fields =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(arrow_schema_)->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[0,  0,  1, 11],
[2,  0,  2, 22],
[1,  0,  3, 33],
[3,  0,  4, 44]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(compact_file_name, table_schema, "orc", expected_array);
}

}  // namespace paimon::test
