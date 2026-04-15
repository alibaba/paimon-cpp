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

#include "paimon/core/mergetree/compact/merge_tree_compact_manager_factory.h"

#include <map>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/compact/noop_compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/force_up_level0_compaction.h"
#include "paimon/core/mergetree/compact/universal_compaction.h"
#include "paimon/disk/io_manager.h"
#include "paimon/file_store_write.h"
#include "paimon/record_batch.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

namespace {

class MergeTreeCompactManagerFactoryStrategyTest : public ::testing::Test {
 protected:
    LevelSortedRun CreateLevelSortedRun(int32_t level, int64_t size) const {
        auto file_meta = std::make_shared<DataFileMeta>(
            "factory-direct.data", /*file_size=*/size, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(), /*value_stats=*/SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
            /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
        return {level, SortedRun::FromSingle(file_meta)};
    }

    std::vector<LevelSortedRun> CreateRuns(const std::vector<int32_t>& levels,
                                           const std::vector<int64_t>& sizes) const {
        std::vector<LevelSortedRun> runs;
        for (size_t i = 0; i < levels.size(); ++i) {
            runs.push_back(CreateLevelSortedRun(levels[i], sizes[i]));
        }
        return runs;
    }

    Result<MergeTreeCompactManagerFactory> CreateFactory(
        const std::map<std::string, std::string>& options_map) const {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_map));
        return MergeTreeCompactManagerFactory(options,
                                              /*key_comparator=*/nullptr,
                                              /*user_defined_seq_comparator=*/nullptr,
                                              /*compaction_metrics=*/nullptr,
                                              /*table_schema=*/nullptr,
                                              /*schema=*/nullptr,
                                              /*schema_manager=*/nullptr,
                                              /*io_manager=*/nullptr,
                                              /*cache_manager=*/nullptr,
                                              /*file_store_path_factory=*/nullptr,
                                              /*root_path=*/"",
                                              /*pool=*/nullptr);
    }
};

class MergeTreeCompactManagerFactoryWriteTest : public ::testing::Test {
 protected:
    Result<std::unique_ptr<FileStoreWrite>> CreateFileStoreWrite(
        const std::vector<std::shared_ptr<arrow::Field>>& fields,
        const std::vector<std::string>& primary_keys,
        const std::map<std::string, std::string>& table_options, bool with_io_manager,
        const std::map<std::string, std::string>& context_options = {}) {
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
                                                  /*partition_keys=*/{}, primary_keys,
                                                  table_options,
                                                  /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
        if (!context_options.empty()) {
            context_builder.SetOptions(context_options);
        }
        if (with_io_manager) {
            auto io_manager = IOManager::Create(dir->Str());
            context_builder.WithIOManager(std::shared_ptr<IOManager>(std::move(io_manager)));
        }

        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context,
                               context_builder.Finish());
        return FileStoreWrite::Create(std::move(write_context));
    }

    Result<std::unique_ptr<FileStoreWrite>> CreateSingleStringFileStoreWrite(
        const std::map<std::string, std::string>& table_options, bool with_io_manager,
        const std::map<std::string, std::string>& context_options = {}) {
        return CreateFileStoreWrite({arrow::field("f0", arrow::utf8(), /*nullable=*/false)},
                                    /*primary_keys=*/{"f0"}, table_options, with_io_manager,
                                    context_options);
    }

    Result<std::unique_ptr<FileStoreWrite>> CreateStringAndInt64FileStoreWrite(
        const std::map<std::string, std::string>& table_options, bool with_io_manager,
        const std::map<std::string, std::string>& context_options = {}) {
        return CreateFileStoreWrite({arrow::field("f0", arrow::utf8(), /*nullable=*/false),
                                     arrow::field("f1", arrow::int64())},
                                    /*primary_keys=*/{"f0"}, table_options, with_io_manager,
                                    context_options);
    }

    Status WriteSingleStringRow(FileStoreWrite* file_store_write, int32_t bucket,
                                const std::string& value) const {
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

    Status WriteStringAndInt64Row(FileStoreWrite* file_store_write, int32_t bucket,
                                  const std::string& key, int64_t sequence) const {
        auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false),
                       arrow::field("f1", arrow::int64())};
        auto struct_type = arrow::struct_(fields);
        arrow::StructBuilder struct_builder(
            struct_type, arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int64Builder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto int64_builder = static_cast<arrow::Int64Builder*>(struct_builder.field_builder(1));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Append());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(string_builder->Append(key));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(int64_builder->Append(sequence));

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
};

}  // namespace

TEST_F(MergeTreeCompactManagerFactoryStrategyTest, TestCreateCompactStrategyDefaultUniversal) {
    ASSERT_OK_AND_ASSIGN(auto factory, CreateFactory({}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);
    ASSERT_NE(std::dynamic_pointer_cast<UniversalCompaction>(strategy), nullptr);
    ASSERT_EQ(std::dynamic_pointer_cast<ForceUpLevel0Compaction>(strategy), nullptr);
}

TEST_F(MergeTreeCompactManagerFactoryStrategyTest,
       TestCreateCompactStrategyLookupShouldForceUpLevel0) {
    ASSERT_OK_AND_ASSIGN(auto factory,
                         CreateFactory({{Options::FORCE_LOOKUP, "true"}, {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);
    ASSERT_NE(std::dynamic_pointer_cast<ForceUpLevel0Compaction>(strategy), nullptr);
}

TEST_F(MergeTreeCompactManagerFactoryStrategyTest,
       TestCreateCompactStrategyLookupRadicalShouldNotSetMaxInterval) {
    ASSERT_OK_AND_ASSIGN(auto factory, CreateFactory({{Options::FORCE_LOOKUP, "true"},
                                                      {Options::LOOKUP_COMPACT, "radical"},
                                                      {Options::LOOKUP_COMPACT_MAX_INTERVAL, "7"},
                                                      {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);
    auto force_strategy = std::dynamic_pointer_cast<ForceUpLevel0Compaction>(strategy);
    ASSERT_NE(force_strategy, nullptr);
    ASSERT_EQ(force_strategy->MaxCompactInterval(), std::nullopt);
}

TEST_F(MergeTreeCompactManagerFactoryStrategyTest,
       TestCreateCompactStrategyLookupGentleShouldSetMaxInterval) {
    ASSERT_OK_AND_ASSIGN(auto factory, CreateFactory({{Options::FORCE_LOOKUP, "true"},
                                                      {Options::LOOKUP_COMPACT, "gentle"},
                                                      {Options::LOOKUP_COMPACT_MAX_INTERVAL, "7"},
                                                      {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);
    auto force_strategy = std::dynamic_pointer_cast<ForceUpLevel0Compaction>(strategy);
    ASSERT_NE(force_strategy, nullptr);
    ASSERT_EQ(force_strategy->MaxCompactInterval(), 7);
}

TEST_F(MergeTreeCompactManagerFactoryStrategyTest,
       TestCreateCompactStrategyLookupGentleBehaviorShouldForceAfterThreshold) {
    ASSERT_OK_AND_ASSIGN(auto factory, CreateFactory({{Options::FORCE_LOOKUP, "true"},
                                                      {Options::LOOKUP_COMPACT, "gentle"},
                                                      {Options::LOOKUP_COMPACT_MAX_INTERVAL, "7"},
                                                      {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);

    auto runs = CreateRuns({0, 0}, {1, 1});
    std::optional<CompactUnit> unit;
    for (int i = 0; i < 6; ++i) {
        ASSERT_OK_AND_ASSIGN(unit, strategy->Pick(/*num_levels=*/3, runs));
        ASSERT_FALSE(unit);
    }

    ASSERT_OK_AND_ASSIGN(unit, strategy->Pick(/*num_levels=*/3, runs));
    ASSERT_TRUE(unit);
    ASSERT_EQ(unit->output_level, 2);
}

TEST_F(MergeTreeCompactManagerFactoryStrategyTest,
       TestCreateCompactStrategyLookupRadicalBehaviorShouldForceImmediately) {
    ASSERT_OK_AND_ASSIGN(auto factory, CreateFactory({{Options::FORCE_LOOKUP, "true"},
                                                      {Options::LOOKUP_COMPACT, "radical"},
                                                      {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);

    auto runs = CreateRuns({0, 0}, {1, 1});
    ASSERT_OK_AND_ASSIGN(auto unit, strategy->Pick(/*num_levels=*/3, runs));
    ASSERT_TRUE(unit);
    ASSERT_EQ(unit->output_level, 2);
}

TEST_F(MergeTreeCompactManagerFactoryStrategyTest,
       TestCreateCompactStrategyForceUpLevel0OptionShouldTakeEffect) {
    ASSERT_OK_AND_ASSIGN(
        auto factory,
        CreateFactory({{Options::COMPACTION_FORCE_UP_LEVEL_0, "true"}, {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);
    ASSERT_NE(std::dynamic_pointer_cast<ForceUpLevel0Compaction>(strategy), nullptr);
}

TEST_F(MergeTreeCompactManagerFactoryStrategyTest,
       TestCreateCompactManagerWriteOnlyShouldReturnNoopCompactManager) {
    ASSERT_OK_AND_ASSIGN(auto factory,
                         CreateFactory({{Options::WRITE_ONLY, "true"}, {Options::BUCKET, "1"}}));

    BinaryRow partition;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CompactManager> manager,
                         factory.CreateCompactManager(partition, /*bucket=*/0,
                                                      /*compact_strategy=*/nullptr,
                                                      /*compact_executor=*/nullptr,
                                                      /*levels=*/nullptr,
                                                      /*dv_maintainer=*/nullptr));
    ASSERT_NE(manager, nullptr);
    ASSERT_NE(std::dynamic_pointer_cast<NoopCompactManager>(manager), nullptr);
}

TEST_F(MergeTreeCompactManagerFactoryWriteTest,
       TestWriteShouldFailWhenLookupEnabledButIOManagerMissing) {
    ASSERT_OK_AND_ASSIGN(
        auto file_store_write,
        CreateSingleStringFileStoreWrite({{"bucket", "1"}, {Options::FORCE_LOOKUP, "true"}},
                                         /*with_io_manager=*/false));

    ASSERT_NOK_WITH_MSG(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "k1"),
                        "Can not use lookup, there is no temp disk directory to use");
}

TEST_F(MergeTreeCompactManagerFactoryWriteTest,
       TestCompactShouldUseNoopCompactManagerWhenWriteOnly) {
    ASSERT_OK_AND_ASSIGN(
        auto file_store_write,
        CreateSingleStringFileStoreWrite({{"bucket", "1"}, {Options::WRITE_ONLY, "true"}},
                                         /*with_io_manager=*/false));

    ASSERT_NOK_WITH_MSG(file_store_write->Compact(/*partition=*/{}, /*bucket=*/0,
                                                  /*full_compaction=*/true),
                        "NoopCompactManager does not support user triggered compaction");
}

TEST_F(MergeTreeCompactManagerFactoryWriteTest,
       TestCreateFileStoreWriteShouldFailWhenFullCompactionChangelogConfigured) {
    ASSERT_NOK_WITH_MSG(CreateSingleStringFileStoreWrite(
                            {{"bucket", "1"}, {Options::CHANGELOG_PRODUCER, "full-compaction"}},
                            /*with_io_manager=*/false),
                        "C++ Paimon does not support changelog-producer yet");
}

TEST_F(MergeTreeCompactManagerFactoryWriteTest,
       TestPrepareCommitShouldSucceedWhenLookupDeletionVectorsTakeNoEffectInWriteProcess) {
    ASSERT_OK_AND_ASSIGN(auto file_store_write, CreateSingleStringFileStoreWrite(
                                                    {{"bucket", "1"},
                                                     {Options::FORCE_LOOKUP, "true"},
                                                     {Options::DELETION_VECTORS_ENABLED, "true"}},
                                                    /*with_io_manager=*/true));

    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "k1"));
    ASSERT_OK(file_store_write->PrepareCommit(/*wait_compaction=*/true).status());
}

TEST_F(MergeTreeCompactManagerFactoryWriteTest,
       TestWriteShouldFailWhenFirstRowWithDeletionVectors) {
    ASSERT_OK_AND_ASSIGN(
        auto file_store_write,
        CreateSingleStringFileStoreWrite({{"bucket", "1"}, {Options::MERGE_ENGINE, "first-row"}},
                                         /*with_io_manager=*/false,
                                         {{Options::BUCKET, "1"},
                                          {Options::MERGE_ENGINE, "first-row"},
                                          {Options::DELETION_VECTORS_ENABLED, "true"}}));

    ASSERT_NOK_WITH_MSG(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "k1"),
                        "First row merge engine does not need deletion vectors because there "
                        "is no deletion of old "
                        "data in this merge engine");
}

TEST_F(MergeTreeCompactManagerFactoryWriteTest,
       TestPrepareCommitShouldSucceedWhenLookupDeletionVectorEnabledWithSequenceField) {
    ASSERT_OK_AND_ASSIGN(auto file_store_write, CreateStringAndInt64FileStoreWrite(
                                                    {{"bucket", "1"},
                                                     {Options::FORCE_LOOKUP, "true"},
                                                     {Options::DELETION_VECTORS_ENABLED, "true"},
                                                     {Options::SEQUENCE_FIELD, "f1"}},
                                                    /*with_io_manager=*/true));

    ASSERT_OK(WriteStringAndInt64Row(file_store_write.get(), /*bucket=*/0, "k1", 1));
    ASSERT_OK(file_store_write->PrepareCommit(/*wait_compaction=*/true).status());
}

TEST_F(MergeTreeCompactManagerFactoryWriteTest,
       TestCreateFileStoreWriteShouldFailWhenLookupChangelogConfigured) {
    ASSERT_NOK_WITH_MSG(
        CreateSingleStringFileStoreWrite({{"bucket", "1"},
                                          {Options::DELETION_VECTORS_ENABLED, "true"},
                                          {Options::CHANGELOG_PRODUCER, "lookup"}},
                                         /*with_io_manager=*/true),
        "C++ Paimon does not support changelog-producer yet");
}

}  // namespace paimon::test
