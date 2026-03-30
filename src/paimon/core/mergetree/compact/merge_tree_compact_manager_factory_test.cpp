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

LevelSortedRun CreateLevelSortedRunForFactoryDirect(int32_t level, int64_t size) {
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

std::vector<LevelSortedRun> CreateRunsForFactoryDirect(const std::vector<int32_t>& levels,
                                                       const std::vector<int64_t>& sizes) {
    std::vector<LevelSortedRun> runs;
    for (size_t i = 0; i < levels.size(); ++i) {
        runs.push_back(CreateLevelSortedRunForFactoryDirect(levels[i], sizes[i]));
    }
    return runs;
}

Status WriteSingleStringRowForFactory(FileStoreWrite* file_store_write, int32_t bucket,
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

Status WriteStringAndInt64RowForFactory(FileStoreWrite* file_store_write, int32_t bucket,
                                        const std::string& key, int64_t sequence) {
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

Result<MergeTreeCompactManagerFactory> CreateFactoryForOptions(
    const std::map<std::string, std::string>& options_map) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_map));
    return MergeTreeCompactManagerFactory(options,
                                          /*key_comparator=*/nullptr,
                                          /*user_defined_seq_comparator=*/nullptr,
                                          /*compaction_metrics=*/nullptr,
                                          /*table_schema=*/nullptr,
                                          /*schema=*/nullptr,
                                          /*schema_manager=*/nullptr,
                                          /*io_manager=*/nullptr,
                                          /*file_store_path_factory=*/nullptr,
                                          /*root_path=*/"",
                                          /*pool=*/nullptr);
}

}  // namespace

TEST(MergeTreeCompactManagerFactoryDirectTest, TestCreateCompactStrategyDefaultUniversal) {
    ASSERT_OK_AND_ASSIGN(auto factory, CreateFactoryForOptions({}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);
    ASSERT_NE(std::dynamic_pointer_cast<UniversalCompaction>(strategy), nullptr);
    ASSERT_EQ(std::dynamic_pointer_cast<ForceUpLevel0Compaction>(strategy), nullptr);
}

TEST(MergeTreeCompactManagerFactoryDirectTest, TestCreateCompactStrategyLookupShouldForceUpLevel0) {
    ASSERT_OK_AND_ASSIGN(auto factory, CreateFactoryForOptions({{Options::FORCE_LOOKUP, "true"},
                                                                {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);
    ASSERT_NE(std::dynamic_pointer_cast<ForceUpLevel0Compaction>(strategy), nullptr);
}

TEST(MergeTreeCompactManagerFactoryDirectTest,
     TestCreateCompactStrategyLookupRadicalShouldNotSetMaxInterval) {
    ASSERT_OK_AND_ASSIGN(auto factory,
                         CreateFactoryForOptions({{Options::FORCE_LOOKUP, "true"},
                                                  {Options::LOOKUP_COMPACT, "radical"},
                                                  {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);
    auto force_strategy = std::dynamic_pointer_cast<ForceUpLevel0Compaction>(strategy);
    ASSERT_NE(force_strategy, nullptr);
    ASSERT_EQ(force_strategy->MaxCompactInterval(), std::nullopt);
}

TEST(MergeTreeCompactManagerFactoryDirectTest,
     TestCreateCompactStrategyLookupGentleShouldSetMaxInterval) {
    ASSERT_OK_AND_ASSIGN(auto factory,
                         CreateFactoryForOptions({{Options::FORCE_LOOKUP, "true"},
                                                  {Options::LOOKUP_COMPACT, "gentle"},
                                                  {Options::LOOKUP_COMPACT_MAX_INTERVAL, "7"},
                                                  {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);
    auto force_strategy = std::dynamic_pointer_cast<ForceUpLevel0Compaction>(strategy);
    ASSERT_NE(force_strategy, nullptr);
    ASSERT_EQ(force_strategy->MaxCompactInterval(), 7);
}

TEST(MergeTreeCompactManagerFactoryDirectTest,
     TestCreateCompactStrategyLookupGentleBehaviorShouldForceAfterThreshold) {
    ASSERT_OK_AND_ASSIGN(auto factory,
                         CreateFactoryForOptions({{Options::FORCE_LOOKUP, "true"},
                                                  {Options::LOOKUP_COMPACT, "gentle"},
                                                  {Options::LOOKUP_COMPACT_MAX_INTERVAL, "7"},
                                                  {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);

    auto runs = CreateRunsForFactoryDirect({0, 0}, {1, 1});
    std::optional<CompactUnit> unit;
    for (int i = 0; i < 6; ++i) {
        ASSERT_OK_AND_ASSIGN(unit, strategy->Pick(/*num_levels=*/3, runs));
        ASSERT_FALSE(unit);
    }

    ASSERT_OK_AND_ASSIGN(unit, strategy->Pick(/*num_levels=*/3, runs));
    ASSERT_TRUE(unit);
    ASSERT_EQ(unit->output_level, 2);
}

TEST(MergeTreeCompactManagerFactoryDirectTest,
     TestCreateCompactStrategyLookupRadicalBehaviorShouldForceImmediately) {
    ASSERT_OK_AND_ASSIGN(auto factory,
                         CreateFactoryForOptions({{Options::FORCE_LOOKUP, "true"},
                                                  {Options::LOOKUP_COMPACT, "radical"},
                                                  {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);

    auto runs = CreateRunsForFactoryDirect({0, 0}, {1, 1});
    ASSERT_OK_AND_ASSIGN(auto unit, strategy->Pick(/*num_levels=*/3, runs));
    ASSERT_TRUE(unit);
    ASSERT_EQ(unit->output_level, 2);
}

TEST(MergeTreeCompactManagerFactoryDirectTest,
     TestCreateCompactStrategyForceUpLevel0OptionShouldTakeEffect) {
    ASSERT_OK_AND_ASSIGN(auto factory,
                         CreateFactoryForOptions({{Options::COMPACTION_FORCE_UP_LEVEL_0, "true"},
                                                  {Options::BUCKET, "1"}}));
    auto strategy = factory.CreateCompactStrategy();
    ASSERT_NE(strategy, nullptr);
    ASSERT_NE(std::dynamic_pointer_cast<ForceUpLevel0Compaction>(strategy), nullptr);
}

TEST(MergeTreeCompactManagerFactoryDirectTest,
     TestCreateCompactManagerWriteOnlyShouldReturnNoopCompactManager) {
    ASSERT_OK_AND_ASSIGN(auto factory, CreateFactoryForOptions({{Options::WRITE_ONLY, "true"},
                                                                {Options::BUCKET, "1"}}));

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

TEST(MergeTreeCompactManagerFactoryTest, TestWriteShouldFailWhenLookupEnabledButIOManagerMissing) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                   /*primary_keys=*/{"f0"},
                                   /*options=*/{{"bucket", "1"}, {Options::FORCE_LOOKUP, "true"}},
                                   /*ignore_if_exists=*/false));

    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    ASSERT_NOK_WITH_MSG(WriteSingleStringRowForFactory(file_store_write.get(), /*bucket=*/0, "k1"),
                        "Can not use lookup, there is no temp disk directory to use");
}

TEST(MergeTreeCompactManagerFactoryTest, TestCompactShouldUseNoopCompactManagerWhenWriteOnly) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                   /*primary_keys=*/{"f0"},
                                   /*options=*/{{"bucket", "1"}, {Options::WRITE_ONLY, "true"}},
                                   /*ignore_if_exists=*/false));

    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    ASSERT_NOK_WITH_MSG(file_store_write->Compact(/*partition=*/{}, /*bucket=*/0,
                                                  /*full_compaction=*/true),
                        "NoopCompactManager does not support user triggered compaction");
}

TEST(MergeTreeCompactManagerFactoryTest,
     TestWriteShouldFailForUnsupportedFullCompactionChangelogRewriter) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(
        Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
        /*primary_keys=*/{"f0"},
        /*options=*/{{"bucket", "1"}, {Options::CHANGELOG_PRODUCER, "full-compaction"}},
        /*ignore_if_exists=*/false));

    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    ASSERT_NOK_WITH_MSG(WriteSingleStringRowForFactory(file_store_write.get(), /*bucket=*/0, "k1"),
                        "not support full changelog merge tree compact rewriter");
}

TEST(MergeTreeCompactManagerFactoryTest,
     TestWriteShouldSucceedWhenLookupDeletionVectorEnabledWithIOManager) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                   /*primary_keys=*/{"f0"},
                                   /*options=*/
                                   {{"bucket", "1"},
                                    {Options::FORCE_LOOKUP, "true"},
                                    {Options::DELETION_VECTORS_ENABLED, "true"}},
                                   /*ignore_if_exists=*/false));

    auto io_manager = IOManager::Create(dir->Str());
    auto io_manager_shared = std::shared_ptr<IOManager>(std::move(io_manager));
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.WithIOManager(io_manager_shared).Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    ASSERT_OK(WriteSingleStringRowForFactory(file_store_write.get(), /*bucket=*/0, "k1"));
}

TEST(MergeTreeCompactManagerFactoryTest, TestWriteShouldFailWhenFirstRowWithDeletionVectors) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(
        catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                             /*primary_keys=*/{"f0"},
                             /*options=*/{{"bucket", "1"}, {Options::MERGE_ENGINE, "first-row"}},
                             /*ignore_if_exists=*/false));

    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder
                             .SetOptions({{Options::BUCKET, "1"},
                                          {Options::MERGE_ENGINE, "first-row"},
                                          {Options::DELETION_VECTORS_ENABLED, "true"}})
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    ASSERT_NOK_WITH_MSG(WriteSingleStringRowForFactory(file_store_write.get(), /*bucket=*/0, "k1"),
                        "First row merge engine does not need deletion vectors because there "
                        "is no deletion of old "
                        "data in this merge engine");
}

TEST(MergeTreeCompactManagerFactoryTest,
     TestWriteShouldSucceedWhenLookupDeletionVectorEnabledWithSequenceField) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false),
                   arrow::field("f1", arrow::int64())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                   /*primary_keys=*/{"f0"},
                                   /*options=*/
                                   {{"bucket", "1"},
                                    {Options::FORCE_LOOKUP, "true"},
                                    {Options::DELETION_VECTORS_ENABLED, "true"},
                                    {Options::SEQUENCE_FIELD, "f1"}},
                                   /*ignore_if_exists=*/false));

    auto io_manager = IOManager::Create(dir->Str());
    auto io_manager_shared = std::shared_ptr<IOManager>(std::move(io_manager));
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.WithIOManager(io_manager_shared).Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    ASSERT_OK(WriteStringAndInt64RowForFactory(file_store_write.get(), /*bucket=*/0, "k1", 1));
}

TEST(MergeTreeCompactManagerFactoryTest,
     TestWriteShouldSucceedWhenLookupDeletionVectorEnabledWithLookupChangelog) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                   /*primary_keys=*/{"f0"},
                                   /*options=*/
                                   {{"bucket", "1"},
                                    {Options::DELETION_VECTORS_ENABLED, "true"},
                                    {Options::CHANGELOG_PRODUCER, "lookup"}},
                                   /*ignore_if_exists=*/false));

    auto io_manager = IOManager::Create(dir->Str());
    auto io_manager_shared = std::shared_ptr<IOManager>(std::move(io_manager));
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.WithIOManager(io_manager_shared).Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    ASSERT_OK(WriteSingleStringRowForFactory(file_store_write.get(), /*bucket=*/0, "k1"));
}

}  // namespace paimon::test
