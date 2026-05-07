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

#include "paimon/core/operation/file_store_scan.h"

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class FileStoreScanTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

 private:
    std::shared_ptr<arrow::Schema> schema_ = arrow::schema(arrow::FieldVector(
        {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
         arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())}));
};

TEST_F(FileStoreScanTest, TestCreatePartitionPredicate) {
    std::vector<std::string> partition_keys = {"f1"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, {partition_filters}));
    ASSERT_EQ(partition_predicate->ToString(), "Equal(f1, 1)");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithMultiPartitionKeys) {
    std::vector<std::string> partition_keys = {"f1", "f0"};

    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    partition_filters["f0"] = "hello";
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, {partition_filters}));
    ASSERT_EQ(partition_predicate->ToString(), "And([Equal(f0, hello), Equal(f1, 1)])");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithDefaultPartitionName) {
    std::vector<std::string> partition_keys = {"f1", "f0"};

    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    partition_filters["f0"] = partition_default_name;
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, {partition_filters}));
    ASSERT_EQ(partition_predicate->ToString(), "And([IsNull(f0), Equal(f1, 1)])");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithMultiPartitionFilters) {
    std::vector<std::string> partition_keys = {"f1", "f0"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::vector<std::map<std::string, std::string>> partition_filters = {
        {{"f1", "1"}, {"f0", "hello"}}, {{"f1", "2"}, {"f0", "world"}}};
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, partition_filters));
    ASSERT_EQ(partition_predicate->ToString(),
              "Or([And([Equal(f0, hello), Equal(f1, 1)]), And([Equal(f0, world), Equal(f1, 2)])])");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithOneEmptyPartitionFilter) {
    std::vector<std::string> partition_keys = {"f1", "f0"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    // empty map in partition_filters indicates a all_true partition filter
    std::vector<std::map<std::string, std::string>> partition_filters = {
        {{"f1", "1"}, {"f0", "hello"}}, {}};
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, partition_filters));
    ASSERT_FALSE(partition_predicate);
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithEmptyPartitionFilters) {
    std::vector<std::string> partition_keys = {"f1", "f0"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::vector<std::map<std::string, std::string>> partition_filters = {};
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, partition_filters));
    ASSERT_FALSE(partition_predicate);
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithInvalidPartitionKeys) {
    std::vector<std::string> partition_keys = {"invalid"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    ASSERT_NOK_WITH_MSG(FileStoreScan::CreatePartitionPredicate(
                            partition_keys, partition_default_name, schema_, {partition_filters}),
                        "field invalid does not exist in schema");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithInvalidPartitionFilter) {
    std::vector<std::string> partition_keys = {"f1"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["invalid"] = "1";
    ASSERT_NOK_WITH_MSG(FileStoreScan::CreatePartitionPredicate(
                            partition_keys, partition_default_name, schema_, {partition_filters}),
                        "field invalid does not exist in partition keys");
}

TEST_F(FileStoreScanTest, TestFilterManifestByRowRanges) {
    class FakeFileStoreScan : public FileStoreScan {
     public:
        FakeFileStoreScan(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                          const std::shared_ptr<SchemaManager>& schema_manager,
                          const std::shared_ptr<ManifestList>& manifest_list,
                          const std::shared_ptr<ManifestFile>& manifest_file,
                          const std::shared_ptr<TableSchema>& table_schema,
                          const std::shared_ptr<arrow::Schema>& schema,
                          const CoreOptions& core_options,
                          const std::shared_ptr<Executor>& executor,
                          const std::shared_ptr<MemoryPool>& pool)
            : FileStoreScan(snapshot_manager, schema_manager, manifest_list, manifest_file,
                            table_schema, schema, core_options, executor, pool) {}
        Result<bool> FilterByStats(const ManifestEntry& entry) const override {
            return false;
        }
    };
    // row id [10, 20]
    auto manifest1 =
        ManifestFileMeta("manifest-65b0d403-a1bc-4157-b242-bff73c46596d-0", /*file_size=*/2779,
                         /*num_added_files=*/1, /*num_deleted_files=*/0, SimpleStats::EmptyStats(),
                         /*schema_id=*/0, /*min_bucket=*/0, /*max_bucket=*/0,
                         /*min_level=*/0, /*max_level=*/0,
                         /*min_row_id=*/10, /*max_row_id=*/20);

    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({{}}));
    auto file_store_scan = std::make_shared<FakeFileStoreScan>(
        /*snapshot_manager=*/nullptr, /*schema_manager=*/nullptr, /*manifest_list=*/nullptr,
        /*manifest_file=*/nullptr, /*table_schema=*/nullptr, /*schema=*/nullptr, options,
        /*executor=*/CreateDefaultExecutor(), GetDefaultPool());
    ASSERT_TRUE(file_store_scan->FilterManifestByRowRanges(manifest1));

    ASSERT_OK_AND_ASSIGN(
        RowRangeIndex row_range_index,
        RowRangeIndex::Create(std::vector<Range>({Range(0, 15), Range(100, 200)})));
    file_store_scan->WithRowRangeIndex(row_range_index);
    ASSERT_TRUE(file_store_scan->FilterManifestByRowRanges(manifest1));

    ASSERT_OK_AND_ASSIGN(row_range_index,
                         RowRangeIndex::Create(std::vector<Range>({Range(0, 5), Range(100, 200)})));
    file_store_scan->WithRowRangeIndex(row_range_index);
    ASSERT_FALSE(file_store_scan->FilterManifestByRowRanges(manifest1));

    auto manifest2 =
        ManifestFileMeta("manifest-65b0d403-a1bc-4157-b242-bff73c46596d-0", /*file_size=*/2779,
                         /*num_added_files=*/1, /*num_deleted_files=*/0, SimpleStats::EmptyStats(),
                         /*schema_id=*/0, /*min_bucket=*/0, /*max_bucket=*/0,
                         /*min_level=*/0, /*max_level=*/0,
                         /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    ASSERT_OK_AND_ASSIGN(row_range_index, RowRangeIndex::Create(std::vector<Range>({Range(0, 0)})));
    file_store_scan->WithRowRangeIndex(row_range_index);
    ASSERT_TRUE(file_store_scan->FilterManifestByRowRanges(manifest2));
}
}  // namespace paimon::test
