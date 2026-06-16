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

#include "paimon/scan_context.h"

#include "gtest/gtest.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(ScanContextTest, TestDefaultValue) {
    ScanContextBuilder builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_EQ(ctx->GetPath(), "table_root_path");
    ASSERT_FALSE(ctx->IsStreamingMode());
    ASSERT_FALSE(ctx->GetLimit());
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetScanFilters());
    ASSERT_FALSE(ctx->GetScanFilters()->GetBucketFilter());
    ASSERT_FALSE(ctx->GetScanFilters()->GetPredicate());
    ASSERT_TRUE(ctx->GetScanFilters()->GetPartitionFilters().empty());
    ASSERT_TRUE(ctx->GetOptions().empty());
    ASSERT_FALSE(ctx->GetGlobalIndexResult());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
}

TEST(ScanContextTest, TestSetContent) {
    ScanContextBuilder builder("table_root_path");
    std::shared_ptr<MemoryPool> memory_pool = GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();

    builder.SetBucketFilter(10);
    std::vector<std::map<std::string, std::string>> partition_filters = {{{"f1", "20"}}};
    builder.SetPartitionFilter(partition_filters);
    auto predicate =
        PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
    builder.SetPredicate(predicate);
    std::vector<Range> row_ranges = {Range(1, 2), Range(4, 5)};
    auto global_index_result = BitmapGlobalIndexResult::FromRanges(row_ranges);
    builder.SetGlobalIndexResult(global_index_result);
    builder.SetLimit(1000);
    builder.AddOption("key", "value");
    builder.WithStreamingMode(true);
    builder.WithMemoryPool(memory_pool);
    builder.WithExecutor(executor);
    auto fs = std::make_shared<MockFileSystem>();
    builder.WithFileSystem(fs);
    auto manifest_cache = std::make_shared<LruCache>(1024);
    builder.WithCache(manifest_cache);
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_EQ(ctx->GetPath(), "table_root_path");
    ASSERT_TRUE(ctx->IsStreamingMode());
    ASSERT_EQ(1000, ctx->GetLimit());
    ASSERT_TRUE(ctx->GetScanFilters());
    ASSERT_EQ(10, ctx->GetScanFilters()->GetBucketFilter());
    ASSERT_EQ(*predicate, *(ctx->GetScanFilters()->GetPredicate()));
    ASSERT_EQ(partition_filters, ctx->GetScanFilters()->GetPartitionFilters());
    ASSERT_EQ("{1,2,4,5}", ctx->GetGlobalIndexResult()->ToString());
    ASSERT_EQ(memory_pool, ctx->GetMemoryPool());
    ASSERT_EQ(executor, ctx->GetExecutor());
    std::map<std::string, std::string> expected_options = {{"key", "value"}};
    ASSERT_EQ(expected_options, ctx->GetOptions());
    ASSERT_EQ(fs, ctx->GetSpecificFileSystem());
    ASSERT_TRUE(ctx->GetCache());
}

TEST(ScanContextTest, TestSetOptionsOverridesAddedOptions) {
    ScanContextBuilder builder("table_root_path");
    builder.AddOption("old", "value");
    builder.SetOptions({{"key1", "value1"}, {"key2", "value2"}});

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    std::map<std::string, std::string> expected_options = {{"key1", "value1"}, {"key2", "value2"}};
    ASSERT_EQ(expected_options, ctx->GetOptions());
}

}  // namespace paimon::test
