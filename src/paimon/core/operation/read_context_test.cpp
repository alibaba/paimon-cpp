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

#include "paimon/read_context.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/defs.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(ReadContextTest, TestSimple) {
    ReadContextBuilder builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_EQ(ctx->GetPath(), "table_root_path");
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetReadSchema().empty());
    ASSERT_TRUE(ctx->GetReadFieldIds().empty());
    ASSERT_TRUE(ctx->GetOptions().empty());
    ASSERT_FALSE(ctx->GetPredicate());
    ASSERT_FALSE(ctx->EnablePredicateFilter());
    ASSERT_FALSE(ctx->EnablePrefetch());
    ASSERT_EQ(PrefetchCacheMode::ALWAYS, ctx->GetPrefetchCacheMode());
    ASSERT_EQ(600, ctx->GetPrefetchBatchCount());
    ASSERT_EQ(3, ctx->GetPrefetchMaxParallelNum());
    ASSERT_FALSE(ctx->EnableMultiThreadRowToBatch());
    ASSERT_EQ(1, ctx->GetRowToBatchThreadNumber());
    ASSERT_EQ("main", ctx->GetBranch());
    ASSERT_TRUE(ctx->GetFileSystemSchemeToIdentifierMap().empty());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
}

TEST(ReadContextTest, TestSetContent) {
    ReadContextBuilder builder("table_root_path");
    builder.AddOption("key", "value");
    builder.SetReadSchema({"f1", "f2"});
    builder.SetReadFieldIds({0, 1});
    auto predicate =
        PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f1", FieldType::INT);
    builder.SetPredicate(predicate);
    builder.EnablePredicateFilter(true);
    builder.EnablePrefetch(true);
    builder.SetPrefetchCacheMode(PrefetchCacheMode::NEVER);
    builder.SetPrefetchBatchCount(1200);
    builder.SetPrefetchMaxParallelNum(6);
    builder.EnableMultiThreadRowToBatch(true);
    builder.SetRowToBatchThreadNumber(9);
    builder.WithBranch("rt");
    builder.WithFileSystemSchemeToIdentifierMap({{"file", "local"}});
    auto fs = std::make_shared<MockFileSystem>();
    builder.WithFileSystem(fs);
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    // test result
    ASSERT_EQ(ctx->GetPath(), "table_root_path");
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_EQ(ctx->GetReadSchema(), std::vector<std::string>({"f1", "f2"}));
    ASSERT_EQ(ctx->GetReadFieldIds(), std::vector<int32_t>({0, 1}));
    ASSERT_EQ(*predicate, *(ctx->GetPredicate()));
    ASSERT_TRUE(ctx->EnablePredicateFilter());
    ASSERT_TRUE(ctx->EnablePrefetch());
    ASSERT_EQ(PrefetchCacheMode::NEVER, ctx->GetPrefetchCacheMode());
    ASSERT_EQ(1200, ctx->GetPrefetchBatchCount());
    ASSERT_EQ(6, ctx->GetPrefetchMaxParallelNum());
    ASSERT_TRUE(ctx->EnableMultiThreadRowToBatch());
    ASSERT_EQ(9, ctx->GetRowToBatchThreadNumber());
    ASSERT_EQ("rt", ctx->GetBranch());
    std::map<std::string, std::string> expected_fs_map = {{"file", "local"}};
    ASSERT_EQ(expected_fs_map, ctx->GetFileSystemSchemeToIdentifierMap());
    std::map<std::string, std::string> expected_options = {{"key", "value"}};
    ASSERT_EQ(expected_options, ctx->GetOptions());
    ASSERT_EQ(ctx->GetSpecificFileSystem(), fs);
}

}  // namespace paimon::test
