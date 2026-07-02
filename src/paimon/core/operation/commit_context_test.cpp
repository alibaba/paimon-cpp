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

#include "paimon/commit_context.h"

#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(CommitContextTest, TestDefaultValue) {
    CommitContextBuilder builder("table_root_path", "commit_user_1");

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto expected_root_path, PathUtil::NormalizePath("table_root_path"));

    ASSERT_EQ(ctx->GetRootPath(), expected_root_path);
    ASSERT_EQ(ctx->GetCommitUser(), "commit_user_1");
    ASSERT_TRUE(ctx->IgnoreEmptyCommit());
    ASSERT_FALSE(ctx->UseRESTCatalogCommit());
    ASSERT_FALSE(ctx->AppendCommitCheckConflict());
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
    ASSERT_TRUE(ctx->GetOptions().empty());
}

TEST(CommitContextTest, TestSetContent) {
    CommitContextBuilder builder("table_root_path", "commit_user_1");

    auto memory_pool = GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto fs = std::make_shared<MockFileSystem>();

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.IgnoreEmptyCommit(false)
                                       .UseRESTCatalogCommit(true)
                                       .AppendCommitCheckConflict(true)
                                       .WithMemoryPool(memory_pool)
                                       .WithExecutor(executor)
                                       .WithFileSystem(fs)
                                       .AddOption("key", "value")
                                       .Finish());

    ASSERT_OK_AND_ASSIGN(auto expected_root_path, PathUtil::NormalizePath("table_root_path"));
    ASSERT_EQ(ctx->GetRootPath(), expected_root_path);
    ASSERT_EQ(ctx->GetCommitUser(), "commit_user_1");
    ASSERT_FALSE(ctx->IgnoreEmptyCommit());
    ASSERT_TRUE(ctx->UseRESTCatalogCommit());
    ASSERT_TRUE(ctx->AppendCommitCheckConflict());
    ASSERT_EQ(ctx->GetMemoryPool(), memory_pool);
    ASSERT_EQ(ctx->GetExecutor(), executor);
    ASSERT_EQ(ctx->GetSpecificFileSystem(), fs);

    std::map<std::string, std::string> expected_options = {{"key", "value"}};
    ASSERT_EQ(ctx->GetOptions(), expected_options);
}

TEST(CommitContextTest, TestSetOptionsOverridesAddedOptions) {
    CommitContextBuilder builder("table_root_path", "commit_user_1");
    builder.AddOption("old", "value");
    builder.SetOptions({{"key1", "value1"}, {"key2", "value2"}});

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    std::map<std::string, std::string> expected_options = {{"key1", "value1"}, {"key2", "value2"}};
    ASSERT_EQ(ctx->GetOptions(), expected_options);
}

}  // namespace paimon::test
