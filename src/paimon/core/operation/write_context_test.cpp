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

#include "paimon/write_context.h"

#include "gtest/gtest.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(WriteContextTest, TestSimple) {
    WriteContextBuilder builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_EQ(ctx->GetRootPath(), "table_root_path");
    ASSERT_EQ(ctx->GetCommitUser(), "commit_user_1");
    ASSERT_FALSE(ctx->IsStreamingMode());
    ASSERT_FALSE(ctx->IgnoreNumBucketCheck());
    ASSERT_FALSE(ctx->IgnorePreviousFiles());
    ASSERT_EQ(ctx->GetWriteId(), std::nullopt);
    ASSERT_EQ(ctx->GetBranch(), "main");
    ASSERT_TRUE(ctx->GetWriteSchema().empty());
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetTempDirectory().empty());
    ASSERT_TRUE(ctx->GetOptions().empty());
    ASSERT_TRUE(ctx->GetFileSystemSchemeToIdentifierMap().empty());
}

TEST(WriteContextTest, TestAllWithMethods) {
    WriteContextBuilder builder("table_root_path", "commit_user_1");

    auto memory_pool = GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto file_system = std::make_shared<MockFileSystem>();
    std::vector<std::string> write_schema = {"f0", "f1"};
    std::map<std::string, std::string> fs_scheme_to_identifier_map = {{"file", "local"},
                                                                      {"oss", "jindo"}};

    ASSERT_OK_AND_ASSIGN(auto ctx,
                         builder.WithStreamingMode(true)
                             .WithIgnoreNumBucketCheck(true)
                             .WithIgnorePreviousFiles(true)
                             .WithMemoryPool(memory_pool)
                             .WithExecutor(executor)
                             .WithTempDirectory("/tmp/with-all")
                             .WithWriteId(123)
                             .WithBranch("test_branch")
                             .WithWriteSchema(write_schema)
                             .WithFileSystemSchemeToIdentifierMap(fs_scheme_to_identifier_map)
                             .WithFileSystem(file_system)
                             .Finish());

    ASSERT_TRUE(ctx->IsStreamingMode());
    ASSERT_TRUE(ctx->IgnoreNumBucketCheck());
    ASSERT_TRUE(ctx->IgnorePreviousFiles());
    ASSERT_EQ(ctx->GetMemoryPool(), memory_pool);
    ASSERT_EQ(ctx->GetExecutor(), executor);
    ASSERT_EQ(ctx->GetTempDirectory(), "/tmp/with-all");
    ASSERT_EQ(ctx->GetWriteId(), 123);
    ASSERT_EQ(ctx->GetBranch(), "test_branch");
    ASSERT_EQ(ctx->GetWriteSchema(), write_schema);
    ASSERT_EQ(ctx->GetFileSystemSchemeToIdentifierMap(), fs_scheme_to_identifier_map);
    ASSERT_EQ(ctx->GetSpecificFileSystem(), file_system);
}

}  // namespace paimon::test
