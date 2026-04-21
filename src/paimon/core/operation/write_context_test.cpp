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
#include "paimon/result.h"
#include "paimon/status.h"
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

TEST(WriteContextTest, TestWithTempDirectory) {
    WriteContextBuilder builder("table_root_path", "commit_user_1");

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.WithTempDirectory("/tmp").Finish());
    ASSERT_EQ(ctx->GetTempDirectory(), "/tmp");
}

}  // namespace paimon::test
