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

#include "paimon/core/table/bucket_mode.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

std::shared_ptr<TableSchema> CreateTableSchema(const std::vector<std::string>& primary_keys) {
    auto schema = arrow::schema({arrow::field("f0", arrow::int32(), /*nullable=*/false),
                                 arrow::field("f1", arrow::int32(), /*nullable=*/false)});
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options;
    EXPECT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    return table_schema;
}

}  // namespace

TEST(BucketModeTest, TestResolveBucketMode) {
    std::shared_ptr<TableSchema> append_schema = CreateTableSchema(/*primary_keys=*/{});
    std::shared_ptr<TableSchema> pk_schema = CreateTableSchema(/*primary_keys=*/{"f0"});

    EXPECT_EQ(BucketMode::POSTPONE_MODE,
              ResolveBucketMode(BucketModeDefine::POSTPONE_BUCKET, append_schema));
    EXPECT_EQ(BucketMode::BUCKET_UNAWARE, ResolveBucketMode(-1, append_schema));
    EXPECT_EQ(BucketMode::HASH_DYNAMIC, ResolveBucketMode(-1, pk_schema));
    EXPECT_EQ(BucketMode::BUCKET_UNAWARE,
              ResolveBucketMode(BucketModeDefine::UNAWARE_BUCKET, pk_schema));
    EXPECT_EQ(BucketMode::HASH_FIXED, ResolveBucketMode(4, append_schema));
}

}  // namespace paimon::test
