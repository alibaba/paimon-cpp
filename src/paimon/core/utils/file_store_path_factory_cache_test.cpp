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

#include "paimon/core/utils/file_store_path_factory_cache.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(FileStorePathFactoryCacheTest, TestSimple) {
    std::vector<DataField> fields = {DataField(0, arrow::field("f0", arrow::int32())),
                                     DataField(1, arrow::field("f1", arrow::utf8()))};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::InitSchema(/*schema_id=*/0, fields, /*highest_field_id=*/fields.size(),
                                /*partition_keys=*/{},
                                /*primary_keys=*/{}, /*options=*/{}, /*time_millis=*/0));

    ASSERT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap({}));
    FileStorePathFactoryCache cache("/test_root/", table_schema, options, GetDefaultPool());

    ASSERT_EQ(cache.format_to_path_factory_.size(), 0);

    ASSERT_OK_AND_ASSIGN(auto factory, cache.GetOrCreatePathFactory("orc"));
    ASSERT_TRUE(factory);
    ASSERT_EQ(factory->format_identifier_, "orc");
    ASSERT_EQ(cache.format_to_path_factory_.size(), 1);

    ASSERT_OK_AND_ASSIGN(factory, cache.GetOrCreatePathFactory("parquet"));
    ASSERT_TRUE(factory);
    ASSERT_EQ(factory->format_identifier_, "parquet");
    ASSERT_EQ(cache.format_to_path_factory_.size(), 2);

    ASSERT_OK_AND_ASSIGN(factory, cache.GetOrCreatePathFactory("orc"));
    ASSERT_TRUE(factory);
    ASSERT_EQ(factory->format_identifier_, "orc");
    ASSERT_EQ(cache.format_to_path_factory_.size(), 2);
}

}  // namespace paimon::test
