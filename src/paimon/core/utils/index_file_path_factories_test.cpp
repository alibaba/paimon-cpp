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

#include "paimon/core/utils/index_file_path_factories.h"

#include <string>
#include <variant>
#include <vector>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(IndexFilePathFactoriesTest, TestSimple) {
    auto pool = GetDefaultPool();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int64()), arrow::field("f3", arrow::int16())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> file_store_path_factory,
                         FileStorePathFactory::Create("/tmp", schema, {"f0", "f1"}, "default",
                                                      /*identifier=*/"mock_format",
                                                      /*data_file_prefix=*/"data-",
                                                      /*legacy_partition_name_enabled=*/true,
                                                      /*external_paths=*/{},
                                                      /*global_index_external_path=*/std::nullopt,
                                                      /*index_file_in_data_file_dir=*/false, pool));
    auto uuid = file_store_path_factory->uuid_;
    auto factories = std::make_shared<IndexFilePathFactories>(file_store_path_factory);
    ASSERT_EQ(factories->cache_.Size(), 0);
    std::shared_ptr<IndexPathFactory> cached_factory;
    {
        // partition {true, 10}, bucket 2
        auto partition = BinaryRowGenerator::GenerateRow({true, 10}, pool.get());
        ASSERT_OK_AND_ASSIGN(auto index_path_factory, factories->Get(partition, /*bucket=*/2));
        cached_factory = index_path_factory;
        ASSERT_EQ(index_path_factory->NewPath(), "/tmp/index/index-" + uuid + "-0");
        ASSERT_EQ(factories->cache_.Size(), 1);
    }
    {
        // partition {false, 20}, bucket 1, not in cache
        auto partition = BinaryRowGenerator::GenerateRow({false, 20}, pool.get());
        ASSERT_OK_AND_ASSIGN(auto index_path_factory, factories->Get(partition, /*bucket=*/1));
        // test different index path factory ptr
        ASSERT_NE(index_path_factory.get(), cached_factory.get());

        ASSERT_EQ(index_path_factory->NewPath(), "/tmp/index/index-" + uuid + "-1");
        ASSERT_EQ(factories->cache_.Size(), 2);
    }
    {
        // partition {true, 10}, bucket 2, already in cache
        auto partition = BinaryRowGenerator::GenerateRow({true, 10}, pool.get());
        ASSERT_OK_AND_ASSIGN(auto index_path_factory, factories->Get(partition, /*bucket=*/2));
        // test same index path factory ptr
        ASSERT_EQ(index_path_factory.get(), cached_factory.get());

        ASSERT_EQ(index_path_factory->NewPath(), "/tmp/index/index-" + uuid + "-2");
        ASSERT_EQ(factories->cache_.Size(), 2);
    }
}

TEST(IndexFilePathFactoriesTest, TestWithExternalPath) {
    auto pool = GetDefaultPool();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int64()), arrow::field("f3", arrow::int16())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> file_store_path_factory,
                         FileStorePathFactory::Create("/tmp", schema, {"f0", "f1"}, "default",
                                                      /*identifier=*/"mock_format",
                                                      /*data_file_prefix=*/"data-",
                                                      /*legacy_partition_name_enabled=*/true,
                                                      /*external_paths=*/{"/tmp/external-path"},
                                                      /*global_index_external_path=*/std::nullopt,
                                                      /*index_file_in_data_file_dir=*/true, pool));
    auto uuid = file_store_path_factory->uuid_;
    auto factories = std::make_shared<IndexFilePathFactories>(file_store_path_factory);
    ASSERT_EQ(factories->cache_.Size(), 0);
    std::shared_ptr<IndexPathFactory> cached_factory;
    {
        // partition {true, 10}, bucket 2
        auto partition = BinaryRowGenerator::GenerateRow({true, 10}, pool.get());
        ASSERT_OK_AND_ASSIGN(auto index_path_factory, factories->Get(partition, /*bucket=*/2));
        cached_factory = index_path_factory;
        ASSERT_EQ(index_path_factory->NewPath(),
                  "/tmp/external-path/f0=true/f1=10/bucket-2/index-" + uuid + "-0");
        ASSERT_EQ(factories->cache_.Size(), 1);
    }
    {
        // partition {false, 20}, bucket 1, not in cache
        auto partition = BinaryRowGenerator::GenerateRow({false, 20}, pool.get());
        ASSERT_OK_AND_ASSIGN(auto index_path_factory, factories->Get(partition, /*bucket=*/1));
        // test different index path factory ptr
        ASSERT_NE(index_path_factory.get(), cached_factory.get());

        ASSERT_EQ(index_path_factory->NewPath(),
                  "/tmp/external-path/f0=false/f1=20/bucket-1/index-" + uuid + "-1");
        ASSERT_EQ(factories->cache_.Size(), 2);
    }
    {
        // partition {true, 10}, bucket 2, already in cache
        auto partition = BinaryRowGenerator::GenerateRow({true, 10}, pool.get());
        ASSERT_OK_AND_ASSIGN(auto index_path_factory, factories->Get(partition, /*bucket=*/2));
        // test same index path factory ptr
        ASSERT_EQ(index_path_factory.get(), cached_factory.get());

        ASSERT_EQ(index_path_factory->NewPath(),
                  "/tmp/external-path/f0=true/f1=10/bucket-2/index-" + uuid + "-2");
        ASSERT_EQ(factories->cache_.Size(), 2);
    }
}

}  // namespace paimon::test
