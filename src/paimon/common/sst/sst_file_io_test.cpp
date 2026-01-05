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

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/sst/sst_file_reader.h"
#include "paimon/common/sst/sst_file_writer.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::test {
class SstFileIOTest : public ::testing::Test {
 public:
    void SetUp() override {
        fields_ = {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int64()),
                   arrow::field("f2", arrow::boolean())};
        data_type_ = arrow::struct_(fields_);
    }

    void TearDown() override {}

 private:
    arrow::FieldVector fields_;
    std::shared_ptr<arrow::DataType> data_type_;
};

TEST_F(SstFileIOTest, TestSimple) {
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    auto fs = dir->GetFileSystem();
    std::string index_path = dir->Str() + "/sst_file_test.data";
    // write content
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs->Create(index_path, /*overwrite=*/false));
    auto pool = GetDefaultPool();

    auto writer = std::make_shared<SstFileWriter>(out, 10, pool);
    writer->Write(std::make_shared<Bytes>("k1", pool), std::make_shared<Bytes>("1", pool));
    writer->Write(std::make_shared<Bytes>("k2", pool), std::make_shared<Bytes>("2", pool));
    writer->Write(std::make_shared<Bytes>("k3", pool), std::make_shared<Bytes>("3", pool));
    writer->Write(std::make_shared<Bytes>("k4", pool), std::make_shared<Bytes>("4", pool));
    writer->Write(std::make_shared<Bytes>("k5", pool), std::make_shared<Bytes>("5", pool));
    auto bloom_filter_handle = writer->WriteBloomFilter();
    ASSERT_OK(bloom_filter_handle);
    auto index_block_handle = writer->WriteIndexBlock();
    ASSERT_OK(index_block_handle);
    ASSERT_OK(writer->Flush());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(index_path));
    auto cache_manager = std::make_unique<CacheManager>();
    auto block_cache = std::make_unique<BlockCache>(index_path, in, pool, cache_manager);
    auto comparator = [](const std::string& a, const std::string& b) -> int32_t {
        if (a == b) {
            return 0;
        }
        return a > b ? 1 : -1;
    };

    auto reader = std::make_shared<SstFileReader>(pool, block_cache, index_block_handle.value(),
                                                  nullptr, comparator);
    auto k1 = std::make_shared<Bytes>("k4", pool);
    auto v1 = reader->Lookup(k1);
    ASSERT_EQ(v1, std::make_shared<Bytes>("4", pool));

    auto k2 = std::make_shared<Bytes>("k44", pool);
    auto v2 = reader->Lookup(k1);
    ASSERT_EQ(v1.get(), nullptr);

    fs->Delete(index_path);
}

}  // namespace paimon::test
