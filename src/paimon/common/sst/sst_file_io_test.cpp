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
        dir_ = paimon::test::UniqueTestDirectory::Create();
        fs_ = dir_->GetFileSystem();
        index_path_ = dir_->Str() + "/sst_file_test.data";
        pool_ = GetDefaultPool();
    }

    void TearDown() override {
        fs_->Delete(dir_->Str());
    }

 protected:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<paimon::FileSystem> fs_;
    std::string index_path_;
    std::shared_ptr<paimon::MemoryPool> pool_;
};

TEST_F(SstFileIOTest, TestSimple) {
    // write content
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(index_path_, /*overwrite=*/false));

    auto writer = std::make_shared<SstFileWriter>(out, 50, pool_);
    for (size_t i = 1; i <= 5; i++) {
        std::string key = "k" + std::to_string(i);
        std::string value = std::to_string(i);
        writer->Write(std::make_shared<Bytes>(key, pool_.get()),
                      std::make_shared<Bytes>(value, pool_.get()));
    }
    for (size_t i = 10; i <= 20; i++) {
        std::string key = "k9" + std::to_string(i);
        std::string value = "looooooooooong-值-" + std::to_string(i);
        writer->Write(std::make_shared<Bytes>(key, pool_.get()),
                      std::make_shared<Bytes>(value, pool_.get()));
    }
    ASSERT_OK(writer->Flush());

    ASSERT_EQ(6, writer->IndexWriter()->Size());

    auto bloom_filter_handle = writer->WriteBloomFilter();
    ASSERT_OK(bloom_filter_handle);
    auto index_block_handle = writer->WriteIndexBlock();
    ASSERT_OK(index_block_handle);

    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(index_path_));
    auto cache_manager = std::make_unique<CacheManager>();
    auto block_cache = std::make_unique<BlockCache>(index_path_, in, pool_, cache_manager);
    auto comparator = [](const std::shared_ptr<MemorySlice>& a,
                         const std::shared_ptr<MemorySlice>& b) -> int32_t {
        std::string_view va = a->ReadStringView();
        std::string_view vb = b->ReadStringView();
        if (va == vb) {
            return 0;
        }
        return va > vb ? 1 : -1;
    };

    auto reader = std::make_shared<SstFileReader>(pool_, std::move(block_cache),
                                                  index_block_handle.value(), nullptr, comparator);
    std::string k0 = "k0";
    ASSERT_EQ(nullptr, reader->Lookup(std::make_shared<Bytes>(k0, pool_.get())));

    std::string k4 = "k4";
    auto v4 = reader->Lookup(std::make_shared<Bytes>(k4, pool_.get()));
    ASSERT_TRUE(v4);
    std::string string4{v4->data(), v4->size()};
    ASSERT_EQ("4", string4);

    std::string k55 = "k55";
    ASSERT_EQ(nullptr, reader->Lookup(std::make_shared<Bytes>(k55, pool_.get())));

    std::string k915 = "k915";
    auto v15 = reader->Lookup(std::make_shared<Bytes>(k915, pool_.get()));
    ASSERT_TRUE(v15);
    std::string string15{v15->data(), v15->size()};
    ASSERT_EQ("looooooooooong-值-15", string15);
}

}  // namespace paimon::test
