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
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/lookup/sort/sort_lookup_store_footer.h"
#include "paimon/common/sst/sst_file_reader.h"
#include "paimon/common/sst/sst_file_writer.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::test {
struct SstFileParam {
    std::string file_path;
    BlockCompressionType type;
};

class SstFileIOTest : public ::testing::TestWithParam<SstFileParam> {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        fs_ = dir_->GetFileSystem();
        pool_ = GetDefaultPool();
        comparator_ = [](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
            std::string_view va = a.ReadStringView();
            std::string_view vb = b.ReadStringView();
            if (va == vb) {
                return 0;
            }
            return va > vb ? 1 : -1;
        };
    }

    void TearDown() override {
        ASSERT_OK(fs_->Delete(dir_->Str()));
    }

 protected:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<paimon::FileSystem> fs_;
    std::shared_ptr<paimon::MemoryPool> pool_;

    MemorySlice::SliceComparator comparator_;
    std::shared_ptr<CacheManager> cache_manager_ = std::make_shared<CacheManager>(1024 * 1024, 0.0);
};

TEST_P(SstFileIOTest, TestSimple) {
    auto param = GetParam();
    auto index_path = dir_->Str() + "/sst_file_test.data";

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlockCompressionFactory> factory,
                         BlockCompressionFactory::Create(param.type));

    // write content
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(index_path, /*overwrite=*/false));

    // write data
    auto bf = BloomFilter::Create(30, 0.01);
    auto seg_for_bf = MemorySegment::AllocateHeapMemory(bf->ByteLength(), pool_.get());
    ASSERT_OK(bf->SetMemorySegment(seg_for_bf));
    auto writer = std::make_shared<SstFileWriter>(out, bf, 50, factory, pool_);
    std::set<int32_t> value_hash;
    // k1-k5
    for (size_t i = 1; i <= 5; i++) {
        std::string key = "k" + std::to_string(i);
        std::string value = std::to_string(i);
        ASSERT_OK(writer->Write(std::make_shared<Bytes>(key, pool_.get()),
                                std::make_shared<Bytes>(value, pool_.get())));
        auto bytes = std::make_shared<Bytes>(key, pool_.get());
        value_hash.insert(MurmurHashUtils::HashBytes(bytes));
    }
    // k910-k920
    for (size_t i = 10; i <= 20; i++) {
        std::string key = "k9" + std::to_string(i);
        std::string value = "looooooooooong-值-" + std::to_string(i);
        ASSERT_OK(writer->Write(std::make_shared<Bytes>(key, pool_.get()),
                                std::make_shared<Bytes>(value, pool_.get())));
        auto bytes = std::make_shared<Bytes>(key, pool_.get());
        value_hash.insert(MurmurHashUtils::HashBytes(bytes));
    }
    ASSERT_OK(writer->Flush());

    ASSERT_EQ(6, writer->IndexWriter()->Size());

    ASSERT_OK_AND_ASSIGN(auto bloom_filter_handle, writer->WriteBloomFilter());
    ASSERT_OK_AND_ASSIGN(auto index_block_handle, writer->WriteIndexBlock());
    SortLookupStoreFooter footer(index_block_handle, bloom_filter_handle);
    auto slice = footer.WriteSortLookupStoreFooter(pool_.get());
    ASSERT_OK(writer->WriteSlice(slice));

    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    // bloom filter test
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(index_path));
    auto entries = bloom_filter_handle->ExpectedEntries();
    auto offset = bloom_filter_handle->Offset();
    auto size = bloom_filter_handle->Size();
    ASSERT_OK(in->Seek(offset, SeekOrigin::FS_SEEK_SET));
    auto bloom_filer_bytes = Bytes::AllocateBytes(size, pool_.get());
    ASSERT_OK(in->Read(bloom_filer_bytes->data(), bloom_filer_bytes->size()));
    auto seg = MemorySegment::Wrap(std::move(bloom_filer_bytes));
    auto bloom_filter = std::make_shared<BloomFilter>(entries, size);
    ASSERT_OK(bloom_filter->SetMemorySegment(seg));
    for (const auto& value : value_hash) {
        ASSERT_TRUE(bloom_filter->TestHash(value));
    }

    // test read
    ASSERT_OK_AND_ASSIGN(in, fs_->Open(index_path));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         SstFileReader::CreateFromStream(in, comparator_, cache_manager_, pool_));

    // not exist key
    std::string k0 = "k0";
    ASSERT_FALSE(reader->Lookup(std::make_shared<Bytes>(k0, pool_.get())).value());

    // k4
    std::string k4 = "k4";
    ASSERT_OK_AND_ASSIGN(auto v4, reader->Lookup(std::make_shared<Bytes>(k4, pool_.get())));
    ASSERT_TRUE(v4);
    std::string string4{v4->data(), v4->size()};
    ASSERT_EQ("4", string4);

    // not exist key
    std::string k55 = "k55";
    ASSERT_FALSE(reader->Lookup(std::make_shared<Bytes>(k55, pool_.get())).value());

    // k915
    std::string k915 = "k915";
    ASSERT_OK_AND_ASSIGN(auto v15, reader->Lookup(std::make_shared<Bytes>(k915, pool_.get())));
    ASSERT_TRUE(v15);
    std::string string15{v15->data(), v15->size()};
    ASSERT_EQ("looooooooooong-值-15", string15);
}

TEST_P(SstFileIOTest, TestJavaCompatibility) {
    auto param = GetParam();

    // key range [1_000_000, 2_000_000], value is equal to the key
    std::string file = GetDataDir() + "/sst/" + param.file_path;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file));

    // test read
    ASSERT_OK_AND_ASSIGN(auto reader,
                         SstFileReader::CreateFromStream(in, comparator_, cache_manager_, pool_));
    // not exist key
    std::string k0 = "10000";
    ASSERT_FALSE(reader->Lookup(std::make_shared<Bytes>(k0, pool_.get())).value());

    // k1314520
    std::string k1314520 = "1314520";
    ASSERT_OK_AND_ASSIGN(auto v1314520,
                         reader->Lookup(std::make_shared<Bytes>(k1314520, pool_.get())));
    ASSERT_TRUE(v1314520);
    std::string string1314520{v1314520->data(), v1314520->size()};
    ASSERT_EQ("1314520", string1314520);

    // not exist key
    std::string k13145200 = "13145200";
    ASSERT_FALSE(reader->Lookup(std::make_shared<Bytes>(k13145200, pool_.get())).value());

    std::string k1314521 = "1314521";
    ASSERT_OK_AND_ASSIGN(auto v1314521,
                         reader->Lookup(std::make_shared<Bytes>(k1314521, pool_.get())));
    ASSERT_TRUE(v1314521);
    std::string string1314521{v1314521->data(), v1314521->size()};
    ASSERT_EQ("1314521", string1314521);

    std::string k1999999 = "1999999";
    ASSERT_OK_AND_ASSIGN(auto v1999999,
                         reader->Lookup(std::make_shared<Bytes>(k1999999, pool_.get())));
    ASSERT_TRUE(v1999999);
    std::string string1999999{v1999999->data(), v1999999->size()};
    ASSERT_EQ("1999999", string1999999);
}

TEST_F(SstFileIOTest, TestIOException) {
    bool run_complete = false;
    auto io_hook = paimon::IOHook::GetInstance();
    for (size_t i = 0; i < 200; i++) {
        auto test_dir = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(test_dir);
        paimon::ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, paimon::IOHook::Mode::RETURN_ERROR);

        auto index_path = test_dir->Str() + "/sst_io_exception_test.data";

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlockCompressionFactory> factory,
                             BlockCompressionFactory::Create(BlockCompressionType::ZSTD));

        // write
        auto out_result = fs_->Create(index_path, /*overwrite=*/false);
        CHECK_HOOK_STATUS(out_result.status(), i);
        std::shared_ptr<OutputStream> out = std::move(out_result).value();

        auto bf = BloomFilter::Create(30, 0.01);
        MemorySegment seg_for_bf = MemorySegment::AllocateHeapMemory(bf->ByteLength(), pool_.get());
        ASSERT_OK(bf->SetMemorySegment(seg_for_bf));
        auto writer = std::make_shared<SstFileWriter>(out, bf, 50, factory, pool_);

        bool write_failed = false;
        for (size_t j = 1; j <= 5; j++) {
            std::string key = "k" + std::to_string(j);
            std::string value = std::to_string(j);
            auto write_status = writer->Write(std::make_shared<Bytes>(key, pool_.get()),
                                              std::make_shared<Bytes>(value, pool_.get()));
            if (!write_status.ok()) {
                CHECK_HOOK_STATUS(write_status, i);
                write_failed = true;
                break;
            }
        }
        if (write_failed) {
            continue;
        }

        CHECK_HOOK_STATUS(writer->Flush(), i);

        auto bloom_filter_handle_result = writer->WriteBloomFilter();
        CHECK_HOOK_STATUS(bloom_filter_handle_result.status(), i);
        auto index_block_handle_result = writer->WriteIndexBlock();
        CHECK_HOOK_STATUS(index_block_handle_result.status(), i);
        SortLookupStoreFooter test_footer(index_block_handle_result.value(),
                                          bloom_filter_handle_result.value());
        auto test_slice = test_footer.WriteSortLookupStoreFooter(pool_.get());
        CHECK_HOOK_STATUS(writer->WriteSlice(test_slice), i);

        CHECK_HOOK_STATUS(out->Flush(), i);
        CHECK_HOOK_STATUS(out->Close(), i);

        // read
        auto in_result = fs_->Open(index_path);
        CHECK_HOOK_STATUS(in_result.status(), i);
        std::shared_ptr<InputStream> in = std::move(in_result).value();

        auto reader_result =
            SstFileReader::CreateFromStream(in, comparator_, cache_manager_, pool_);
        CHECK_HOOK_STATUS(reader_result.status(), i);
        std::shared_ptr<SstFileReader> reader = std::move(reader_result).value();

        std::string k4 = "k4";
        auto v4_result = reader->Lookup(std::make_shared<Bytes>(k4, pool_.get()));
        CHECK_HOOK_STATUS(v4_result.status(), i);
        ASSERT_TRUE(v4_result.value());
        std::string string4{v4_result.value()->data(), v4_result.value()->size()};
        ASSERT_EQ("4", string4);

        ASSERT_OK(reader->Close());
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

INSTANTIATE_TEST_SUITE_P(Group, SstFileIOTest,
                         ::testing::Values(SstFileParam{"none/79d01717-8380-4504-86e1-387e6c058d0a",
                                                        BlockCompressionType::NONE},
                                           SstFileParam{"zstd/83d05c53-2353-4160-b756-d50dd851b474",
                                                        BlockCompressionType::ZSTD},
                                           SstFileParam{"lz4/10540951-41d3-4216-aa2c-b15dfd25eb75",
                                                        BlockCompressionType::LZ4}));

}  // namespace paimon::test
