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

#include "paimon/common/sst/block_cache.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/io/cache/cache_manager.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {

class BlockCacheTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create();
        fs_ = dir_->GetFileSystem();
        pool_ = GetDefaultPool();
    }

    void TearDown() override {}

    Status WriteTestFile(const std::string& path, int32_t num_blocks, int32_t block_size) const {
        PAIMON_ASSIGN_OR_RAISE(auto out, fs_->Create(path, false));
        for (int32_t i = 0; i < num_blocks; i++) {
            auto segment = MemorySegment::AllocateHeapMemory(block_size, pool_.get());
            std::memset(segment.GetHeapMemory()->data(), i & 0xFF, block_size);
            PAIMON_RETURN_NOT_OK(out->Write(segment.GetHeapMemory()->data(), block_size));
        }
        PAIMON_RETURN_NOT_OK(out->Flush());
        PAIMON_RETURN_NOT_OK(out->Close());
        return Status::OK();
    }

    Result<MemorySegment> GetBlock(int32_t block_id, int32_t block_size, BlockCache* block_cache,
                                   bool is_index = false) const {
        return block_cache->GetBlock(/*position=*/block_id * block_size, /*length=*/block_size,
                                     is_index,
                                     /*decompress_func=*/nullptr);
    }

    bool ContainsBlock(int32_t block_id, int32_t block_size, BlockCache* block_cache,
                       bool is_index = false) const {
        return block_cache->ContainsBlock(/*position=*/block_id * block_size, /*length=*/block_size,
                                          is_index);
    }

 private:
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
};

/// Verifies that the first GetBlock call reads from IO and subsequent calls return from the
/// local blocks_ cache without re-reading.
TEST_F(BlockCacheTest, TestBasicCacheHit) {
    const int32_t block_size = 64;
    const int32_t num_blocks = 4;
    auto file_path = dir_->Str() + "/basic_hit.data";
    ASSERT_OK(WriteTestFile(file_path, num_blocks, block_size));

    auto cache_manager = std::make_shared<CacheManager>(block_size * num_blocks * 2, 0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_path));
    BlockCache block_cache(file_path, in, cache_manager, pool_);

    // Initially blocks_ is empty
    ASSERT_EQ(block_cache.BlocksSize(), 0);

    // First access: populates both blocks_ and LRU
    ASSERT_OK_AND_ASSIGN(auto seg1, GetBlock(0, block_size, &block_cache));
    ASSERT_EQ(seg1.Size(), block_size);
    ASSERT_EQ(seg1.Get(0), static_cast<char>(0));
    ASSERT_EQ(block_cache.BlocksSize(), 1);
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 1);

    // Second access: returns from blocks_, no new LRU entry
    ASSERT_OK_AND_ASSIGN(auto seg2, GetBlock(0, block_size, &block_cache));
    ASSERT_EQ(seg2.Size(), block_size);
    ASSERT_EQ(block_cache.BlocksSize(), 1);
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 1);

    // Load a different block
    ASSERT_OK_AND_ASSIGN(auto seg3, GetBlock(1, block_size, &block_cache));
    ASSERT_EQ(seg3.Get(0), static_cast<char>(1));
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache));
    ASSERT_TRUE(ContainsBlock(1, block_size, &block_cache));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);
}

/// Verifies that when LRU evicts an entry due to capacity pressure, the eviction callback
/// removes the corresponding entry from BlockCache's blocks_ map.
TEST_F(BlockCacheTest, TestLruEvictionSyncsWithBlocks) {
    const int32_t block_size = 100;
    auto file_path = dir_->Str() + "/eviction.data";
    ASSERT_OK(WriteTestFile(file_path, 5, block_size));

    // Cache can hold at most 2 blocks (200 bytes)
    auto cache_manager = std::make_shared<CacheManager>(block_size * 2, 0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_path));
    BlockCache block_cache(file_path, in, cache_manager, pool_);

    // Load block 0 at position 0
    ASSERT_OK_AND_ASSIGN(auto seg0, GetBlock(0, block_size, &block_cache));
    ASSERT_EQ(seg0.Get(0), static_cast<char>(0));
    ASSERT_EQ(block_cache.BlocksSize(), 1);
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache));

    // Load block 1 at position block_size
    ASSERT_OK_AND_ASSIGN(auto seg1, GetBlock(1, block_size, &block_cache));
    ASSERT_EQ(seg1.Get(0), static_cast<char>(1));
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache));
    ASSERT_TRUE(ContainsBlock(1, block_size, &block_cache));

    // Load block 2: evicts block 0 (LRU) from both LRU and blocks_
    ASSERT_OK_AND_ASSIGN(auto seg2, GetBlock(2, block_size, &block_cache));
    ASSERT_EQ(seg2.Get(0), static_cast<char>(2));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    // block 0 should be evicted from blocks_
    ASSERT_FALSE(ContainsBlock(0, block_size, &block_cache));
    // block 1 and block 2 should remain
    ASSERT_TRUE(ContainsBlock(1, block_size, &block_cache));
    ASSERT_TRUE(ContainsBlock(2, block_size, &block_cache));

    // Re-access block 0: triggers fresh IO read, evicts block 1 (now LRU)
    ASSERT_OK_AND_ASSIGN(auto seg0_reloaded, GetBlock(0, block_size, &block_cache));
    ASSERT_EQ(seg0_reloaded.Get(0), static_cast<char>(0));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    // block 1 should now be evicted
    ASSERT_FALSE(ContainsBlock(1, block_size, &block_cache));
    // block 0 and block 2 should remain
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache));
    ASSERT_TRUE(ContainsBlock(2, block_size, &block_cache));
}

/// Verifies that Close() invalidates all entries from both blocks_ and the LRU cache.
TEST_F(BlockCacheTest, TestClose) {
    const int32_t block_size = 64;
    auto file_path = dir_->Str() + "/close.data";
    ASSERT_OK(WriteTestFile(file_path, 3, block_size));

    auto cache_manager = std::make_shared<CacheManager>(block_size * 10, 0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_path));
    BlockCache block_cache(file_path, in, cache_manager, pool_);

    // Load 3 blocks and verify blocks_ keys
    for (int32_t i = 0; i < 3; i++) {
        ASSERT_OK_AND_ASSIGN(auto seg, GetBlock(i, block_size, &block_cache));
        ASSERT_EQ(seg.Get(0), static_cast<char>(i));
    }
    ASSERT_EQ(block_cache.BlocksSize(), 3);
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache));
    ASSERT_TRUE(ContainsBlock(1, block_size, &block_cache));
    ASSERT_TRUE(ContainsBlock(2, block_size, &block_cache));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 3);

    block_cache.Close();

    // After Close, both blocks_ and LRU should be empty
    ASSERT_EQ(block_cache.BlocksSize(), 0);
    ASSERT_FALSE(ContainsBlock(0, block_size, &block_cache));
    ASSERT_FALSE(ContainsBlock(1, block_size, &block_cache));
    ASSERT_FALSE(ContainsBlock(2, block_size, &block_cache));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 0);
}

/// Verifies that two BlockCache instances sharing the same CacheManager have independent blocks_
/// maps, but eviction in the shared LRU only affects the owning BlockCache's blocks_.
TEST_F(BlockCacheTest, TestSharedCacheManagerEvictionIsolation) {
    const int32_t block_size = 100;
    auto file_path_a = dir_->Str() + "/file_a.data";
    auto file_path_b = dir_->Str() + "/file_b.data";
    ASSERT_OK(WriteTestFile(file_path_a, 3, block_size));
    ASSERT_OK(WriteTestFile(file_path_b, 3, block_size));

    // Shared cache can hold 3 blocks total
    auto cache_manager = std::make_shared<CacheManager>(block_size * 3, 0.0);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in_a, fs_->Open(file_path_a));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in_b, fs_->Open(file_path_b));
    BlockCache cache_a(file_path_a, in_a, cache_manager, pool_);
    BlockCache cache_b(file_path_b, in_b, cache_manager, pool_);

    // Load 2 blocks from file_a
    ASSERT_OK_AND_ASSIGN(auto seg_a0, GetBlock(0, block_size, &cache_a));
    ASSERT_OK_AND_ASSIGN(auto seg_a1, GetBlock(1, block_size, &cache_a));
    ASSERT_EQ(cache_a.BlocksSize(), 2);
    ASSERT_TRUE(ContainsBlock(0, block_size, &cache_a));
    ASSERT_TRUE(ContainsBlock(1, block_size, &cache_a));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);

    // Load 1 block from file_b (total 3, at capacity)
    ASSERT_OK_AND_ASSIGN(auto seg_b0, GetBlock(0, block_size, &cache_b));
    ASSERT_EQ(cache_b.BlocksSize(), 1);
    ASSERT_TRUE(ContainsBlock(0, block_size, &cache_b));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 3);

    // Load another block from file_b: should evict file_a's block 0 (the LRU entry)
    ASSERT_OK_AND_ASSIGN(auto seg_b1, GetBlock(1, block_size, &cache_b));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 3);

    // cache_b should have 2 entries
    ASSERT_EQ(cache_b.BlocksSize(), 2);
    ASSERT_TRUE(ContainsBlock(0, block_size, &cache_b));
    ASSERT_TRUE(ContainsBlock(1, block_size, &cache_b));

    // cache_a's block 0 was evicted by LRU callback, only block 1 remains
    ASSERT_EQ(cache_a.BlocksSize(), 1);
    ASSERT_FALSE(ContainsBlock(0, block_size, &cache_a));
    ASSERT_TRUE(ContainsBlock(1, block_size, &cache_a));

    // Re-access file_a's block 0: triggers fresh IO read, evicts file_a's block 1 (now LRU)
    ASSERT_OK_AND_ASSIGN(auto seg_a0_reloaded, GetBlock(0, block_size, &cache_a));
    ASSERT_EQ(seg_a0_reloaded.Get(0), static_cast<char>(0));
    ASSERT_EQ(cache_a.BlocksSize(), 1);
    ASSERT_TRUE(ContainsBlock(0, block_size, &cache_a));
    ASSERT_FALSE(ContainsBlock(1, block_size, &cache_a));

    // cache_b should be unaffected
    ASSERT_EQ(cache_b.BlocksSize(), 2);
    ASSERT_TRUE(ContainsBlock(0, block_size, &cache_b));
    ASSERT_TRUE(ContainsBlock(1, block_size, &cache_b));

    cache_a.Close();
    cache_b.Close();
}

/// Verifies the REFRESH_COUNT mechanism interacts correctly with LRU eviction ordering.
/// After refreshing a block (re-inserting into LRU front), it should not be the first to be
/// evicted when capacity pressure occurs.
TEST_F(BlockCacheTest, TestRefreshPreventsEviction) {
    const int32_t block_size = 100;
    auto file_path = dir_->Str() + "/refresh_eviction.data";
    ASSERT_OK(WriteTestFile(file_path, 4, block_size));

    // Cache can hold 2 blocks
    auto cache_manager = std::make_shared<CacheManager>(block_size * 2, 0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_path));
    BlockCache block_cache(file_path, in, cache_manager, pool_);

    // Load block 0 and block 1
    ASSERT_OK_AND_ASSIGN(auto seg0, GetBlock(0, block_size, &block_cache));
    ASSERT_OK_AND_ASSIGN(auto seg1, GetBlock(1, block_size, &block_cache));
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache));
    ASSERT_TRUE(ContainsBlock(1, block_size, &block_cache));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);

    // Access block 0 REFRESH_COUNT times to trigger a refresh (moves it to LRU front)
    for (int32_t i = 1; i < CacheManager::REFRESH_COUNT; i++) {
        ASSERT_OK_AND_ASSIGN(seg0, GetBlock(0, block_size, &block_cache));
    }
    // This 11th access triggers refresh, moving block 0 to LRU front
    ASSERT_OK_AND_ASSIGN(seg0, GetBlock(0, block_size, &block_cache));

    // blocks_ should still have both entries after refresh
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache));
    ASSERT_TRUE(ContainsBlock(1, block_size, &block_cache));

    // Load block 2: should evict block 1 (not block 0, since block 0 was just refreshed)
    ASSERT_OK_AND_ASSIGN(auto seg2, GetBlock(2, block_size, &block_cache));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    // block 0 should still be in blocks_ (was refreshed to LRU front)
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache));
    // block 1 should be evicted from blocks_
    ASSERT_FALSE(ContainsBlock(1, block_size, &block_cache));
    // block 2 should be in blocks_
    ASSERT_TRUE(ContainsBlock(2, block_size, &block_cache));

    // Block 0 should still be accessible from blocks_ cache
    ASSERT_OK_AND_ASSIGN(seg0, GetBlock(0, block_size, &block_cache));
    ASSERT_EQ(seg0.Get(0), static_cast<char>(0));

    // Block 1 was evicted, re-accessing triggers IO read
    ASSERT_OK_AND_ASSIGN(seg1, GetBlock(1, block_size, &block_cache));
    ASSERT_EQ(seg1.Get(0), static_cast<char>(1));
}

TEST_F(BlockCacheTest, TestIndexAndDataCache) {
    const int32_t block_size = 100;
    const int32_t num_blocks = 6;
    auto file_path = dir_->Str() + "/index_and_data.data";
    ASSERT_OK(WriteTestFile(file_path, num_blocks, block_size));

    // index cache max weight = 100
    // data cache max weight = 300
    auto cache_manager = std::make_shared<CacheManager>(block_size * 4, 0.25);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_path));
    BlockCache block_cache(file_path, in, cache_manager, pool_);

    // First access for seg0, index
    ASSERT_OK_AND_ASSIGN(auto seg0, GetBlock(0, block_size, &block_cache, /*is_index=*/true));
    ASSERT_EQ(seg0.Get(0), static_cast<char>(0));
    ASSERT_EQ(block_cache.BlocksSize(), 1);
    ASSERT_TRUE(ContainsBlock(0, block_size, &block_cache, /*is_index=*/true));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 0);
    ASSERT_EQ(cache_manager->IndexCache()->Size(), 1);

    // Second access for seg1, index, seg0 will be evicted
    ASSERT_OK_AND_ASSIGN(auto seg1, GetBlock(1, block_size, &block_cache, /*is_index=*/true));
    ASSERT_EQ(seg1.Get(0), static_cast<char>(1));
    ASSERT_EQ(block_cache.BlocksSize(), 1);
    ASSERT_TRUE(ContainsBlock(1, block_size, &block_cache, /*is_index=*/true));
    ASSERT_FALSE(ContainsBlock(0, block_size, &block_cache, /*is_index=*/true));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 0);
    ASSERT_EQ(cache_manager->IndexCache()->Size(), 1);

    // Fills data cache
    for (int32_t i = 2; i < 5; i++) {
        ASSERT_OK_AND_ASSIGN(auto seg, GetBlock(i, block_size, &block_cache, /*is_index=*/false));
        ASSERT_EQ(seg.Get(0), static_cast<char>(i));
        ASSERT_EQ(block_cache.BlocksSize(), 1 + i - 1);
        ASSERT_TRUE(ContainsBlock(i, block_size, &block_cache, /*is_index=*/false));
        ASSERT_EQ(cache_manager->IndexCache()->Size(), 1);
        ASSERT_EQ(cache_manager->DataCache()->Size(), i - 1);
    }

    ASSERT_OK_AND_ASSIGN(auto seg5, GetBlock(5, block_size, &block_cache, /*is_index=*/false));
    ASSERT_EQ(seg5.Get(0), static_cast<char>(5));
    ASSERT_EQ(block_cache.BlocksSize(), 4);
    ASSERT_TRUE(ContainsBlock(5, block_size, &block_cache, /*is_index=*/false));
    ASSERT_TRUE(ContainsBlock(4, block_size, &block_cache, /*is_index=*/false));
    ASSERT_TRUE(ContainsBlock(3, block_size, &block_cache, /*is_index=*/false));
    ASSERT_FALSE(ContainsBlock(2, block_size, &block_cache, /*is_index=*/false));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 3);
    ASSERT_EQ(cache_manager->IndexCache()->Size(), 1);
}

}  // namespace paimon::test
