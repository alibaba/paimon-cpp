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
    ASSERT_OK_AND_ASSIGN(auto seg1, block_cache.GetBlock(0, block_size, false));
    ASSERT_EQ(seg1.Size(), block_size);
    ASSERT_EQ(seg1.Get(0), static_cast<char>(0));
    ASSERT_EQ(block_cache.BlocksSize(), 1);
    ASSERT_TRUE(block_cache.ContainsBlock(0, block_size, false));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 1);

    // Second access: returns from blocks_, no new LRU entry
    ASSERT_OK_AND_ASSIGN(auto seg2, block_cache.GetBlock(0, block_size, false));
    ASSERT_EQ(seg2.Size(), block_size);
    ASSERT_EQ(block_cache.BlocksSize(), 1);
    ASSERT_TRUE(block_cache.ContainsBlock(0, block_size, false));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 1);

    // Load a different block
    ASSERT_OK_AND_ASSIGN(auto seg3, block_cache.GetBlock(block_size, block_size, false));
    ASSERT_EQ(seg3.Get(0), static_cast<char>(1));
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    ASSERT_TRUE(block_cache.ContainsBlock(0, block_size, false));
    ASSERT_TRUE(block_cache.ContainsBlock(block_size, block_size, false));
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
    ASSERT_OK_AND_ASSIGN(auto seg0, block_cache.GetBlock(0, block_size, false));
    ASSERT_EQ(seg0.Get(0), static_cast<char>(0));
    ASSERT_EQ(block_cache.BlocksSize(), 1);
    ASSERT_TRUE(block_cache.ContainsBlock(0, block_size, false));

    // Load block 1 at position block_size
    ASSERT_OK_AND_ASSIGN(auto seg1, block_cache.GetBlock(block_size, block_size, false));
    ASSERT_EQ(seg1.Get(0), static_cast<char>(1));
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    ASSERT_TRUE(block_cache.ContainsBlock(0, block_size, false));
    ASSERT_TRUE(block_cache.ContainsBlock(block_size, block_size, false));

    // Load block 2: evicts block 0 (LRU) from both LRU and blocks_
    ASSERT_OK_AND_ASSIGN(auto seg2, block_cache.GetBlock(block_size * 2, block_size, false));
    ASSERT_EQ(seg2.Get(0), static_cast<char>(2));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    // block 0 should be evicted from blocks_
    ASSERT_FALSE(block_cache.ContainsBlock(0, block_size, false));
    // block 1 and block 2 should remain
    ASSERT_TRUE(block_cache.ContainsBlock(block_size, block_size, false));
    ASSERT_TRUE(block_cache.ContainsBlock(block_size * 2, block_size, false));

    // Re-access block 0: triggers fresh IO read, evicts block 1 (now LRU)
    ASSERT_OK_AND_ASSIGN(auto seg0_reloaded, block_cache.GetBlock(0, block_size, false));
    ASSERT_EQ(seg0_reloaded.Get(0), static_cast<char>(0));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    // block 1 should now be evicted
    ASSERT_FALSE(block_cache.ContainsBlock(block_size, block_size, false));
    // block 0 and block 2 should remain
    ASSERT_TRUE(block_cache.ContainsBlock(0, block_size, false));
    ASSERT_TRUE(block_cache.ContainsBlock(block_size * 2, block_size, false));
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
    for (int i = 0; i < 3; i++) {
        ASSERT_OK_AND_ASSIGN(auto seg, block_cache.GetBlock(i * block_size, block_size, false));
        ASSERT_EQ(seg.Get(0), static_cast<char>(i));
    }
    ASSERT_EQ(block_cache.BlocksSize(), 3);
    ASSERT_TRUE(block_cache.ContainsBlock(0, block_size, false));
    ASSERT_TRUE(block_cache.ContainsBlock(block_size, block_size, false));
    ASSERT_TRUE(block_cache.ContainsBlock(block_size * 2, block_size, false));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 3);

    block_cache.Close();

    // After Close, both blocks_ and LRU should be empty
    ASSERT_EQ(block_cache.BlocksSize(), 0);
    ASSERT_FALSE(block_cache.ContainsBlock(0, block_size, false));
    ASSERT_FALSE(block_cache.ContainsBlock(block_size, block_size, false));
    ASSERT_FALSE(block_cache.ContainsBlock(block_size * 2, block_size, false));
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
    ASSERT_OK_AND_ASSIGN(auto seg_a0, cache_a.GetBlock(0, block_size, false));
    ASSERT_OK_AND_ASSIGN(auto seg_a1, cache_a.GetBlock(block_size, block_size, false));
    ASSERT_EQ(cache_a.BlocksSize(), 2);
    ASSERT_TRUE(cache_a.ContainsBlock(0, block_size, false));
    ASSERT_TRUE(cache_a.ContainsBlock(block_size, block_size, false));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);

    // Load 1 block from file_b (total 3, at capacity)
    ASSERT_OK_AND_ASSIGN(auto seg_b0, cache_b.GetBlock(0, block_size, false));
    ASSERT_EQ(cache_b.BlocksSize(), 1);
    ASSERT_TRUE(cache_b.ContainsBlock(0, block_size, false));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 3);

    // Load another block from file_b: should evict file_a's block 0 (the LRU entry)
    ASSERT_OK_AND_ASSIGN(auto seg_b1, cache_b.GetBlock(block_size, block_size, false));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 3);

    // cache_b should have 2 entries
    ASSERT_EQ(cache_b.BlocksSize(), 2);
    ASSERT_TRUE(cache_b.ContainsBlock(0, block_size, false));
    ASSERT_TRUE(cache_b.ContainsBlock(block_size, block_size, false));

    // cache_a's block 0 was evicted by LRU callback, only block 1 remains
    ASSERT_EQ(cache_a.BlocksSize(), 1);
    ASSERT_FALSE(cache_a.ContainsBlock(0, block_size, false));
    ASSERT_TRUE(cache_a.ContainsBlock(block_size, block_size, false));

    // Re-access file_a's block 0: triggers fresh IO read, evicts file_a's block 1 (now LRU)
    ASSERT_OK_AND_ASSIGN(auto seg_a0_reloaded, cache_a.GetBlock(0, block_size, false));
    ASSERT_EQ(seg_a0_reloaded.Get(0), static_cast<char>(0));
    ASSERT_EQ(cache_a.BlocksSize(), 1);
    ASSERT_TRUE(cache_a.ContainsBlock(0, block_size, false));
    ASSERT_FALSE(cache_a.ContainsBlock(block_size, block_size, false));

    // cache_b should be unaffected
    ASSERT_EQ(cache_b.BlocksSize(), 2);
    ASSERT_TRUE(cache_b.ContainsBlock(0, block_size, false));
    ASSERT_TRUE(cache_b.ContainsBlock(block_size, block_size, false));

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
    ASSERT_OK_AND_ASSIGN(auto seg0, block_cache.GetBlock(0, block_size, false));
    ASSERT_OK_AND_ASSIGN(auto seg1, block_cache.GetBlock(block_size, block_size, false));
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    ASSERT_TRUE(block_cache.ContainsBlock(0, block_size, false));
    ASSERT_TRUE(block_cache.ContainsBlock(block_size, block_size, false));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);

    // Access block 0 REFRESH_COUNT times to trigger a refresh (moves it to LRU front)
    for (int i = 1; i < CacheManager::REFRESH_COUNT; i++) {
        ASSERT_OK_AND_ASSIGN(seg0, block_cache.GetBlock(0, block_size, false));
    }
    // This 11th access triggers refresh, moving block 0 to LRU front
    ASSERT_OK_AND_ASSIGN(seg0, block_cache.GetBlock(0, block_size, false));

    // blocks_ should still have both entries after refresh
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    ASSERT_TRUE(block_cache.ContainsBlock(0, block_size, false));
    ASSERT_TRUE(block_cache.ContainsBlock(block_size, block_size, false));

    // Load block 2: should evict block 1 (not block 0, since block 0 was just refreshed)
    ASSERT_OK_AND_ASSIGN(auto seg2, block_cache.GetBlock(block_size * 2, block_size, false));
    ASSERT_EQ(cache_manager->DataCache()->Size(), 2);
    ASSERT_EQ(block_cache.BlocksSize(), 2);
    // block 0 should still be in blocks_ (was refreshed to LRU front)
    ASSERT_TRUE(block_cache.ContainsBlock(0, block_size, false));
    // block 1 should be evicted from blocks_
    ASSERT_FALSE(block_cache.ContainsBlock(block_size, block_size, false));
    // block 2 should be in blocks_
    ASSERT_TRUE(block_cache.ContainsBlock(block_size * 2, block_size, false));

    // Block 0 should still be accessible from blocks_ cache
    ASSERT_OK_AND_ASSIGN(seg0, block_cache.GetBlock(0, block_size, false));
    ASSERT_EQ(seg0.Get(0), static_cast<char>(0));

    // Block 1 was evicted, re-accessing triggers IO read
    ASSERT_OK_AND_ASSIGN(seg1, block_cache.GetBlock(block_size, block_size, false));
    ASSERT_EQ(seg1.Get(0), static_cast<char>(1));
}

}  // namespace paimon::test
