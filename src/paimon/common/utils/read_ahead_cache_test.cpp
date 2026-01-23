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

#include "paimon/utils/read_ahead_cache.h"

#include <fstream>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// Helper to create a test file, write content, and return a ready ReadAheadCache.
struct TestCacheEnv {
    std::string path;
    std::shared_ptr<paimon::ReadAheadCache> cache;
    std::shared_ptr<paimon::MemoryPool> pool;
};

TestCacheEnv CreateTestFileAndCache(const std::string& filename, const std::string& content,
                                    const paimon::CacheConfig& config,
                                    std::vector<paimon::ByteRange> ranges) {
    auto dir = UniqueTestDirectory::Create();
    EXPECT_TRUE(dir);
    std::string path = dir->Str() + "/" + filename;
    std::ofstream file(path, std::ios::binary);
    EXPECT_TRUE(file.is_open());
    file.write(content.data(), content.size());
    EXPECT_FALSE(file.fail());
    file.close();

    auto fs_result = FileSystemFactory::Get("local", path, {});
    EXPECT_TRUE(fs_result.ok());
    auto fs = std::move(fs_result).value();
    auto in_result = fs->Open(path);
    EXPECT_TRUE(in_result.ok());
    auto in = std::move(in_result).value();

    auto pool = GetDefaultPool();
    auto cache = std::make_shared<ReadAheadCache>(std::move(in), config, pool);
    EXPECT_OK(cache->Init(std::move(ranges)));
    return {path, cache, pool};
}

TEST(TestReadAheadCache, TestBasics) {
    CacheConfig config(/*buffer_size_limit=*/256 * 1024 * 1024, /*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_range_count=*/6);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache(
        "data_file", content, config,
        {{1, 2}, {3, 2}, {8, 2}, {10, 4}, {14, 0}, {15, 4}, {20, 2}, {25, 0}});
    auto& cache = *env.cache;

    auto assert_slice_equal = [](const ByteSlice& slice, const std::string& expected) {
        ASSERT_TRUE(slice.buffer) << expected;
        EXPECT_EQ(expected, std::string_view(slice.buffer->data() + slice.offset, slice.length));
    };

    ByteSlice slice;

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({20, 2}));
    assert_slice_equal(slice, "uv");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({1, 2}));
    assert_slice_equal(slice, "bc");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({3, 2}));
    assert_slice_equal(slice, "de");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({8, 2}));
    assert_slice_equal(slice, "ij");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({10, 4}));
    assert_slice_equal(slice, "klmn");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({15, 4}));
    assert_slice_equal(slice, "pqrs");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({19, 3}));
    assert_slice_equal(slice, "tuv");

    // Zero-sized
    ASSERT_OK_AND_ASSIGN(slice, cache.Read({14, 0}));
    assert_slice_equal(slice, "");
    ASSERT_OK_AND_ASSIGN(slice, cache.Read({25, 0}));
    assert_slice_equal(slice, "");

    // Non-cached ranges

    ASSERT_FALSE(cache.Read({20, 3}).value().buffer);
    ASSERT_FALSE(cache.Read({0, 3}).value().buffer);
    ASSERT_FALSE(cache.Read({25, 2}).value().buffer);
}

// Test repeated reads to the same range to ensure cache reuse.
TEST(TestReadAheadCache, TestRepeatedReadCacheReuse) {
    CacheConfig config(/*buffer_size_limit=*/64, /*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_range_count=*/2);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 5}, {7, 5}});
    auto& cache = *env.cache;

    ByteSlice slice;
    ASSERT_OK_AND_ASSIGN(slice, cache.Read({0, 5}));
    ASSERT_TRUE(slice.buffer);
    std::string first_read(slice.buffer->data() + slice.offset, slice.length);
    ASSERT_EQ(first_read, "abcde");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({0, 5}));
    ASSERT_TRUE(slice.buffer);
    std::string second_read(slice.buffer->data() + slice.offset, slice.length);
    ASSERT_EQ(second_read, "abcde");
}

// Test cache eviction when buffer size is limited.
TEST(TestReadAheadCache, TestCacheEviction) {
    CacheConfig config(/*buffer_size_limit=*/10, /*range_size_limit=*/5,
                       /*hole_size_limit=*/2, /*pre_buffer_range_count=*/1);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 5}, {8, 5}, {16, 5}});
    auto& cache = *env.cache;

    ByteSlice slice;
    ASSERT_OK_AND_ASSIGN(slice, cache.Read({0, 5}));
    ASSERT_TRUE(slice.buffer);
    std::string first_read(slice.buffer->data() + slice.offset, slice.length);
    ASSERT_EQ(first_read, "abcde");

    // Reading another range should evict the first one due to buffer size limit
    ASSERT_OK_AND_ASSIGN(slice, cache.Read({8, 5}));
    ASSERT_TRUE(slice.buffer);
    std::string second_read(slice.buffer->data() + slice.offset, slice.length);
    ASSERT_EQ(second_read, "ijklm");

    // The first range should now be a cache miss (buffer is nullptr)
    auto miss = cache.Read({0, 5});
    ASSERT_FALSE(miss.value().buffer);
}

}  // namespace paimon::test
