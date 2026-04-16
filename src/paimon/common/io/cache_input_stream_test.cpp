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

#include "paimon/common/io/cache_input_stream.h"

#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/read_ahead_cache.h"

namespace paimon::test {

class CacheInputStreamTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        test_dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(test_dir_);
        content_ = "abcdefghijklmnopqrstuvwxyz0123456789";
        file_path_ = test_dir_->Str() + "/test_data";
        std::ofstream file(file_path_, std::ios::binary);
        ASSERT_TRUE(file.is_open());
        file.write(content_.data(), content_.size());
        file.close();
    }

    std::unique_ptr<InputStream> OpenFile() const {
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                             FileSystemFactory::Get("local", file_path_, {}));
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<InputStream> in, fs->Open(file_path_));
        return in;
    }

    std::shared_ptr<ReadAheadCache> CreateCache(std::vector<ByteRange> ranges) {
        auto stream = OpenFile();
        CacheConfig config(/*buffer_size_limit=*/1024 * 1024, /*range_size_limit=*/1024,
                           /*hole_size_limit=*/0, /*pre_buffer_limit=*/1024 * 1024);
        auto cache = std::make_shared<ReadAheadCache>(std::move(stream), config, pool_);
        EXPECT_OK(cache->Init(std::move(ranges)));
        return cache;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::string content_;
    std::string file_path_;
};

// Test proxy methods: Seek, GetPos, Read (sequential), Close, GetUri, Length
TEST_F(CacheInputStreamTest, TestProxyMethods) {
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), /*cache=*/nullptr);

    // Length
    ASSERT_OK_AND_ASSIGN(uint64_t length, stream.Length());
    ASSERT_EQ(length, content_.size());

    // GetUri
    ASSERT_OK_AND_ASSIGN(std::string uri, stream.GetUri());
    ASSERT_FALSE(uri.empty());

    // Seek + GetPos
    ASSERT_OK(stream.Seek(5, SeekOrigin::FS_SEEK_SET));
    ASSERT_OK_AND_ASSIGN(int64_t pos, stream.GetPos());
    ASSERT_EQ(pos, 5);

    // Read (sequential, no offset)
    std::string buffer(3, '\0');
    ASSERT_OK_AND_ASSIGN(int32_t bytes_read, stream.Read(buffer.data(), 3));
    ASSERT_EQ(bytes_read, 3);
    ASSERT_EQ(buffer, "fgh");

    // Close
    ASSERT_OK(stream.Close());
}

// Test Read(offset) with cache == nullptr → direct fallback to input_stream
TEST_F(CacheInputStreamTest, TestReadWithOffsetNullCache) {
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), /*cache=*/nullptr);

    std::string buffer(5, '\0');
    ASSERT_OK_AND_ASSIGN(int32_t bytes_read, stream.Read(buffer.data(), 5, /*offset=*/2));
    ASSERT_EQ(bytes_read, 5);
    ASSERT_EQ(buffer, "cdefg");
}

// Test Read(offset) with cache hit → memcpy from cache
TEST_F(CacheInputStreamTest, TestReadWithOffsetCacheHit) {
    // Cache range [2, 5) i.e. offset=2, length=5
    auto cache = CreateCache({{2, 5}});
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), cache);

    std::string buffer(5, '\0');
    ASSERT_OK_AND_ASSIGN(int32_t bytes_read, stream.Read(buffer.data(), 5, /*offset=*/2));
    ASSERT_EQ(bytes_read, 5);
    ASSERT_EQ(buffer, "cdefg");
}

// Test Read(offset) with cache miss → fallback to input_stream
TEST_F(CacheInputStreamTest, TestReadWithOffsetCacheMiss) {
    // Cache range [2, 5) but read from offset 10 which is not cached
    auto cache = CreateCache({{2, 5}});
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), cache);

    std::string buffer(3, '\0');
    ASSERT_OK_AND_ASSIGN(int32_t bytes_read, stream.Read(buffer.data(), 3, /*offset=*/10));
    ASSERT_EQ(bytes_read, 3);
    ASSERT_EQ(buffer, "klm");
}

// Test ReadAsync with cache == nullptr → direct fallback to input_stream->ReadAsync
TEST_F(CacheInputStreamTest, TestReadAsyncNullCache) {
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), /*cache=*/nullptr);

    std::string buffer(4, '\0');
    bool callback_called = false;
    Status callback_status = Status::Invalid("not called");
    stream.ReadAsync(buffer.data(), 4, /*offset=*/0, [&](Status status) {
        callback_called = true;
        callback_status = status;
    });
    ASSERT_TRUE(callback_called);
    ASSERT_OK(callback_status);
    ASSERT_EQ(buffer, "abcd");
}

// Test ReadAsync with cache hit → memcpy + callback(OK)
TEST_F(CacheInputStreamTest, TestReadAsyncCacheHit) {
    auto cache = CreateCache({{0, 10}});
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), cache);

    std::string buffer(4, '\0');
    bool callback_called = false;
    Status callback_status = Status::Invalid("not called");
    stream.ReadAsync(buffer.data(), 4, /*offset=*/3, [&](Status status) {
        callback_called = true;
        callback_status = status;
    });
    ASSERT_TRUE(callback_called);
    ASSERT_OK(callback_status);
    ASSERT_EQ(buffer, "defg");
}

// Test ReadAsync with cache miss → fallback to input_stream->ReadAsync
TEST_F(CacheInputStreamTest, TestReadAsyncCacheMiss) {
    // Cache range [0, 5) but read from offset 20 which is not cached
    auto cache = CreateCache({{0, 5}});
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), cache);

    std::string buffer(4, '\0');
    bool callback_called = false;
    Status callback_status = Status::Invalid("not called");
    stream.ReadAsync(buffer.data(), 4, /*offset=*/20, [&](Status status) {
        callback_called = true;
        callback_status = status;
    });
    ASSERT_TRUE(callback_called);
    ASSERT_OK(callback_status);
    ASSERT_EQ(buffer, "uvwx");
}

// Test ReadAsync when cache_->Read() returns error status.
// Uses IOHook to inject IO error during ReadAheadCache's prefetch, causing cache_->Read() to fail.
TEST_F(CacheInputStreamTest, TestReadAsyncCacheReadError) {
    auto io_hook = paimon::IOHook::GetInstance();
    bool error_triggered = false;

    for (size_t i = 0; i < 20; i++) {
        // Open the cache stream and underlying stream BEFORE activating IOHook
        ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", file_path_, {}));
        ASSERT_OK_AND_ASSIGN(auto cache_stream, fs->Open(file_path_));
        ASSERT_OK_AND_ASSIGN(auto underlying, fs->Open(file_path_));
        CacheConfig config(/*buffer_size_limit=*/1024 * 1024, /*range_size_limit=*/1024,
                           /*hole_size_limit=*/0, /*pre_buffer_limit=*/1024 * 1024);
        auto cache = std::make_shared<ReadAheadCache>(std::move(cache_stream), config, pool_);
        ASSERT_OK(cache->Init(std::vector<ByteRange>{{0, 10}}));

        // Now activate IOHook so that the prefetch IO (triggered by cache_->Read -> PreBuffer)
        // will fail at the i-th IO operation
        paimon::ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, paimon::IOHook::Mode::RETURN_ERROR);
        CacheInputStream stream(std::move(underlying), cache);

        std::string buffer(5, '\0');
        bool callback_called = false;
        Status callback_status = Status::OK();
        stream.ReadAsync(buffer.data(), 5, /*offset=*/0, [&](Status status) {
            callback_called = true;
            callback_status = status;
        });
        ASSERT_TRUE(callback_called);
        CHECK_HOOK_STATUS(callback_status, i);
        ASSERT_EQ(buffer, "abcde");
        error_triggered = true;
        break;
    }
    ASSERT_TRUE(error_triggered);
}

}  // namespace paimon::test
