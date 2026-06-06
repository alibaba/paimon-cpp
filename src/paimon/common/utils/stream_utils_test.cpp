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

#include "paimon/common/utils/stream_utils.h"

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {

class StreamUtilsTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        file_system_ = std::make_shared<LocalFileSystem>();
    }

    void TearDown() override {
        dir_.reset();
        file_system_.reset();
        pool_.reset();
    }

    std::string CreateTestFile(const std::string& content) {
        std::string file_path = dir_->Str() + "/test_file.txt";
        EXPECT_OK_AND_ASSIGN(auto output_stream, file_system_->Create(file_path, true));
        EXPECT_OK_AND_ASSIGN(int64_t length, output_stream->Write(content.data(), content.size()));
        EXPECT_EQ(length, static_cast<int64_t>(content.size()));
        EXPECT_OK(output_stream->Close());
        return file_path;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> file_system_;
};

TEST_F(StreamUtilsTest, ReadBasicTest) {
    std::string test_content = "Hello, World! This is a test file for StreamUtils.";
    std::string file_path = CreateTestFile(test_content);

    {
        // ReadFully
        ASSERT_OK_AND_ASSIGN(auto input_stream, file_system_->Open(file_path));
        ASSERT_OK(input_stream->Seek(10, FS_SEEK_SET));  // ReadFully ignore any seek
        ASSERT_OK_AND_ASSIGN(auto result, StreamUtils::ReadFully(std::move(input_stream), pool_));
        ASSERT_EQ(result->size(), test_content.size());
        ASSERT_EQ(std::string(result->data(), result->size()), test_content);
    }
    {
        // ReadAsyncFully
        ASSERT_OK_AND_ASSIGN(auto input_stream, file_system_->Open(file_path));
        ASSERT_OK(input_stream->Seek(10, FS_SEEK_SET));  // ReadAsyncFully ignore any seek
        ASSERT_OK_AND_ASSIGN(auto result,
                             StreamUtils::ReadAsyncFully(std::move(input_stream), pool_));
        ASSERT_EQ(result->size(), test_content.size());
        ASSERT_EQ(std::string(result->data(), result->size()), test_content);
    }
    {
        // ReadAsyncFully with buffer
        ASSERT_OK_AND_ASSIGN(auto input_stream, file_system_->Open(file_path));
        ASSERT_OK(input_stream->Seek(10, FS_SEEK_SET));  // ReadAsyncFully ignore any seek
        std::vector<char> buffer(test_content.size());
        ASSERT_OK(StreamUtils::ReadAsyncFully(std::move(input_stream), buffer.data()));
        ASSERT_EQ(std::string(buffer.data(), buffer.size()), test_content);
    }
}

TEST_F(StreamUtilsTest, ReadEmptyFile) {
    std::string file_path = CreateTestFile("");

    {
        // ReadFully
        ASSERT_OK_AND_ASSIGN(auto input_stream, file_system_->Open(file_path));
        ASSERT_OK_AND_ASSIGN(auto result, StreamUtils::ReadFully(std::move(input_stream), pool_));
        ASSERT_EQ(result->size(), 0);
    }
    {
        // ReadAsyncFully
        ASSERT_OK_AND_ASSIGN(auto input_stream, file_system_->Open(file_path));
        ASSERT_OK_AND_ASSIGN(auto result,
                             StreamUtils::ReadAsyncFully(std::move(input_stream), pool_));
        ASSERT_EQ(result->size(), 0);
    }
}

TEST_F(StreamUtilsTest, ReadLargeFile) {
    std::string large_content;
    const size_t file_size = 3 * 1024 * 1024 + 512;  // 3.5MB
    large_content.reserve(file_size);
    for (size_t i = 0; i < file_size; ++i) {
        large_content += static_cast<char>('A' + (i % 26));
    }
    std::string file_path = CreateTestFile(large_content);

    {
        // ReadFully
        ASSERT_OK_AND_ASSIGN(auto input_stream, file_system_->Open(file_path));
        ASSERT_OK_AND_ASSIGN(auto result, StreamUtils::ReadFully(std::move(input_stream), pool_));
        ASSERT_EQ(result->size(), large_content.size());
        ASSERT_EQ(std::string(result->data(), result->size()), large_content);
    }
    {
        // ReadAsyncFully
        ASSERT_OK_AND_ASSIGN(auto input_stream, file_system_->Open(file_path));
        ASSERT_OK_AND_ASSIGN(auto result,
                             StreamUtils::ReadAsyncFully(std::move(input_stream), pool_));
        ASSERT_EQ(result->size(), large_content.size());
        ASSERT_EQ(std::string(result->data(), result->size()), large_content);
    }
    {
        // ReadAsyncFully with buffer
        ASSERT_OK_AND_ASSIGN(auto input_stream, file_system_->Open(file_path));
        std::vector<char> buffer(file_size);
        ASSERT_OK(StreamUtils::ReadAsyncFully(std::move(input_stream), buffer.data()));
        ASSERT_EQ(std::string(buffer.data(), buffer.size()), large_content);
    }
}

}  // namespace paimon
