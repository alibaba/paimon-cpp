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

#include "paimon/format/avro/avro_input_stream_impl.h"

#include <filesystem>
#include <fstream>
#include <string>

#include "gtest/gtest.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {

TEST(AvroInputStreamImplTest, TestNext) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::filesystem::path file_path = dir->Str() + "/file";
    std::ofstream output_file(file_path);
    ASSERT_TRUE(output_file.is_open());
    std::string test_data = "hello world";
    output_file << test_data;
    output_file.close();

    size_t buffer_size = 10;
    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(file_path));
    ASSERT_OK_AND_ASSIGN(auto stream,
                         AvroInputStreamImpl::Create(in, buffer_size, GetDefaultPool()));
    const uint8_t* data;
    size_t size;
    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(size, 10);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(data), size), test_data.substr(0, 10));
    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(size, 1);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(data), size), test_data.substr(10, 1));
    ASSERT_FALSE(stream->next(&data, &size));
}

TEST(AvroInputStreamImplTest, TestBackup) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::filesystem::path file_path = dir->Str() + "/file";
    std::ofstream output_file(file_path);
    ASSERT_TRUE(output_file.is_open());
    std::string test_data = "abcdefghij";
    output_file << test_data;
    output_file.close();

    size_t buffer_size = 10;
    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(file_path));
    ASSERT_OK_AND_ASSIGN(auto stream,
                         AvroInputStreamImpl::Create(in, buffer_size, GetDefaultPool()));
    const uint8_t* data;
    size_t size;
    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(size, 10);

    stream->backup(3);
    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(size, 3);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(data), size), "hij");
    stream->backup(10);
    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(size, 10);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(data), size), test_data);
}

TEST(AvroInputStreamImplTest, TestSkip) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::filesystem::path file_path = dir->Str() + "/file";
    std::ofstream output_file(file_path);
    ASSERT_TRUE(output_file.is_open());
    std::string test_data = "abcdefghij";
    output_file << test_data;
    output_file.close();

    size_t buffer_size = 10;
    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(file_path));
    ASSERT_OK_AND_ASSIGN(auto stream,
                         AvroInputStreamImpl::Create(in, buffer_size, GetDefaultPool()));
    const uint8_t* data;
    size_t size;
    stream->skip(5);
    ASSERT_EQ(stream->byteCount(), 5);

    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(size, 5);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(data), size), "fghij");
    ASSERT_THROW(stream->skip(5), ::avro::Exception);  // already eof, cannot skip more
    ASSERT_EQ(stream->byteCount(), 10);
    ASSERT_FALSE(stream->next(&data, &size));  // reach eof

    ASSERT_THROW(stream->backup(7), ::avro::Exception);  // buffer item is 5, cannot backup 7
    stream->backup(4);
    ASSERT_EQ(stream->byteCount(), 6);
    stream->skip(2);  // skip 2 bytes from the available buffer data
    ASSERT_EQ(stream->byteCount(), 8);

    // verify we can read the remaining 2 bytes from buffer
    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(size, 2);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(data), size), "ij");
    ASSERT_EQ(stream->byteCount(), 10);
}

TEST(AvroInputStreamImplTest, TestSkipWithAvailableData) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::filesystem::path file_path = dir->Str() + "/file";
    std::ofstream output_file(file_path);
    ASSERT_TRUE(output_file.is_open());
    std::string test_data = "abcdefghij";
    output_file << test_data;
    output_file.close();

    size_t buffer_size = 10;
    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(file_path));
    ASSERT_OK_AND_ASSIGN(auto stream,
                         AvroInputStreamImpl::Create(in, buffer_size, GetDefaultPool()));

    const uint8_t* data;
    size_t size;
    // First, load data into buffer by calling next()
    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(size, 10);
    ASSERT_EQ(stream->byteCount(), 10);

    // Now backup some data to make it available in buffer
    stream->backup(7);
    ASSERT_EQ(stream->byteCount(), 3);

    // Skip 3 bytes from the available buffer data
    stream->skip(3);
    ASSERT_EQ(stream->byteCount(), 6);

    // Verify we can read the remaining 4 bytes from buffer
    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(size, 4);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(data), size), "ghij");
    ASSERT_EQ(stream->byteCount(), 10);
}

TEST(AvroInputStreamImplTest, TestSeek) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::filesystem::path file_path = dir->Str() + "/file";
    std::ofstream output_file(file_path);
    ASSERT_TRUE(output_file.is_open());
    std::string test_data = "abcdefghij";
    output_file << test_data;
    output_file.close();

    size_t buffer_size = 5;
    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(file_path));
    ASSERT_OK_AND_ASSIGN(auto stream,
                         AvroInputStreamImpl::Create(in, buffer_size, GetDefaultPool()));

    const uint8_t* data;
    size_t size;
    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(stream->byteCount(), 5);
    ASSERT_EQ(size, 5);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(data), size), "abcde");
    stream->seek(2);
    ASSERT_EQ(stream->byteCount(), 2);

    // after seek, buffer will be cleared, cannot backup
    ASSERT_THROW(stream->backup(2), ::avro::Exception);
    ASSERT_TRUE(stream->next(&data, &size));
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(data), size), "cdefg");
    ASSERT_EQ(stream->byteCount(), 7);
    ASSERT_EQ(size, 5);
}

}  // namespace paimon::avro::test
