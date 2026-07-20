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

#include <chrono>
#include <cstdint>
#include <future>
#include <string>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/fs/jindo/jindo_file_system_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::jindo::test {

// This test shows inconsistent behavior with the local file system in some abnormal scenarios.
class JindoFileSystemTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create("jindo");
        ASSERT_TRUE(dir_);
        test_dir_ = dir_->Str() + "/";
        fs_ = dir_->GetFileSystem();
    }

    void TearDown() override {
        dir_.reset();
        fs_.reset();
    }

 protected:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::string test_dir_;
    std::shared_ptr<FileSystem> fs_;
};

TEST_F(JindoFileSystemTest, TestLifeCycle) {
    std::string file_path = test_dir_ + "file.data";
    std::string content = "content";
    std::map<std::string, std::string> options = paimon::test::GetJindoTestOptions();
    auto fs_factory = std::make_shared<paimon::jindo::JindoFileSystemFactory>();
    ASSERT_OK_AND_ASSIGN(auto tmp_fs, fs_factory->Create(file_path, options));
    ASSERT_OK(fs_->WriteFile(file_path, content, /*overwrite=*/false));

    // read process
    ASSERT_OK_AND_ASSIGN(auto in_stream, tmp_fs->Open(file_path));
    std::string read_content(content.size(), '\0');
    ASSERT_OK_AND_ASSIGN(int64_t read_len,
                         in_stream->Read(read_content.data(), read_content.size()));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ(content, read_content);
    // The lifecycle of the fs used to create the Jindo Reader must be longer than the lifecycle of
    // the Jindo Reader.
    tmp_fs.reset();
    ASSERT_OK(in_stream->Close());
}

TEST_F(JindoFileSystemTest, TestRename) {
    // test rename file to non-exist dir
    std::string file_path = test_dir_ + "file5/file6/file7";
    ASSERT_OK(fs_->WriteFile(file_path, "content", /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
    ASSERT_TRUE(is_exist);
    std::string file_path2 = test_dir_ + "file8/file9";
    ASSERT_NOK_WITH_MSG(fs_->Rename(/*src=*/file_path, /*dst=*/file_path2), "file8 not found");
    ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path));
    ASSERT_TRUE(is_exist);
}

TEST_F(JindoFileSystemTest, TestSeek) {
    std::string content = "abcdefghijk";
    std::string file_path = test_dir_ + "file.data";
    // write process
    ASSERT_OK_AND_ASSIGN(auto out_stream, fs_->Create(file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(int64_t write_len, out_stream->Write(content.data(), content.size()));
    ASSERT_EQ(write_len, content.size());
    ASSERT_OK(out_stream->Flush());
    ASSERT_OK(out_stream->Close());

    // read process
    ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
    ASSERT_OK_AND_ASSIGN(auto pos, in_stream->GetPos());
    ASSERT_EQ(pos, 0);

    // invalid seek
    ASSERT_NOK_WITH_MSG(in_stream->Seek(/*offset=*/20, SeekOrigin::FS_SEEK_SET),
                        "seek file failed: seek EOF");
    ASSERT_OK_AND_ASSIGN(pos, in_stream->GetPos());
    ASSERT_EQ(pos, 0);
    // valid seek
    ASSERT_OK(in_stream->Seek(/*offset=*/2, SeekOrigin::FS_SEEK_SET));
    ASSERT_OK_AND_ASSIGN(pos, in_stream->GetPos());
    ASSERT_EQ(pos, 2);

    ASSERT_OK(in_stream->Seek(/*offset=*/4, SeekOrigin::FS_SEEK_CUR));
    ASSERT_OK_AND_ASSIGN(pos, in_stream->GetPos());
    ASSERT_EQ(pos, 6);

    ASSERT_OK(in_stream->Seek(/*offset=*/-3, SeekOrigin::FS_SEEK_END));
    ASSERT_OK_AND_ASSIGN(pos, in_stream->GetPos());
    ASSERT_EQ(pos, 8);

    // read from cur pos
    std::string read_content(3, '\0');
    ASSERT_OK_AND_ASSIGN(int64_t read_len,
                         in_stream->Read(read_content.data(), read_content.size()));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ("ijk", read_content);

    // read from offset
    ASSERT_OK_AND_ASSIGN(read_len,
                         in_stream->Read(read_content.data(), read_content.size(), /*offset=*/
                                         4));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ("efg", read_content);

    ASSERT_OK_AND_ASSIGN(pos, in_stream->GetPos());
    ASSERT_EQ(pos, 11);
    ASSERT_OK(in_stream->Close());
}

TEST(JindoFileSystemPaginationTest, TestListDirAcrossOssPageBoundary) {
    constexpr int32_t kFileCount = 1234;
    const std::string test_dir = "oss://paimon-unittest/test_data/jindo_listdir_truncated_1234/";
    std::map<std::string, std::string> options = paimon::test::GetJindoTestOptions();

    auto fs_factory = std::make_shared<JindoFileSystemFactory>();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileSystem> fs, fs_factory->Create(test_dir, options));

    std::vector<std::unique_ptr<BasicFileStatus>> file_statuses;
    ASSERT_OK(fs->ListDir(test_dir, &file_statuses));
    ASSERT_EQ(file_statuses.size(), kFileCount);

    std::unordered_set<std::string> actual_paths;
    for (const std::unique_ptr<BasicFileStatus>& file_status : file_statuses) {
        ASSERT_TRUE(actual_paths.insert(file_status->GetPath()).second)
            << "duplicate path: " << file_status->GetPath();
    }
    for (int32_t i = 0; i < kFileCount; ++i) {
        std::string index = std::to_string(i);
        index.insert(/*pos=*/0, /*count=*/4 - index.size(), /*ch=*/'0');
        ASSERT_NE(actual_paths.find(test_dir + "file-" + index + ".txt"), actual_paths.end());
    }
}

TEST(JindoFileSystemAsyncReadTest, TestConcurrentReadAsyncAndReadFromOss) {
    constexpr int32_t kConcurrentReads = 64;
    constexpr int64_t kAsyncReadSize = 7;
    const std::string file_path = "oss://paimon-unittest/test_data/jindo_read_async_128mb.bin";
    std::map<std::string, std::string> options = paimon::test::GetJindoTestOptions();
    auto fs_factory = std::make_shared<JindoFileSystemFactory>();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileSystem> fs, fs_factory->Create(file_path, options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> input_stream, fs->Open(file_path));

    std::vector<std::vector<char>> async_buffers(kConcurrentReads,
                                                 std::vector<char>(kAsyncReadSize));
    std::vector<std::promise<Status>> promises(kConcurrentReads);
    std::vector<std::future<Status>> futures;
    futures.reserve(kConcurrentReads);
    for (std::promise<Status>& promise : promises) {
        futures.push_back(promise.get_future());
    }

    std::vector<Status> sync_read_statuses;
    std::vector<int64_t> sync_read_sizes;
    sync_read_statuses.reserve(kConcurrentReads);
    sync_read_sizes.reserve(kConcurrentReads);
    for (int32_t i = 0; i < kConcurrentReads; ++i) {
        input_stream->ReadAsync(
            async_buffers[i].data(), async_buffers[i].size(), /*offset=*/0,
            [&promises, i](Status status) { promises[i].set_value(std::move(status)); });

        char sync_buffer = 0;
        Result<int64_t> sync_read_result =
            input_stream->Read(&sync_buffer, /*size=*/1, /*offset=*/i);
        sync_read_statuses.push_back(sync_read_result.status());
        sync_read_sizes.push_back(sync_read_result.ok() ? sync_read_result.value() : -1);
    }

    for (int32_t i = 0; i < kConcurrentReads; ++i) {
        ASSERT_EQ(futures[i].wait_for(std::chrono::seconds(60)), std::future_status::ready)
            << "async read=" << i;
        ASSERT_OK(futures[i].get());
    }
    for (int32_t i = 0; i < kConcurrentReads; ++i) {
        ASSERT_OK(sync_read_statuses[i]) << "sync read=" << i;
        ASSERT_EQ(sync_read_sizes[i], 1) << "sync read=" << i;
    }
}

}  // namespace paimon::jindo::test
