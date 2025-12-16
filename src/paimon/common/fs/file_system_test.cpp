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

#include "paimon/fs/file_system.h"

#include <cassert>
#include <cstdlib>
#include <future>
#include <map>
#include <set>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/executor.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class FileSystemTest : public ::testing::Test, public ::testing::WithParamInterface<std::string> {
 public:
    void SetUp() override {
        std::string file_system = GetParam();
        dir_ = paimon::test::UniqueTestDirectory::Create(file_system);
        ASSERT_TRUE(dir_);
        test_root_ = dir_->Str();
        fs_ = dir_->GetFileSystem();
    }

    void TearDown() override {
        dir_.reset();
        fs_.reset();
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------
    /// Creates a random string with a length within the given interval. The string contains
    /// only characters that can be represented as a single code point.
    ///
    /// @param min_length The minimum string length.
    /// @param max_length The maximum string length (inclusive).
    /// @param min_value The minimum character value to occur.
    /// @param max_value The maximum character value to occur.
    /// @return A random String.
    static std::string GetRandomString(int32_t min_length, int32_t max_length, char min_value,
                                       char max_value) {
        int32_t len = std::rand() % (max_length - min_length + 1) + min_length;

        std::vector<char> data;
        data.resize(len);
        int32_t diff = max_value - min_value + 1;

        for (int32_t i = 0; i < len; i++) {
            data[i] = static_cast<char>(std::rand() % diff + min_value);
        }
        return std::string(data.data(), data.size());
    }

    static std::string RandomName() {
        return GetRandomString(16, 16, 'a', 'z');
    }

    std::string MakeDir(const std::string& test_root) {
        EXPECT_OK_AND_ASSIGN(bool exist, fs_->Exists(test_root));
        if (!exist) {
            EXPECT_OK(fs_->Mkdirs(test_root));
        }
        return test_root;
    }

    void CreateFile(const std::string& file) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> out,
                             fs_->Create(file, /*overwrite=*/true));
        std::string input = "paimon";
        char chars[8] = {1, 2, 3, 4, 5, 6, 7, 8};
        ASSERT_OK_AND_ASSIGN(int32_t size, out->Write(chars, input.size()));
        ASSERT_EQ(size, input.size());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
    }

    std::string CreateRandomFileInDirectory(const std::string& test_root) {
        std::string directory = MakeDir(test_root);
        std::string file_path = PathUtil::JoinPath(directory, RandomName());
        CreateFile(file_path);
        return file_path;
    }

    std::string RemoveLastSlashInPath(const std::string& path) const {
        std::string new_path = path;
        PathUtil::TrimLastDelim(&new_path);
        return new_path;
    }

    void CheckFileStatus(const std::vector<std::unique_ptr<FileStatus>>& actual_statuses,
                         const std::set<std::string>& expected_files,
                         const std::set<std::string>& expected_dirs) const {
        ASSERT_EQ(actual_statuses.size(), expected_files.size() + expected_dirs.size());
        std::set<std::string> actual_files;
        std::set<std::string> actual_dirs;
        for (const auto& file_status : actual_statuses) {
            if (file_status->IsDir()) {
                actual_dirs.insert(RemoveLastSlashInPath(file_status->GetPath()));
            } else {
                actual_files.insert(file_status->GetPath());
                ASSERT_TRUE(file_status->GetLen() > 0);
                int64_t modification_time = file_status->GetModificationTime();
                ASSERT_TRUE(modification_time > 10000000000L);     // MIN_VALID_FILE_MODIFICATION_MS
                ASSERT_TRUE(modification_time < 10000000000000L);  // MAX_VALID_FILE_MODIFICATION_MS
            }
        }
        std::set<std::string> normalized_expected_dirs;
        for (const auto& path : expected_dirs) {
            normalized_expected_dirs.insert(RemoveLastSlashInPath(path));
        }
        ASSERT_EQ(actual_files, expected_files);
        ASSERT_EQ(actual_dirs, normalized_expected_dirs);
    }

    void CheckBasicFileStatus(const std::vector<std::unique_ptr<BasicFileStatus>>& actual_statuses,
                              const std::set<std::string>& expected_files,
                              const std::set<std::string>& expected_dirs) const {
        ASSERT_EQ(actual_statuses.size(), expected_files.size() + expected_dirs.size());
        std::set<std::string> actual_files;
        std::set<std::string> actual_dirs;
        for (const auto& file_status : actual_statuses) {
            if (file_status->IsDir()) {
                actual_dirs.insert(RemoveLastSlashInPath(file_status->GetPath()));
            } else {
                actual_files.insert(file_status->GetPath());
            }
        }
        std::set<std::string> normalized_expected_dirs;
        for (const auto& path : expected_dirs) {
            normalized_expected_dirs.insert(RemoveLastSlashInPath(path));
        }
        ASSERT_EQ(actual_files, expected_files);
        ASSERT_EQ(actual_dirs, normalized_expected_dirs);
    }

    std::string GetTestDir() const {
        std::string file_system = GetParam();
        if (file_system == "local") {
            return paimon::test::GetDataDir();
        } else if (file_system == "jindo") {
            return "oss://paimon-unittest/test_data/";
        }
        assert(false);
        return "";
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::string test_root_;
};

TEST_P(FileSystemTest, TestNoneFileSystemFactory) {
    std::map<std::string, std::string> fs_options;
    Result<std::unique_ptr<FileSystem>> fs =
        FileSystemFactory::Get("not exist", "/tmp", fs_options);
    ASSERT_TRUE(fs.status().IsInvalid());
}

TEST_P(FileSystemTest, TestFactoryCreator) {
    std::vector<std::string> factory_registered_type =
        FactoryCreator::GetInstance()->GetRegisteredType();

    auto test_registered = [&](const std::string& identifier) {
        bool is_exist = false;
        for (auto registered_type : factory_registered_type) {
            if (registered_type == identifier) {
                is_exist = true;
            }
        }
        ASSERT_TRUE(is_exist);
    };
    test_registered(GetParam());
}

// --- create
TEST_P(FileSystemTest, TestCreate) {
    std::string path = PathUtil::JoinPath(test_root_, "/test_file");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> out, fs_->Create(path, /*overwrite=*/true));
    ASSERT_TRUE(out);
    std::string input = "paimon";
    ASSERT_OK_AND_ASSIGN(int32_t size, out->Write(input.data(), input.size()));
    ASSERT_EQ(size, input.size());
    ASSERT_OK(out->Close());

    ASSERT_NOK_WITH_MSG(fs_->Create(path, /*overwrite=*/false), "already exists");
}

// --- write&read
TEST_P(FileSystemTest, TestSimpleWriteAndRead) {
    std::string content = "abcdefghijk";
    std::string file_path = test_root_ + "/file.data";
    // write process
    ASSERT_OK_AND_ASSIGN(auto out_stream, fs_->Create(file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(int32_t write_len, out_stream->Write(content.data(), content.size()));
    ASSERT_EQ(write_len, content.size());

    ASSERT_OK(out_stream->Flush());
    ASSERT_OK_AND_ASSIGN(int64_t pos, out_stream->GetPos());
    ASSERT_EQ(pos, content.size());

    ASSERT_OK_AND_ASSIGN(std::string uri, out_stream->GetUri());
    ASSERT_EQ(uri, file_path);
    ASSERT_OK(out_stream->Close());

    // read process
    ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
    ASSERT_OK_AND_ASSIGN(pos, in_stream->GetPos());
    ASSERT_EQ(pos, 0);

    // read from cur pos
    std::string read_content(content.size(), '\0');
    ASSERT_OK_AND_ASSIGN(int32_t read_len,
                         in_stream->Read(read_content.data(), read_content.size()));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ(content, read_content);

    // read from offset
    ASSERT_OK_AND_ASSIGN(read_len, in_stream->Read(read_content.data(), read_content.size(), 0));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ(content, read_content);

    ASSERT_OK_AND_ASSIGN(uri, in_stream->GetUri());
    ASSERT_EQ(uri, file_path);
    ASSERT_OK_AND_ASSIGN(uint64_t file_len, in_stream->Length());
    ASSERT_EQ(file_len, content.size());

    ASSERT_OK_AND_ASSIGN(pos, in_stream->GetPos());
    ASSERT_EQ(pos, content.size());
    ASSERT_OK(in_stream->Close());
}

TEST_P(FileSystemTest, TestWriteMultipleTimes) {
    std::vector<std::string> content_vec = {"abc", "defg", "hi", "j", "k"};
    std::string content = "abcdefghijk";
    std::string file_path = test_root_ + "/file.data";
    // write process
    ASSERT_OK_AND_ASSIGN(auto out_stream, fs_->Create(file_path, /*overwrite=*/true));
    for (const auto& str : content_vec) {
        ASSERT_OK_AND_ASSIGN(int32_t write_len, out_stream->Write(str.data(), str.size()));
        ASSERT_EQ(write_len, str.size());
    }
    ASSERT_OK(out_stream->Flush());
    ASSERT_OK_AND_ASSIGN(int64_t pos, out_stream->GetPos());
    ASSERT_EQ(pos, content.size());
    ASSERT_OK(out_stream->Close());

    // read process
    ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
    std::string read_content(content.size(), '\0');
    ASSERT_OK_AND_ASSIGN(int32_t read_len,
                         in_stream->Read(read_content.data(), read_content.size()));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ(content, read_content);
}

TEST_P(FileSystemTest, TestWriteInNotExistDir) {
    std::string file_path = test_root_ + "/no_exist/file.data";
    // write process
    std::string content = "abcdefghijk";
    ASSERT_OK_AND_ASSIGN(auto out_stream, fs_->Create(file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] int32_t write_len,
                         out_stream->Write(content.data(), content.size()));
    ASSERT_OK(out_stream->Flush());
    ASSERT_OK(out_stream->Close());

    // read process
    ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
    std::string read_content(content.size(), '\0');
    ASSERT_OK_AND_ASSIGN(int32_t read_len,
                         in_stream->Read(read_content.data(), read_content.size()));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ(content, read_content);

    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(test_root_ + "/no_exist/"));
    ASSERT_TRUE(is_exist);
}

TEST_P(FileSystemTest, TestWriteEmptyFile) {
    std::string file_path = test_root_ + "/file.data";
    // write process
    std::string content = "";
    ASSERT_OK_AND_ASSIGN(auto out_stream, fs_->Create(file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(int32_t write_len, out_stream->Write(content.data(), content.size()));
    ASSERT_EQ(write_len, 0);
    ASSERT_OK(out_stream->Flush());
    ASSERT_OK(out_stream->Close());

    // get file status
    ASSERT_OK_AND_ASSIGN(auto st, fs_->GetFileStatus(file_path));
    ASSERT_EQ(st->GetPath(), file_path);
    ASSERT_FALSE(st->IsDir());
    ASSERT_EQ(st->GetLen(), 0);
    auto modification_time = st->GetModificationTime();
    ASSERT_TRUE(modification_time > 10000000000L);
    ASSERT_TRUE(modification_time < 10000000000000L);

    // read process
    ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
    std::string read_content(content.size(), '\0');
    ASSERT_OK_AND_ASSIGN(int32_t read_len,
                         in_stream->Read(read_content.data(), read_content.size()));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ(content, read_content);
}

TEST_P(FileSystemTest, TestWriteWithOverwrite) {
    std::string content = "abcdefghijk";
    std::string file_path = test_root_ + "/file.data";
    // write process
    ASSERT_OK_AND_ASSIGN(auto out_stream, fs_->Create(file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(int32_t write_len, out_stream->Write(content.data(), content.size()));
    ASSERT_EQ(write_len, content.size());
    ASSERT_OK(out_stream->Flush());
    ASSERT_OK(out_stream->Close());

    // write file which already exist
    {
        std::string new_content = "helloworld";
        ASSERT_OK_AND_ASSIGN(auto out_stream2, fs_->Create(file_path, /*overwrite=*/true));
        ASSERT_OK_AND_ASSIGN(write_len, out_stream2->Write(new_content.data(), new_content.size()));
        ASSERT_EQ(write_len, new_content.size());
        ASSERT_OK(out_stream2->Flush());
        ASSERT_OK(out_stream2->Close());

        // read process
        ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
        std::string read_content(new_content.size(), '\0');
        ASSERT_OK_AND_ASSIGN(int32_t read_len,
                             in_stream->Read(read_content.data(), read_content.size()));
        ASSERT_EQ(read_len, read_content.size());
        ASSERT_EQ(new_content, read_content);
    }
    {
        // test file exist and overwrite = false
        ASSERT_NOK_WITH_MSG(fs_->Create(file_path, /*overwrite=*/false), "do not allow overwrite");
    }
}

TEST_P(FileSystemTest, TestAsyncRead) {
    std::string content = "abcdefghijk";
    std::string file_path = test_root_ + "/file.data";
    // write process
    ASSERT_OK_AND_ASSIGN(auto out_stream, fs_->Create(file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] int32_t write_len,
                         out_stream->Write(content.data(), content.size()));
    ASSERT_OK(out_stream->Flush());
    ASSERT_OK(out_stream->Close());

    // read process
    ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
    std::string read_content(content.size(), '\0');
    bool read_finished = false;
    std::promise<int32_t> promise;
    std::future<int32_t> future = promise.get_future();
    auto callback = [&](Status status) {
        EXPECT_OK(status);
        if (status.ok()) {
            read_finished = true;
            promise.set_value(10);
        } else {
            read_finished = false;
            promise.set_value(20);
        }
    };
    in_stream->ReadAsync(read_content.data(), read_content.size(), /*offset=*/0, callback);
    ASSERT_EQ(future.get(), 10);
    ASSERT_TRUE(read_finished);
    ASSERT_EQ(content, read_content);
    ASSERT_OK(in_stream->Close());
}

TEST_P(FileSystemTest, TestInvalidRead) {
    std::string content = "abcdefghijk";
    std::string file_path = test_root_ + "/file.data";
    // write process
    ASSERT_OK_AND_ASSIGN(auto out_stream, fs_->Create(file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] int32_t write_len,
                         out_stream->Write(content.data(), content.size()));
    ASSERT_OK(out_stream->Flush());
    ASSERT_OK(out_stream->Close());
    {
        // seek to end, then read
        ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
        ASSERT_OK(in_stream->Seek(/*offset=*/11, SeekOrigin::FS_SEEK_SET));
        ASSERT_OK_AND_ASSIGN(auto pos, in_stream->GetPos());
        ASSERT_EQ(pos, 11);
        // read from cur pos
        std::string read_content(3, '\0');
        ASSERT_NOK(in_stream->Read(read_content.data(), read_content.size()));
        ASSERT_OK_AND_ASSIGN(size_t actual_read, in_stream->Read(read_content.data(), 0));
        ASSERT_EQ(actual_read, 0);
    }
    {
        // read invalid bulk data
        ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
        std::string read_content(20, '\0');
        ASSERT_NOK(in_stream->Read(read_content.data(), read_content.size()));
        ASSERT_OK_AND_ASSIGN(auto pos, in_stream->GetPos());
        ASSERT_EQ(pos, 11);
    }
    {
        // read invalid with oversize offset
        ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
        std::string read_content(4, '\0');
        ASSERT_NOK(in_stream->Read(read_content.data(), read_content.size(), /*offset=*/20));
    }
}

TEST_P(FileSystemTest, TestInvalidAsyncRead) {
    std::string content = "abcdefghijk";
    std::string file_path = test_root_ + "/file.data";
    // write process
    ASSERT_OK_AND_ASSIGN(auto out_stream, fs_->Create(file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] int32_t write_len,
                         out_stream->Write(content.data(), content.size()));
    ASSERT_OK(out_stream->Flush());
    ASSERT_OK(out_stream->Close());

    {
        // test read overflow
        ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
        std::string read_content(20, '\0');
        bool read_finished = false;
        std::promise<int> promise;
        std::future<int> future = promise.get_future();
        auto callback = [&](Status status) {
            EXPECT_NOK(status);
            if (status.ok()) {
                read_finished = true;
                promise.set_value(10);
            } else {
                read_finished = false;
                promise.set_value(20);
            }
        };
        // invalid async read
        in_stream->ReadAsync(read_content.data(), read_content.size(), /*offset=*/0, callback);
        ASSERT_EQ(future.get(), 20);
        ASSERT_FALSE(read_finished);
        ASSERT_NE(read_content, content);
        ASSERT_OK(in_stream->Close());
    }
    {
        // test read with invalid offset
        ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
        std::string read_content(content.size(), '\0');
        bool read_finished = false;
        std::promise<int> promise;
        std::future<int> future = promise.get_future();
        auto callback = [&](Status status) {
            EXPECT_NOK(status);
            if (status.ok()) {
                read_finished = true;
                promise.set_value(10);
            } else {
                read_finished = false;
                promise.set_value(20);
            }
        };
        // invalid async read
        in_stream->ReadAsync(read_content.data(), read_content.size(), /*offset=*/20, callback);
        ASSERT_EQ(future.get(), 20);
        ASSERT_FALSE(read_finished);
        ASSERT_NE(read_content, content);
        ASSERT_OK(in_stream->Close());
    }
}

TEST_P(FileSystemTest, TestReadAndWriteAndAtomicStoreFile) {
    std::string file_path = test_root_ + "/file.data";
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
    ASSERT_FALSE(is_exist);

    std::string content = "content";
    ASSERT_OK(fs_->AtomicStore(file_path, content));
    std::string read_content;
    ASSERT_OK(fs_->ReadFile(file_path, &read_content));
    ASSERT_EQ(read_content, content);

    std::string new_content = "hello world";
    ASSERT_OK(fs_->WriteFile(file_path, new_content, /*overwrite=*/true));
    ASSERT_OK(fs_->ReadFile(file_path, &read_content));
    ASSERT_EQ(read_content, new_content);

    ASSERT_NOK_WITH_MSG(fs_->WriteFile(file_path, content, /*overwrite=*/false),
                        "do not allow overwrite");
    ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path));
    ASSERT_TRUE(is_exist);

    ASSERT_NOK_WITH_MSG(fs_->AtomicStore(file_path, content), "dst file already exist");
    ASSERT_OK(fs_->ReadFile(file_path, &read_content));
    ASSERT_EQ(read_content, new_content);
}

TEST_P(FileSystemTest, TestIsObjectStore) {
    ASSERT_OK_AND_ASSIGN(bool is_object_store, FileSystem::IsObjectStore("file:///tmp/test.txt"));
    ASSERT_FALSE(is_object_store);
    ASSERT_OK_AND_ASSIGN(is_object_store, FileSystem::IsObjectStore("/tmp/test.txt"));
    ASSERT_FALSE(is_object_store);
    ASSERT_OK_AND_ASSIGN(is_object_store, FileSystem::IsObjectStore("dfs://tmp/test.txt"));
    ASSERT_FALSE(is_object_store);
    ASSERT_OK_AND_ASSIGN(is_object_store, FileSystem::IsObjectStore("hdfs://tmp/test.txt"));
    ASSERT_FALSE(is_object_store);

    ASSERT_OK_AND_ASSIGN(is_object_store, FileSystem::IsObjectStore("oss://bucket/test.txt"));
    ASSERT_TRUE(is_object_store);
    ASSERT_OK_AND_ASSIGN(is_object_store, FileSystem::IsObjectStore("s3://bucket/test.txt"));
    ASSERT_TRUE(is_object_store);
}

// --- seek
TEST_P(FileSystemTest, TestSeek) {
    std::string path = PathUtil::JoinPath(test_root_, "/test_file");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> out, fs_->Create(path, /*overwrite=*/true));
    std::string input = "paimon";
    ASSERT_OK_AND_ASSIGN(int32_t size, out->Write(input.data(), input.size()));
    ASSERT_EQ(size, input.size());
    ASSERT_OK(out->Close());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> in, fs_->Open(path));
    ASSERT_OK(in->Seek(/*offset=*/1, SeekOrigin::FS_SEEK_SET));
    ASSERT_OK_AND_ASSIGN(int64_t pos, in->GetPos());
    ASSERT_EQ(pos, 1);

    ASSERT_OK(in->Seek(/*offset=*/1, SeekOrigin::FS_SEEK_CUR));
    ASSERT_OK_AND_ASSIGN(int64_t pos2, in->GetPos());
    ASSERT_EQ(pos2, 2);

    ASSERT_OK(in->Seek(/*offset=*/-5, SeekOrigin::FS_SEEK_END));
    ASSERT_OK_AND_ASSIGN(int64_t pos3, in->GetPos());
    ASSERT_EQ(pos3, 1);

    std::string read_content(3, '\0');
    ASSERT_OK_AND_ASSIGN(int32_t read_len, in->Read(read_content.data(), read_content.size()));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ("aim", read_content);

    // read from offset
    ASSERT_OK_AND_ASSIGN(read_len,
                         in->Read(read_content.data(), read_content.size(), /*offset=*/2));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ("imo", read_content);

    ASSERT_OK_AND_ASSIGN(pos, in->GetPos());
    ASSERT_EQ(pos, 4);
}

TEST_P(FileSystemTest, TestSeek2) {
    std::string content = "abcdefghijk";
    std::string file_path = test_root_ + "/file.data";
    // write process
    ASSERT_OK_AND_ASSIGN(auto out_stream, fs_->Create(file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(int32_t write_len, out_stream->Write(content.data(), content.size()));
    ASSERT_EQ(write_len, content.size());
    ASSERT_OK(out_stream->Flush());
    ASSERT_OK(out_stream->Close());

    // read process
    ASSERT_OK_AND_ASSIGN(auto in_stream, fs_->Open(file_path));
    ASSERT_OK_AND_ASSIGN(auto pos, in_stream->GetPos());
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
    ASSERT_OK_AND_ASSIGN(int32_t read_len,
                         in_stream->Read(read_content.data(), read_content.size()));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ("ijk", read_content);

    // read from offset
    ASSERT_OK_AND_ASSIGN(read_len,
                         in_stream->Read(read_content.data(), read_content.size(), /*offset=*/4));
    ASSERT_EQ(read_len, read_content.size());
    ASSERT_EQ("efg", read_content);

    ASSERT_OK_AND_ASSIGN(pos, in_stream->GetPos());
    ASSERT_EQ(pos, 11);
    ASSERT_OK(in_stream->Close());
}

// --- rename
TEST_P(FileSystemTest, TestRename) {
    std::string path = PathUtil::JoinPath(test_root_, "/test_file");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> out, fs_->Create(path, /*overwrite=*/true));
    ASSERT_TRUE(out);
    std::string input = "paimon";
    ASSERT_OK_AND_ASSIGN(int32_t size, out->Write(input.data(), input.size()));
    ASSERT_EQ(size, input.size());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> in, fs_->Open(path));
    ASSERT_TRUE(in);
    char* data = new char[input.size() * 2];
    ASSERT_OK_AND_ASSIGN(int32_t size_read, in->Read(data, input.size(), /*offset=*/0));
    ASSERT_EQ(size_read, input.size());
    std::string read_data(data, input.size());
    ASSERT_EQ(read_data, input);
    delete[] data;
    ASSERT_OK(in->Close());

    std::string path2 = PathUtil::JoinPath(test_root_, "/test_file_renamed");
    ASSERT_OK(fs_->Rename(path, path2));

    std::string no_exist_path = PathUtil::JoinPath(test_root_, "/no_exist_file");
    ASSERT_NOK_WITH_MSG(
        fs_->Rename(no_exist_path, PathUtil::JoinPath(test_root_, "/no_exist_file_renamed")),
        "src file not exist");

    ASSERT_NOK_WITH_MSG(
        fs_->Rename(path2, PathUtil::JoinPath(test_root_, "/wrong_path_file_renamed/")),
        "src file is not a dir");
}

TEST_P(FileSystemTest, TestRename2) {
    {
        // test rename dir
        std::string dir_path = test_root_ + "/file_dir/";
        std::string dir_path2 = test_root_ + "/file_dir2/";
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
        ASSERT_OK(fs_->Rename(/*src=*/dir_path, /*dst=*/dir_path2));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path2));
        ASSERT_TRUE(is_exist);
    }
    {
        // test rename itself
        std::string dir_path = test_root_ + "/file_dir_itself/";
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
        ASSERT_NOK_WITH_MSG(fs_->Rename(/*src=*/dir_path, /*dst=*/dir_path),
                            "dst file already exist");
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
    }
    {
        // test rename from non-exist dir
        std::string dir_path = test_root_ + "/non_exist_file_dir/";
        std::string dir_path2 = test_root_ + "/non_exist_file_dir2/";
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
        ASSERT_NOK_WITH_MSG(fs_->Rename(/*src=*/dir_path, /*dst=*/dir_path2), "src file not exist");
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path2));
        ASSERT_FALSE(is_exist);
    }
    {
        // test rename to exist dir
        std::string dir_path = test_root_ + "/file_src_dir/";
        std::string dir_path2 = test_root_ + "/file_dst_dir/";
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK(fs_->Mkdirs(dir_path2));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path2));
        ASSERT_TRUE(is_exist);

        ASSERT_NOK_WITH_MSG(fs_->Rename(/*src=*/dir_path, /*dst=*/dir_path2),
                            "dst file already exist");
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path2));
        ASSERT_TRUE(is_exist);
    }
    {
        // test rename file
        std::string file_path = test_root_ + "/file1/file2/file3";
        ASSERT_OK(fs_->WriteFile(file_path, "content", /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_TRUE(is_exist);
        std::string file_path2 = test_root_ + "/file1/file4";
        ASSERT_OK(fs_->Rename(/*src=*/file_path, /*dst=*/file_path2));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path));
        ASSERT_FALSE(is_exist);

        std::string data;
        ASSERT_OK(fs_->ReadFile(file_path2, &data));
        ASSERT_EQ(data, "content");
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(test_root_ + "/file1/file2/"));
        ASSERT_TRUE(is_exist);
    }
    {
        // test rename file to exist file
        std::string file_path = test_root_ + "/file11/file12/file13";
        ASSERT_OK(fs_->WriteFile(file_path, "content", /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_TRUE(is_exist);
        std::string file_path2 = test_root_ + "/file15/file16/file13";
        ASSERT_OK(fs_->WriteFile(file_path2, "HelloWorld", /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path2));
        ASSERT_TRUE(is_exist);

        ASSERT_NOK_WITH_MSG(fs_->Rename(/*src=*/file_path, /*dst=*/file_path2),
                            "dst file already exist");
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path));
        ASSERT_TRUE(is_exist);
        std::string data;
        ASSERT_OK(fs_->ReadFile(file_path2, &data));
        ASSERT_EQ(data, "HelloWorld");
    }
    {
        // test rename from non-exist file
        std::string file_path = test_root_ + "/non_exist_file";
        std::string file_path2 = test_root_ + "/non_exist_file2";
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_FALSE(is_exist);
        ASSERT_NOK_WITH_MSG(fs_->Rename(/*src=*/file_path, /*dst=*/file_path2),
                            "src file not exist");
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path2));
        ASSERT_FALSE(is_exist);
    }
}

// --- exists
TEST_P(FileSystemTest, TestFileExists) {
    std::string file_path = CreateRandomFileInDirectory(test_root_);
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
    ASSERT_TRUE(is_exist);
}

TEST_P(FileSystemTest, TestFileDoesNotExist) {
    std::string path = PathUtil::JoinPath(test_root_, RandomName());
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(path));
    ASSERT_FALSE(is_exist);
}

TEST_P(FileSystemTest, TestExists) {
    std::string file_path = test_root_ + "/file.data";
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
    ASSERT_FALSE(is_exist);

    ASSERT_OK(fs_->WriteFile(file_path, "/content", /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path));
    ASSERT_TRUE(is_exist);

    std::string dir_path = test_root_ + "/file_dir/";
    ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
    ASSERT_FALSE(is_exist);

    ASSERT_OK(fs_->WriteFile(dir_path + "/file.data", "content", /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
    ASSERT_TRUE(is_exist);
}

// --- delete
TEST_P(FileSystemTest, TestExistingFileDeletion) {
    auto check = [&](bool recursive) {
        std::string file_path = CreateRandomFileInDirectory(test_root_);
        ASSERT_OK(fs_->Delete(file_path, recursive));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_FALSE(is_exist);
    };
    check(true);
    check(false);
}

TEST_P(FileSystemTest, TestNotExistingFileDeletion) {
    auto check = [&](bool recursive) {
        std::string path = PathUtil::JoinPath(test_root_, RandomName());
        ASSERT_TRUE(fs_->Delete(path, recursive).IsIOError());
    };
    check(true);
    check(false);
}

TEST_P(FileSystemTest, TestExistingEmptyDirectoryDeletion) {
    auto check = [&](bool recursive) {
        std::string path = PathUtil::JoinPath(test_root_, RandomName());
        ASSERT_OK(fs_->Mkdirs(path));
        ASSERT_OK(fs_->Delete(path, recursive));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(path));
        ASSERT_FALSE(is_exist);
    };
    check(true);
    check(false);
}

TEST_P(FileSystemTest, TestExistingNonEmptyDirectoryRecursiveDeletion) {
    {
        std::string file_path = CreateRandomFileInDirectory(test_root_);
        ASSERT_OK(fs_->Delete(test_root_, /*recursive=*/true));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_FALSE(is_exist);
    }
    {
        std::string file_path = CreateRandomFileInDirectory(test_root_);
        ASSERT_NOK(fs_->Delete(test_root_, /*recursive=*/false));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_TRUE(is_exist);
    }
}

TEST_P(FileSystemTest, TestExistingNonEmptyDirectoryWithSubDirRecursiveDeletion) {
    {
        std::string level1_subdir_with_file = PathUtil::JoinPath(test_root_, RandomName());
        const std::string file_in_level1_subdir =
            CreateRandomFileInDirectory(level1_subdir_with_file);
        std::string level2_subdir_with_file =
            PathUtil::JoinPath(level1_subdir_with_file, RandomName());
        const std::string file_in_level2_subdir =
            CreateRandomFileInDirectory(level2_subdir_with_file);
        ASSERT_OK(fs_->Delete(test_root_, /*recursive=*/true));
        ASSERT_FALSE(fs_->Exists(file_in_level1_subdir).value());
        ASSERT_FALSE(fs_->Exists(level2_subdir_with_file).value());
        ASSERT_FALSE(fs_->Exists(file_in_level2_subdir).value());
    }
    {
        std::string level1_subdir_with_file = PathUtil::JoinPath(test_root_, RandomName());
        const std::string file_in_level1_subdir =
            CreateRandomFileInDirectory(level1_subdir_with_file);
        std::string level2_subdir_with_file =
            PathUtil::JoinPath(level1_subdir_with_file, RandomName());
        const std::string file_in_level2_subdir =
            CreateRandomFileInDirectory(level2_subdir_with_file);
        ASSERT_NOK(fs_->Delete(test_root_, /*recursive=*/false));
        ASSERT_TRUE(fs_->Exists(file_in_level1_subdir).value());
        ASSERT_TRUE(fs_->Exists(level2_subdir_with_file).value());
        ASSERT_TRUE(fs_->Exists(file_in_level2_subdir).value());
    }
}

TEST_P(FileSystemTest, TestDelete) {
    {
        std::string dir_path = test_root_ + "/file_dir/";
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
        ASSERT_OK(fs_->Delete(dir_path, /*recursive=*/true));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
    }
    {
        // test recursive delete
        std::string dir_path = test_root_ + "/file_dir1/file_dir2/file_dir3";
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);

        // rm file_dir1/file_dir2/file_dir3/
        ASSERT_OK(fs_->Delete(dir_path, /*recursive=*/true));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(test_root_ + "/file_dir1/"));
        ASSERT_TRUE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(test_root_ + "/file_dir1/file_dir2"));
        ASSERT_TRUE(is_exist);

        // rm file_dir1/
        ASSERT_OK(fs_->Delete(test_root_ + "/file_dir1/", /*recursive=*/true));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(test_root_ + "/file_dir1/"));
        ASSERT_FALSE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(test_root_ + "/file_dir1/file_dir2"));
        ASSERT_FALSE(is_exist);
    }
    {
        // test delete file
        std::string file_path = test_root_ + "/file1/file2/file3";
        ASSERT_OK(fs_->WriteFile(file_path, "content", /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_TRUE(is_exist);

        ASSERT_OK(fs_->Delete(file_path, /*recursive=*/true));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path));
        ASSERT_FALSE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(test_root_ + "/file1/file2/"));
        ASSERT_TRUE(is_exist);

        ASSERT_NOK_WITH_MSG(fs_->Delete(test_root_ + "/file1", /*recursive=*/false),
                            "is not empty");
    }
    {
        // test recursive delete
        std::string file_path = test_root_ + "/file1/file2/file3";
        ASSERT_OK(fs_->WriteFile(file_path, "content", /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_TRUE(is_exist);

        ASSERT_OK(fs_->Delete(test_root_ + "/file1/", /*recursive=*/true));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path));
        ASSERT_FALSE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(test_root_ + "/file1/file2/"));
        ASSERT_FALSE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(test_root_ + "/file1/"));
        ASSERT_FALSE(is_exist);
    }
    {
        // test recursive delete
        std::string dir_path = test_root_ + "/file_recursive_dir1/file_dir2/file_dir3";
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);

        ASSERT_NOK_WITH_MSG(fs_->Delete(test_root_ + "/file_recursive_dir1", /*recursive=*/false),
                            "is not empty");
    }
    {
        // test delete non-exist file
        std::string file_path = test_root_ + "/file_non_exist";
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_FALSE(is_exist);
        ASSERT_NOK(fs_->Delete(file_path, /*recursive=*/false));
    }
}

// --- mkdirs
TEST_P(FileSystemTest, TestMkdirsReturnsTrueWhenCreatingDirectory) {
    ASSERT_OK(fs_->Mkdirs(test_root_));
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(test_root_));
    ASSERT_TRUE(is_exist);
}

TEST_P(FileSystemTest, TestMkdirsCreatesParentDirectories) {
    std::string deep_path = PathUtil::JoinPath(test_root_, RandomName());
    ASSERT_OK(fs_->Mkdirs(deep_path));
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(deep_path));
    ASSERT_TRUE(is_exist);
}

TEST_P(FileSystemTest, TestMkdirsReturnsTrueForExistingDirectory) {
    std::string file_path = CreateRandomFileInDirectory(test_root_);
    ASSERT_OK(fs_->Mkdirs(test_root_));
}

TEST_P(FileSystemTest, TestMkdirsFailsForExistingFile) {
    std::string file_path = CreateRandomFileInDirectory(test_root_);
    auto status = fs_->Mkdirs(file_path);
    ASSERT_TRUE(status.IsIOError());
}

TEST_P(FileSystemTest, TestMkdirsFailsWithExistingParentFile) {
    std::string file_path = CreateRandomFileInDirectory(test_root_);
    std::string dir_under_file = PathUtil::JoinPath(file_path, RandomName());
    ASSERT_TRUE(fs_->Mkdirs(dir_under_file).IsIOError());
}

TEST_P(FileSystemTest, TestMkdir) {
    {
        std::string path = PathUtil::JoinPath(test_root_, "/tmp.txt/tmpB");
        ASSERT_OK(fs_->Mkdirs(path));
    }
    {
        std::string path = PathUtil::JoinPath(test_root_, "/tmpA/tmpB/");
        ASSERT_OK(fs_->Mkdirs(path));
    }
    {
        std::string path = "/";
        ASSERT_OK(fs_->Mkdirs(path));
    }
}

TEST_P(FileSystemTest, TestMkdir2) {
    {
        std::string dir_path = test_root_ + "/file_dir/";
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
    }
    {
        // test recursive mkdir
        std::string dir_path = test_root_ + "/file_dir1/file_dir2/file_dir3";
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(test_root_ + "/file_dir1/"));
        ASSERT_TRUE(is_exist);
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(test_root_ + "/file_dir1/file_dir2"));
        ASSERT_TRUE(is_exist);
    }
}

// test for create multi dir such as "/table/partition1/bucket1" and "/table/partition1/bucket2"
TEST_P(FileSystemTest, TestMkdirMultiThread) {
    uint32_t runs_count = 10;
    uint32_t thread_count = 10;
    auto executor = CreateDefaultExecutor(thread_count);

    for (uint32_t i = 0; i < runs_count; i++) {
        std::string uuid;
        ASSERT_TRUE(UUID::Generate(&uuid));
        std::vector<std::future<void>> futures;
        for (uint32_t thread_idx = 0; thread_idx < thread_count; thread_idx++) {
            futures.push_back(Via(executor.get(), [this, &uuid, thread_idx]() -> void {
                std::string dir_path =
                    PathUtil::JoinPath(test_root_, uuid + "/" + std::to_string(thread_idx));
                ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
                ASSERT_FALSE(is_exist);
                ASSERT_OK(fs_->Mkdirs(dir_path));
                ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
                ASSERT_TRUE(is_exist);
            }));
        }
        Wait(futures);
    }
}

// test for create multi dir such as "/table/partition1" and "/table/partition1"
TEST_P(FileSystemTest, TestMkdirMultiThread2) {
    uint32_t runs_count = 10;
    uint32_t thread_count = 10;
    auto executor = CreateDefaultExecutor(thread_count);

    for (uint32_t i = 0; i < runs_count; i++) {
        std::string uuid;
        ASSERT_TRUE(UUID::Generate(&uuid));
        std::vector<std::future<void>> futures;
        for (uint32_t thread_idx = 0; thread_idx < thread_count; thread_idx++) {
            futures.push_back(Via(executor.get(), [this, &uuid]() -> void {
                std::string dir_path = PathUtil::JoinPath(test_root_, uuid);
                ASSERT_OK(fs_->Mkdirs(dir_path));
                ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
                ASSERT_TRUE(is_exist);
            }));
        }
        Wait(futures);
    }
}

// test for create multi dir such as "partition1" and "partition1" (relative path)
TEST_P(FileSystemTest, TestMkdirMultiThread3) {
    uint32_t runs_count = 10;
    uint32_t thread_count = 10;
    auto executor = CreateDefaultExecutor(thread_count);

    for (uint32_t i = 0; i < runs_count; i++) {
        std::string uuid;
        ASSERT_TRUE(UUID::Generate(&uuid));
        std::vector<std::future<void>> futures;
        for (uint32_t thread_idx = 0; thread_idx < thread_count; thread_idx++) {
            futures.push_back(Via(executor.get(), [this, &uuid]() -> void {
                std::string dir_path = uuid;
                ASSERT_OK(fs_->Mkdirs(dir_path));
                ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
                ASSERT_TRUE(is_exist);
            }));
        }
        Wait(futures);
    }
}

TEST_P(FileSystemTest, TestInvalidMkdir) {
    {
        // test mkdir with one exist dir
        std::string dir_path = test_root_ + "/file_dir/";
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
    }
    {
        // test mkdir with one exist file with same name
        std::string file_path = test_root_ + "/file_dir/file.data";
        ASSERT_OK(fs_->WriteFile(file_path, "content", /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_TRUE(is_exist);

        ASSERT_NOK_WITH_MSG(fs_->Mkdirs(file_path), "already exists");
    }
}

// --- file status
TEST_P(FileSystemTest, TestGetFileStatus1) {
    {
        // test dir simple
        std::string dir_path = test_root_ + "/file_dir/";
        ASSERT_OK(fs_->Mkdirs(dir_path));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_TRUE(is_exist);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> st, fs_->GetFileStatus(dir_path));
        ASSERT_EQ(RemoveLastSlashInPath(st->GetPath()), RemoveLastSlashInPath(dir_path));
        ASSERT_TRUE(st->IsDir());
        auto modification_time = st->GetModificationTime();
        ASSERT_TRUE(modification_time > 10000000000L);
        ASSERT_TRUE(modification_time < 10000000000000L);

        std::string file_path = test_root_ + "/file_dir/file.data";
        ASSERT_OK(fs_->WriteFile(file_path, "content", /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path));
        ASSERT_TRUE(is_exist);

        // check meta in dir
        ASSERT_OK_AND_ASSIGN(st, fs_->GetFileStatus(dir_path));
        ASSERT_EQ(RemoveLastSlashInPath(st->GetPath()), RemoveLastSlashInPath(dir_path));
        ASSERT_TRUE(st->IsDir());
        modification_time = st->GetModificationTime();
        ASSERT_TRUE(modification_time > 10000000000L);
        ASSERT_TRUE(modification_time < 10000000000000L);
    }
    {
        // test non-exist dir
        std::string dir_path = test_root_ + "/non_exist_file_dir/";
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
        ASSERT_NOK(fs_->GetFileStatus(dir_path));
    }
    {
        // test file simple
        std::string content = "content";
        std::string file_path = test_root_ + "/file.data";
        ASSERT_OK(fs_->WriteFile(file_path, content, /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_TRUE(is_exist);

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> st, fs_->GetFileStatus(file_path));
        ASSERT_EQ(st->GetPath(), file_path);
        ASSERT_FALSE(st->IsDir());
        ASSERT_EQ(st->GetLen(), content.size());
        auto modification_time = st->GetModificationTime();
        ASSERT_TRUE(modification_time > 10000000000L);
        ASSERT_TRUE(modification_time < 10000000000000L);
    }
    {
        // test non-exist file
        std::string dir_path = test_root_ + "/non_exist_file";
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(dir_path));
        ASSERT_FALSE(is_exist);
        ASSERT_NOK(fs_->GetFileStatus(dir_path));
    }
}

TEST_P(FileSystemTest, TestGetFileStatus2) {
    const std::string test_path = GetTestDir() + "orc/append_09.db/append_09";
    {
        // input is a dir, with a trailing '/'
        std::string dir_name = test_path + "/";
        std::vector<std::unique_ptr<FileStatus>> status_list;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> file_status, fs_->GetFileStatus(dir_name));
        status_list.emplace_back(std::move(file_status));
        CheckFileStatus(status_list, /*expected_files=*/{}, /*expected_dirs=*/{dir_name});
    }
    {
        // input is a dir, without a trailing '/'
        std::vector<std::unique_ptr<FileStatus>> status_list;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> file_status,
                             fs_->GetFileStatus(test_path));
        status_list.emplace_back(std::move(file_status));
        CheckFileStatus(status_list, /*expected_files=*/{}, /*expected_dirs=*/{test_path});
    }
    {
        // input is a file
        std::vector<std::unique_ptr<FileStatus>> status_list;
        std::string file_name = PathUtil::JoinPath(test_path, "README");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> file_status,
                             fs_->GetFileStatus(file_name));
        status_list.emplace_back(std::move(file_status));
        CheckFileStatus(status_list, /*expected_files=*/{file_name}, /*expected_dirs=*/{});
    }
    {
        // input is not exist
        std::vector<std::unique_ptr<FileStatus>> status_list;
        ASSERT_NOK(fs_->GetFileStatus(PathUtil::JoinPath(test_path, "NOT_EXIST")));
    }
}

TEST_P(FileSystemTest, TestInvalidListFileStatus) {
    {
        // list non exist dir will return ok
        std::vector<std::unique_ptr<FileStatus>> file_status_list;
        ASSERT_OK(fs_->ListFileStatus(test_root_ + "/non-exist/", &file_status_list));
        ASSERT_TRUE(file_status_list.empty());
    }
    {
        // test data path
        std::string file_path = test_root_ + "/file.data";
        ASSERT_OK(fs_->WriteFile(file_path, "hello", /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
        ASSERT_TRUE(is_exist);
        std::vector<std::unique_ptr<FileStatus>> file_status_list;
        ASSERT_OK(fs_->ListFileStatus(file_path, &file_status_list));
        ASSERT_EQ(file_status_list[0]->GetPath(), file_path);
        ASSERT_EQ(file_status_list[0]->GetLen(), 5);
        ASSERT_FALSE(file_status_list[0]->IsDir());
    }
}

TEST_P(FileSystemTest, TestListFileStatus1) {
    std::string file_path = test_root_ + "/file_dir1/file.data";
    ASSERT_OK(fs_->WriteFile(file_path, "content", /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
    ASSERT_TRUE(is_exist);

    std::vector<std::unique_ptr<FileStatus>> file_status_list;
    ASSERT_OK(fs_->ListFileStatus(test_root_, &file_status_list));
    ASSERT_EQ(file_status_list.size(), 1);
    std::set<std::string> expected_dirs = {test_root_ + "/file_dir1/"};
    CheckFileStatus(file_status_list, /*expected_files=*/{}, expected_dirs);

    auto dir_path2 = test_root_ + "/file_dir2/";
    ASSERT_OK(fs_->Mkdirs(dir_path2));
    ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path2));
    ASSERT_TRUE(is_exist);

    auto dir_path3 = test_root_ + "/file_dir1/file_dir3/";
    ASSERT_OK(fs_->Mkdirs(dir_path3));
    ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path3));
    ASSERT_TRUE(is_exist);

    std::string file_path2 = test_root_ + "/file_dir1/second_file.data";
    ASSERT_OK(fs_->WriteFile(file_path2, "hello!", /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(file_path2));
    ASSERT_TRUE(is_exist);

    std::vector<std::unique_ptr<FileStatus>> file_status_list2;
    ASSERT_OK(fs_->ListFileStatus(test_root_, &file_status_list2));
    ASSERT_EQ(file_status_list2.size(), 2);
    expected_dirs = {test_root_ + "/file_dir1/", dir_path2};
    CheckFileStatus(file_status_list2, /*expected_files=*/{}, expected_dirs);

    std::vector<std::unique_ptr<FileStatus>> file_status_list3;
    ASSERT_OK(fs_->ListFileStatus(test_root_ + "/file_dir1/", &file_status_list3));
    ASSERT_EQ(file_status_list3.size(), 3);
    expected_dirs = {dir_path3};
    std::set<std::string> expected_files = {file_path, file_path2};
    CheckFileStatus(file_status_list3, expected_files, expected_dirs);
}

TEST_P(FileSystemTest, TestListFileStatus2) {
    const std::string test_path = GetTestDir() + "orc/append_09.db/append_09";
    const std::set<std::string> expected_files = {PathUtil::JoinPath(test_path, "README")};
    const std::set<std::string> expected_dirs = {
        PathUtil::JoinPath(test_path, "f1=10"),    PathUtil::JoinPath(test_path, "commit_messages"),
        PathUtil::JoinPath(test_path, "f1=20"),    PathUtil::JoinPath(test_path, "data_splits"),
        PathUtil::JoinPath(test_path, "manifest"), PathUtil::JoinPath(test_path, "schema"),
        PathUtil::JoinPath(test_path, "snapshot")};
    {
        // input is a dir, with a trailing '/'
        std::vector<std::unique_ptr<FileStatus>> status_list;
        ASSERT_OK(fs_->ListFileStatus(test_path + "/", &status_list));
        CheckFileStatus(status_list, expected_files, expected_dirs);
    }
    {
        // input is a dir, without a trailing '/'
        std::vector<std::unique_ptr<FileStatus>> status_list;
        ASSERT_OK(fs_->ListFileStatus(test_path, &status_list));
        CheckFileStatus(status_list, expected_files, expected_dirs);
    }
    {
        // input is a file
        std::vector<std::unique_ptr<FileStatus>> status_list;
        ASSERT_OK(fs_->ListFileStatus(PathUtil::JoinPath(test_path, "README"), &status_list));
        CheckFileStatus(status_list, expected_files, /*expected_dirs=*/{});
    }
    {
        // input is not exist
        std::vector<std::unique_ptr<FileStatus>> status_list;
        ASSERT_OK(fs_->ListFileStatus(PathUtil::JoinPath(test_path, "NOT_EXIST"), &status_list));
        CheckFileStatus(status_list, /*expected_files=*/{}, /*expected_dirs=*/{});
    }
}

TEST_P(FileSystemTest, TestListDir1) {
    std::string file_path = test_root_ + "/file_dir1/file.data";
    ASSERT_OK(fs_->WriteFile(file_path, "content", /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
    ASSERT_TRUE(is_exist);

    auto dir_path1 = test_root_ + "/file_dir1/";
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    ASSERT_OK(fs_->ListDir(test_root_, &file_status_list));
    ASSERT_EQ(file_status_list.size(), 1);
    std::set<std::string> expected_dirs = {dir_path1};
    CheckBasicFileStatus(file_status_list, std::set<std::string>(), expected_dirs);

    auto dir_path2 = test_root_ + "/file_dir2/";
    ASSERT_OK(fs_->Mkdirs(dir_path2));
    ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path2));
    ASSERT_TRUE(is_exist);

    auto dir_path3 = test_root_ + "/file_dir1/file_dir3/";
    ASSERT_OK(fs_->Mkdirs(dir_path3));
    ASSERT_OK_AND_ASSIGN(is_exist, fs_->Exists(dir_path3));
    ASSERT_TRUE(is_exist);

    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list2;
    ASSERT_OK(fs_->ListDir(test_root_, &file_status_list2));
    ASSERT_EQ(file_status_list2.size(), 2);
    expected_dirs = {dir_path1, dir_path2};
    CheckBasicFileStatus(file_status_list2, std::set<std::string>(), expected_dirs);

    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list3;
    ASSERT_OK(fs_->ListDir(dir_path1, &file_status_list3));
    ASSERT_EQ(file_status_list3.size(), 2);
    std::set<std::string> expected_files = {file_path};
    expected_dirs = {dir_path3};
    CheckBasicFileStatus(file_status_list3, expected_files, expected_dirs);

    // list non exist dir will return ok
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list4;
    ASSERT_OK(fs_->ListDir(test_root_ + "/non-exist/", &file_status_list4));
    ASSERT_TRUE(file_status_list4.empty());

    // list invalid path, a data file path
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list5;
    ASSERT_NOK_WITH_MSG(fs_->ListDir(file_path, &file_status_list5), "is not a directory");
}

TEST_P(FileSystemTest, TestListDir2) {
    const std::string test_path = GetTestDir() + "orc/append_09.db/append_09";
    const std::set<std::string> expected_files = {PathUtil::JoinPath(test_path, "README")};
    const std::set<std::string> expected_dirs = {
        PathUtil::JoinPath(test_path, "f1=10"),    PathUtil::JoinPath(test_path, "commit_messages"),
        PathUtil::JoinPath(test_path, "f1=20"),    PathUtil::JoinPath(test_path, "data_splits"),
        PathUtil::JoinPath(test_path, "manifest"), PathUtil::JoinPath(test_path, "schema"),
        PathUtil::JoinPath(test_path, "snapshot")};
    {
        // input is a dir, with a trailing '/'
        std::vector<std::unique_ptr<BasicFileStatus>> status_list;
        ASSERT_OK(fs_->ListDir(test_path + "/", &status_list));
        CheckBasicFileStatus(status_list, expected_files, expected_dirs);
    }
    {
        // input is a dir, without a trailing '/'
        std::vector<std::unique_ptr<BasicFileStatus>> status_list;
        ASSERT_OK(fs_->ListDir(test_path, &status_list));
        CheckBasicFileStatus(status_list, expected_files, expected_dirs);
    }
    {
        // input is a file
        std::vector<std::unique_ptr<BasicFileStatus>> status_list;
        ASSERT_NOK_WITH_MSG(fs_->ListDir(PathUtil::JoinPath(test_path, "README"), &status_list),
                            "file " + PathUtil::JoinPath(test_path, "README") +
                                " already exists and is not a directory");
    }
    {
        // input is not exist
        std::vector<std::unique_ptr<BasicFileStatus>> status_list;
        ASSERT_OK(fs_->ListDir(PathUtil::JoinPath(test_path, "NOT_EXIST"), &status_list));
        CheckBasicFileStatus(status_list, /*expected_files=*/{},
                             /*expected_dirs=*/{});
    }
}

// --- exception
TEST_P(FileSystemTest, TestGetNotExistFileStatus) {
    std::string path = PathUtil::JoinPath(test_root_, RandomName());
    ASSERT_NOK(fs_->GetFileStatus(path));
}

// --- atomic store
TEST_P(FileSystemTest, TestAtomicStore) {
    std::string path = PathUtil::JoinPath(test_root_, RandomName());
    std::string content = "test_content";
    ASSERT_OK(fs_->AtomicStore(path, content));
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(path));
    ASSERT_TRUE(is_exist);
}

TEST_P(FileSystemTest, TestAtomicStoreAlreadyExist) {
    std::string file_path = CreateRandomFileInDirectory(test_root_);
    std::string content = "test_content";
    ASSERT_NOK(fs_->AtomicStore(file_path, content));
    ASSERT_OK_AND_ASSIGN(bool is_exist, fs_->Exists(file_path));
    ASSERT_TRUE(is_exist);
}

INSTANTIATE_TEST_SUITE_P(UseLocal, FileSystemTest, ::testing::Values("local" /*, "jindo"*/));

}  // namespace paimon::test
