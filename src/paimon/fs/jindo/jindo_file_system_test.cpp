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

}  // namespace paimon::jindo::test
