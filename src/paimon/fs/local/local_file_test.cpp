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

#include "paimon/fs/local/local_file.h"

#include <cstring>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(LocalFileTest, TestReadWriteEmptyContent) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    LocalFile dir = LocalFile(test_root);
    if (dir.Exists().ok()) {
        ASSERT_TRUE(dir.Delete().ok());
    }
    ASSERT_OK_AND_ASSIGN(bool success, dir.Mkdir());
    ASSERT_TRUE(success);
    std::string path = test_root + "/test.txt";
    LocalFile file = LocalFile(path);
    if (file.Exists().ok()) {
        ASSERT_TRUE(file.Delete().ok());
    }
    ASSERT_OK_AND_ASSIGN(bool is_exist, file.Exists());
    ASSERT_FALSE(is_exist);

    ASSERT_OK(file.OpenFile(/*is_read_file=*/false));

    const char* str = "";
    const int32_t str_size = 0;
    ASSERT_OK_AND_ASSIGN(int32_t write_size, file.Write(str, str_size));
    ASSERT_EQ(write_size, str_size);

    ASSERT_OK(file.Flush());
    ASSERT_TRUE(file.Exists().value());

    ASSERT_OK(file.Close());

    LocalFile file2(path);
    ASSERT_OK(file2.OpenFile(/*is_read_file=*/true));
    char buffer[10];
    ASSERT_OK_AND_ASSIGN(int32_t read_len, file2.Read(buffer, 10));
    ASSERT_EQ(0, read_len);
}

TEST(LocalFileTest, TestSimple) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    LocalFile dir = LocalFile(test_root);
    if (dir.Exists().ok()) {
        ASSERT_OK(dir.Delete());
    }
    ASSERT_OK_AND_ASSIGN(bool success, dir.Mkdir());
    ASSERT_TRUE(success);
    std::string path = test_root + "/test.txt";
    LocalFile file = LocalFile(path);
    if (file.Exists().ok()) {
        ASSERT_OK(file.Delete());
    }
    ASSERT_OK_AND_ASSIGN(bool is_exist, file.Exists());
    ASSERT_FALSE(is_exist);

    ASSERT_OK(file.OpenFile(/*is_read_file=*/false));

    const char* str = "test_data";
    const int32_t str_size = 9;
    ASSERT_OK_AND_ASSIGN(int32_t write_size, file.Write(str, str_size));
    ASSERT_EQ(write_size, str_size);

    ASSERT_OK(file.Flush());
    ASSERT_OK(file.Close());

    ASSERT_OK_AND_ASSIGN(bool is_file, file.IsFile());
    ASSERT_TRUE(is_file);

    ASSERT_OK_AND_ASSIGN(bool is_dir, file.IsDir());
    ASSERT_FALSE(is_dir);

    std::vector<std::string> file_list;
    ASSERT_NOK(file.List(&file_list));

    ASSERT_OK_AND_ASSIGN(size_t len, file.Length());
    ASSERT_EQ(len, str_size);

    LocalFile file2 = LocalFile(path);
    ASSERT_OK(file2.Exists());

    ASSERT_OK(file2.OpenFile(true));
    char str_read[str_size + 1];
    {
        ASSERT_OK_AND_ASSIGN(int32_t read_size, file2.Read(str_read, 4));
        ASSERT_EQ(read_size, 4);
        str_read[read_size] = '\0';
        ASSERT_EQ(strcmp(str_read, "test"), 0);
    }
    {
        ASSERT_OK_AND_ASSIGN(int64_t pos, file2.Tell());
        ASSERT_EQ(pos, 4);
        ASSERT_OK(file2.Seek(5, SEEK_SET));
        ASSERT_OK_AND_ASSIGN(int32_t read_size, file2.Read(str_read, 4));
        ASSERT_EQ(read_size, 4);
        str_read[read_size] = '\0';
        ASSERT_EQ(strcmp(str_read, "data"), 0);
    }
    {
        ASSERT_OK_AND_ASSIGN(int32_t read_size, file2.Read(str_read, str_size, 0));
        ASSERT_EQ(read_size, str_size);
        str_read[read_size] = '\0';
        ASSERT_EQ(strcmp(str_read, "test_data"), 0);
    }

    // dir already exists
    ASSERT_OK_AND_ASSIGN(success, dir.Mkdir());
    ASSERT_FALSE(success);

    ASSERT_OK(file2.Delete());
    ASSERT_FALSE(file2.Exists().value());
}

TEST(LocalFileTest, TestUsage) {
    std::string test_root = "local_file_test_usage";
    LocalFile dir = LocalFile(test_root);
    ASSERT_OK_AND_ASSIGN(bool success, dir.Mkdir());
    ASSERT_TRUE(success);
    std::vector<std::string> file_list;
    ASSERT_OK(dir.List(&file_list));
    std::string path_deep_dir = test_root + "/tmp2";
    LocalFile deep_dir = LocalFile(path_deep_dir);
    ASSERT_OK_AND_ASSIGN(success, deep_dir.Mkdir());
    ASSERT_TRUE(success);
    LocalFile parent_deep_dir = deep_dir.GetParentFile();
    ASSERT_EQ(parent_deep_dir.GetAbsolutePath(), test_root);
    ASSERT_OK(deep_dir.Delete());
    ASSERT_OK(parent_deep_dir.Delete());
    ASSERT_OK(dir.Delete());
}

TEST(LocalFileTest, TestOpenFile) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    LocalFile dir = LocalFile(test_root);
    if (dir.Exists().ok()) {
        ASSERT_OK(dir.Delete());
    }
    ASSERT_OK_AND_ASSIGN(bool success, dir.Mkdir());
    ASSERT_TRUE(success);
    std::string path = test_root + "/test.txt";
    LocalFile file = LocalFile(path);
    if (file.Exists().ok()) {
        ASSERT_OK(file.Delete());
    }
    ASSERT_OK_AND_ASSIGN(bool is_exist, file.Exists());
    ASSERT_FALSE(is_exist);

    ASSERT_NOK_WITH_MSG(file.OpenFile(/*is_read_file=*/true), "file not exist");
    ASSERT_NOK_WITH_MSG(dir.OpenFile(/*is_read_file=*/true), "cannot open a directory");

    std::string path3 = "test.txt";
    LocalFile file3 = LocalFile(path3);
    ASSERT_OK(file3.OpenFile(/*is_read_file=*/false));
    ASSERT_OK_AND_ASSIGN(int64_t modify_time, file3.LastModifiedTimeMs());
    ASSERT_GE(modify_time, -1);

    LocalFile dir2 = LocalFile("/");
    ASSERT_OK_AND_ASSIGN(success, dir2.Mkdir());
    ASSERT_FALSE(success);
    LocalFile dir3 = LocalFile(test_root + "/");
    ASSERT_OK_AND_ASSIGN(success, dir3.Mkdir());
    ASSERT_FALSE(success);
}

TEST(LocalFileTest, TestMkdir) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    {
        LocalFile dir = LocalFile(test_root + "tmp/local/f/1");
        ASSERT_OK_AND_ASSIGN(bool success, dir.Mkdir());
        ASSERT_FALSE(success);
    }
    {
        LocalFile dir = LocalFile(test_root + "tmp1");
        ASSERT_OK_AND_ASSIGN(bool success, dir.Mkdir());
        ASSERT_TRUE(success);
    }
    {
        LocalFile dir = LocalFile(test_root + "tmp1/f2/");
        ASSERT_OK_AND_ASSIGN(bool success, dir.Mkdir());
        ASSERT_TRUE(success);
    }
    {
        LocalFile dir = LocalFile("/");
        ASSERT_OK_AND_ASSIGN(bool success, dir.Mkdir());
        ASSERT_FALSE(success);
    }
    {
        LocalFile dir = LocalFile("");
        ASSERT_OK_AND_ASSIGN(bool success, dir.Mkdir());
        ASSERT_FALSE(success);
    }
}

}  // namespace paimon::test
