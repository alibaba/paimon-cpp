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

#include "paimon/common/fs/resolving_file_system.h"

#include <future>
#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/executor.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/fs/local/local_file_system_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class GmockFileSystem : public FileSystem {
 public:
    GmockFileSystem(const std::string& identifier, const std::string& authority)
        : identifier_(identifier), authority_(authority) {}

    MOCK_METHOD(Result<std::unique_ptr<InputStream>>, Open, (const std::string& path),
                (const, override));
    MOCK_METHOD(Result<std::unique_ptr<OutputStream>>, Create,
                (const std::string& path, bool overwrite), (const, override));
    MOCK_METHOD(Status, Mkdirs, (const std::string& path), (const, override));
    MOCK_METHOD(Status, Rename, (const std::string& src, const std::string& dst),
                (const, override));
    MOCK_METHOD(Status, Delete, (const std::string& path, bool recursive), (const, override));
    MOCK_METHOD(Result<std::unique_ptr<FileStatus>>, GetFileStatus, (const std::string& path),
                (const, override));
    MOCK_METHOD(Status, ListDir,
                (const std::string& directory,
                 std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list),
                (const, override));
    MOCK_METHOD(Status, ListFileStatus,
                (const std::string& path,
                 std::vector<std::unique_ptr<FileStatus>>* file_status_list),
                (const, override));
    MOCK_METHOD(Result<bool>, Exists, (const std::string& path), (const, override));

    const std::string& GetIdentifier() const {
        return identifier_;
    }
    const std::string& GetAuthority() const {
        return authority_;
    }

 private:
    std::string identifier_;
    std::string authority_;
};

class GmockFileSystemFactory : public FileSystemFactory {
 public:
    explicit GmockFileSystemFactory(const std::string& identifier) : identifier_(identifier) {}

    const char* Identifier() const override {
        return identifier_.c_str();
    }

    Result<std::unique_ptr<FileSystem>> Create(
        const std::string& path_str,
        const std::map<std::string, std::string>& options) const override {
        PAIMON_ASSIGN_OR_RAISE(Path path, PathUtil::ToPath(path_str));
        return std::make_unique<testing::NiceMock<GmockFileSystem>>(identifier_, path.authority);
    }

 private:
    std::string identifier_;
};

class ResolvingFileSystemTest : public testing::Test {
 public:
    void SetUp() override {
        auto factory_creator = FactoryCreator::GetInstance();

        // Register mock factories
        factory_creator->Register("mock_local", new GmockFileSystemFactory("mock_local"));
        factory_creator->Register("mock_hdfs", new GmockFileSystemFactory("mock_hdfs"));
        factory_creator->Register("mock_oss", new GmockFileSystemFactory("mock_oss"));
    }

    void TearDown() override {
        auto factory_creator = FactoryCreator::GetInstance();
        factory_creator->TEST_Unregister("mock_local");
        factory_creator->TEST_Unregister("mock_hdfs");
        factory_creator->TEST_Unregister("mock_oss");
    }
};

TEST_F(ResolvingFileSystemTest, GetRealFileSystemWithFileScheme) {
    std::map<std::string, std::string> scheme_mapping = {
        {"file", "mock_local"}, {"hdfs", "mock_hdfs"}, {"oss", "mock_oss"}};
    ResolvingFileSystem resolving_fs(scheme_mapping, "mock_local", /*options=*/{});

    auto check_result = [&](const std::string& uri, const std::string& expected_identifier) {
        ASSERT_OK_AND_ASSIGN(auto real_fs, resolving_fs.GetRealFileSystem(uri));
        auto casted_fs = dynamic_cast<GmockFileSystem*>(real_fs.get());
        ASSERT_TRUE(casted_fs);
        ASSERT_EQ(casted_fs->GetIdentifier(), expected_identifier);
    };
    check_result("file:///tmp/test.txt", "mock_local");
    check_result("hdfs://node:9000/tmp/test.txt", "mock_hdfs");
    check_result("oss://bucket/tmp/test.txt", "mock_oss");
    // Empty scheme should map to "file" scheme
    check_result("/tmp/test.txt", "mock_local");
    check_result("tmp/test.txt", "mock_local");
    // Unknown scheme should use default file system
    check_result("s3://bucket/tmp/test.txt", "mock_local");
}

TEST_F(ResolvingFileSystemTest, EmptySchemeMapping) {
    std::map<std::string, std::string> empty_scheme_mapping;
    ResolvingFileSystem resolving_fs(empty_scheme_mapping, "mock_local", /*options=*/{});

    // Should use default file system for all schemes
    ASSERT_OK_AND_ASSIGN(auto real_fs, resolving_fs.GetRealFileSystem("any_scheme://path/test"));
    auto casted_fs = dynamic_cast<GmockFileSystem*>(real_fs.get());
    ASSERT_TRUE(casted_fs);
    ASSERT_EQ(casted_fs->GetIdentifier(), "mock_local");
}

TEST_F(ResolvingFileSystemTest, FileSystemCaching) {
    std::map<std::string, std::string> scheme_mapping = {{"file", "mock_local"}};
    ResolvingFileSystem resolving_fs(scheme_mapping, "mock_local", /*options=*/{});

    // First call should create and cache the file system
    ASSERT_OK_AND_ASSIGN(auto fs1, resolving_fs.GetRealFileSystem("file:///tmp/test1.txt"));
    // Second call with same scheme should use cached file system
    ASSERT_OK_AND_ASSIGN(auto fs2, resolving_fs.GetRealFileSystem("file:///tmp/test2.txt"));
    ASSERT_EQ(fs1, fs2);
}

TEST_F(ResolvingFileSystemTest, DifferentAuthoritiesCacheSeparately) {
    std::map<std::string, std::string> scheme_mapping = {{"hdfs", "mock_hdfs"}};
    ResolvingFileSystem resolving_fs(scheme_mapping, "mock_local", /*options=*/{});

    // Different authorities should create separate file system instances
    ASSERT_OK_AND_ASSIGN(auto fs1, resolving_fs.GetRealFileSystem("hdfs://node1:9000/test.txt"));
    ASSERT_OK_AND_ASSIGN(auto fs2, resolving_fs.GetRealFileSystem("hdfs://node2:9000/test.txt"));
    ASSERT_NE(fs1, fs2);
}

TEST_F(ResolvingFileSystemTest, ThreadSafety) {
    std::map<std::string, std::string> scheme_mapping = {{"file", "mock_local"}};
    ResolvingFileSystem resolving_fs(scheme_mapping, "mock_local", /*options=*/{});
    const int32_t num_threads = 100;
    const int32_t operations_per_thread = 10;

    auto executor = CreateDefaultExecutor();
    std::vector<std::future<std::vector<std::shared_ptr<FileSystem>>>> futures;
    for (int32_t i = 0; i < num_threads; ++i) {
        futures.push_back(
            Via(executor.get(), [&resolving_fs, i]() -> std::vector<std::shared_ptr<FileSystem>> {
                std::vector<std::shared_ptr<FileSystem>> fs_list;
                for (int32_t j = 0; j < operations_per_thread; ++j) {
                    std::string path = "file:///tmp/thread_" + std::to_string(i) + "_" +
                                       std::to_string(j) + ".txt";
                    EXPECT_OK_AND_ASSIGN(auto fs, resolving_fs.GetRealFileSystem(path));
                    EXPECT_TRUE(fs);
                    fs_list.push_back(fs);
                }
                return fs_list;
            }));
    }

    // Verify that all threads should get the same file system instance
    auto results = CollectAll(futures);
    auto fs0 = results[0][0];
    for (const auto& fs_list : results) {
        for (const auto& fs : fs_list) {
            ASSERT_EQ(fs, fs0);
        }
    }
}

TEST_F(ResolvingFileSystemTest, ThreadSafety2) {
    std::map<std::string, std::string> scheme_mapping = {{"oss", "mock_oss"}};
    ResolvingFileSystem resolving_fs(scheme_mapping, "mock_oss", /*options=*/{});
    const int32_t num_threads = 100;
    const int32_t operations_per_thread = 10;

    auto executor = CreateDefaultExecutor();
    std::vector<std::future<std::vector<std::shared_ptr<FileSystem>>>> futures;
    for (int32_t i = 0; i < num_threads; ++i) {
        futures.push_back(
            Via(executor.get(), [&resolving_fs, i]() -> std::vector<std::shared_ptr<FileSystem>> {
                std::vector<std::shared_ptr<FileSystem>> fs_list;
                for (int32_t j = 0; j < operations_per_thread; ++j) {
                    std::string path =
                        "oss://bucket_" + std::to_string(i) + "/" + std::to_string(j) + ".txt";
                    EXPECT_OK_AND_ASSIGN(auto fs, resolving_fs.GetRealFileSystem(path));
                    EXPECT_TRUE(fs);
                    fs_list.push_back(fs);
                }
                return fs_list;
            }));
    }

    auto results = CollectAll(futures);
    for (size_t i = 0; i < results.size(); ++i) {
        for (size_t j = 0; j < results[i].size(); ++j) {
            auto casted_fs = dynamic_cast<GmockFileSystem*>(results[i][j].get());
            ASSERT_TRUE(casted_fs);
            ASSERT_EQ(casted_fs->GetAuthority(), "bucket_" + std::to_string(i));
        }
    }
}

}  // namespace paimon::test
