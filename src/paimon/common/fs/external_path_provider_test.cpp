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

#include "paimon/common/fs/external_path_provider.h"

#include <cstdint>
#include <future>
#include <mutex>
#include <set>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(ExternalPathProviderTest, TestInvalidExternalDataPath) {
    std::vector<std::string> external_table_paths;
    std::string relative_bucket_path = "p0=1/p1=0/bucket-0";

    auto provider = ExternalPathProvider::Create(external_table_paths, relative_bucket_path);
    ASSERT_NOK_WITH_MSG(provider.status(), "external table paths cannot be empty");
}

TEST(ExternalPathProviderTest, TestGetNextExternalDataPath) {
    std::vector<std::string> external_table_paths;
    external_table_paths.emplace_back("/tmp/external_path/");
    std::string relative_bucket_path = "p0=1/p1=0/bucket-0";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ExternalPathProvider> provider,
                         ExternalPathProvider::Create(external_table_paths, relative_bucket_path));
    ASSERT_EQ(provider->GetNextExternalDataPath("file.orc"),
              "/tmp/external_path/p0=1/p1=0/bucket-0/file.orc");
}

TEST(ExternalPathProviderTest, TestGetNextExternalDataPath2) {
    std::vector<std::string> external_table_paths;
    external_table_paths.emplace_back("/tmp/external_path_a/");
    external_table_paths.emplace_back("/tmp/external_path_b/");
    external_table_paths.emplace_back("/tmp/external_path_c/");
    std::string relative_bucket_path = "p0=1/p1=0/bucket-0";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ExternalPathProvider> provider,
                         ExternalPathProvider::Create(external_table_paths, relative_bucket_path));

    std::set<std::string> result_data_paths;
    result_data_paths.insert(provider->GetNextExternalDataPath("file.orc"));
    result_data_paths.insert(provider->GetNextExternalDataPath("file.orc"));
    result_data_paths.insert(provider->GetNextExternalDataPath("file.orc"));

    ASSERT_EQ(result_data_paths, std::set<std::string>({
                                     "/tmp/external_path_a/p0=1/p1=0/bucket-0/file.orc",
                                     "/tmp/external_path_b/p0=1/p1=0/bucket-0/file.orc",
                                     "/tmp/external_path_c/p0=1/p1=0/bucket-0/file.orc",
                                 }));
}

TEST(ExternalPathProviderTest, TestGetNextExternalDataPathConcurrently) {
    std::vector<std::string> external_table_paths;
    external_table_paths.emplace_back("/tmp/external_path_a/");
    external_table_paths.emplace_back("/tmp/external_path_b/");
    external_table_paths.emplace_back("/tmp/external_path_c/");
    std::string relative_bucket_path = "p0=1/p1=0/bucket-0";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ExternalPathProvider> provider,
                         ExternalPathProvider::Create(external_table_paths, relative_bucket_path));

    const std::set<std::string> expected_data_paths = {
        "/tmp/external_path_a/p0=1/p1=0/bucket-0/file.orc",
        "/tmp/external_path_b/p0=1/p1=0/bucket-0/file.orc",
        "/tmp/external_path_c/p0=1/p1=0/bucket-0/file.orc",
    };
    std::mutex mutex;
    std::vector<std::string> result_data_paths;
    constexpr int32_t kThreadCount = 8;
    constexpr int32_t kPathCountPerThread = 1000;

    std::vector<std::future<void>> futures;
    futures.reserve(kThreadCount);
    for (int32_t i = 0; i < kThreadCount; ++i) {
        futures.emplace_back(std::async(std::launch::async, [&]() {
            std::vector<std::string> local_paths;
            local_paths.reserve(kPathCountPerThread);
            for (int32_t j = 0; j < kPathCountPerThread; ++j) {
                local_paths.push_back(provider->GetNextExternalDataPath("file.orc"));
            }

            std::lock_guard<std::mutex> lock(mutex);
            result_data_paths.insert(result_data_paths.end(), local_paths.begin(),
                                     local_paths.end());
        }));
    }
    for (auto& future : futures) {
        future.get();
    }

    ASSERT_EQ(result_data_paths.size(), kThreadCount * kPathCountPerThread);
    for (const auto& data_path : result_data_paths) {
        ASSERT_TRUE(expected_data_paths.count(data_path)) << data_path;
    }
}
}  // namespace paimon::test
