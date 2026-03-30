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

#include "paimon/disk/io_manager.h"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(IOManagerTest, CreateShouldReturnManagerWithGivenTempDir) {
    auto tmp_dir = UniqueTestDirectory::Create();

    std::unique_ptr<IOManager> manager = IOManager::Create(tmp_dir->Str());
    ASSERT_NE(manager, nullptr);
    ASSERT_EQ(manager->GetTempDir(), tmp_dir->Str());
}

TEST(IOManagerTest, GenerateTempFilePathShouldContainPrefixAndSuffix) {
    auto tmp_dir = UniqueTestDirectory::Create();
    const std::string prefix = "spill";

    std::unique_ptr<IOManager> manager = IOManager::Create(tmp_dir->Str());
    ASSERT_OK_AND_ASSIGN(std::string temp_path, manager->GenerateTempFilePath(prefix));

    std::string expected_prefix = PathUtil::JoinPath(tmp_dir->Str(), "");
    ASSERT_TRUE(StringUtils::StartsWith(temp_path, expected_prefix));

    std::string file_name = PathUtil::GetName(temp_path);
    std::string file_prefix = prefix + "-";
    ASSERT_TRUE(StringUtils::StartsWith(file_name, file_prefix));

    const std::string suffix = ".channel";
    ASSERT_GE(file_name.size(), file_prefix.size() + suffix.size() + 1);
    ASSERT_TRUE(StringUtils::EndsWith(file_name, suffix));
}

TEST(IOManagerTest, GenerateTempFilePathShouldBeDifferentAcrossCalls) {
    auto tmp_dir = UniqueTestDirectory::Create();
    std::unique_ptr<IOManager> manager = IOManager::Create(tmp_dir->Str());

    ASSERT_OK_AND_ASSIGN(std::string path1, manager->GenerateTempFilePath("spill"));
    ASSERT_OK_AND_ASSIGN(std::string path2, manager->GenerateTempFilePath("spill"));

    ASSERT_NE(path1, path2);
}

}  // namespace paimon::test
