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

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Assert utilities is adapted from RocksDB
// https://github.com/facebook/rocksdb/blob/main/test_util/testharness.cc

// Copyright 2024 Google LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// UniqueTestDirectory utility is adapted from LiteRT
// https://github.com/google-ai-edge/LiteRT/blob/main/litert/test/common.cc

#include "paimon/testing/utils/testharness.h"

#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/status.h"

namespace paimon::test {

std::string GetDataDir() {
    const auto result = std::getenv("PAIMON_TEST_DATA");
    if (!result || !result[0]) {
        return "test/test_data/";
    }
    return std::string(result);
}

std::map<std::string, std::string> GetJindoTestOptions() {
    const char* home = std::getenv("HOME");
    std::string home_str = "";
    if (home) {
        home_str = std::string(home);
    }
    std::string config_file_path = home_str + "/.osscredentials";
    std::ifstream config_file(config_file_path);
    std::string access_key_id = "";
    std::string access_key_secret = "";
    if (config_file.is_open()) {
        std::string line;
        while (std::getline(config_file, line)) {
            std::vector<std::string> key_value = StringUtils::Split(line, "=");
            if (key_value.size() != 2) {
                continue;
            }
            if (key_value[0].find("accessid") != std::string::npos) {
                StringUtils::Trim(&key_value[1]);
                access_key_id = key_value[1];
            }
            if (key_value[0].find("accesskey") != std::string::npos) {
                StringUtils::Trim(&key_value[1]);
                access_key_secret = key_value[1];
            }
        }
        config_file.close();
    }
    std::map<std::string, std::string> options = {
        {"fs.oss.bucket.paimon-unittest.endpoint", "oss-cn-hangzhou-zmf.aliyuncs.com"},
        {"fs.oss.bucket.paimon-unittest.accessKeyId", access_key_id},
        {"fs.oss.bucket.paimon-unittest.accessKeySecret", access_key_secret},
        {"fs.oss.user", "paimon"},
    };
    return options;
}
std::string GetJindoTestDir() {
    static const std::string dir = "oss://paimon-unittest/temp/";
    return dir;
}

int32_t RandomNumber(int32_t min, int32_t max) {
    static thread_local std::mt19937 generator(
        std::random_device{}());  // NOLINT(whitespace/braces)
    std::uniform_int_distribution<int32_t> distribution(min, max);
    return distribution(generator);
}

std::string GetPidStr() {
    return std::to_string(getpid());
}

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s) {
    if (s.ok()) {
        return ::testing::AssertionSuccess();
    } else {
        return ::testing::AssertionFailure() << s_expr << std::endl << s.ToString();
    }
}

std::unique_ptr<UniqueTestDirectory> UniqueTestDirectory::Create(const std::string& fs_identifier) {
    static const size_t kMaxTries = 1000;
    std::string tmp_dir = fs_identifier == "jindo"
                              ? GetJindoTestDir()
                              : std::filesystem::temp_directory_path().string() + "/";
    std::map<std::string, std::string> fs_options =
        fs_identifier == "jindo" ? GetJindoTestOptions() : std::map<std::string, std::string>();
    auto fs = FileSystemFactory::Get(fs_identifier, tmp_dir, fs_options);
    if (!fs.ok()) {
        return nullptr;
    }
    for (size_t i = 0; i < kMaxTries; ++i) {
        std::string uuid;
        if (!UUID::Generate(&uuid)) {
            continue;
        }
        std::string test_dir = tmp_dir + "paimon_test_" + uuid;
        auto is_exist = fs.value()->Exists(test_dir);
        if (!is_exist.ok() || is_exist.value()) {
            continue;
        }
        auto status = fs.value()->Mkdirs(test_dir);
        if (status.ok()) {
            return std::unique_ptr<UniqueTestDirectory>(
                new UniqueTestDirectory(test_dir, std::move(fs).value()));
        }
    }
    return nullptr;
}

UniqueTestDirectory::~UniqueTestDirectory() {
    auto is_exist = fs_->Exists(tmpdir_);
    assert(is_exist.ok());
    if (is_exist.value()) {
        [[maybe_unused]] auto status = fs_->Delete(tmpdir_, /*recursive=*/true);
        assert(status.ok());
    }
    fs_.reset();
}

bool TestUtil::CopyDirectory(const std::filesystem::path& source,
                             const std::filesystem::path& destination) {
    namespace fs = std::filesystem;
    try {
        if (!fs::exists(destination)) {
            fs::create_directories(destination);
        }

        for (const auto& entry : fs::directory_iterator(source)) {
            const auto& source_path = entry.path();
            auto destination_path = destination / source_path.filename();

            if (fs::is_directory(source_path)) {
                CopyDirectory(source_path, destination_path);
            } else if (fs::is_regular_file(source_path)) {
                fs::copy(source_path, destination_path, fs::copy_options::overwrite_existing);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error while copying directory: " << e.what() << std::endl;
        return false;
    }
    return true;
}

}  // namespace paimon::test
