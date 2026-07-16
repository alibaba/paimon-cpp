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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/fs/jindo/jindo_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class TestOutputStream : public OutputStream {
 public:
    Result<int64_t> GetPos() const override {
        return 0;
    }

    Result<int64_t> Write(const char*, int64_t size) override {
        return size;
    }

    Status Flush() override {
        return Status::OK();
    }

    Status Close() override {
        return Status::OK();
    }

    Result<std::string> GetUri() const override {
        return std::string();
    }
};

class TestJindoFileSystem : public jindo::JindoFileSystem {
 public:
    TestJindoFileSystem() : JindoFileSystem(std::make_unique<JdoFileSystem>()) {}

    Result<bool> Exists(const std::string&) const override {
        return false;
    }

    Status Mkdirs(const std::string& path) const override {
        parent_path_ = path;
        return mkdirs_status_;
    }

    void SetMkdirsStatus(Status status) {
        mkdirs_status_ = std::move(status);
    }

    const std::string& GetParentPath() const {
        return parent_path_;
    }

    bool IsWriterOpened() const {
        return writer_opened_;
    }

 protected:
    Result<std::unique_ptr<OutputStream>> OpenWriter(const std::string&) const override {
        if (parent_path_.empty()) {
            return Status::IOError("parent directory was not created");
        }
        writer_opened_ = true;
        std::unique_ptr<OutputStream> output = std::make_unique<TestOutputStream>();
        return output;
    }

 private:
    mutable std::string parent_path_;
    mutable bool writer_opened_ = false;
    Status mkdirs_status_ = Status::OK();
};

TEST(JindoFileSystemUnitTest, CreateMakesParentDirectoryBeforeOpeningWriter) {
    TestJindoFileSystem fs;
    const std::string path = "oss://bucket/table/bucket-24/data.orc";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> output,
                         fs.Create(path, /*overwrite=*/false));
    ASSERT_TRUE(output);
    ASSERT_EQ(fs.GetParentPath(), "oss://bucket/table/bucket-24");
    ASSERT_TRUE(fs.IsWriterOpened());
}

TEST(JindoFileSystemUnitTest, CreateReturnsParentDirectoryFailure) {
    TestJindoFileSystem fs;
    const std::string path = "oss://bucket/table/bucket-24/data.orc";
    fs.SetMkdirsStatus(Status::IOError("failed to create parent directory"));

    ASSERT_NOK_WITH_MSG(fs.Create(path, /*overwrite=*/false), "failed to create parent directory");
    ASSERT_EQ(fs.GetParentPath(), "oss://bucket/table/bucket-24");
    ASSERT_FALSE(fs.IsWriterOpened());
}

}  // namespace paimon::test
