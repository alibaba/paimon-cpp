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
#include <future>

#include "paimon/global_index/lumina/lumina_file_reader.h"
#include "paimon/global_index/lumina/lumina_file_writer.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::lumina::test {
class LuminaFileIOTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(LuminaFileIOTest, TestSimple) {
    std::string content = "Hello World.";
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    auto fs = dir->GetFileSystem();
    std::string index_path = dir->Str() + "/lumina_test.index";
    // write content
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs->Create(index_path, /*overwrite=*/false));
    auto writer = std::make_shared<LuminaFileWriter>(out);
    ASSERT_EQ(writer->GetLength().Value(), 0);
    ASSERT_TRUE(writer->Write(content.data(), content.length()).IsOk());
    ASSERT_TRUE(writer->Close().IsOk());

    // check file exist
    ASSERT_OK_AND_ASSIGN(bool exist, fs->Exists(index_path));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> file_status, fs->GetFileStatus(index_path));
    ASSERT_FALSE(file_status->IsDir());
    ASSERT_EQ(file_status->GetLen(), content.length());

    // read content
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(index_path));
    auto reader = std::make_shared<LuminaFileReader>(in);
    ASSERT_EQ(reader->GetLength().Value(), content.length());
    ASSERT_EQ(reader->GetPosition().Value(), 0);
    std::string read_content(content.size(), 0);
    ASSERT_TRUE(reader->Read(read_content.data(), read_content.size()).IsOk());
    ASSERT_EQ(read_content, content);
    ASSERT_EQ(reader->GetPosition().Value(), content.size());

    // test seek
    ASSERT_TRUE(reader->Seek(2).IsOk());
    std::string read_content2(3, 0);
    ASSERT_TRUE(reader->Read(read_content2.data(), read_content2.size()).IsOk());
    ASSERT_EQ(read_content2, "llo");
    ASSERT_EQ(reader->GetPosition().Value(), 5);
    ASSERT_TRUE(reader->Close().IsOk());
}

TEST_F(LuminaFileIOTest, TestReadAsync) {
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    auto fs = dir->GetFileSystem();
    std::string index_path = dir->Str() + "/lumina_test.index";
    std::string content = "abcdefghijk";
    ASSERT_OK(fs->WriteFile(index_path, content, /*overwrite*/ false));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(index_path));
    auto reader = std::make_shared<LuminaFileReader>(in);

    auto check_read_result = [&](std::string& read_content) {
        bool read_finished = false;
        std::promise<int32_t> promise;
        std::future<int32_t> future = promise.get_future();
        auto callback = [&](::lumina::core::Status status) {
            if (status.IsOk()) {
                read_finished = true;
                promise.set_value(10);
            } else {
                read_finished = true;
                promise.set_value(20);
            }
        };
        reader->ReadAsync(read_content.data(), read_content.size(), /*offset=*/0, callback);

        ASSERT_EQ(future.get(), 10);
        ASSERT_TRUE(read_finished);
        ASSERT_EQ(content.substr(0, read_content.size()), read_content);
    };

    std::string read_content(content.size(), '\0');
    check_read_result(read_content);
    // test read empty
    std::string empty_content;
    check_read_result(empty_content);
}
}  // namespace paimon::lumina::test
