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
#include "paimon/global_index/lucene/lucene_directory.h"

#include "gtest/gtest.h"
#include "lucene++/FileUtils.h"
#include "lucene++/LuceneHeaders.h"
#include "lucene++/MiscUtils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/lucene/lucene_utils.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::lucene::test {
class LuceneDirectoryTest : public ::testing::Test, public ::testing::WithParamInterface<int32_t> {
 public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_P(LuceneDirectoryTest, TestSimple) {
    int32_t read_buffer_size = GetParam();
    // write 3 files in a single concat file
    std::vector<std::string> data = {"helloworld", "abcdefg", "paimoncpp"};
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    auto lucene_directory = Lucene::FSDirectory::open(LuceneUtils::StringToWstring(dir->Str()),
                                                      Lucene::NoLockFactory::getNoLockFactory());
    std::string single_file_name = "lucene-file";
    Lucene::IndexOutputPtr output =
        lucene_directory->createOutput(LuceneUtils::StringToWstring(single_file_name));
    std::map<std::string, std::pair<int64_t, int64_t>> file_name_to_offset_and_length;
    int64_t offset = 0;
    for (size_t i = 0; i < data.size(); i++) {
        const auto& data_str = data[i];
        output->writeBytes(reinterpret_cast<const uint8_t*>(data_str.data()), /*offset=*/0,
                           data_str.size());
        file_name_to_offset_and_length["file" + std::to_string(i)] = {offset, data_str.size()};
        offset += data_str.size();
    }
    output->close();

    // create paimon directory
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input,
                         fs->Open(PathUtil::JoinPath(dir->Str(), single_file_name)));
    auto paimon_directory = std::make_shared<LuceneDirectory>(
        dir->Str(), file_name_to_offset_and_length, input, read_buffer_size);
    // test list all
    auto list_file_names = paimon_directory->listAll();
    Lucene::HashSet<Lucene::String> expect_file_list(
        Lucene::HashSet<Lucene::String>::newInstance());
    expect_file_list.add(L"file0");
    expect_file_list.add(L"file1");
    expect_file_list.add(L"file2");
    ASSERT_EQ(list_file_names, expect_file_list);

    // test non-exist file
    ASSERT_FALSE(paimon_directory->fileExists(L"non-exist-file"));
    ASSERT_THROW(paimon_directory->fileLength(L"non-exist-file"), Lucene::IOException);

    for (size_t i = 0; i < data.size(); i++) {
        std::wstring file_name = LuceneUtils::StringToWstring("file" + std::to_string(i));
        // check file exist
        ASSERT_TRUE(paimon_directory->fileExists(file_name));
        // check file length
        ASSERT_EQ(paimon_directory->fileLength(file_name), data[i].size());
        // check read data
        Lucene::IndexInputPtr data_input = paimon_directory->openInput(file_name);
        ASSERT_TRUE(data_input);
        std::string read_data(data[i].size(), '\0');
        data_input->readBytes(reinterpret_cast<uint8_t*>(read_data.data()),
                              /*offset=*/0, /*length=*/data[i].size(),
                              /*useBuffer=*/true);
        ASSERT_EQ(read_data, data[i]);
        // check seek
        data_input->seek(1);
        ASSERT_EQ(1l, data_input->getFilePointer());
        // check read after seek
        read_data.resize(data[i].size() - 1, '\0');
        data_input->readBytes(reinterpret_cast<uint8_t*>(read_data.data()),
                              /*offset=*/0, /*length=*/data[i].size() - 1,
                              /*useBuffer=*/true);
        ASSERT_EQ(read_data, data[i].substr(1));
        ASSERT_EQ(data[i].size(), data_input->length());
        data_input->close();
    }
}
INSTANTIATE_TEST_SUITE_P(ReadBufferSize, LuceneDirectoryTest,
                         ::testing::ValuesIn(std::vector<int32_t>({10, 100, 1024})));

}  // namespace paimon::lucene::test
