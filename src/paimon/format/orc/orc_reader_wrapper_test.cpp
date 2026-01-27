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

#include "paimon/format/orc/orc_reader_wrapper.h"

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "gtest/gtest.h"
#include "orc/OrcFile.hh"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::orc::test {

class OrcReaderWrapperTest : public ::testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(OrcReaderWrapperTest, NextRowToRead) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string file_path = dir->Str() + "/file.orc";
    {
        std::unique_ptr<::orc::OutputStream> outStream = ::orc::writeLocalFile(file_path);
        ::orc::WriterOptions options;
        std::unique_ptr<::orc::Type> schema =
            ::orc::Type::buildTypeFromString("struct<col1:int,col2:string>");
        std::unique_ptr<::orc::Writer> writer = createWriter(*schema, outStream.get(), options);
        auto col_batch = writer->createRowBatch(3);
        auto batch = dynamic_cast<::orc::StructVectorBatch*>(col_batch.get());
        auto* col1 = dynamic_cast<::orc::LongVectorBatch*>(batch->fields[0]);
        auto* col2 = dynamic_cast<::orc::StringVectorBatch*>(batch->fields[1]);
        batch->numElements = 3;
        col1->numElements = 3;
        col2->numElements = 3;
        col1->data[0] = 1;
        col1->data[1] = 2;
        col1->data[2] = 3;
        col2->data[0] = const_cast<char*>("a");
        col2->length[0] = 1;
        col2->data[1] = const_cast<char*>("b");
        col2->length[1] = 1;
        col2->data[2] = const_cast<char*>("c");
        col2->length[2] = 1;
        writer->add(*batch);
        writer->close();
    }

    ::orc::ReaderOptions reader_opts;
    std::unique_ptr<::orc::Reader> reader =
        ::orc::createReader(::orc::readLocalFile(file_path), reader_opts);
    std::map<std::string, std::string> options;
    ASSERT_OK_AND_ASSIGN(auto wrapper, OrcReaderWrapper::Create(
                                           /*reader=*/std::move(reader),
                                           /*file_name=*/file_path,
                                           /*batch_size=*/2,
                                           /*natural_read_size=*/0,
                                           /*options=*/options,
                                           /*arrow_pool=*/GetArrowPool(GetDefaultPool()),
                                           /*orc_pool=*/nullptr));
    auto data_types =
        arrow::struct_({arrow::field("col1", arrow::int64()), arrow::field("col2", arrow::utf8())});
    ::orc::RowReaderOptions row_opts;
    ASSERT_TRUE(wrapper->SetReadSchema(data_types, row_opts).ok());

    ASSERT_OK_AND_ASSIGN(auto batch1, wrapper->Next());
    EXPECT_EQ(wrapper->GetNextRowToRead(), 2u);  // batch_size=2
    ReaderUtils::ReleaseReadBatch(std::move(batch1));

    ASSERT_OK_AND_ASSIGN(auto batch2, wrapper->Next());
    EXPECT_EQ(wrapper->GetNextRowToRead(), 3u);  // only 1 row left
    ReaderUtils::ReleaseReadBatch(std::move(batch2));

    ASSERT_OK_AND_ASSIGN(auto batch3, wrapper->Next());
    EXPECT_EQ(wrapper->GetNextRowToRead(), 3u);
    ReaderUtils::ReleaseReadBatch(std::move(batch3));
}

}  // namespace paimon::orc::test
