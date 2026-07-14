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

#include "paimon/format/blob/blob_writer_builder.h"

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::blob::test {
class BlobWriterBuilderTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        file_system_ = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(output_stream_,
                             file_system_->Create(dir_->Str() + "/file.blob", /*overwrite=*/true));
        struct_type_ = arrow::struct_({BlobUtils::ToArrowField("blob_col", false)});
    }
    void TearDown() override {}

 private:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<OutputStream> output_stream_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<arrow::DataType> struct_type_;
};

TEST_F(BlobWriterBuilderTest, TestSimple) {
    BlobWriterBuilder builder(struct_type_, {});
    ASSERT_NOK_WITH_MSG(builder.Build(output_stream_, "none"),
                        "File system is nullptr. Please call WithFileSystem() first.");

    builder.WithFileSystem(file_system_);
    ASSERT_OK(builder.Build(output_stream_, "none"));
}

}  // namespace paimon::blob::test
