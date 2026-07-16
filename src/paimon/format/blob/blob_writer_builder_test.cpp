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
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/data/blob.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/test_helper.h"
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
        struct_type_ = arrow::struct_({BlobUtils::ToArrowField("blob_col", true)});
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

TEST_F(BlobWriterBuilderTest, TestWriteNullOptions) {
    auto prepare_batch = [&](const std::shared_ptr<Blob>& blob, ArrowArray* c_array) -> Status {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array,
                               paimon::test::TestHelper::MakeBlobDescriptorArray(struct_type_, blob,
                                                                                 GetDefaultPool()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array));
        return Status::OK();
    };
    // Each writer gets its own output file so that a Finish() in one scenario cannot leak
    // bytes into the next.
    int32_t file_index = 0;
    auto build_writer = [&](const std::map<std::string, std::string>& options)
        -> Result<std::unique_ptr<FormatWriter>> {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<OutputStream> out,
            file_system_->Create(dir_->Str() + "/file" + std::to_string(file_index++) + ".blob",
                                 /*overwrite=*/true));
        BlobWriterBuilder builder(struct_type_, options);
        builder.WithFileSystem(file_system_);
        return builder.Build(out, "none");
    };

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    std::string data_file = dir_->Str() + "/data_file.bin";
    ASSERT_OK_AND_ASSIGN(auto data_file_stream,
                         file_system_->Create(data_file, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(int64_t written, data_file_stream->Write("blob data", 9));
    ASSERT_EQ(written, 9);
    ASSERT_OK(data_file_stream->Flush());
    ASSERT_OK(data_file_stream->Close());
    // The offset beyond the end of the 9-byte file makes the descriptor a fetch failure.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> bad_offset_blob,
                         Blob::FromPath(data_file, /*offset=*/100, /*length=*/10));

    // Both options default to false.
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatWriter> writer, build_writer({}));
        ArrowArray c_array;
        ASSERT_OK(prepare_batch(missing_blob, &c_array));
        ASSERT_NOK_WITH_MSG(writer->AddBatch(&c_array), "not exists");
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatWriter> writer, build_writer({}));
        ArrowArray c_array;
        ASSERT_OK(prepare_batch(bad_offset_blob, &c_array));
        ASSERT_NOK_WITH_MSG(writer->AddBatch(&c_array), "exceed total length");
    }

    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatWriter> writer,
                             build_writer({{Options::BLOB_WRITE_NULL_ON_MISSING_FILE, "true"}}));
        ArrowArray c_array;
        ASSERT_OK(prepare_batch(missing_blob, &c_array));
        ASSERT_OK(writer->AddBatch(&c_array));
        ASSERT_OK(writer->Finish());
    }

    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatWriter> writer,
                             build_writer({{Options::BLOB_WRITE_NULL_ON_FETCH_FAILURE, "true"}}));
        ArrowArray c_array;
        ASSERT_OK(prepare_batch(bad_offset_blob, &c_array));
        ASSERT_OK(writer->AddBatch(&c_array));
        ASSERT_OK(writer->Finish());
    }

    // An explicit "false" exercises value parsing, not just the absent-key default.
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatWriter> writer,
                             build_writer({{Options::BLOB_WRITE_NULL_ON_MISSING_FILE, "false"},
                                           {Options::BLOB_WRITE_NULL_ON_FETCH_FAILURE, "false"}}));
        ArrowArray c_array;
        ASSERT_OK(prepare_batch(missing_blob, &c_array));
        ASSERT_NOK_WITH_MSG(writer->AddBatch(&c_array), "not exists");
    }

    {
        ASSERT_NOK_WITH_MSG(build_writer({{Options::BLOB_WRITE_NULL_ON_FETCH_FAILURE, "invalid"}}),
                            "convert key blob-write-null-on-fetch-failure");
    }
}

}  // namespace paimon::blob::test
