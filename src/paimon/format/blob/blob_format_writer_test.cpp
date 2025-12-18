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

#include "paimon/format/blob/blob_format_writer.h"

#include <string>
#include <utility>

#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/stream_utils.h"
#include "paimon/data/blob.h"
#include "paimon/format/blob/blob_file_batch_reader.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::blob::test {
class BlobFormatWriterTest : public ::testing::Test, public ::testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        blob_as_descriptor_ = GetParam();
        pool_ = GetDefaultPool();
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        file_system_ = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(output_stream_,
                             file_system_->Create(dir_->Str() + "/file.blob", /*overwrite=*/true));

        struct_type_ = arrow::struct_({BlobUtils::ToArrowField("blob_col", true)});
    }
    void TearDown() override {
        ASSERT_OK(output_stream_->Flush());
        ASSERT_OK(output_stream_->Close());
    }

    Result<std::shared_ptr<arrow::Array>> PrepareBlobArray(
        const std::shared_ptr<Blob>& blob) const {
        arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::LargeBinaryBuilder>()});
        auto blob_builder =
            static_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Append());
        if (blob_as_descriptor_) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(blob_builder->Append(
                blob->ToDescriptor(pool_)->data(), blob->ToDescriptor(pool_)->size()));
        } else {
            PAIMON_ASSIGN_OR_RAISE(auto blob_data, blob->ToData(file_system_, pool_));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                blob_builder->Append(blob_data->data(), blob_data->size()));
        }
        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Finish(&array));
        return array;
    }

    Status AddBatchOnce(const std::shared_ptr<BlobFormatWriter>& format_writer,
                        const std::shared_ptr<arrow::Array>& blob_array) const {
        auto c_array = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*blob_array, c_array.get()));
        return format_writer->AddBatch(c_array.get());
    }

 private:
    bool blob_as_descriptor_;
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<OutputStream> output_stream_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<arrow::DataType> struct_type_;
};

INSTANTIATE_TEST_SUITE_P(BlobAsDescriptor, BlobFormatWriterTest, ::testing::Values(false, true));

TEST_P(BlobFormatWriterTest, TestSimple) {
    // write
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, struct_type_,
                                                  file_system_, pool_));

    std::vector<std::shared_ptr<Blob>> expected_blobs;
    std::string file1 = paimon::test::GetDataDir() + "/avro/data/avro_with_null";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob1, Blob::FromPath(file1));
    expected_blobs.emplace_back(blob1);
    ASSERT_OK_AND_ASSIGN(auto array1, PrepareBlobArray(blob1));
    ASSERT_OK(AddBatchOnce(writer, array1));
    ASSERT_OK(writer->Flush());

    std::string file2 = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob2,
                         Blob::FromPath(file2, /*offset=*/0, /*length=*/91));
    expected_blobs.emplace_back(blob2);
    ASSERT_OK_AND_ASSIGN(auto array2, PrepareBlobArray(blob2));
    ASSERT_OK(AddBatchOnce(writer, array2));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob3,
                         Blob::FromPath(file2, /*offset=*/92, /*length=*/85));
    expected_blobs.emplace_back(blob3);
    ASSERT_OK_AND_ASSIGN(auto array3, PrepareBlobArray(blob3));
    ASSERT_OK(AddBatchOnce(writer, array3));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // read
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_TRUE(input_stream);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<BlobFileBatchReader> reader,
        BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024, blob_as_descriptor_, pool_));
    auto schema = arrow::schema(struct_type_->fields());
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK(
        reader->SetReadSchema(&c_schema, /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto chunked_array,
                         paimon::test::ReadResultCollector::CollectResult(reader.get()));

    // check result
    if (blob_as_descriptor_) {
        auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(concat_array);
        ASSERT_TRUE(struct_array);
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Blob>> result_blobs,
                             paimon::test::TestHelper::ToBlobs(struct_array));
        ASSERT_OK_AND_ASSIGN(bool equal, paimon::test::TestHelper::CheckBlobsEqual(
                                             result_blobs, expected_blobs, file_system_));
        ASSERT_TRUE(equal);
    } else {
        auto expected_chunk_array =
            arrow::ChunkedArray::Make({array1, array2, array3}).ValueOrDie();
        ASSERT_TRUE(expected_chunk_array->Equals(chunked_array))
            << expected_chunk_array->ToString() << chunked_array->ToString();
    }
}

TEST_P(BlobFormatWriterTest, TestCreateWithInvalidParameters) {
    // Test with nullptr output stream
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(blob_as_descriptor_, nullptr, struct_type_, file_system_, pool_),
        "blob format writer create failed. out is nullptr");

    // Test with nullptr data type
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, nullptr, file_system_, pool_),
        "blob format writer create failed. data_type is nullptr");

    // Test with nullptr memory pool
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, struct_type_,
                                                 file_system_, nullptr),
                        "blob format writer create failed. pool is nullptr");

    // Test with invalid field count (more than 1 field)
    auto multi_field_type = arrow::struct_(
        {arrow::field("blob_col1", arrow::binary()), arrow::field("blob_col2", arrow::binary())});
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(blob_as_descriptor_, output_stream_,
                                                 multi_field_type, file_system_, pool_),
                        "blob data type field number 2 is not 1");

    // Test with non-blob field (missing blob metadata)
    auto non_blob_field = arrow::field("regular_col", arrow::binary());
    auto non_blob_type = arrow::struct_({non_blob_field});
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, non_blob_type,
                                                 file_system_, pool_),
                        "field regular_col: binary is not BLOB");
}

TEST_P(BlobFormatWriterTest, TestInvalidCase) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, struct_type_,
                                                  file_system_, pool_));

    // Test nullptr batch
    ASSERT_NOK_WITH_MSG(writer->AddBatch(nullptr),
                        "blob format writer add batch failed. batch is nullptr");

    // Test invalid blob
    ASSERT_OK_AND_ASSIGN(auto blob, Blob::FromPath("test_path", 0, 10));
    if (blob_as_descriptor_) {
        ASSERT_OK_AND_ASSIGN(auto array, PrepareBlobArray(std::move(blob)));
        ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, array), "File 'test_path' not exists");
    } else {
        ASSERT_NOK_WITH_MSG(PrepareBlobArray(std::move(blob)), "File 'test_path' not exists");
    }
}

TEST_P(BlobFormatWriterTest, TestAddBatchWithInvalidBatchLength) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, struct_type_,
                                                  file_system_, pool_));

    // Test batch with wrong length (not 1)
    arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::BinaryBuilder>()});
    auto blob_builder = static_cast<arrow::BinaryBuilder*>(struct_builder.field_builder(0));

    // Add two rows instead of one
    ASSERT_OK_AND_ASSIGN(auto blob, Blob::FromPath(paimon::test::GetDataDir() + "/xxhash.data"));
    ASSERT_TRUE(struct_builder.Append().ok());
    auto blob_descriptor = blob->ToDescriptor(pool_);
    ASSERT_TRUE(blob_builder->Append(blob_descriptor->data(), blob_descriptor->size()).ok());
    ASSERT_TRUE(struct_builder.Append().ok());
    ASSERT_TRUE(blob_builder->Append(blob_descriptor->data(), blob_descriptor->size()).ok());

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(struct_builder.Finish(&array).ok());
    auto c_array = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*array, c_array.get()).ok());

    ASSERT_NOK_WITH_MSG(writer->AddBatch(c_array.get()),
                        "BlobFormatWriter only supports batch with a row count of 1");
    ArrowArrayRelease(c_array.get());
}

TEST_P(BlobFormatWriterTest, TestReachTargetSize) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, struct_type_,
                                                  file_system_, pool_));

    // Initially should not reach target size
    ASSERT_OK_AND_ASSIGN(bool reached, writer->ReachTargetSize(true, 1000));
    ASSERT_FALSE(reached);

    // Add some data
    std::string file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob, Blob::FromPath(file));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareBlobArray(blob));
    ASSERT_OK(AddBatchOnce(writer, array));
    ASSERT_OK(writer->Flush());

    // Check if we reach a small target size
    ASSERT_OK_AND_ASSIGN(reached, writer->ReachTargetSize(true, 10));
    ASSERT_TRUE(reached);

    // Check if we don't reach a large target size
    ASSERT_OK_AND_ASSIGN(reached, writer->ReachTargetSize(true, 100000));
    ASSERT_FALSE(reached);
}

TEST_P(BlobFormatWriterTest, TestGetWriterMetrics) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, struct_type_,
                                                  file_system_, pool_));

    auto metrics = writer->GetWriterMetrics();
    ASSERT_TRUE(metrics);
}

TEST_P(BlobFormatWriterTest, TestEmptyWriter) {
    // Test creating a writer and finishing without adding any data
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, struct_type_,
                                                  file_system_, pool_));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Verify the file is the same with java
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_TRUE(input_stream);
    ASSERT_OK_AND_ASSIGN(uint64_t file_length, input_stream->Length());
    ASSERT_EQ(file_length, 5);  // Should have footer even if no data
    std::vector<char> buffer(file_length);
    ASSERT_OK_AND_ASSIGN(auto read_length, input_stream->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(read_length, 5);
    std::vector<char> expected = {0x00, 0x00, 0x00, 0x00, 0x01};
    ASSERT_EQ(buffer, expected);
}

TEST_P(BlobFormatWriterTest, TestLargeBlob) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, struct_type_,
                                                  file_system_, pool_));

    // Create a temporary large file for testing
    std::string large_file_path = dir_->Str() + "/large_test_file.bin";
    ASSERT_OK_AND_ASSIGN(auto large_file_stream,
                         file_system_->Create(large_file_path, /*overwrite=*/true));

    // Write data larger than TMP_BUFFER_SIZE (1MB)
    const size_t large_size = BlobFormatWriter::TMP_BUFFER_SIZE * 2 + 1000;  // ~2MB
    std::vector<char> large_data(large_size, 'A');
    ASSERT_OK_AND_ASSIGN(int32_t written, large_file_stream->Write(large_data.data(), large_size));
    ASSERT_EQ(written, large_size);
    ASSERT_OK(large_file_stream->Flush());
    ASSERT_OK(large_file_stream->Close());

    // Create blob from large file and write it
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> large_blob, Blob::FromPath(large_file_path));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareBlobArray(large_blob));
    ASSERT_OK(AddBatchOnce(writer, array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Verify we can read it back
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<BlobFileBatchReader> reader,
        BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024, blob_as_descriptor_, pool_));
    auto schema = arrow::schema(struct_type_->fields());
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK(
        reader->SetReadSchema(&c_schema, /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto chunked_array,
                         paimon::test::ReadResultCollector::CollectResult(reader.get()));

    // check result
    if (blob_as_descriptor_) {
        auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(concat_array);
        ASSERT_TRUE(struct_array);
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Blob>> result_blobs,
                             paimon::test::TestHelper::ToBlobs(struct_array));
        ASSERT_OK_AND_ASSIGN(bool equal, paimon::test::TestHelper::CheckBlobsEqual(
                                             result_blobs, {large_blob}, file_system_));
        ASSERT_TRUE(equal);
    } else {
        auto expected_chunk_array = arrow::ChunkedArray::Make({array}).ValueOrDie();
        ASSERT_TRUE(expected_chunk_array->Equals(chunked_array));
    }
}

TEST_P(BlobFormatWriterTest, TestAddBatchWithNullValues) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, struct_type_,
                                                  file_system_, pool_));

    // Test with null struct array
    arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::BinaryBuilder>()});
    ASSERT_TRUE(struct_builder.AppendNull().ok());  // Append null instead of valid value

    std::shared_ptr<arrow::Array> null_struct_array;
    ASSERT_TRUE(struct_builder.Finish(&null_struct_array).ok());
    auto null_c_array = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*null_struct_array, null_c_array.get()).ok());

    ASSERT_NOK_WITH_MSG(writer->AddBatch(null_c_array.get()),
                        "BlobFormatWriter only support non-null blob.");
    ArrowArrayRelease(null_c_array.get());

    // Test with null child array (blob field is null)
    arrow::StructBuilder struct_builder2(struct_type_, arrow::default_memory_pool(),
                                         {std::make_shared<arrow::BinaryBuilder>()});
    auto blob_builder = static_cast<arrow::BinaryBuilder*>(struct_builder2.field_builder(0));
    ASSERT_TRUE(struct_builder2.Append().ok());
    ASSERT_TRUE(blob_builder->AppendNull().ok());  // Append null blob

    std::shared_ptr<arrow::Array> null_child_array;
    ASSERT_TRUE(struct_builder2.Finish(&null_child_array).ok());
    auto null_child_c_array = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*null_child_array, null_child_c_array.get()).ok());

    ASSERT_NOK_WITH_MSG(writer->AddBatch(null_child_c_array.get()),
                        "BlobFormatWriter only support non-null blob.");
    ArrowArrayRelease(null_child_c_array.get());
}

TEST_P(BlobFormatWriterTest, TestAddBatchWithZeroLengthBlob) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(blob_as_descriptor_, output_stream_, struct_type_,
                                                  file_system_, pool_));

    // Create a zero-length file
    std::string zero_file_path = dir_->Str() + "/zero_length_file.bin";
    ASSERT_OK_AND_ASSIGN(auto zero_file_stream,
                         file_system_->Create(zero_file_path, /*overwrite=*/true));
    ASSERT_OK(zero_file_stream->Flush());
    ASSERT_OK(zero_file_stream->Close());

    // Create blob from zero-length file
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> zero_blob, Blob::FromPath(zero_file_path));

    // This should work - zero-length blobs should be supported
    ASSERT_OK_AND_ASSIGN(auto array, PrepareBlobArray(zero_blob));
    ASSERT_OK(AddBatchOnce(writer, array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Verify the file is the same with java
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_TRUE(input_stream);
    ASSERT_OK_AND_ASSIGN(uint64_t file_length, input_stream->Length());
    ASSERT_EQ(file_length, 22);
    std::vector<uint8_t> buffer(file_length);
    ASSERT_OK_AND_ASSIGN(auto read_length,
                         input_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_EQ(read_length, 22);
    std::vector<uint8_t> expected = {{0xcf, 0x11, 0x4e, 0x58, 0x10, 0x00, 0x00, 0x00,
                                      0x00, 0x00, 0x00, 0x00, 0x53, 0x7f, 0xdf, 0x03,
                                      0x20, 0x01, 0x00, 0x00, 0x00, 0x01}};
    ASSERT_EQ(buffer, expected);
}

}  // namespace paimon::blob::test
