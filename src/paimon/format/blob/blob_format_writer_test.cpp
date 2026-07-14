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
#include <vector>

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

/// A file system whose Open() always fails with the configured status, for verifying how the
/// writer classifies open failures by status code.
class OpenFailFileSystem : public LocalFileSystem {
 public:
    explicit OpenFailFileSystem(Status open_status) : open_status_(std::move(open_status)) {}

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        return open_status_;
    }

 private:
    Status open_status_;
};

class BlobFormatWriterTestBase : public ::testing::Test {
 public:
    void SetUp() override {
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

    Status AddBatchOnce(const std::shared_ptr<BlobFormatWriter>& format_writer,
                        const std::shared_ptr<arrow::Array>& blob_array) const {
        auto c_array = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*blob_array, c_array.get()));
        return format_writer->AddBatch(c_array.get());
    }

    Result<std::shared_ptr<arrow::Array>> PrepareDescriptorArray(
        const std::shared_ptr<Blob>& blob) const {
        return paimon::test::TestHelper::MakeBlobDescriptorArray(struct_type_, blob, pool_);
    }

    Result<std::shared_ptr<arrow::StructArray>> ReadBackAsData() const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                               file_system_->Open(dir_->Str() + "/file.blob"));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlobFileBatchReader> reader,
                               BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                                           /*blob_as_descriptor=*/false, pool_));
        auto schema = arrow::schema(struct_type_->fields());
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &c_schema));
        PAIMON_RETURN_NOT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                                   /*selection_bitmap=*/std::nullopt));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> chunked_array,
                               paimon::test::ReadResultCollector::CollectResult(reader.get()));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> concat_array,
                                          arrow::Concatenate(chunked_array->chunks()));
        return arrow::internal::checked_pointer_cast<arrow::StructArray>(concat_array);
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<OutputStream> output_stream_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<arrow::DataType> struct_type_;
};

class BlobFormatWriterTest : public BlobFormatWriterTestBase,
                             public ::testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        blob_as_descriptor_ = GetParam();
        BlobFormatWriterTestBase::SetUp();
    }

    Result<std::shared_ptr<arrow::Array>> PrepareBlobArray(
        const std::shared_ptr<Blob>& blob) const {
        if (blob_as_descriptor_) {
            return PrepareDescriptorArray(blob);
        }
        arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::LargeBinaryBuilder>()});
        auto blob_builder =
            static_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Append());
        PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> blob_data,
                               blob->ToData(file_system_, pool_));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(blob_builder->Append(blob_data->data(), blob_data->size()));
        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Finish(&array));
        return array;
    }

 private:
    bool blob_as_descriptor_;
};

/// The write-null tests always feed descriptor bytes, so they do not depend on the
/// blob_as_descriptor_ parameter and run once on the non-parameterized fixture.
using BlobFormatWriterWriteNullTest = BlobFormatWriterTestBase;

INSTANTIATE_TEST_SUITE_P(BlobAsDescriptor, BlobFormatWriterTest, ::testing::Values(false, true));

TEST_P(BlobFormatWriterTest, TestSimple) {
    // write
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_));

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
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(nullptr, struct_type_, file_system_, pool_),
                        "blob format writer create failed. out is nullptr");

    // Test with nullptr data type
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(output_stream_, nullptr, file_system_, pool_),
                        "blob format writer create failed. data_type is nullptr");

    // Test with nullptr memory pool
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, nullptr),
        "blob format writer create failed. pool is nullptr");

    // Test with invalid field count (more than 1 field)
    auto multi_field_type = arrow::struct_(
        {arrow::field("blob_col1", arrow::binary()), arrow::field("blob_col2", arrow::binary())});
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(output_stream_, multi_field_type, file_system_, pool_),
        "blob data type field number 2 is not 1");

    // Test with non-blob field (missing blob metadata)
    auto non_blob_field = arrow::field("regular_col", arrow::binary());
    auto non_blob_type = arrow::struct_({non_blob_field});
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(output_stream_, non_blob_type, file_system_, pool_),
        "field regular_col: binary is not BLOB");
}

TEST_P(BlobFormatWriterTest, TestInvalidCase) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_));

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
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_));

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
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_));

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
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_));

    auto metrics = writer->GetWriterMetrics();
    ASSERT_TRUE(metrics);
}

TEST_P(BlobFormatWriterTest, TestEmptyWriter) {
    // Test creating a writer and finishing without adding any data
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Verify the file is the same with java
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_TRUE(input_stream);
    ASSERT_OK_AND_ASSIGN(int64_t file_length, input_stream->Length());
    ASSERT_EQ(file_length, 5);  // Should have footer even if no data
    std::vector<char> buffer(file_length);
    ASSERT_OK_AND_ASSIGN(auto read_length, input_stream->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(read_length, 5);
    std::vector<char> expected = {0x00, 0x00, 0x00, 0x00, 0x01};
    ASSERT_EQ(buffer, expected);
}

TEST_P(BlobFormatWriterTest, TestLargeBlob) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_));

    // Create a temporary large file for testing
    std::string large_file_path = dir_->Str() + "/large_test_file.bin";
    ASSERT_OK_AND_ASSIGN(auto large_file_stream,
                         file_system_->Create(large_file_path, /*overwrite=*/true));

    // Write data larger than TMP_BUFFER_SIZE (1MB)
    const size_t large_size = BlobFormatWriter::kTmpBufferSize * 2 + 1000;  // ~2MB
    std::vector<char> large_data(large_size, 'A');
    ASSERT_OK_AND_ASSIGN(int64_t written, large_file_stream->Write(large_data.data(), large_size));
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
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_));

    // Write one row with child-level null blob
    arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::LargeBinaryBuilder>()});
    auto blob_builder = static_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
    ASSERT_TRUE(struct_builder.Append().ok());
    ASSERT_TRUE(blob_builder->AppendNull().ok());
    std::shared_ptr<arrow::Array> null_child_array;
    ASSERT_TRUE(struct_builder.Finish(&null_child_array).ok());
    auto c_array = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*null_child_array, c_array.get()).ok());
    ASSERT_OK(writer->AddBatch(c_array.get()));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Read back and verify
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

    auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
    auto result_struct = arrow::internal::checked_pointer_cast<arrow::StructArray>(concat_array);
    ASSERT_TRUE(result_struct);
    ASSERT_EQ(result_struct->length(), 1);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));

    // Struct-level null should still be rejected
    arrow::StructBuilder struct_builder2(struct_type_, arrow::default_memory_pool(),
                                         {std::make_shared<arrow::LargeBinaryBuilder>()});
    ASSERT_TRUE(struct_builder2.AppendNull().ok());
    std::shared_ptr<arrow::Array> null_struct_array;
    ASSERT_TRUE(struct_builder2.Finish(&null_struct_array).ok());
    auto null_c_array = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*null_struct_array, null_c_array.get()).ok());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer2,
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_));
    ASSERT_NOK_WITH_MSG(writer2->AddBatch(null_c_array.get()),
                        "BlobFormatWriter does not support struct-level null.");
    ArrowArrayRelease(null_c_array.get());
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnMissingFile) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_,
                                                  /*write_null_on_missing_file=*/true,
                                                  /*write_null_on_fetch_failure=*/false));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto missing_array, PrepareDescriptorArray(missing_blob));
    ASSERT_OK(AddBatchOnce(writer, missing_array));

    // A fetch failure is not converted to NULL by write_null_on_missing_file alone (aligned
    // with Java); the rejected row leaves the writer usable.
    std::string file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> bad_offset_blob,
                         Blob::FromPath(file, /*offset=*/1 << 20, /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto bad_offset_array, PrepareDescriptorArray(bad_offset_blob));
    ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, bad_offset_array), "exceed total length");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob, Blob::FromPath(file));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareDescriptorArray(blob));
    ASSERT_OK(AddBatchOnce(writer, array));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 2);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));
    ASSERT_FALSE(result_struct->field(0)->IsNull(1));
    auto binary_array =
        arrow::internal::checked_pointer_cast<arrow::LargeBinaryArray>(result_struct->field(0));
    ASSERT_OK_AND_ASSIGN(auto expected_data, blob->ToData(file_system_, pool_));
    ASSERT_EQ(binary_array->GetView(1),
              std::string_view(expected_data->data(), expected_data->size()));
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnFetchFailure) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_,
                                                  /*write_null_on_missing_file=*/false,
                                                  /*write_null_on_fetch_failure=*/true));

    std::string file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> bad_offset_blob,
                         Blob::FromPath(file, /*offset=*/1 << 20, /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto bad_offset_array, PrepareDescriptorArray(bad_offset_blob));
    ASSERT_OK(AddBatchOnce(writer, bad_offset_array));

    // A missing file is not converted to NULL by write_null_on_fetch_failure alone (aligned
    // with Java); the rejected row leaves the writer usable.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto missing_array, PrepareDescriptorArray(missing_blob));
    ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, missing_array), "not exists");

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 1);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnBothOptionsEnabled) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_,
                                                  /*write_null_on_missing_file=*/true,
                                                  /*write_null_on_fetch_failure=*/true));

    // Row 0: missing file -> NULL.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto missing_array, PrepareDescriptorArray(missing_blob));
    ASSERT_OK(AddBatchOnce(writer, missing_array));

    // Row 1: fetch failure (offset beyond EOF) -> NULL.
    std::string file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> bad_offset_blob,
                         Blob::FromPath(file, /*offset=*/1 << 20, /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto bad_offset_array, PrepareDescriptorArray(bad_offset_blob));
    ASSERT_OK(AddBatchOnce(writer, bad_offset_array));

    // Row 2: valid blob.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob, Blob::FromPath(file));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareDescriptorArray(blob));
    ASSERT_OK(AddBatchOnce(writer, array));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 3);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));
    ASSERT_TRUE(result_struct->field(0)->IsNull(1));
    ASSERT_FALSE(result_struct->field(0)->IsNull(2));
    auto binary_array =
        arrow::internal::checked_pointer_cast<arrow::LargeBinaryArray>(result_struct->field(0));
    ASSERT_OK_AND_ASSIGN(auto expected_data, blob->ToData(file_system_, pool_));
    ASSERT_EQ(binary_array->GetView(2),
              std::string_view(expected_data->data(), expected_data->size()));
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullClassifiesByStatusCode) {
    // The missing-file vs fetch-failure split keys on the open status code (NotExist <-> missing
    // file), independent of the file system implementation.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob,
                         Blob::FromPath(dir_->Str() + "/any_file", /*offset=*/0, /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareDescriptorArray(blob));
    auto not_exist_fs = std::make_shared<OpenFailFileSystem>(Status::NotExist("mock not exist"));
    auto io_error_fs = std::make_shared<OpenFailFileSystem>(Status::IOError("mock io error"));

    {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<BlobFormatWriter> writer,
            BlobFormatWriter::Create(output_stream_, struct_type_, not_exist_fs, pool_,
                                     /*write_null_on_missing_file=*/true,
                                     /*write_null_on_fetch_failure=*/false));
        ASSERT_OK(AddBatchOnce(writer, array));
    }
    {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<BlobFormatWriter> writer,
            BlobFormatWriter::Create(output_stream_, struct_type_, io_error_fs, pool_,
                                     /*write_null_on_missing_file=*/true,
                                     /*write_null_on_fetch_failure=*/false));
        ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, array), "mock io error");
    }
    {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<BlobFormatWriter> writer,
            BlobFormatWriter::Create(output_stream_, struct_type_, io_error_fs, pool_,
                                     /*write_null_on_missing_file=*/false,
                                     /*write_null_on_fetch_failure=*/true));
        ASSERT_OK(AddBatchOnce(writer, array));
    }
    {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<BlobFormatWriter> writer,
            BlobFormatWriter::Create(output_stream_, struct_type_, not_exist_fs, pool_,
                                     /*write_null_on_missing_file=*/false,
                                     /*write_null_on_fetch_failure=*/true));
        ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, array), "mock not exist");
    }
}

TEST_P(BlobFormatWriterTest, TestAddBatchWithZeroLengthBlob) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, file_system_, pool_));

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
    ASSERT_OK_AND_ASSIGN(int64_t file_length, input_stream->Length());
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
