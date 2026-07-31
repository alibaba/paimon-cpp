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
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/stream_utils.h"
#include "paimon/data/blob.h"
#include "paimon/format/blob/blob_file_batch_reader.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::blob::test {

/// A file system whose Open() always fails with the configured status while Exists() keeps the
/// real local check, standing in for a plugin that reports a missing file as something other than
/// Status::NotExist. Open() calls are counted so a test can assert a missing file is never opened.
class OpenFailFileSystem : public LocalFileSystem {
 public:
    explicit OpenFailFileSystem(Status open_status) : open_status_(std::move(open_status)) {}

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        ++open_call_count_;
        return open_status_;
    }

    int64_t OpenCallCount() const {
        return open_call_count_;
    }

 private:
    Status open_status_;
    mutable int64_t open_call_count_ = 0;
};

/// A file system whose Exists() always fails with the configured status, counting the calls. By
/// default Open() is delegated to a separate LocalFileSystem so that it still succeeds
/// (LocalFileSystem::Open() calls Exists() on itself, so without the delegation a failed check
/// would also fail the open); a non-OK `open_status` makes Open() fail with it instead.
class ExistsFailFileSystem : public LocalFileSystem {
 public:
    explicit ExistsFailFileSystem(Status exists_status, Status open_status = Status::OK())
        : exists_status_(std::move(exists_status)), open_status_(std::move(open_status)) {}

    Result<bool> Exists(const std::string& path) const override {
        ++exists_call_count_;
        return exists_status_;
    }

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        if (!open_status_.ok()) {
            return open_status_;
        }
        return real_fs_.Open(path);
    }

    int64_t ExistsCallCount() const {
        return exists_call_count_;
    }

 private:
    Status exists_status_;
    Status open_status_;
    LocalFileSystem real_fs_;
    mutable int64_t exists_call_count_ = 0;
};

/// A file system that reports a file as present on the first Exists() and absent afterwards,
/// standing in for a file deleted between the check and the open. Open() fails with a plain
/// IOError, so a test can tell a re-checked classification apart from one taken from the open.
class VanishingFileSystem : public LocalFileSystem {
 public:
    Result<bool> Exists(const std::string& path) const override {
        return ++exists_call_count_ == 1;
    }

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        return Status::IOError("mock io error");
    }

    int64_t ExistsCallCount() const {
        return exists_call_count_;
    }

 private:
    mutable int64_t exists_call_count_ = 0;
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

    /// Create a writer on output_stream_ with both write-null options disabled.
    Result<std::unique_ptr<BlobFormatWriter>> CreateDefaultWriter() const {
        return BlobFormatWriter::Create(output_stream_, struct_type_,
                                        /*write_null_on_missing_file=*/false,
                                        /*write_null_on_fetch_failure=*/false,
                                        /*write_placeholder=*/false, file_system_, pool_);
    }

    /// Create a writer in placeholder mode, as used by data-evolution partial updates.
    Result<std::unique_ptr<BlobFormatWriter>> CreatePlaceholderWriter() const {
        return BlobFormatWriter::Create(output_stream_, struct_type_,
                                        /*write_null_on_missing_file=*/false,
                                        /*write_null_on_fetch_failure=*/false,
                                        /*write_placeholder=*/true, file_system_, pool_);
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

    /// Build a single-row blob array holding `bytes` verbatim, bypassing the Blob helpers.
    Result<std::shared_ptr<arrow::Array>> MakeBlobArrayFromBytes(const std::string& bytes) const {
        arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::LargeBinaryBuilder>()});
        auto blob_builder =
            static_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Append());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(blob_builder->Append(bytes.data(), bytes.size()));
        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Finish(&array));
        return array;
    }

    Result<std::shared_ptr<arrow::StructArray>> ReadBackAsData() const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                               file_system_->Open(dir_->Str() + "/file.blob"));
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<BlobFileBatchReader> reader,
            BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                        /*blob_as_descriptor=*/false,
                                        /*emit_placeholder_sentinel=*/false, pool_));
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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

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
        BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024, blob_as_descriptor_,
                                    /*emit_placeholder_sentinel=*/false, pool_));
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
        BlobFormatWriter::Create(nullptr, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system_, pool_),
        "blob format writer create failed. out is nullptr");

    // Test with nullptr data type
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(output_stream_, nullptr, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system_, pool_),
        "blob format writer create failed. data_type is nullptr");

    // Test with nullptr memory pool
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system_, nullptr),
        "blob format writer create failed. pool is nullptr");

    // Test with nullptr file system
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, nullptr, pool_),
        "blob format writer create failed. fs is nullptr");

    // Test with invalid field count (more than 1 field)
    auto multi_field_type = arrow::struct_(
        {arrow::field("blob_col1", arrow::binary()), arrow::field("blob_col2", arrow::binary())});
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(output_stream_, multi_field_type,
                                                 /*write_null_on_missing_file=*/false,
                                                 /*write_null_on_fetch_failure=*/false,
                                                 /*write_placeholder=*/false, file_system_, pool_),
                        "blob data type field number 2 is not 1");

    // Test with non-blob field (missing blob metadata)
    auto non_blob_field = arrow::field("regular_col", arrow::binary());
    auto non_blob_type = arrow::struct_({non_blob_field});
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(output_stream_, non_blob_type,
                                                 /*write_null_on_missing_file=*/false,
                                                 /*write_null_on_fetch_failure=*/false,
                                                 /*write_placeholder=*/false, file_system_, pool_),
                        "field regular_col: binary is not BLOB");
}

TEST_P(BlobFormatWriterTest, TestInvalidCase) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

    auto metrics = writer->GetWriterMetrics();
    ASSERT_TRUE(metrics);
}

TEST_P(BlobFormatWriterTest, TestEmptyWriter) {
    // Test creating a writer and finishing without adding any data
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

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
        BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024, blob_as_descriptor_,
                                    /*emit_placeholder_sentinel=*/false, pool_));
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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

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
        BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024, blob_as_descriptor_,
                                    /*emit_placeholder_sentinel=*/false, pool_));
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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer2, CreateDefaultWriter());
    ASSERT_NOK_WITH_MSG(writer2->AddBatch(null_c_array.get()),
                        "BlobFormatWriter does not support struct-level null.");
    ArrowArrayRelease(null_c_array.get());
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnMissingFile) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system_, pool_));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto missing_array, PrepareDescriptorArray(missing_blob));
    ASSERT_OK(AddBatchOnce(writer, missing_array));

    // A fetch failure is not converted to NULL by write_null_on_missing_file alone; the
    // rejected row leaves the writer usable.
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
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, file_system_, pool_));

    std::string file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> bad_offset_blob,
                         Blob::FromPath(file, /*offset=*/1 << 20, /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto bad_offset_array, PrepareDescriptorArray(bad_offset_blob));
    ASSERT_OK(AddBatchOnce(writer, bad_offset_array));

    // Without write_null_on_missing_file no existence check runs, so a missing file is not told
    // apart from any other failed open and is converted by this option.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto missing_array, PrepareDescriptorArray(missing_blob));
    ASSERT_OK(AddBatchOnce(writer, missing_array));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Both rows count as fetch failures.
    ASSERT_OK_AND_ASSIGN(
        uint64_t missing_nulls,
        writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
    ASSERT_EQ(missing_nulls, 0);
    ASSERT_OK_AND_ASSIGN(
        uint64_t fetch_failure_nulls,
        writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
    ASSERT_EQ(fetch_failure_nulls, 2);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 2);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));
    ASSERT_TRUE(result_struct->field(0)->IsNull(1));
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnBothOptionsEnabled) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, file_system_, pool_));

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

    // The two NULL rows had different causes, counted separately.
    ASSERT_OK_AND_ASSIGN(
        uint64_t missing_nulls,
        writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
    ASSERT_EQ(missing_nulls, 1);
    ASSERT_OK_AND_ASSIGN(
        uint64_t fetch_failure_nulls,
        writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
    ASSERT_EQ(fetch_failure_nulls, 1);

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

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullClassifiesByExistence) {
    // Each case needs its own option pair and therefore its own writer, so none of them finishes
    // the shared output stream: this test only asserts how a failure is classified. That the
    // resulting NULL element is written correctly is covered by the TestWriteNullOn* tests.
    auto io_error_fs = std::make_shared<OpenFailFileSystem>(Status::IOError("mock io error"));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto missing_array, PrepareDescriptorArray(missing_blob));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> existing_blob,
                         Blob::FromPath(paimon::test::GetDataDir() + "/xxhash.data"));
    ASSERT_OK_AND_ASSIGN(auto existing_array, PrepareDescriptorArray(existing_blob));

    // Missing file: classified without opening it, so what Open would return is irrelevant.
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, io_error_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, missing_array));
        ASSERT_EQ(io_error_fs->OpenCallCount(), 0);
    }
    // Existing file that cannot be opened: a fetch failure, which this writer does not convert.
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, io_error_fs, pool_));
        ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, existing_array), "mock io error");
    }
    // The same fetch failure, now converted to NULL.
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, io_error_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, existing_array));
    }
    // Missing file with only fetch-failure enabled: no existence check runs, so the file is
    // opened and the failure is converted like any other fetch failure. The mock's count
    // accumulates across cases, so compare against it.
    {
        const int64_t open_calls_before = io_error_fs->OpenCallCount();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, io_error_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, missing_array));
        ASSERT_EQ(io_error_fs->OpenCallCount(), open_calls_before + 1);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 1);
    }

    // A file deleted between the check and the open is still a missing file: the failed open
    // triggers one more check rather than being classified by its status. Without it the deletion
    // would defeat write_null_on_missing_file, which does not convert a fetch failure. Each case
    // needs its own file system, since the mock reports the file as present only on the first call.
    {
        auto vanishing_fs = std::make_shared<VanishingFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, vanishing_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, existing_array));
        // One check before the open and one after it.
        ASSERT_EQ(vanishing_fs->ExistsCallCount(), 2);
        ASSERT_OK_AND_ASSIGN(
            uint64_t missing_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
        ASSERT_EQ(missing_nulls, 1);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 0);
    }
    // The same deletion with both options enabled: classified as missing rather than swallowed
    // by fetch-failure.
    {
        auto vanishing_fs = std::make_shared<VanishingFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, vanishing_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, existing_array));
        ASSERT_EQ(vanishing_fs->ExistsCallCount(), 2);
        ASSERT_OK_AND_ASSIGN(
            uint64_t missing_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
        ASSERT_EQ(missing_nulls, 1);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 0);
    }
    // The same deletion with only fetch-failure enabled: existence is never consulted, and the
    // failed open is converted like any other fetch failure.
    {
        auto vanishing_fs = std::make_shared<VanishingFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, vanishing_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, existing_array));
        ASSERT_EQ(vanishing_fs->ExistsCallCount(), 0);
    }
    // Neither option: existence is never consulted, and the open failure propagates as it is.
    {
        auto vanishing_fs = std::make_shared<VanishingFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, vanishing_fs, pool_));
        ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, existing_array), "mock io error");
        ASSERT_EQ(vanishing_fs->ExistsCallCount(), 0);
    }
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnInvalidDescriptor) {
    // Descriptor detection only inspects version and magic, so a descriptor truncated after those
    // passes detection and then fails to deserialize: a fetch failure, not a missing file.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob,
                         Blob::FromPath(paimon::test::GetDataDir() + "/xxhash.data"));
    PAIMON_UNIQUE_PTR<Bytes> descriptor = blob->ToDescriptor(pool_);
    ASSERT_GT(descriptor->size(), 8);
    std::string truncated(descriptor->data(), descriptor->size() - 8);
    ASSERT_OK_AND_ASSIGN(bool is_descriptor,
                         BlobDescriptor::IsBlobDescriptor(truncated.data(), truncated.size()));
    ASSERT_TRUE(is_descriptor);
    ASSERT_OK_AND_ASSIGN(auto array, MakeBlobArrayFromBytes(truncated));

    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system_, pool_));
        ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, array), "invalid blob descriptor");
    }
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, file_system_, pool_));
        ASSERT_OK(AddBatchOnce(writer, array));
        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());
    }

    // Only the second case reaches the stream; the first fails before writing any byte.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 1);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnExistsCheckFailure) {
    // An existence check that cannot answer leaves it unknown whether the file is there. With no
    // fetch-failure handling to defer to, the write fails; otherwise the failed check is deferred
    // to the open, whose own outcome decides.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob,
                         Blob::FromPath(paimon::test::GetDataDir() + "/xxhash.data"));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareDescriptorArray(blob));

    // Deferred check failure whose open succeeds: the blob is written as data, not as NULL.
    // This case finishes the shared output stream, so it runs first and is read back below.
    {
        auto exists_fail_fs =
            std::make_shared<ExistsFailFileSystem>(Status::IOError("mock exists error"));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, exists_fail_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, array));
        ASSERT_EQ(exists_fail_fs->ExistsCallCount(), 1);
        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());
        ASSERT_OK_AND_ASSIGN(
            uint64_t missing_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
        ASSERT_EQ(missing_nulls, 0);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 0);
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 1);
    ASSERT_FALSE(result_struct->field(0)->IsNull(0));
    auto binary_array =
        arrow::internal::checked_pointer_cast<arrow::LargeBinaryArray>(result_struct->field(0));
    ASSERT_OK_AND_ASSIGN(auto expected_data, blob->ToData(file_system_, pool_));
    ASSERT_EQ(binary_array->GetView(0),
              std::string_view(expected_data->data(), expected_data->size()));

    // With no fetch-failure handling to defer to, the check failure fails the write.
    {
        auto exists_fail_fs =
            std::make_shared<ExistsFailFileSystem>(Status::IOError("mock exists error"));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, exists_fail_fs, pool_));
        // The reported failure names the check and keeps the underlying status message.
        Status check_status = AddBatchOnce(writer, array);
        ASSERT_NOK_WITH_MSG(check_status, "failed to check existence of blob file");
        ASSERT_NOK_WITH_MSG(check_status, "mock exists error");
    }
    // Deferred check failure whose open then fails: a fetch failure. The re-check after the
    // failed open cannot answer either, so it falls through to the open failure.
    {
        auto exists_fail_fs = std::make_shared<ExistsFailFileSystem>(
            Status::IOError("mock exists error"), Status::IOError("mock open error"));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, exists_fail_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, array));
        // One check before the open and one after it failed.
        ASSERT_EQ(exists_fail_fs->ExistsCallCount(), 2);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 1);
    }
    // With only write_null_on_fetch_failure, no existence check runs at all; the open succeeds
    // and the blob is written as data. A separate output stream keeps the data bytes out of the
    // already finished shared stream.
    {
        auto exists_fail_fs =
            std::make_shared<ExistsFailFileSystem>(Status::IOError("mock exists error"));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> side_stream,
                             file_system_->Create(dir_->Str() + "/side.blob", /*overwrite=*/true));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 side_stream, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, exists_fail_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, array));
        ASSERT_EQ(exists_fail_fs->ExistsCallCount(), 0);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 0);
        ASSERT_OK(side_stream->Flush());
        ASSERT_OK(side_stream->Close());
    }
}

TEST_P(BlobFormatWriterTest, TestAddBatchWithZeroLengthBlob) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

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

/// Placeholder tests always feed the sentinel bytes of the placeholder write protocol, so
/// they do not depend on the blob_as_descriptor_ parameter and run once on the
/// non-parameterized fixture.
using BlobFormatWriterPlaceholderTest = BlobFormatWriterTestBase;

std::string PlaceholderSentinelBytes() {
    return std::string(BlobDefs::PlaceholderSentinelView());
}

TEST_F(BlobFormatWriterPlaceholderTest, TestWritePlaceholderGoldenBytes) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreatePlaceholderWriter());

    // row 0: inline bytes "inline"; row 1: null; row 2: placeholder
    ASSERT_OK_AND_ASSIGN(auto inline_array, MakeBlobArrayFromBytes("inline"));
    ASSERT_OK(AddBatchOnce(writer, inline_array));

    arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::LargeBinaryBuilder>()});
    auto blob_builder = static_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
    ASSERT_TRUE(struct_builder.Append().ok());
    ASSERT_TRUE(blob_builder->AppendNull().ok());
    std::shared_ptr<arrow::Array> null_array;
    ASSERT_TRUE(struct_builder.Finish(&null_array).ok());
    ASSERT_OK(AddBatchOnce(writer, null_array));

    ASSERT_OK_AND_ASSIGN(auto placeholder_array,
                         MakeBlobArrayFromBytes(PlaceholderSentinelBytes()));
    ASSERT_OK(AddBatchOnce(writer, placeholder_array));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Verify byte-level alignment with the Java writer (BlobFormatWriterTest
    // testRawBlobGoldenBytes): null and placeholder rows occupy no data bytes; the index
    // records [22, -1, -2] as zigzag varint deltas [0x2c, 0x2d, 0x01].
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_TRUE(input_stream);
    ASSERT_OK_AND_ASSIGN(int64_t file_length, input_stream->Length());
    ASSERT_EQ(file_length, 30);
    std::vector<uint8_t> buffer(file_length);
    ASSERT_OK_AND_ASSIGN(auto read_length,
                         input_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_EQ(read_length, 30);
    std::vector<uint8_t> expected = {{// record 0: magic + "inline" + bin_length(22) + crc32
                                      0xcf, 0x11, 0x4e, 0x58, 0x69, 0x6e, 0x6c, 0x69, 0x6e, 0x65,
                                      0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x60,
                                      0xc8, 0xe9,
                                      // index of [22, -1, -2]
                                      0x2c, 0x2d, 0x01,
                                      // footer: index length + version
                                      0x03, 0x00, 0x00, 0x00, 0x01}};
    ASSERT_EQ(buffer, expected);
}

TEST_F(BlobFormatWriterPlaceholderTest, TestReadPlaceholderStrictAndAwareModes) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreatePlaceholderWriter());
    ASSERT_OK_AND_ASSIGN(auto inline_array, MakeBlobArrayFromBytes("inline"));
    ASSERT_OK(AddBatchOnce(writer, inline_array));
    ASSERT_OK_AND_ASSIGN(auto placeholder_array,
                         MakeBlobArrayFromBytes(PlaceholderSentinelBytes()));
    ASSERT_OK(AddBatchOnce(writer, placeholder_array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    auto schema = arrow::schema(struct_type_->fields());

    // default (strict) mode: reading a placeholder entry fails
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(dir_->Str() + "/file.blob"));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<BlobFileBatchReader> reader,
            BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                        /*blob_as_descriptor=*/false,
                                        /*emit_placeholder_sentinel=*/false, pool_));
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), "placeholder");
    }

    // placeholder-aware mode: the entry is returned as the sentinel bytes
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(dir_->Str() + "/file.blob"));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<BlobFileBatchReader> reader,
            BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                        /*blob_as_descriptor=*/false,
                                        /*emit_placeholder_sentinel=*/true, pool_));
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto chunked_array,
                             paimon::test::ReadResultCollector::CollectResult(reader.get()));
        auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(concat_array);
        ASSERT_EQ(struct_array->length(), 2);
        auto binary_array =
            arrow::internal::checked_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
        ASSERT_EQ(binary_array->GetString(0), "inline");
        ASSERT_FALSE(binary_array->IsNull(1));
        ASSERT_EQ(binary_array->GetString(1), PlaceholderSentinelBytes());
    }

    // placeholder-aware descriptor mode also returns the sentinel bytes
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(dir_->Str() + "/file.blob"));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<BlobFileBatchReader> reader,
            BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                        /*blob_as_descriptor=*/true,
                                        /*emit_placeholder_sentinel=*/true, pool_));
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto chunked_array,
                             paimon::test::ReadResultCollector::CollectResult(reader.get()));
        auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(concat_array);
        auto binary_array =
            arrow::internal::checked_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
        ASSERT_EQ(binary_array->GetString(1), PlaceholderSentinelBytes());
    }
}

TEST_F(BlobFormatWriterPlaceholderTest, TestReadPlaceholderWithSelectionBitmap) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreatePlaceholderWriter());
    ASSERT_OK_AND_ASSIGN(auto array0, MakeBlobArrayFromBytes("first"));
    ASSERT_OK(AddBatchOnce(writer, array0));
    ASSERT_OK_AND_ASSIGN(auto array1, MakeBlobArrayFromBytes(PlaceholderSentinelBytes()));
    ASSERT_OK(AddBatchOnce(writer, array1));
    ASSERT_OK_AND_ASSIGN(auto array2, MakeBlobArrayFromBytes("third"));
    ASSERT_OK(AddBatchOnce(writer, array2));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                                     /*blob_as_descriptor=*/false,
                                                     /*emit_placeholder_sentinel=*/true, pool_));
    auto schema = arrow::schema(struct_type_->fields());
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    RoaringBitmap32 selection;
    selection.Add(1);
    selection.Add(2);
    ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr, selection));
    ASSERT_OK_AND_ASSIGN(auto chunked_array,
                         paimon::test::ReadResultCollector::CollectResult(reader.get()));
    auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
    auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(concat_array);
    ASSERT_EQ(struct_array->length(), 2);
    auto binary_array =
        arrow::internal::checked_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
    ASSERT_EQ(binary_array->GetString(0), PlaceholderSentinelBytes());
    ASSERT_EQ(binary_array->GetString(1), "third");
}

TEST_F(BlobFormatWriterPlaceholderTest, TestSentinelBytesVerbatimWithoutPlaceholderMode) {
    // outside placeholder mode a user blob whose bytes equal the sentinel is a normal value: it
    // must be stored as a real entry (not persisted as bin_length -2) and read back unchanged
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());
    ASSERT_OK_AND_ASSIGN(auto sentinel_array, MakeBlobArrayFromBytes(PlaceholderSentinelBytes()));
    ASSERT_OK(AddBatchOnce(writer, sentinel_array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                                     /*blob_as_descriptor=*/false,
                                                     /*emit_placeholder_sentinel=*/false, pool_));
    auto schema = arrow::schema(struct_type_->fields());
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto chunked_array,
                         paimon::test::ReadResultCollector::CollectResult(reader.get()));
    auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
    auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(concat_array);
    ASSERT_EQ(struct_array->length(), 1);
    auto binary_array =
        arrow::internal::checked_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
    ASSERT_FALSE(binary_array->IsNull(0));
    ASSERT_EQ(binary_array->GetString(0), PlaceholderSentinelBytes());
}

TEST_F(BlobFormatWriterPlaceholderTest, TestSentinelPrefixedValueVerbatimInPlaceholderMode) {
    // placeholders are identified by exact equality only: even in placeholder mode a real
    // value that merely starts with the sentinel bytes is stored verbatim and read back
    // unchanged in both strict and placeholder-aware modes
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreatePlaceholderWriter());
    std::string sentinel = PlaceholderSentinelBytes();
    ASSERT_OK_AND_ASSIGN(auto doubled_sentinel_array, MakeBlobArrayFromBytes(sentinel + sentinel));
    ASSERT_OK(AddBatchOnce(writer, doubled_sentinel_array));
    ASSERT_OK_AND_ASSIGN(auto prefixed_array, MakeBlobArrayFromBytes(sentinel + "suffix"));
    ASSERT_OK(AddBatchOnce(writer, prefixed_array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    auto schema = arrow::schema(struct_type_->fields());
    for (bool emit_placeholder_sentinel : {false, true}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(dir_->Str() + "/file.blob"));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                             BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                                         /*blob_as_descriptor=*/false,
                                                         emit_placeholder_sentinel, pool_));
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto chunked_array,
                             paimon::test::ReadResultCollector::CollectResult(reader.get()));
        auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(concat_array);
        auto binary_array =
            arrow::internal::checked_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
        ASSERT_EQ(struct_array->length(), 2);
        ASSERT_EQ(binary_array->GetString(0), sentinel + sentinel);
        ASSERT_EQ(binary_array->GetString(1), sentinel + "suffix");
    }
}

}  // namespace paimon::blob::test
