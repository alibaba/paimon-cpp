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

#include "paimon/format/blob/blob_file_batch_reader.h"

#include "arrow/api.h"
#include "arrow/c/helpers.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/data/blob.h"
#include "paimon/format/blob/blob_format_writer.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::blob::test {

class BlobFileBatchReaderTest : public testing::Test, public ::testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void CheckResult(const std::string& table_path, const std::string& paimon_blob_file,
                     const std::vector<std::string>& original_blob_files, bool blob_as_descriptor,
                     const std::optional<RoaringBitmap32>& selection_bitmap = std::nullopt) {
        auto schema = arrow::schema({BlobUtils::ToArrowField(blob_field_name_, false)});
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             fs->Open(table_path + "/bucket-0/" + paimon_blob_file));
        ASSERT_OK_AND_ASSIGN(auto reader,
                             BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                                         blob_as_descriptor, pool_));
        ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, selection_bitmap));
        ASSERT_OK_AND_ASSIGN(auto chunked_array,
                             paimon::test::ReadResultCollector::CollectResult(reader.get()));
        if (chunked_array == nullptr) {
            ASSERT_EQ(0, original_blob_files.size());
            return;
        }

        std::shared_ptr<arrow::Array> combined_array =
            arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        if (original_blob_files.size() == 0) {
            ASSERT_EQ(0, combined_array->length());
            return;
        }
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(combined_array);
        ASSERT_TRUE(struct_array);
        auto blob_array =
            std::dynamic_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
        ASSERT_EQ(blob_array->length(), original_blob_files.size());
        for (size_t i = 0; i < original_blob_files.size(); i++) {
            ASSERT_OK_AND_ASSIGN(auto origin_input_stream,
                                 fs->Open(table_path + "/" + original_blob_files[i]));
            ASSERT_OK_AND_ASSIGN(auto origin_length, origin_input_stream->Length());
            auto origin_bytes = Bytes::AllocateBytes(origin_length, pool_.get());
            ASSERT_OK_AND_ASSIGN(auto actual_read_length,
                                 origin_input_stream->Read(origin_bytes->data(), origin_length));
            ASSERT_EQ(actual_read_length, origin_length);
            if (blob_as_descriptor) {
                auto blob_descriptor = blob_array->GetString(i);
                ASSERT_OK_AND_ASSIGN(auto blob, Blob::FromDescriptor(blob_descriptor.data(),
                                                                     blob_descriptor.size()));
                ASSERT_OK_AND_ASSIGN(auto input_stream, blob->NewInputStream(fs));
                ASSERT_OK_AND_ASSIGN(auto pos, input_stream->GetPos());
                ASSERT_EQ(pos, 0);
                ASSERT_OK_AND_ASSIGN(auto length, input_stream->Length());
                auto bytes = Bytes::AllocateBytes(length, pool_.get());
                ASSERT_OK_AND_ASSIGN(auto actual_read_length,
                                     input_stream->Read(bytes->data(), length));
                ASSERT_EQ(actual_read_length, length);
                ASSERT_EQ(length, origin_length);
                ASSERT_EQ(*bytes, *origin_bytes);
            } else {
                auto blob_data = blob_array->GetString(i);
                ASSERT_EQ(blob_data.size(), origin_length);
                std::string origin_data(origin_bytes->data(), origin_length);
                ASSERT_EQ(blob_data, origin_data);
            }
        }
    }

 private:
    std::string blob_field_name_;
    std::shared_ptr<MemoryPool> pool_;
};

TEST_P(BlobFileBatchReaderTest, TestSimple) {
    std::string test_data_path = paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    bool blob_as_descriptor = GetParam();
    ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob",
                {"blob_0_811d5dab.bin", "blob_1_b81cf9f4.bin", "blob_2_470e1dfe.bin"},
                blob_as_descriptor);
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-2.blob",
                {"blob_3_07b08c4d.bin", "blob_4_67007c96.bin"}, blob_as_descriptor);
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-3.blob",
                {"blob_5_f7099dea.bin", "blob_6_6b6706ef.bin", "blob_7_6bcae65e.bin",
                 "blob_8_5fba0737.bin"},
                blob_as_descriptor);
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-4.blob",
                {"blob_9_f54d253c.bin"}, blob_as_descriptor);
}

TEST_P(BlobFileBatchReaderTest, TestPushdownBitmap) {
    std::string test_data_path = paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    bool blob_as_descriptor = GetParam();
    ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));
    RoaringBitmap32 roaring_0;
    roaring_0.Add(0);
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob",
                {"blob_0_811d5dab.bin"}, blob_as_descriptor, roaring_0);
    RoaringBitmap32 roaring_1;
    roaring_1.Add(1);

    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-2.blob",
                {"blob_4_67007c96.bin"}, blob_as_descriptor, roaring_1);
    RoaringBitmap32 roaring_2;
    roaring_2.Add(0);
    roaring_2.Add(1);
    roaring_2.Add(3);
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-3.blob",
                {"blob_5_f7099dea.bin", "blob_6_6b6706ef.bin", "blob_8_5fba0737.bin"},
                blob_as_descriptor, roaring_2);
    RoaringBitmap32 roaring_3;
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-4.blob", {},
                blob_as_descriptor, roaring_3);
}

TEST_F(BlobFileBatchReaderTest, TestRowNumbers) {
    auto schema = arrow::schema({BlobUtils::ToArrowField("my_blob_field", false)});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

    std::string test_data_path = paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<InputStream> input_stream,
        fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
    ASSERT_OK_AND_ASSIGN(auto reader, BlobFileBatchReader::Create(
                                          input_stream,
                                          /*batch_size=*/1, /*blob_as_descriptor=*/true, pool_));

    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto number_of_rows, reader->GetNumberOfRows());
    ASSERT_EQ(3, number_of_rows);
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), reader->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(auto batch1, reader->NextBatch());
    ArrowArrayRelease(batch1.first.get());
    ArrowSchemaRelease(batch1.second.get());
    ASSERT_EQ(0, reader->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(auto batch2, reader->NextBatch());
    ASSERT_EQ(1, reader->GetPreviousBatchFirstRowNumber());
    ArrowArrayRelease(batch2.first.get());
    ArrowSchemaRelease(batch2.second.get());
    ASSERT_OK_AND_ASSIGN(auto batch3, reader->NextBatch());
    ASSERT_EQ(2, reader->GetPreviousBatchFirstRowNumber());
    ArrowArrayRelease(batch3.first.get());
    ArrowSchemaRelease(batch3.second.get());
    ASSERT_OK_AND_ASSIGN(auto batch4, reader->NextBatch());
    ASSERT_EQ(3, reader->GetPreviousBatchFirstRowNumber());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch4));
}

TEST_F(BlobFileBatchReaderTest, TestRowNumbersWithBitmap) {
    auto schema = arrow::schema({BlobUtils::ToArrowField("my_blob_field", false)});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

    std::string test_data_path = paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<InputStream> input_stream,
        fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
    ASSERT_OK_AND_ASSIGN(auto reader, BlobFileBatchReader::Create(
                                          input_stream,
                                          /*batch_size=*/1, /*blob_as_descriptor=*/true, pool_));
    RoaringBitmap32 roaring;
    roaring.Add(1);
    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, roaring));
    ASSERT_OK_AND_ASSIGN(auto number_of_rows, reader->GetNumberOfRows());
    ASSERT_EQ(3, number_of_rows);
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), reader->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(auto batch1, reader->NextBatch());
    ASSERT_EQ(1, reader->GetPreviousBatchFirstRowNumber());
    ArrowArrayRelease(batch1.first.get());
    ArrowSchemaRelease(batch1.second.get());
    ASSERT_OK_AND_ASSIGN(auto batch2, reader->NextBatch());
    ASSERT_EQ(3, reader->GetPreviousBatchFirstRowNumber());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch2));
}

TEST_F(BlobFileBatchReaderTest, InvalidScenario) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_system = std::make_shared<LocalFileSystem>();
    std::string test_data_path = paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
    std::string table_path = dir->Str();
    ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<InputStream> input_stream,
        fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
    {
        ASSERT_NOK_WITH_MSG(
            BlobFileBatchReader::Create(input_stream,
                                        /*batch_size=*/0, /*blob_as_descriptor=*/true, pool_),
            "blob file batch reader create failed: read batch size '0' should be larger than zero");
    }
    {
        ASSERT_NOK_WITH_MSG(
            BlobFileBatchReader::Create(/*input_stream=*/nullptr,
                                        /*batch_size=*/1, /*blob_as_descriptor=*/true, pool_),
            "blob file batch reader create failed: input stream is nullptr");
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            BlobFileBatchReader::Create(/*input_stream=*/input_stream,
                                        /*batch_size=*/1, /*blob_as_descriptor=*/true, pool_));
        ASSERT_NOK_WITH_MSG(reader->GetFileSchema(),
                            "blob file has no self-describing file schema");
        ASSERT_TRUE(reader->GetReaderMetrics());
        ASSERT_NOK_WITH_MSG(reader->NextBatch(),
                            "target type is nullptr, call SetReadSchema first");
        reader->Close();
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), "blob file batch reader is closed");
    }
}

TEST_P(BlobFileBatchReaderTest, EmptyFile) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output_stream,
                         file_system->Create(dir->Str() + "/file.blob", /*overwrite=*/true));
    std::shared_ptr<arrow::Field> blob_field = BlobUtils::ToArrowField("blob_col");
    auto struct_type = arrow::struct_({blob_field});
    bool blob_as_descriptor = GetParam();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(blob_as_descriptor, output_stream, struct_type,
                                                  file_system, pool_));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());
    ASSERT_OK(output_stream->Flush());
    auto schema = arrow::schema({blob_field});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system->Open(dir->Str() + "/file.blob"));
    ASSERT_OK_AND_ASSIGN(auto reader, BlobFileBatchReader::Create(
                                          input_stream,
                                          /*batch_size=*/1, /*blob_as_descriptor=*/true, pool_));

    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto number_of_rows, reader->GetNumberOfRows());
    ASSERT_EQ(0, number_of_rows);
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), reader->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
}

TEST_F(BlobFileBatchReaderTest, SetReadSchemaWithInvalidInputs) {
    {
        std::string test_data_path =
            paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
        auto dir = paimon::test::UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<InputStream> input_stream,
            fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            BlobFileBatchReader::Create(input_stream,
                                        /*batch_size=*/1, /*blob_as_descriptor=*/true, pool_));
        ASSERT_NOK_WITH_MSG(reader->SetReadSchema(/*read_schema=*/nullptr, /*predicate=*/nullptr,
                                                  /*selection_bitmap=*/std::nullopt),
                            "SetReadSchema failed: read schema cannot be nullptr");
    }
    {
        auto schema = arrow::schema({BlobUtils::ToArrowField("my_blob_field", false),
                                     BlobUtils::ToArrowField("my_blob_field_2", false)});

        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        std::string test_data_path =
            paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
        auto dir = paimon::test::UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<InputStream> input_stream,
            fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            BlobFileBatchReader::Create(input_stream,
                                        /*batch_size=*/1, /*blob_as_descriptor=*/true, pool_));
        ASSERT_NOK_WITH_MSG(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                                  /*selection_bitmap=*/std::nullopt),
                            "read schema field number 2 is not 1");
    }
    {
        auto blob_field = arrow::field("my_blob_field", arrow::large_binary());

        auto schema = arrow::schema({blob_field});
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        std::string test_data_path =
            paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
        auto dir = paimon::test::UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<InputStream> input_stream,
            fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            BlobFileBatchReader::Create(input_stream,
                                        /*batch_size=*/1, /*blob_as_descriptor=*/true, pool_));
        ASSERT_NOK_WITH_MSG(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                                  /*selection_bitmap=*/std::nullopt),
                            "field my_blob_field: large_binary is not BLOB");
    }
    {
        auto schema = arrow::schema({BlobUtils::ToArrowField("my_blob_field", false)});
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        std::string test_data_path =
            paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
        auto dir = paimon::test::UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<InputStream> input_stream,
            fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            BlobFileBatchReader::Create(input_stream,
                                        /*batch_size=*/1, /*blob_as_descriptor=*/true, pool_));
        RoaringBitmap32 roaring;
        roaring.Add(0);
        roaring.Add(1);
        roaring.Add(2);
        roaring.Add(3);
        roaring.Add(4);
        ASSERT_NOK_WITH_MSG(
            reader->SetReadSchema(&c_schema, /*predicate=*/nullptr, /*selection_bitmap=*/roaring),
            "Invalid: row index 3 is out of bound of total row number 3");
    }
}

INSTANTIATE_TEST_SUITE_P(BlobAsDescriptor, BlobFileBatchReaderTest, ::testing::Values(true, false));

}  // namespace paimon::blob::test
