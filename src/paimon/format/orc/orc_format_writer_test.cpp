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

#include "paimon/format/orc/orc_format_writer.h"

#include <cstddef>
#include <list>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "orc/Common.hh"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/Writer.hh"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/format/orc/orc_input_stream_impl.h"
#include "paimon/format/orc/orc_metrics.h"
#include "paimon/format/orc/orc_output_stream_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::orc::test {

class OrcFormatWriterTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    std::pair<arrow::Schema, std::shared_ptr<arrow::DataType>> PrepareArrowSchema() const {
        auto string_field = arrow::field("col1", arrow::utf8());
        auto int_field = arrow::field("col2", arrow::int32());
        auto bool_field = arrow::field("col3", arrow::boolean());
        auto struct_type = arrow::struct_({string_field, int_field, bool_field});
        return std::make_pair(
            arrow::Schema(arrow::FieldVector({string_field, int_field, bool_field})), struct_type);
    }

    std::shared_ptr<arrow::Array> PrepareArray(const std::shared_ptr<arrow::DataType>& data_type,
                                               int32_t record_batch_size,
                                               int32_t offset = 0) const {
        arrow::StructBuilder struct_builder(
            data_type, arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int32Builder>(),
             std::make_shared<arrow::BooleanBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto int_builder = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(1));
        auto bool_builder = static_cast<arrow::BooleanBuilder*>(struct_builder.field_builder(2));
        for (int32_t i = 0 + offset; i < record_batch_size + offset; ++i) {
            EXPECT_TRUE(struct_builder.Append().ok());
            if (i % 2 == 0) {
                EXPECT_TRUE(string_builder->AppendNull().ok());
                EXPECT_TRUE(int_builder->AppendNull().ok());
                EXPECT_TRUE(bool_builder->AppendNull().ok());
            } else {
                EXPECT_TRUE(string_builder->Append("str_" + std::to_string(i)).ok());
                EXPECT_TRUE(int_builder->Append(i).ok());
                EXPECT_TRUE(bool_builder->Append(true).ok());
            }
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    void AddRecordBatchOnce(const std::shared_ptr<OrcFormatWriter>& format_writer,
                            const std::shared_ptr<arrow::DataType>& struct_type,
                            int32_t record_batch_size, int32_t offset) const {
        auto array = PrepareArray(struct_type, record_batch_size, offset);
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());
        auto batch = std::make_shared<RecordBatch>(
            /*partition=*/std::map<std::string, std::string>(), /*bucket=*/-1,
            /*row_kinds=*/std::vector<RecordBatch::RowKind>(), arrow_array.get());
        ASSERT_OK(format_writer->AddBatch(batch->GetData()));
    }

    void CheckResult(const std::string& file_name, int32_t batch_size) const {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(file_name));
        ASSERT_OK_AND_ASSIGN(auto in_stream,
                             OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
        ::orc::ReaderOptions reader_options;
        std::unique_ptr<::orc::Reader> reader =
            ::orc::createReader(std::move(in_stream), reader_options);
        ::orc::RowReaderOptions options;
        options.setUseTightNumericVector(true);
        std::unique_ptr<::orc::RowReader> row_reader = reader->createRowReader(options);
        auto sub_type_count = row_reader->getSelectedType().getSubtypeCount();
        ASSERT_EQ(3, sub_type_count);
        std::list<std::string> expect_selected_type = {"col1", "col2", "col3"};
        std::list<std::string> result_selected_type;
        for (size_t i = 0; i < sub_type_count; i++) {
            result_selected_type.push_back(row_reader->getSelectedType().getFieldName(i));
        }
        ASSERT_EQ(expect_selected_type, result_selected_type);

        auto batch = row_reader->createRowBatch(batch_size);
        ASSERT_TRUE(row_reader->next(*batch));
        ASSERT_EQ(batch_size, batch->numElements);

        auto struct_batch = dynamic_cast<::orc::StructVectorBatch*>(batch.get());
        ASSERT_TRUE(struct_batch);
        auto string_batch = static_cast<::orc::StringVectorBatch*>(struct_batch->fields[0]);
        ASSERT_TRUE(string_batch);
        auto int_batch = dynamic_cast<::orc::IntVectorBatch*>(struct_batch->fields[1]);
        ASSERT_TRUE(int_batch);
        auto bool_batch = dynamic_cast<::orc::ByteVectorBatch*>(struct_batch->fields[2]);
        ASSERT_TRUE(bool_batch);
        ASSERT_TRUE(string_batch->hasNulls);
        ASSERT_TRUE(int_batch->hasNulls);
        ASSERT_TRUE(bool_batch->hasNulls);
        for (int32_t i = 0; i < batch_size; i++) {
            if (i % 2 == 0) {
                ASSERT_FALSE(string_batch->notNull[i]);
                ASSERT_FALSE(int_batch->notNull[i]);
                ASSERT_FALSE(bool_batch->notNull[i]);
            } else {
                ASSERT_EQ("str_" + std::to_string(i),
                          std::string(string_batch->data[i], string_batch->length[i]));
                ASSERT_EQ(i, int_batch->data[i]);
                ASSERT_EQ(true, bool_batch->data[i]);
            }
        }
    }

 private:
    std::shared_ptr<LocalFileSystem> file_system_ = std::make_shared<LocalFileSystem>();
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(OrcFormatWriterTest, TestWriteWithVariousBatchSize) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    ASSERT_OK(file_system_->Mkdirs(test_root));
    std::string file_name = test_root + "/test.orc";

    auto schema_pair = PrepareArrowSchema();
    const auto& arrow_schema = schema_pair.first;
    const auto& struct_type = schema_pair.second;
    std::map<std::string, std::string> options = {{ORC_WRITE_ENABLE_METRICS, "true"}};
    for (auto record_batch_size : {1, 2, 3, 5, 20}) {
        for (auto orc_batch_capacity : {1, 2, 3, 5, 20}) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                                 file_system_->Create(file_name, /*overwrite=*/true));
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> output_stream,
                                 OrcOutputStreamImpl::Create(out));
            ASSERT_OK_AND_ASSIGN(
                auto format_writer,
                OrcFormatWriter::Create(std::move(output_stream), arrow_schema, options,
                                        /*compression=*/"lz4", orc_batch_capacity, pool_));
            auto array = PrepareArray(struct_type, record_batch_size);
            auto arrow_array = std::make_unique<ArrowArray>();
            ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());

            auto batch = std::make_shared<RecordBatch>(
                /*partition=*/std::map<std::string, std::string>(), /*bucket=*/-1,
                /*row_kinds=*/std::vector<RecordBatch::RowKind>(), arrow_array.get());
            ASSERT_OK(format_writer->AddBatch(batch->GetData()));
            ASSERT_OK(format_writer->Finish());
            ASSERT_OK(out->Flush());
            ASSERT_OK(out->Close());
            CheckResult(file_name, record_batch_size);
            // test metrics
            auto writer_metrics = format_writer->GetWriterMetrics();
            ASSERT_OK_AND_ASSIGN(uint64_t io_count,
                                 writer_metrics->GetCounter(OrcMetrics::WRITE_IO_COUNT));
            ASSERT_GT(io_count, 0);
        }
    }
}
TEST_F(OrcFormatWriterTest, TestPrepareOptionsFileCompression) {
    arrow::FieldVector fields;
    std::shared_ptr<arrow::DataType> data_type = arrow::struct_(fields);
    {
        ASSERT_OK_AND_ASSIGN(auto writer_options,
                             OrcFormatWriter::PrepareWriterOptions({}, "lz4", data_type));
        ASSERT_EQ(writer_options.getCompression(), ::orc::CompressionKind_LZ4);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto writer_options,
                             OrcFormatWriter::PrepareWriterOptions({}, "zstd", data_type));
        ASSERT_EQ(writer_options.getCompression(), ::orc::CompressionKind_ZSTD);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto writer_options,
                             OrcFormatWriter::PrepareWriterOptions({}, "LZ4", data_type));
        ASSERT_EQ(writer_options.getCompression(), ::orc::CompressionKind_LZ4);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto writer_options,
                             OrcFormatWriter::PrepareWriterOptions({}, "ZSTd", data_type));
        ASSERT_EQ(writer_options.getCompression(), ::orc::CompressionKind_ZSTD);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto writer_options,
                             OrcFormatWriter::PrepareWriterOptions({}, "zlib", data_type));
        ASSERT_EQ(writer_options.getCompression(), ::orc::CompressionKind_ZLIB);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto writer_options,
                             OrcFormatWriter::PrepareWriterOptions({}, "snappy", data_type));
        ASSERT_EQ(writer_options.getCompression(), ::orc::CompressionKind_SNAPPY);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto writer_options,
                             OrcFormatWriter::PrepareWriterOptions({}, "lzo", data_type));
        ASSERT_EQ(writer_options.getCompression(), ::orc::CompressionKind_LZO);
    }
    {
        ASSERT_NOK_WITH_MSG(OrcFormatWriter::PrepareWriterOptions({}, "unknown", data_type),
                            "unknown compression");
    }
}
TEST_F(OrcFormatWriterTest, TestPrepareWriterOptions) {
    arrow::FieldVector fields;
    std::shared_ptr<arrow::DataType> data_type = arrow::struct_(fields);
    {
        // test default value
        std::map<std::string, std::string> options = {};
        ASSERT_OK_AND_ASSIGN(auto writer_options,
                             OrcFormatWriter::PrepareWriterOptions(options, "zstd", data_type));
        ASSERT_EQ(writer_options.getCompression(), ::orc::CompressionKind_ZSTD);
        ASSERT_TRUE(writer_options.getEnableDictionary());
        ASSERT_EQ(writer_options.getDictionaryKeySizeThreshold(),
                  DEFAULT_DICTIONARY_KEY_SIZE_THRESHOLD);
        ASSERT_EQ(writer_options.getStripeSize(), DEFAULT_STRIPE_SIZE);
        ASSERT_EQ(writer_options.getRowIndexStride(), DEFAULT_ROW_INDEX_STRIDE);
        ASSERT_EQ(writer_options.getCompressionBlockSize(), DEFAULT_COMPRESSION_BLOCK_SIZE);
        ASSERT_TRUE(writer_options.getUseTightNumericVector());
    }
    {
        // test specific value
        std::map<std::string, std::string> options = {{ORC_STRIPE_SIZE, "4096"},
                                                      {ORC_ROW_INDEX_STRIDE, "10"},
                                                      {ORC_COMPRESSION_BLOCK_SIZE, "2048"},
                                                      {ORC_DICTIONARY_KEY_SIZE_THRESHOLD, "0.98"}};
        ASSERT_OK_AND_ASSIGN(auto writer_options,
                             OrcFormatWriter::PrepareWriterOptions(options, "zstd", data_type));
        ASSERT_EQ(writer_options.getCompression(), ::orc::CompressionKind_ZSTD);
        ASSERT_TRUE(writer_options.getEnableDictionary());
        ASSERT_EQ(writer_options.getDictionaryKeySizeThreshold(), 0.98);
        ASSERT_EQ(writer_options.getStripeSize(), 4096);
        ASSERT_EQ(writer_options.getRowIndexStride(), 10);
        ASSERT_EQ(writer_options.getCompressionBlockSize(), 2048);
        ASSERT_TRUE(writer_options.getUseTightNumericVector());
    }
    {
        // test disable dictionary
        std::map<std::string, std::string> options = {{ORC_DICTIONARY_KEY_SIZE_THRESHOLD, "0"}};
        ASSERT_OK_AND_ASSIGN(auto writer_options,
                             OrcFormatWriter::PrepareWriterOptions(options, "zstd", data_type));
        ASSERT_FALSE(writer_options.getEnableDictionary());
    }
    {
        // test disable config for timestamp with timezone
        arrow::FieldVector invalid_fields = {
            arrow::field("f0", arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai"))};
        auto invalid_data_type = arrow::struct_(invalid_fields);
        std::map<std::string, std::string> options = {};
        ASSERT_NOK_WITH_MSG(
            OrcFormatWriter::PrepareWriterOptions(options, "zstd", invalid_data_type),
            "invalid config, do not support writing timestamp with timezone in legacy format");
    }
}
// TODO(liancheng.lsz): add tests for GetEstimateLength

}  // namespace paimon::orc::test
