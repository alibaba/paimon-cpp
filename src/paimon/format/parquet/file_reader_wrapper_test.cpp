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

#include "paimon/format/parquet/file_reader_wrapper.h"

#include <map>
#include <string>

#include "arrow/api.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/io/caching.h"
#include "arrow/memory_pool.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_format_writer.h"
#include "paimon/format/parquet/parquet_input_stream_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/testharness.h"
#include "parquet/arrow/reader.h"
#include "parquet/properties.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::parquet::test {

class FileReaderWrapperTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = std::make_shared<LocalFileSystem>();
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
        batch_size_ = 512;
    }
    void TearDown() override {}

    std::pair<std::shared_ptr<arrow::Schema>, std::shared_ptr<arrow::DataType>> PrepareArrowSchema()
        const {
        auto string_field = arrow::field(
            "col1", arrow::utf8(),
            arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID}, {"0"}));
        auto int_field = arrow::field(
            "col2", arrow::int32(),
            arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID}, {"1"}));
        auto bool_field = arrow::field(
            "col3", arrow::boolean(),
            arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID}, {"2"}));
        auto struct_type = arrow::struct_({string_field, int_field, bool_field});
        return std::make_pair(
            arrow::schema(arrow::FieldVector({string_field, int_field, bool_field})), struct_type);
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
            EXPECT_TRUE(string_builder->Append("str_" + std::to_string(i)).ok());
            if (i % 3 == 0) {
                // test null
                EXPECT_TRUE(int_builder->AppendNull().ok());
            } else {
                EXPECT_TRUE(int_builder->Append(i).ok());
            }
            EXPECT_TRUE(bool_builder->Append(static_cast<bool>(i % 2)).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    void AddRecordBatchOnce(const std::shared_ptr<ParquetFormatWriter>& format_writer,
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

    Result<std::unique_ptr<FileReaderWrapper>> PrepareReaderWrapper(const std::string& file_path) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> in, fs_->Open(file_path));
        PAIMON_ASSIGN_OR_RAISE(uint64_t file_length, in->Length());
        auto input_stream = std::make_unique<ParquetInputStreamImpl>(in, arrow_pool_, file_length);
        ::parquet::arrow::FileReaderBuilder file_reader_builder;
        ::parquet::ReaderProperties reader_properties;
        reader_properties.enable_buffered_stream();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            file_reader_builder.Open(std::move(input_stream), reader_properties));

        ::parquet::ArrowReaderProperties arrow_reader_props;
        arrow_reader_props.set_pre_buffer(true);
        arrow_reader_props.set_batch_size(static_cast<int64_t>(batch_size_));
        arrow_reader_props.set_use_threads(true);
        arrow_reader_props.set_cache_options(arrow::io::CacheOptions::Defaults());
        std::unique_ptr<::parquet::arrow::FileReader> file_reader;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_builder.memory_pool(arrow_pool_.get())
                                            ->properties(arrow_reader_props)
                                            ->Build(&file_reader));
        return FileReaderWrapper::Create(std::move(file_reader));
    }

    void PrepareParquetFile(const std::string& file_path, int32_t row_count) {
        auto schema_pair = PrepareArrowSchema();
        const auto& arrow_schema = schema_pair.first;
        const auto& struct_type = schema_pair.second;

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_path, /*overwrite=*/false));
        ::parquet::WriterProperties::Builder builder;
        builder.write_batch_size(10);
        builder.max_row_group_length(1000);
        builder.enable_store_decimal_as_integer();
        auto writer_properties = builder.build();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<ParquetFormatWriter> format_writer,
            ParquetFormatWriter::Create(out, arrow_schema, writer_properties,
                                        DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));

        AddRecordBatchOnce(format_writer, struct_type, /*record_batch_size=*/row_count,
                           /*offset=*/0);
        ASSERT_OK(format_writer->Flush());
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
    }

 private:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    int32_t batch_size_;
};

TEST_F(FileReaderWrapperTest, EmptyFile) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/0);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    ASSERT_EQ(0, reader_wrapper->GetNumberOfRows());
    ASSERT_EQ(0, reader_wrapper->GetNumberOfRowGroups());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              reader_wrapper->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(auto batch, reader_wrapper->Next());
    ASSERT_EQ(0, reader_wrapper->GetPreviousBatchFirstRowNumber());
    ASSERT_EQ(0, reader_wrapper->GetNextRowToRead());
    ASSERT_TRUE(reader_wrapper->GetAllRowGroupRanges().empty());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch, reader_wrapper->Next());
    ASSERT_FALSE(record_batch);
}

TEST_F(FileReaderWrapperTest, NullFileReader) {
    ASSERT_NOK_WITH_MSG(FileReaderWrapper::Create(nullptr),
                        "file reader wrapper create failed. file reader is nullptr");
}

TEST_F(FileReaderWrapperTest, Simple) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/5500);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    ASSERT_EQ(5500, reader_wrapper->GetNumberOfRows());
    ASSERT_EQ(6, reader_wrapper->GetNumberOfRowGroups());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              reader_wrapper->GetPreviousBatchFirstRowNumber());
    std::vector<std::pair<uint64_t, uint64_t>> expected_all_row_group_ranges = {
        {0, 1000}, {1000, 2000}, {2000, 3000}, {3000, 4000}, {4000, 5000}, {5000, 5500}};
    ASSERT_EQ(expected_all_row_group_ranges, reader_wrapper->GetAllRowGroupRanges());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch, reader_wrapper->Next());
    ASSERT_TRUE(record_batch);
    ASSERT_EQ(512, record_batch->num_rows());
    ASSERT_EQ(512, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(0, reader_wrapper->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_TRUE(record_batch);
    ASSERT_EQ(488, record_batch->num_rows());
    ASSERT_EQ(1000, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(512, reader_wrapper->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_TRUE(record_batch);
    ASSERT_EQ(512, record_batch->num_rows());
    ASSERT_EQ(1512, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(1000, reader_wrapper->GetPreviousBatchFirstRowNumber());
    ASSERT_NOK_WITH_MSG(reader_wrapper->SeekToRow(1001),
                        "should not be in the middle of readable range");
    ASSERT_OK(reader_wrapper->SeekToRow(1000));
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_TRUE(record_batch);
    ASSERT_EQ(512, record_batch->num_rows());
    ASSERT_EQ(1512, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(1000, reader_wrapper->GetPreviousBatchFirstRowNumber());

    ASSERT_OK(reader_wrapper->SeekToRow(5600));
    ASSERT_EQ(5500, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(6, reader_wrapper->current_row_group_idx_);
    ASSERT_EQ(1000, reader_wrapper->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_FALSE(record_batch);
    ASSERT_EQ(5500, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(5500, reader_wrapper->GetPreviousBatchFirstRowNumber());
}

TEST_F(FileReaderWrapperTest, GetRowGroupRanges) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/5500);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    ASSERT_OK_AND_ASSIGN(auto ranges, reader_wrapper->GetRowGroupRanges({0, 3, 5}));
    std::vector<std::pair<uint64_t, uint64_t>> expected_read_ranges = {
        {0, 1000}, {3000, 4000}, {5000, 5500}};
    ASSERT_EQ(expected_read_ranges, ranges);
    ASSERT_NOK_WITH_MSG(reader_wrapper->GetRowGroupRanges({0, 3, 6}), "out of bound");
    ASSERT_OK_AND_ASSIGN(ranges, reader_wrapper->GetRowGroupRanges({}));
    ASSERT_TRUE(ranges.empty());
}

TEST_F(FileReaderWrapperTest, ReadRangesToRowGroupIds) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/5500);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    std::set<int32_t> expected_row_group_ids = {0, 3, 5};
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges = {
        {0, 1000}, {3000, 4000}, {5000, 5500}};
    ASSERT_OK_AND_ASSIGN(auto row_group_ids, reader_wrapper->ReadRangesToRowGroupIds(read_ranges));
    ASSERT_EQ(expected_row_group_ids, row_group_ids);
    std::vector<std::pair<uint64_t, uint64_t>> invalid_ranges = {
        {0, 1000}, {3000, 4000}, {5000, 5600}};
    ASSERT_NOK_WITH_MSG(reader_wrapper->ReadRangesToRowGroupIds(invalid_ranges),
                        "not match with row group range bound");
    ASSERT_OK_AND_ASSIGN(row_group_ids, reader_wrapper->ReadRangesToRowGroupIds({}));
    ASSERT_TRUE(row_group_ids.empty());
}

TEST_F(FileReaderWrapperTest, FilterRowGroupsByReadRanges) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/5500);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    std::set<int32_t> expected_row_group_ids = {0, 5};
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges = {
        {0, 1000}, {3000, 4000}, {5000, 5500}};
    ASSERT_OK_AND_ASSIGN(auto row_group_ids,
                         reader_wrapper->FilterRowGroupsByReadRanges(read_ranges, {0, 1, 2, 4, 5}));
    ASSERT_EQ(expected_row_group_ids, row_group_ids);

    ASSERT_OK_AND_ASSIGN(row_group_ids,
                         reader_wrapper->FilterRowGroupsByReadRanges(read_ranges, {}));
    ASSERT_TRUE(row_group_ids.empty());
}

TEST_F(FileReaderWrapperTest, PrepareForReading) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "test.parquet");
    PrepareParquetFile(file_path, /*row_count=*/5500);
    ASSERT_OK_AND_ASSIGN(auto reader_wrapper, PrepareReaderWrapper(file_path));
    ASSERT_OK(reader_wrapper->PrepareForReading(/*row_group_indices=*/{1},
                                                /*column_indices=*/{0}));
    // seek before actual read range
    ASSERT_OK(reader_wrapper->SeekToRow(0));
    ASSERT_EQ(1000, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              reader_wrapper->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(auto record_batch, reader_wrapper->Next());
    ASSERT_EQ(512, record_batch->num_rows());
    ASSERT_EQ(1, record_batch->num_columns());
    ASSERT_EQ(1512, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(1000, reader_wrapper->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_TRUE(record_batch);
    ASSERT_EQ(488, record_batch->num_rows());
    ASSERT_EQ(5500, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(1512, reader_wrapper->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_FALSE(record_batch);

    // empty column indices
    ASSERT_OK(reader_wrapper->PrepareForReading(/*row_group_indices=*/{0, 1},
                                                /*column_indices=*/{}));
    ASSERT_EQ(0, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              reader_wrapper->GetPreviousBatchFirstRowNumber());
    ASSERT_OK_AND_ASSIGN(record_batch, reader_wrapper->Next());
    ASSERT_EQ(512, record_batch->num_rows());
    ASSERT_EQ(0, record_batch->num_columns());

    // empty row group indices
    ASSERT_OK(reader_wrapper->PrepareForReading(/*row_group_indices=*/{},
                                                /*column_indices=*/{0}));
    ASSERT_EQ(5500, reader_wrapper->GetNextRowToRead());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              reader_wrapper->GetPreviousBatchFirstRowNumber());
}

}  // namespace paimon::parquet::test
