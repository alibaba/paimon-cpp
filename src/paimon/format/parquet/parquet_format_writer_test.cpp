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

#include "paimon/format/parquet/parquet_format_writer.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/io/file.h"
#include "arrow/ipc/api.h"
#include "arrow/memory_pool.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/testharness.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::parquet::test {

class ParquetFormatWriterTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = std::make_shared<LocalFileSystem>();
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
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

    void CheckResult(const std::string& file_path, int32_t row_count) const {
        auto file = arrow::io::ReadableFile::Open(file_path, arrow_pool_.get());
        ASSERT_TRUE(file.ok());
        std::unique_ptr<::parquet::arrow::FileReader> reader;
        auto status = ::parquet::arrow::OpenFile(file.ValueOrDie(), arrow_pool_.get(), &reader);
        ASSERT_TRUE(status.ok()) << status.ToString();
        const ::parquet::FileMetaData* metadata = reader->parquet_reader()->metadata().get();
        const ::parquet::SchemaDescriptor* schema = metadata->schema();
        ASSERT_EQ(metadata->num_row_groups(), 1);
        ASSERT_EQ(schema->num_columns(), 3);
        ASSERT_EQ(metadata->num_rows(), row_count);
        ASSERT_EQ("col1", schema->Column(0)->name());
        ASSERT_EQ("col2", schema->Column(1)->name());
        ASSERT_EQ("col3", schema->Column(2)->name());
        ASSERT_EQ(0, schema->Column(0)->schema_node()->field_id());
        ASSERT_EQ(1, schema->Column(1)->schema_node()->field_id());
        ASSERT_EQ(2, schema->Column(2)->schema_node()->field_id());

        std::shared_ptr<::arrow::ChunkedArray> col0_array, col1_array, col2_array;
        ASSERT_TRUE(reader->ReadColumn(0, &col0_array).ok());
        ASSERT_TRUE(reader->ReadColumn(1, &col1_array).ok());
        ASSERT_TRUE(reader->ReadColumn(2, &col2_array).ok());

        const auto& string_array =
            std::static_pointer_cast<arrow::StringArray>(col0_array->chunk(0));
        ASSERT_TRUE(string_array);
        const auto& int_array = std::static_pointer_cast<arrow::Int32Array>(col1_array->chunk(0));
        ASSERT_TRUE(int_array);
        const auto& bool_array =
            std::static_pointer_cast<arrow::BooleanArray>(col2_array->chunk(0));
        ASSERT_TRUE(bool_array);
        ASSERT_EQ(string_array->null_count(), 0);
        ASSERT_EQ(int_array->null_count(), (row_count - 1) / 3 + 1);
        ASSERT_EQ(bool_array->null_count(), 0);

        for (int32_t i = 0; i < row_count; i++) {
            ASSERT_EQ("str_" + std::to_string(i), string_array->GetString(i));
            if (i % 3 == 0) {
                ASSERT_TRUE(int_array->IsNull(i));
            } else {
                ASSERT_FALSE(int_array->IsNull(i));
                ASSERT_EQ(i, int_array->Value(i));
            }
            if (i % 2 == 0) {
                ASSERT_EQ(false, bool_array->Value(i));
            } else {
                ASSERT_EQ(true, bool_array->Value(i));
            }
        }
    }

 private:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

TEST_F(ParquetFormatWriterTest, TestWriteWithVariousBatchSize) {
    auto schema_pair = PrepareArrowSchema();
    const auto& arrow_schema = schema_pair.first;
    const auto& struct_type = schema_pair.second;
    std::map<std::string, std::string> options;
    for (auto record_batch_size : {1, 2, 3, 5, 20}) {
        for (auto batch_capacity : {1, 2, 3, 5, 20}) {
            std::string file_name =
                std::to_string(record_batch_size) + "_" + std::to_string(batch_capacity);
            std::string file_path = PathUtil::JoinPath(dir_->Str(), file_name);
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                                 fs_->Create(file_path, /*overwrite=*/false));
            ::parquet::WriterProperties::Builder builder;
            builder.write_batch_size(batch_capacity);
            auto writer_properties = builder.build();
            ASSERT_OK_AND_ASSIGN(
                auto format_writer,
                ParquetFormatWriter::Create(out, arrow_schema, writer_properties, arrow_pool_));
            auto array = PrepareArray(struct_type, record_batch_size);
            auto arrow_array = std::make_unique<ArrowArray>();
            ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());

            auto batch = std::make_shared<RecordBatch>(
                /*partition=*/std::map<std::string, std::string>(), /*bucket=*/-1,
                /*row_kinds=*/std::vector<RecordBatch::RowKind>(), arrow_array.get());
            ASSERT_OK(format_writer->AddBatch(batch->GetData()));
            ASSERT_OK(format_writer->Flush());
            ASSERT_OK(format_writer->Finish());
            ASSERT_OK(out->Flush());
            ASSERT_OK(out->Close());
            CheckResult(file_path, record_batch_size);
        }
    }
}

TEST_F(ParquetFormatWriterTest, TestWriteMultipleTimes) {
    // arrow array length = 6 + 10 + 15 + 6 = 37
    // parquet batch capacity = 10
    auto schema_pair = PrepareArrowSchema();
    const auto& arrow_schema = schema_pair.first;
    const auto& struct_type = schema_pair.second;

    std::string file_path = PathUtil::JoinPath(dir_->Str(), "write_multiple_times");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/false));
    ::parquet::WriterProperties::Builder builder;
    builder.write_batch_size(10);
    auto writer_properties = builder.build();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ParquetFormatWriter> format_writer,
        ParquetFormatWriter::Create(out, arrow_schema, writer_properties, arrow_pool_));

    // add batch first time, 6 rows
    AddRecordBatchOnce(format_writer, struct_type, 6, 0);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len1, format_writer->GetEstimateLength());
    ASSERT_GT(estimate_len1, 0);

    // add batch second times, 10 rows
    AddRecordBatchOnce(format_writer, struct_type, 10, 6);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len2, format_writer->GetEstimateLength());
    ASSERT_EQ(estimate_len2, estimate_len1);

    // add batch third times, 15 rows (expand internal batch)
    AddRecordBatchOnce(format_writer, struct_type, 15, 16);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len3, format_writer->GetEstimateLength());
    ASSERT_EQ(estimate_len3, estimate_len2);

    // add batch fourth times, 6 rows
    AddRecordBatchOnce(format_writer, struct_type, 6, 31);

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
    CheckResult(file_path, /*row_count=*/37);
    auto metrics = format_writer->GetWriterMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t counter, metrics->GetCounter(ParquetMetrics::WRITE_RECORD_COUNT));
    ASSERT_EQ(37, counter);
}

TEST_F(ParquetFormatWriterTest, TestGetEstimateLength) {
    auto schema_pair = PrepareArrowSchema();
    const auto& arrow_schema = schema_pair.first;
    const auto& struct_type = schema_pair.second;

    std::string file_path = PathUtil::JoinPath(dir_->Str(), "get_estimate_length");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/false));
    ::parquet::WriterProperties::Builder builder;
    auto writer_properties = builder.build();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ParquetFormatWriter> format_writer,
        ParquetFormatWriter::Create(out, arrow_schema, writer_properties, arrow_pool_));

    // add batch first time, 1 row
    AddRecordBatchOnce(format_writer, struct_type, 1, 0);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len1, format_writer->GetEstimateLength());
    ASSERT_GT(estimate_len1, 0);

    // add batch second times, 9998 rows
    AddRecordBatchOnce(format_writer, struct_type, 9998, 1);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len2, format_writer->GetEstimateLength());
    ASSERT_EQ(estimate_len2, estimate_len1);

    AddRecordBatchOnce(format_writer, struct_type, 100000, 9999);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len3, format_writer->GetEstimateLength());
    ASSERT_GT(estimate_len3, estimate_len2);
    ASSERT_OK(format_writer->Finish());
}

TEST_F(ParquetFormatWriterTest, TestTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_utc1", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_utc2", arrow::timestamp(arrow::TimeUnit::MICRO, timezone))};

    std::string file_path = PathUtil::JoinPath(dir_->Str(), "timezone.parquet");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/true));
    ::parquet::WriterProperties::Builder builder;
    auto writer_properties = builder.build();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ParquetFormatWriter> format_writer,
                         ParquetFormatWriter::Create(out, std::make_shared<arrow::Schema>(fields),
                                                     writer_properties, arrow_pool_));

    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001",
"1970-01-01 00:00:02", "1970-01-01 00:00:00.002"],
["1970-01-01 00:00:01", null, "1970-01-01 00:00:00.000001", null,"1970-01-01 00:00:02", null]
    ])")
            .ValueOrDie());

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    ASSERT_OK(format_writer->AddBatch(&c_array));
    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
}

}  // namespace paimon::parquet::test
