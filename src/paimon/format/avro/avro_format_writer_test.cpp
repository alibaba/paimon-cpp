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

#include "paimon/format/avro/avro_format_writer.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/io/file.h"
#include "arrow/memory_pool.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/format/avro/avro_file_batch_reader.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {

class AvroFormatWriterTest : public ::testing::Test {
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
        auto string_field = arrow::field("col1", arrow::utf8());
        auto int_field = arrow::field("col2", arrow::int32());
        auto bool_field = arrow::field("col3", arrow::boolean());
        auto struct_type = arrow::struct_({string_field, int_field, bool_field});
        return std::make_pair(
            arrow::schema(arrow::FieldVector({string_field, int_field, bool_field})), struct_type);
    }

    std::shared_ptr<FormatWriter> CreateFormatWriter(const std::shared_ptr<arrow::Schema>& schema,
                                                     const std::shared_ptr<OutputStream>& out,
                                                     int32_t batch_size) {
        ::ArrowSchema c_schema;
        EXPECT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        EXPECT_OK_AND_ASSIGN(auto file_format, FileFormatFactory::Get("avro", {}));
        EXPECT_OK_AND_ASSIGN(auto writer_builder,
                             file_format->CreateWriterBuilder(&c_schema, batch_size));
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<FormatWriter> writer,
                             writer_builder->Build(out, "zstd"));
        return writer;
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

    void AddRecordBatchOnce(const std::shared_ptr<FormatWriter>& format_writer,
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
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, fs_->Open(file_path));
        ASSERT_OK_AND_ASSIGN(auto file_reader,
                             AvroFileBatchReader::Create(input_stream, 1024, pool_));
        ASSERT_OK_AND_ASSIGN(uint64_t num_rows, file_reader->GetNumberOfRows());
        ASSERT_EQ(num_rows, row_count);

        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ::paimon::test::ReadResultCollector::CollectResult(file_reader.get()));
        const auto& struct_array =
            std::static_pointer_cast<arrow::StructArray>(result_array->chunk(0));
        const auto& string_array =
            std::static_pointer_cast<arrow::StringArray>(struct_array->field(0));
        ASSERT_TRUE(string_array);
        const auto& int_array = std::static_pointer_cast<arrow::Int32Array>(struct_array->field(1));
        ASSERT_TRUE(int_array);
        const auto& bool_array =
            std::static_pointer_cast<arrow::BooleanArray>(struct_array->field(2));
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

TEST_F(AvroFormatWriterTest, TestWriteWithVariousBatchSize) {
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
            auto format_writer = CreateFormatWriter(arrow_schema, out, batch_capacity);
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

TEST_F(AvroFormatWriterTest, TestWriteMultipleTimes) {
    // arrow array length = 6 + 10 + 15 + 6 = 37
    // avro batch capacity = 10
    auto schema_pair = PrepareArrowSchema();
    const auto& arrow_schema = schema_pair.first;
    const auto& struct_type = schema_pair.second;

    std::string file_path = PathUtil::JoinPath(dir_->Str(), "write_multiple_times");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/false));
    auto format_writer = CreateFormatWriter(arrow_schema, out, /*batch_size=*/10);

    // add batch first time, 6 rows
    AddRecordBatchOnce(format_writer, struct_type, 6, 0);
    // add batch second times, 10 rows
    AddRecordBatchOnce(format_writer, struct_type, 10, 6);
    // add batch third times, 15 rows (expand internal batch)
    AddRecordBatchOnce(format_writer, struct_type, 15, 16);
    // add batch fourth times, 6 rows
    AddRecordBatchOnce(format_writer, struct_type, 6, 31);

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
    CheckResult(file_path, /*row_count=*/37);
    auto metrics = format_writer->GetWriterMetrics();
    ASSERT_TRUE(metrics);
}

TEST_F(AvroFormatWriterTest, TestGetEstimateLength) {
    auto schema_pair = PrepareArrowSchema();
    const auto& arrow_schema = schema_pair.first;
    const auto& struct_type = schema_pair.second;

    std::string file_path = PathUtil::JoinPath(dir_->Str(), "get_estimate_length");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/false));
    auto format_writer = CreateFormatWriter(arrow_schema, out, /*batch_size=*/1024);

    // add batch first time, 1 row
    AddRecordBatchOnce(format_writer, struct_type, 1, 0);
    ASSERT_OK_AND_ASSIGN(bool reach_targe_size,
                         format_writer->ReachTargetSize(/*suggested_check=*/true,
                                                        /*target_size=*/102400));
    ASSERT_FALSE(reach_targe_size);

    // add batch second times, 9998 rows
    AddRecordBatchOnce(format_writer, struct_type, 9998, 1);
    ASSERT_OK_AND_ASSIGN(reach_targe_size, format_writer->ReachTargetSize(/*suggested_check=*/true,
                                                                          /*target_size=*/102400));
    ASSERT_FALSE(reach_targe_size);

    AddRecordBatchOnce(format_writer, struct_type, 100000, 9999);
    ASSERT_OK_AND_ASSIGN(reach_targe_size, format_writer->ReachTargetSize(/*suggested_check=*/true,
                                                                          /*target_size=*/102400));
    ASSERT_TRUE(reach_targe_size);
    ASSERT_OK(format_writer->Finish());
}

}  // namespace paimon::avro::test
