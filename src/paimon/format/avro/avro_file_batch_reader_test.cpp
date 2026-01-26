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

#include "paimon/format/avro/avro_file_batch_reader.h"

#include <memory>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::avro::test {

class AvroFileBatchReaderTest : public ::testing::Test, public ::testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        ASSERT_OK_AND_ASSIGN(file_format_, FileFormatFactory::Get("avro", {}));
        fs_ = std::make_shared<LocalFileSystem>();
        dir_ = ::paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        pool_ = GetDefaultPool();
    }
    void TearDown() override {}

    std::pair<std::unique_ptr<FileBatchReader>, std::shared_ptr<arrow::ChunkedArray>>
    ReadBatchWithCustomizedData(const std::shared_ptr<arrow::Array>& src_array,
                                int32_t read_batch_size) {
        std::string file_path = PathUtil::JoinPath(dir_->Str(), "file.avro");
        WriteData(src_array, file_path);
        return ReadData(file_path, read_batch_size);
    }

    void WriteData(const std::shared_ptr<arrow::Array>& src_array, const std::string& file_path) {
        arrow::Schema src_schema(src_array->type()->fields());
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(src_schema, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(auto writer_builder,
                             file_format_->CreateWriterBuilder(&c_schema, /*batch_size=*/-1));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_path, /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(auto writer, writer_builder->Build(out, "zstd"));

        ::ArrowArray arrow_array;
        ASSERT_TRUE(arrow::ExportArray(*src_array, &arrow_array).ok());
        ASSERT_OK(writer->AddBatch(&arrow_array));
        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
    }

    std::pair<std::unique_ptr<FileBatchReader>, std::shared_ptr<arrow::ChunkedArray>> ReadData(
        const std::string& file_path, int32_t read_batch_size) {
        EXPECT_OK_AND_ASSIGN(auto reader_builder,
                             file_format_->CreateReaderBuilder(read_batch_size));
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_path));
        EXPECT_OK_AND_ASSIGN(auto batch_reader, reader_builder->Build(in));
        EXPECT_OK_AND_ASSIGN(auto result_array, ::paimon::test::ReadResultCollector::CollectResult(
                                                    batch_reader.get()));
        return std::make_pair(std::move(batch_reader), result_array);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileFormat> file_format_;
    std::shared_ptr<FileSystem> fs_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
};

TEST_F(AvroFileBatchReaderTest, TestReadDataWithNull) {
    std::string path = paimon::test::GetDataDir() + "/avro/data/avro_with_null";
    auto [reader_holder, result_array] = ReadData(path, /*read_batch_size=*/1024);

    arrow::FieldVector fields = {
        arrow::field("_KEY_f0", arrow::utf8(), /*nullable=*/true),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64(), /*nullable=*/true),
        arrow::field("_VALUE_KIND", arrow::int32(), /*nullable=*/true),
        arrow::field("f0", arrow::utf8(), /*nullable=*/true),
        arrow::field("f1", arrow::utf8(), /*nullable=*/true),
        arrow::field("f2", arrow::int32(), /*nullable=*/true),
        arrow::field("f3", arrow::float64(), /*nullable=*/true)};

    auto arrow_data_type = arrow::struct_(fields);

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        ["Alex", 2, 3, "Alex", "20250326", 18,   10.1],
        ["Bob",  3, 3, "Bob",  "20250326", 19,   11.1],
        ["Evan", 1, 0, "Evan", "20250326", null, 14.1]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok()) << array_status.ToString();
    ASSERT_TRUE(result_array->Equals(expected_array));
    ASSERT_TRUE(expected_array->Equals(result_array));
}

TEST_F(AvroFileBatchReaderTest, TestReadWithDifferentBatchSize) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "file.avro");

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int64()),   arrow::field("f3", arrow::float32()),
        arrow::field("f4", arrow::float64()), arrow::field("f5", arrow::utf8()),
        arrow::field("f6", arrow::binary())};
    auto arrow_data_type = arrow::struct_(fields);

    size_t length = 600;
    std::string data_str = "[";
    for (size_t i = 0; i < length; i++) {
        if (i % 3 == 0) {
            data_str.append(fmt::format(R"([{}, {}, {}, {}, {}, "str_{}", "bin_{}"])", "true", i,
                                        i * 100000000000L, i * 0.12, i * 123.45678901, i, i));
        } else if (i % 3 == 1) {
            data_str.append(fmt::format(R"([{}, -{}, -{}, -{}, -{}, "string_{}", "binary_{}"])",
                                        "false", i, i * 100000000000L, i * 0.12, i * 123.45678901,
                                        i, i));
        } else {
            data_str.append("[null, null, null, null, null, null, null]");
        }
        if (i != length - 1) {
            data_str.append(",");
        }
    }
    data_str.append("]");

    std::shared_ptr<arrow::Array> src_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, data_str).ValueOr(nullptr);
    ASSERT_TRUE(src_array);
    WriteData(src_array, file_path);

    for (int32_t batch_size : {1024, 512, 256, 128, 64, 32, 16, 8, 4, 2, 1}) {
        auto [reader_holder, result_array] = ReadData(file_path, batch_size);
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(
            arrow_data_type, {data_str}, &expected_array);
        ASSERT_TRUE(array_status.ok()) << array_status.ToString();
        ASSERT_TRUE(result_array->Equals(expected_array));
        ASSERT_TRUE(expected_array->Equals(result_array));
    }
}

TEST_F(AvroFileBatchReaderTest, TestReadAllTypes) {
    std::string path = paimon::test::GetDataDir() + "/avro/data/avro_all_types";
    auto [reader_holder, result_array] = ReadData(path, /*read_batch_size=*/1024);

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()),
        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::binary()),
        arrow::field("f10", arrow::list(arrow::float32())),
        arrow::field("f11", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                            arrow::field("f1", arrow::int64())})),
        arrow::field("f12", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("f13", arrow::date32()),
        arrow::field("f14", arrow::decimal128(2, 2)),
        arrow::field("f15", arrow::decimal128(10, 10)),
        arrow::field("f16", arrow::decimal128(19, 19))};

    auto arrow_data_type = arrow::struct_(fields);
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [true, 127, 32767, 2147483647, 9999999999999, 1234.56, 1234567890.0987654321, "aa", "qq", [0.1, 0.2], [true, null], "1970-01-01 00:02:03.123123", 2456, "0.22", "0.1234567890", "0.1234567890987654321"],
        [false, -128, -32768, -2147483648, -9999999999999, -1234.56, -1234567890.0987654321, null, "ww", [-0.1, -0.2, null, 0.3, 0.4], [null, 2], "1970-01-01 00:16:39.999999", null, "-0.22", "-0.1234567890", null],
        [null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok()) << array_status.ToString();
    ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
    ASSERT_TRUE(expected_array->Equals(result_array)) << result_array->ToString();
}

TEST_P(AvroFileBatchReaderTest, TestReadTimestampTypes) {
    auto enable_tz = GetParam();
    std::string timezone_str = enable_tz ? "Asia/Tokyo" : "Asia/Shanghai";
    paimon::test::TimezoneGuard tz_guard(timezone_str);

    std::string path = paimon::test::GetDataDir() +
                       "/avro/append_with_multiple_ts_precision_and_timezone.db/"
                       "append_with_multiple_ts_precision_and_timezone/bucket-0/"
                       "data-441e233b-529d-4a8f-a0a4-25c2c84fb965-0.avro";

    ASSERT_OK_AND_ASSIGN(auto reader_builder,
                         file_format_->CreateReaderBuilder(/*batch_size=*/1024));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(path));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, reader_builder->Build(in));

    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector read_fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
    };
    auto read_schema = arrow::schema(read_fields);
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_schema.get()).ok());
    EXPECT_OK(batch_reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                          /*selection_bitmap=*/std::nullopt));

    // check array
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         ::paimon::test::ReadResultCollector::CollectResult(batch_reader.get()));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(read_fields), {R"([
        ["1970-01-01T00:00:01","1970-01-01T00:00:00.001","1970-01-01T00:00:00.000001","1970-01-01T00:00:02","1970-01-01T00:00:00.002","1970-01-01T00:00:00.000002"],
        [null,"1970-01-01T00:00:00.003",null,null,"1970-01-01T00:00:00.004",null],
        ["1970-01-01T00:00:05",null,"1970-01-01T00:00:00.000005","1970-01-01T00:00:06",null,"1970-01-01T00:00:00.000006"]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok()) << array_status.ToString();
    ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
    ASSERT_TRUE(expected_array->Equals(result_array));
}

TEST_P(AvroFileBatchReaderTest, TestReadMapTypes) {
    std::string path = paimon::test::GetDataDir() +
                       "/avro/append_with_multiple_map.db/"
                       "append_with_multiple_map/bucket-0/"
                       "data-72442742-e49e-48a4-a736-a2475aac2d2c-0.avro";

    ASSERT_OK_AND_ASSIGN(auto reader_builder,
                         file_format_->CreateReaderBuilder(/*batch_size=*/1024));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(path));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, reader_builder->Build(in));

    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::map(arrow::int32(), arrow::int32())),
        arrow::field("f1", arrow::map(arrow::float64(), arrow::float64())),
        arrow::field("f2", arrow::map(arrow::utf8(), arrow::utf8())),
        arrow::field("f3", arrow::map(arrow::utf8(), arrow::binary())),
        arrow::field("f4", arrow::map(arrow::timestamp(arrow::TimeUnit::MICRO),
                                      arrow::timestamp(arrow::TimeUnit::MICRO))),
        arrow::field("f5", arrow::map(arrow::utf8(), arrow::list(arrow::float64()))),
        arrow::field("f6", arrow::map(arrow::utf8(), arrow::map(arrow::float64(), arrow::utf8()))),
        arrow::field("f7", arrow::map(arrow::int64(),
                                      arrow::struct_({field("f0", arrow::int32()),
                                                      field("f1", arrow::utf8()),
                                                      field("f2", arrow::decimal128(5, 2))})))};
    auto read_schema = arrow::schema(read_fields);
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_schema.get()).ok());
    EXPECT_OK(batch_reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                          /*selection_bitmap=*/std::nullopt));

    // check array
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         ::paimon::test::ReadResultCollector::CollectResult(batch_reader.get()));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(read_fields), {R"([
        [
            [[1,10],[2,20]],
            [[1.1,10.1],[2.2,20.2]],
            [["key1","val1"],["key2","val2"]],
            [["123456","abcdef"]],
            [["2023-01-01 12:00:00.123000","2023-01-01 12:00:00.123000"],["2023-01-02 13:30:00.456000","2023-01-02 13:30:00.456000"]],
            [["arr_key",[1.5, 2.5, 3.5]]],
            [["outer_key",[[99.9,"nested_val"]]]],
            [[1000, [42, "row_str", "123.45"]]]
        ]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok()) << array_status.ToString();
    ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString() << std::endl;
    ASSERT_TRUE(expected_array->Equals(result_array));
}

TEST_P(AvroFileBatchReaderTest, TestReadRowNumbers) {
    std::string path = paimon::test::GetDataDir() +
                       "/avro/append_simple.db/"
                       "append_simple/bucket-0/"
                       "data-d7d1c416-6e34-4834-af87-341d09418f0c-0.avro";

    ASSERT_OK_AND_ASSIGN(auto reader_builder, file_format_->CreateReaderBuilder(/*batch_size=*/1));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(path));
    ASSERT_OK_AND_ASSIGN(auto reader, reader_builder->Build(in));

    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::float64()),
        arrow::field("f2", arrow::utf8()),
        arrow::field("f3",
                     arrow::struct_({arrow::field("f0", arrow::map(arrow::utf8(), arrow::int32())),
                                     arrow::field("f1", arrow::list(arrow::int32()))}))};

    auto read_schema = arrow::schema(read_fields);
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_schema.get()).ok());
    EXPECT_OK(reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));

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
    ArrowArrayRelease(batch4.first.get());
    ArrowSchemaRelease(batch4.second.get());
    ASSERT_OK_AND_ASSIGN(auto batch5, reader->NextBatch());
    ASSERT_EQ(4, reader->GetPreviousBatchFirstRowNumber());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch5));
}

INSTANTIATE_TEST_SUITE_P(TestParam, AvroFileBatchReaderTest, ::testing::Values(false, true));

}  // namespace paimon::avro::test
