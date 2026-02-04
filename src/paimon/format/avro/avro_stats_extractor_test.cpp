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

#include "paimon/format/avro/avro_stats_extractor.h"

#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"
#include "paimon/format/avro/avro_file_format.h"
#include "paimon/format/avro/avro_format_writer.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {

class AvroStatsExtractorTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    void WriteAvroFile(const std::string& file_path,
                       const std::shared_ptr<arrow::ChunkedArray>& src_chunk_array,
                       const std::shared_ptr<arrow::Schema>& schema) const {
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileFormat> file_format,
                             FileFormatFactory::Get("avro", options_));
        ASSERT_OK_AND_ASSIGN(auto writer_builder,
                             file_format->CreateWriterBuilder(&c_schema, /*batch_size=*/1024));

        auto fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> output_stream,
                             fs->Create(file_path, true));
        ASSERT_OK_AND_ASSIGN(auto writer, writer_builder->Build(std::move(output_stream), "null"));

        for (const auto& array : src_chunk_array->chunks()) {
            ::ArrowArray c_array;
            ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
            ASSERT_OK(writer->AddBatch(&c_array));
        }
        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());

        ASSERT_OK_AND_ASSIGN(auto file_status, fs->GetFileStatus(file_path));
        ASSERT_GT(file_status->GetLen(), 0);
    }

 private:
    std::map<std::string, std::string> options_ = {{Options::FILE_FORMAT, "avro"},
                                                   {Options::MANIFEST_FORMAT, "avro"}};
};

TEST_F(AvroStatsExtractorTest, TestPrimitiveStatsExtractor) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),
        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::binary()),
        arrow::field("f9", arrow::date32()),
        arrow::field("f10", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f11", arrow::decimal128(5, 2)),
        arrow::field("f12", arrow::boolean()),
        arrow::field("f13", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("f14", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("f15", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("f16", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f17", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("f18", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("f19", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("f20", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [1, 11, 111, 1111, 1.1, 1.11, "Hello", "你好", 1234, "2033-05-18 03:33:20.0", "1.22", true, "2033-05-18 03:33:20", "2033-05-18 03:33:20.0", "2033-05-18 03:33:20.0", "2033-05-18 03:33:20.0", "2033-05-18 03:33:20", "2033-05-18 03:33:20.0", "2033-05-18 03:33:20.0", "2033-05-18 03:33:20.0"],
        [2, 22, 222, 2222, 2.2, 2.22, "World", "世界", -1234, "1899-01-01 00:59:20.001001001", "2.22", false, "1899-01-01 00:59:20", "1899-01-01 00:59:20", "1899-01-01 00:59:20", "1899-01-01 00:59:20.001001001","1899-01-01 00:59:20", "1899-01-01 00:59:20", "1899-01-01 00:59:20", "1899-01-01 00:59:20.001001001"],
        [null, null, 0, null, null, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ])")
            .ValueOrDie());
    auto src_chunk_array = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector({array}));

    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string file_path = dir->Str() + "/test.avro";
    WriteAvroFile(file_path, src_chunk_array, schema);

    AvroFileFormat format(options_);
    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());
    ASSERT_OK_AND_ASSIGN(auto extractor, format.CreateStatsExtractor(&arrow_schema));
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto stats_with_info,
                         extractor->ExtractWithFileInfo(fs, file_path, GetDefaultPool()));
    const auto& column_stats = stats_with_info.first;
    const auto& file_stats = stats_with_info.second;

    ASSERT_EQ(column_stats.size(), 20);
    for (const auto& stats : column_stats) {
        ASSERT_EQ(stats->ToString(), "min null, max null, null count null");
    }
    ASSERT_EQ(3, file_stats.GetRowCount());
}

TEST_F(AvroStatsExtractorTest, TestNestedType) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::list(arrow::float32())),
        arrow::field("f1", arrow::struct_({arrow::field("sub_f0", arrow::boolean()),
                                           arrow::field("sub_f1", arrow::int64())}))};
    auto schema = arrow::schema(fields);
    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [null, [true, 2]],
        [[0.1, 0.3], [true, 1]],
        [[1.1, 1.2], null]
    ])")
            .ValueOrDie());
    auto src_chunk_array = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector({array}));

    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string file_path = dir->Str() + "/test.avro";
    WriteAvroFile(file_path, src_chunk_array, schema);

    AvroStatsExtractor extractor(options_);
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto results, extractor.Extract(fs, file_path, GetDefaultPool()));

    ASSERT_EQ(results.size(), 2);
    for (const auto& stats : results) {
        ASSERT_EQ(stats->ToString(), "min null, max null, null count null");
    }
}

TEST_F(AvroStatsExtractorTest, TestNullForAllType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),
        arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),
        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::binary()),
        arrow::field("f9", arrow::list(arrow::struct_({arrow::field("key", arrow::int8()),
                                                       arrow::field("value", arrow::int16())}))),
        arrow::field("f10", arrow::list(arrow::float32())),
        arrow::field("f11", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                            arrow::field("f1", arrow::int64())})),
        arrow::field("f12", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f13", arrow::date32()),
        arrow::field("f14", arrow::decimal128(2, 2)),
        arrow::field("f15", arrow::decimal128(30, 2)),
        arrow::field("f16", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("f17", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("f18", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("f19", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f20", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("f21", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("f22", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("f23", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ])")
            .ValueOrDie());
    auto src_chunk_array = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector({src_array}));

    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string file_path = dir->Str() + "/test.avro";
    WriteAvroFile(file_path, src_chunk_array, schema);

    AvroStatsExtractor extractor(options_);
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto column_stats, extractor.Extract(fs, file_path, GetDefaultPool()));

    ASSERT_OK_AND_ASSIGN(auto stats,
                         SimpleStatsConverter::ToBinary(column_stats, GetDefaultPool().get()));
    ASSERT_EQ(stats.min_values_.HashCode(), 0xf890741a);
    ASSERT_EQ(stats.max_values_.HashCode(), 0xf890741a);
}

}  // namespace paimon::avro::test
