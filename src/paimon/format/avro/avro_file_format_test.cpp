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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {

class AvroFileFormatTest : public testing::Test, public ::testing::WithParamInterface<std::string> {
 public:
    void SetUp() override {
        ASSERT_OK_AND_ASSIGN(file_format_, FileFormatFactory::Get("avro", {}));
        fs_ = std::make_shared<LocalFileSystem>();
        dir_ = ::paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
    }
    void TearDown() override {}

    void WriteDataAndCheckResult(const arrow::FieldVector& fields, const std::string& data_str) {
        auto schema = arrow::schema(fields);
        auto data_type = arrow::struct_(fields);

        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(auto writer_builder,
                             file_format_->CreateWriterBuilder(&c_schema, 1024));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<OutputStream> out,
            fs_->Create(PathUtil::JoinPath(dir_->Str(), "file.avro"), /*overwrite=*/false));

        auto compression = GetParam();
        ASSERT_OK_AND_ASSIGN(auto writer, writer_builder->Build(out, compression));

        auto input_array =
            arrow::ipc::internal::json::ArrayFromJSON(data_type, data_str).ValueOrDie();
        ASSERT_TRUE(input_array);
        ::ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*input_array, &c_array).ok());
        ASSERT_OK(writer->AddBatch(&c_array));
        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());

        // read
        ASSERT_OK_AND_ASSIGN(auto reader_builder, file_format_->CreateReaderBuilder(1024));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in,
                             fs_->Open(PathUtil::JoinPath(dir_->Str(), "file.avro")));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, reader_builder->Build(in));
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(batch_reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                              /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto output_array, ::paimon::test::ReadResultCollector::CollectResult(
                                                    batch_reader.get()));
        ASSERT_TRUE(output_array->Equals(arrow::ChunkedArray(input_array)))
            << output_array->ToString() << "\n vs \n"
            << input_array->ToString();
    }

 private:
    std::shared_ptr<FileFormat> file_format_;
    std::shared_ptr<FileSystem> fs_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
};

INSTANTIATE_TEST_SUITE_P(Compression, AvroFileFormatTest,
                         ::testing::ValuesIn(std::vector<std::string>(
                             {"zstd", "zstandard", "snappy", "null", "deflate"})));

TEST_P(AvroFileFormatTest, TestSimple) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),       arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),         arrow::field("f3", arrow::int32()),
        arrow::field("field_null", arrow::int32()), arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),       arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),          arrow::field("f8", arrow::binary())};
    std::string data_str =
        R"([[true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "20250327", "banana"],
            [false, 1, 32767, null, null, 4294967296, 1.0, 2.141592658, "20250327", "dog"],
            [null, 1, 32767, 2147483647, null, null, 2.0, 3.141592657, null, "lucy"],
            [true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "20250326", "mouse"]])";
    WriteDataAndCheckResult(fields, data_str);
}

TEST_P(AvroFileFormatTest, TestComplexTypes) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("f1", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("f2", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("f3", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("f5", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("f6", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("f7", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
        arrow::field("f8", arrow::decimal128(30, 5)),
        arrow::field("f9", arrow::decimal128(2, 2)),
        arrow::field("f10", arrow::date32()),
        arrow::field("f11", arrow::list(arrow::int32())),
        arrow::field("f12", arrow::map(arrow::utf8(), arrow::utf8())),
        arrow::field("f13", arrow::map(arrow::int32(), arrow::utf8())),
        arrow::field("f14", arrow::map(arrow::struct_({arrow::field("f0", arrow::int32())}),
                                       arrow::map(arrow::int32(), arrow::utf8()))),
        arrow::field("f15", arrow::struct_({arrow::field("sub1", arrow::int64()),
                                            arrow::field("sub2", arrow::float64()),
                                            arrow::field("sub3", arrow::boolean())})),
    };
    std::string src_data_str = R"([
        ["1970-01-02 00:00:00", "1970-01-02 00:00:00.001", "1970-01-02 00:00:00.000001", "1970-01-02 00:00:00.000000001",
         "1970-01-02 00:00:00", "1970-01-02 00:00:00.001", "1970-01-02 00:00:00.000001", "1970-01-02 00:00:00.000000001",
         "-123456789987654321.45678", "0.78", 12345, [1,2,3], [["test1","a"],["test2","value-3"]], [[1001,"a"],[1002,"value-3"]],
         [[[1001],[[1,"a"],[2,"b"]]],[[1002],[[11,"aa"],[22,"bb"]]]], [10,11.2,false]]
    ])";
    WriteDataAndCheckResult(fields, src_data_str);
}

TEST_P(AvroFileFormatTest, TestNestedMap) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::map(arrow::utf8(), arrow::int16())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::utf8())),
        arrow::field("f2", arrow::map(arrow::utf8(), arrow::timestamp(arrow::TimeUnit::MICRO))),
        arrow::field(
            "f3", arrow::map(arrow::utf8(), arrow::list(arrow::timestamp(arrow::TimeUnit::MICRO)))),
        arrow::field("f4", arrow::map(arrow::utf8(), arrow::list(arrow::utf8()))),
        arrow::field(
            "f5",
            arrow::map(
                arrow::int32(),
                arrow::struct_(
                    {arrow::field(
                         "f5.a",
                         arrow::struct_(
                             {arrow::field("f5.a.0", arrow::utf8()),
                              arrow::field("f5.sub2", arrow::int32()),
                              arrow::field("f5.a.1", arrow::timestamp(arrow::TimeUnit::MICRO))})),
                     arrow::field("f5.b", arrow::list(arrow::utf8())),
                     arrow::field("f5.c", arrow::map(arrow::utf8(), arrow::int32()))}))),
        arrow::field(
            "f6", arrow::map(arrow::utf8(), arrow::map(arrow::utf8(), arrow::list(arrow::utf8())))),
        arrow::field("f7", arrow::map(arrow::int32(), arrow::boolean())),
        arrow::field("f8", arrow::map(arrow::int64(), arrow::decimal128(2, 2))),
        arrow::field("f9", arrow::map(arrow::date32(), arrow::float32())),
        arrow::field("f10", arrow::map(arrow::binary(), arrow::float64())),
        arrow::field("f11", arrow::map(arrow::int32(), arrow::list(arrow::int64()))),
        arrow::field(
            "f12",
            arrow::map(arrow::utf8(),
                       arrow::list(arrow::struct_(
                           {arrow::field("name", arrow::utf8()),
                            arrow::field("scores", arrow::list(arrow::float32())),
                            arrow::field("info", arrow::map(arrow::float32(), arrow::utf8()))}))))};

    std::string src_data_str = R"([
        [
            [["f0_key_0", 1],["f0_key_1", 2]],
            [["f1_key_0","val-1"],["f1_key_1","value-2"]],
            [["f2_key_0","1970-01-01 00:00:00.000001"],["f2_key_1","1970-01-02 00:00:00.000001"]],
            [["f3_key_0",["1970-01-01 00:00:00.000001"]],["f3_key_1",["1970-01-02 00:00:00.000001"]]],
            [["f4_key_0",["val-1", "val-2"]],["f4_key_1",["val-3","val-4"]]],
            [[500,[["sub1",1,"1970-01-01 00:00:00.000001"],["test-1","test-2"],[["subkey_0",1],["subkey_1", 2]]]],[600,[["sub2",2,"1970-01-02 00:00:00.000001"],["test-3","test-4"],[["subkey_2", 1],["subkey_3", 2]]]]],
            [["f6_key_0",[["f6_sub_key_0",["value-0","value-1"]],["f6_sub_key_1",["value-2","value-3"]]]],["f6_key_1",[["f6_sub_key_2",["value-0","value-1"]],["f6_sub_key_3",["value-2","value-3"]]]]],
            [[100, true], [200, false]],
            [[1000, "0.78"], [2000, "0.91"]],
            [[10, 1.5], [20, 2.75]],
            [["aGVsbG8=", 3.14159], ["d29ybGQ=", 2.71828]],
            [[1, [1000000, 2000000]], [2, [3000000, 4000000, 5000000]]],
            [["group1", [["Alice", [95.5, 96.0], [[100.1, "info"]]], ["Bob", [88.0, 89.5],[[200.1,"info"]]]]],["group2", [["Charlie",[92.3, 93.1],[[300.1,"info"]]]]]]
        ],
        [
            null,null,null,null,null,null,null,null,null,null,null,null,null
        ]
    ])";
    WriteDataAndCheckResult(fields, src_data_str);
}

}  // namespace paimon::avro::test
