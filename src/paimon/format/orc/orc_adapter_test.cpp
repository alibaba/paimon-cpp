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

#include "paimon/format/orc/orc_adapter.h"

#include <cassert>
#include <cstdint>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/Writer.hh"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/format/orc/orc_input_stream_impl.h"
#include "paimon/format/orc/orc_memory_pool.h"
#include "paimon/format/orc/orc_output_stream_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/dict_array_converter.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::orc::test {
class OrcAdapterTest : public ::testing::Test,
                       public ::testing::WithParamInterface<std::pair<double, bool>> {
 public:
    void SetUp() override {}
    void TearDown() override {}

    std::pair<std::unique_ptr<::orc::RowReader>, std::unique_ptr<::orc::ColumnVectorBatch>>
    GenerateOrcReadBatch(const std::shared_ptr<arrow::Array>& src_array) const {
        auto [dict_key_size_threshold, enable_lazy_decoding] = GetParam();
        arrow::Schema src_schema(src_array->type()->fields());
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<::orc::Type> orc_type,
                             OrcAdapter::GetOrcType(src_schema));
        EXPECT_TRUE(orc_type);

        auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
        EXPECT_TRUE(test_root_dir);
        std::string test_root = test_root_dir->Str();
        std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
        EXPECT_OK(file_system->Mkdirs(test_root));
        std::string file_name = test_root + "/test.orc";

        EXPECT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             file_system->Create(file_name, /*overwrite=*/true));
        EXPECT_TRUE(out);
        std::unique_ptr<OrcOutputStreamImpl> orc_output_stream =
            OrcOutputStreamImpl::Create(out).value();
        ::orc::WriterOptions writer_options;
        writer_options.setUseTightNumericVector(true);
        writer_options.setDictionaryKeySizeThreshold(dict_key_size_threshold);
        std::unique_ptr<::orc::Writer> writer =
            ::orc::createWriter(*orc_type, orc_output_stream.get(), writer_options);
        std::unique_ptr<::orc::ColumnVectorBatch> write_batch =
            writer->createRowBatch(src_array->length());
        // Convert from arrow array to orc batch
        EXPECT_OK(OrcAdapter::WriteBatch(src_array, write_batch.get()));
        writer->add(*write_batch);
        writer->close();
        EXPECT_OK(out->Close());

        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, file_system->Open(file_name));
        EXPECT_TRUE(in);
        EXPECT_OK_AND_ASSIGN(auto orc_input_stream,
                             OrcInputStreamImpl::Create(in, DEFAULT_NATURAL_READ_SIZE));
        EXPECT_TRUE(orc_input_stream);
        ::orc::ReaderOptions reader_options;
        std::unique_ptr<::orc::Reader> reader =
            ::orc::createReader(std::move(orc_input_stream), reader_options);
        ::orc::RowReaderOptions options;
        options.setUseTightNumericVector(true);
        options.setEnableLazyDecoding(enable_lazy_decoding);
        std::unique_ptr<::orc::RowReader> row_reader = reader->createRowReader(options);
        auto read_batch = row_reader->createRowBatch(src_array->length() + 10);
        [[maybe_unused]] bool not_eof = row_reader->next(*read_batch);
        return std::make_pair(std::move(row_reader), std::move(read_batch));
    }
};

INSTANTIATE_TEST_SUITE_P(EnableDictionaryAndEnableLazyEncoding, OrcAdapterTest,
                         ::testing::ValuesIn({
                             std::make_pair(0.1, true),
                             std::make_pair(0.1, false),
                             std::make_pair(0.8, true),
                             std::make_pair(0.8, false),
                         }));

TEST_F(OrcAdapterTest, TestGetArrowType) {
    {
        // suppose to return null type
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> data_type,
                             OrcAdapter::GetArrowType(nullptr));
        ASSERT_TRUE(data_type);
    }
    {
        auto timezone = DateTimeUtils::GetLocalTimezoneName();
        std::unique_ptr<::orc::Type> orc_type = ::orc::Type::buildTypeFromString(
            "struct<col1:bigint,col2:int,col3:smallint,col4:tinyint,col5:double,col6:float,col7:"
            "boolean,col8:char(3),col9:varchar(8),col10:string,col11:binary,col12:date,col13:"
            "timestamp,"
            "col14:decimal(20,4),col15:decimal(18,5),col16:array<bigint>,col17:map<string,bigint>,"
            "col18:decimal(10,2),col19:timestamp with local time zone>");
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> data_type,
                             OrcAdapter::GetArrowType(orc_type.get()));
        ASSERT_TRUE(data_type);
        ASSERT_EQ(19, data_type->num_fields());
        ASSERT_EQ(
            "struct<col1: int64, col2: int32, col3: int16, col4: int8, col5: double, col6: float, "
            "col7: bool, col8: string, col9: string, col10: string, col11: binary, col12: "
            "date32[day], col13: timestamp[ns], col14: decimal128(20, 4), col15: decimal128(18, "
            "5), col16: list<item: int64>, col17: map<string, int64>, col18: decimal128(10, 2), "
            "col19: timestamp[ns, tz=" +
                timezone + "]>",
            data_type->ToString());
    }
}

TEST_F(OrcAdapterTest, TestGetArrowTypeWithInvalidOrcType) {
    std::unique_ptr<::orc::Type> orc_type =
        ::orc::Type::buildTypeFromString("struct<col1:uniontype<int,string>>");
    ASSERT_TRUE(orc_type);
    ASSERT_NOK(OrcAdapter::GetArrowType(orc_type.get()));
}

TEST_F(OrcAdapterTest, TestGetOrcType) {
    // also test lower and higher case
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    auto col1_field = arrow::field("Col1", arrow::int64());
    auto col2_field = arrow::field("Col2", arrow::int32());
    auto col3_field = arrow::field("col3", arrow::int16());
    auto col4_field = arrow::field("col4", arrow::int8());
    auto col5_field = arrow::field("col5", arrow::float64());
    auto col6_field = arrow::field("col6", arrow::float32());
    auto col7_field = arrow::field("col7", arrow::boolean());
    auto col8_field = arrow::field("col8", arrow::utf8());
    auto col9_field = arrow::field("col9", arrow::binary());
    auto col10_field = arrow::field("col10", arrow::date32());
    auto col11_field = arrow::field("col11", arrow::decimal128(20, 4));
    auto col12_field = arrow::field("col12", arrow::decimal128(18, 5));
    auto col13_field = arrow::field("col13", arrow::list(arrow::int64()));
    auto col14_field = arrow::field("col14", arrow::map(arrow::utf8(), arrow::int64()));
    auto col15_field = arrow::field("col15", arrow::timestamp(arrow::TimeUnit::NANO));
    auto col16_field = arrow::field(
        "col16", arrow::struct_({std::make_shared<arrow::Field>("sub1", arrow::int8()),
                                 std::make_shared<arrow::Field>("sub2", arrow::int16()),
                                 std::make_shared<arrow::Field>("sub3", arrow::int64())}));
    auto col17_field = arrow::field("col17", arrow::timestamp(arrow::TimeUnit::SECOND));
    auto col18_field = arrow::field("col18", arrow::timestamp(arrow::TimeUnit::MILLI));
    auto col19_field = arrow::field("col19", arrow::timestamp(arrow::TimeUnit::MICRO));
    auto col20_field = arrow::field("col20", arrow::timestamp(arrow::TimeUnit::SECOND, timezone));
    auto col21_field = arrow::field("col21", arrow::timestamp(arrow::TimeUnit::MILLI, timezone));
    auto col22_field = arrow::field("col22", arrow::timestamp(arrow::TimeUnit::MICRO, timezone));
    auto col23_field = arrow::field("col23", arrow::timestamp(arrow::TimeUnit::NANO, timezone));

    auto arrow_schema = std::make_shared<arrow::Schema>(arrow::FieldVector(
        {col1_field,  col2_field,  col3_field,  col4_field,  col5_field,  col6_field,
         col7_field,  col8_field,  col9_field,  col10_field, col11_field, col12_field,
         col13_field, col14_field, col15_field, col16_field, col17_field, col18_field,
         col19_field, col20_field, col21_field, col22_field, col23_field}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<::orc::Type> orc_type,
                         OrcAdapter::GetOrcType(*arrow_schema));
    ASSERT_TRUE(orc_type);
    ASSERT_EQ(
        "struct<Col1:bigint,Col2:int,col3:smallint,col4:tinyint,col5:double,col6:float,col7:"
        "boolean,col8:string,col9:binary,col10:date,col11:decimal(20,4),col12:decimal(18,5),col13:"
        "array<bigint>,col14:map<string,bigint>,col15:timestamp,col16:struct<sub1:tinyint,sub2:"
        "smallint,sub3:bigint>,col17:timestamp,col18:timestamp,col19:timestamp,col20:timestamp "
        "with local time zone,col21:timestamp with local time zone,col22:timestamp with local time "
        "zone,col23:timestamp with local time zone>",
        orc_type->toString());
}

TEST_F(OrcAdapterTest, TestGetOrcTypeWithInvalidArrowType) {
    {
        auto col1_field = arrow::field("col1", arrow::large_utf8());
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK(OrcAdapter::GetOrcType(*arrow_schema));
    }
    {
        auto col1_field = arrow::field("col1", arrow::large_binary());
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK(OrcAdapter::GetOrcType(*arrow_schema));
    }
    {
        auto col1_field = arrow::field("col1", arrow::uint32());
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK(OrcAdapter::GetOrcType(*arrow_schema));
    }
    {
        auto union_type = arrow::sparse_union(
            {arrow::field("_union_0", arrow::int32()), arrow::field("_union_1", arrow::utf8())});
        auto col1_field = arrow::field("col1", union_type);
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK(OrcAdapter::GetOrcType(*arrow_schema));
    }
    {
        auto col1_field = arrow::field("col1", arrow::date64());
        auto arrow_schema = arrow::schema(arrow::FieldVector({col1_field}));
        ASSERT_NOK(OrcAdapter::GetOrcType(*arrow_schema));
    }
}

TEST_F(OrcAdapterTest, TestAppendAndWriteBatchWithSimpleBatch) {
    int32_t length = 4;
    arrow::FieldVector fields = {
        arrow::field("col1", arrow::utf8()), arrow::field("col2", arrow::int32()),
        arrow::field("col3", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("col4", arrow::boolean()), arrow::field("col5", arrow::float32())};
    auto arrow_schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
            [null, null, null, null, null],
            ["abc", 1, "1970-01-01 00:02:03.123123", false, 1.1],
            [null, null, null, null, null],
            ["cba", 3, "1970-01-01 00:00:00.0", true, 3.1]
        ])")
            .ValueOrDie();

    // Convert from arrow schema to orc type
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<::orc::Type> orc_type,
                         OrcAdapter::GetOrcType(*arrow_schema));
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    std::string file_name = test_root + "/test.orc";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system->Create(file_name, /*overwrite=*/true));
    ASSERT_TRUE(out);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> orc_output_stream,
                         OrcOutputStreamImpl::Create(out));

    ::orc::WriterOptions writer_options;
    writer_options.setUseTightNumericVector(true);
    std::unique_ptr<::orc::Writer> writer =
        ::orc::createWriter(*orc_type, orc_output_stream.get(), writer_options);
    std::unique_ptr<::orc::ColumnVectorBatch> orc_batch = writer->createRowBatch(length);

    // Convert from arrow array to orc batch
    ASSERT_OK(OrcAdapter::WriteBatch(array, orc_batch.get()));

    // Convert from orc type to arrow type
    ASSERT_OK_AND_ASSIGN(auto struct_type_2, OrcAdapter::GetArrowType(orc_type.get()));
    ASSERT_TRUE(struct_type_2);
    // Convert from orc batch to arrow array
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> array2,
        OrcAdapter::AppendBatch(struct_type_2, orc_batch.get(), arrow::default_memory_pool()));
    // check array equals to array2
    ASSERT_TRUE(array->Equals(array2));
}

TEST_F(OrcAdapterTest, TestAppendAndWriteBatchWithComplexType) {
    int32_t length = 4;
    auto pool = arrow::default_memory_pool();
    // prepare an arrow array with struct<col1:list<item: int64>, col2:map<int8, int16>,
    // col3:date32[day],
    // col4:decimal128(10, 2)>
    auto list_field = arrow::field("col1", arrow::list(arrow::int64()));
    auto map_field = arrow::field("col2", arrow::map(arrow::int8(), arrow::int16()));
    auto date_field = arrow::field("col3", arrow::date32());
    auto decimal128_field = arrow::field("col4", arrow::decimal128(10, 2));

    arrow::FieldVector fields = {list_field, map_field, date_field, decimal128_field};
    auto arrow_schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
            [null, null, null, null],
            [[12345, 54321], [[1, 12]], 2345, "0.22"],
            [[12345], [[1, 12], [-1, -12]], 234, "0.12"],
            [[], [[1, 12], [-1, -12], [0, 128]], 23, "0.02"]
        ])")
            .ValueOrDie();

    // Convert from arrow schema to orc type
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<::orc::Type> orc_type,
                         OrcAdapter::GetOrcType(*arrow_schema));
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    std::string file_name = test_root + "/test.orc";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system->Create(file_name, /*overwrite=*/true));
    ASSERT_TRUE(out);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> orc_output_stream,
                         OrcOutputStreamImpl::Create(out));

    ::orc::WriterOptions writer_options;
    writer_options.setUseTightNumericVector(true);
    std::unique_ptr<::orc::Writer> writer =
        ::orc::createWriter(*orc_type, orc_output_stream.get(), writer_options);
    std::unique_ptr<::orc::ColumnVectorBatch> orc_batch = writer->createRowBatch(length);

    // Convert from arrow array to orc batch
    ASSERT_OK(OrcAdapter::WriteBatch(array, orc_batch.get()));
    // Convert from orc type to arrow type
    ASSERT_OK_AND_ASSIGN(auto struct_type_2, OrcAdapter::GetArrowType(orc_type.get()));
    ASSERT_TRUE(struct_type_2);
    // Convert from orc batch to arrow array
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array2,
                         OrcAdapter::AppendBatch(struct_type_2, orc_batch.get(), pool));
    // check array equals to array2
    ASSERT_TRUE(array->Equals(array2));
}

TEST_F(OrcAdapterTest, TestUnImplementedBatch) {
    int32_t length = 10;
    auto union_type = arrow::sparse_union(
        {arrow::field("_union_0", arrow::int32()), arrow::field("_union_1", arrow::utf8())});
    auto union_field = arrow::field("col1", union_type);

    auto struct_type = arrow::struct_({union_field});
    auto arrow_schema = arrow::schema(arrow::FieldVector({union_field}));
    assert(arrow_schema);
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({union_field}), R"([
            [null]
        ])")
            .ValueOrDie();

    // Convert from arrow schema to orc type
    ASSERT_NOK(OrcAdapter::GetOrcType(*arrow_schema));

    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    std::string file_name = test_root + "/test.orc";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system->Create(file_name, /*overwrite=*/true));
    ASSERT_TRUE(out);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> orc_output_stream,
                         OrcOutputStreamImpl::Create(out));

    auto valid_orc_type = ::orc::Type::buildTypeFromString("struct<col1:uniontype<int,string>>");
    ASSERT_TRUE(valid_orc_type);

    ::orc::WriterOptions writer_options;
    writer_options.setUseTightNumericVector(true);
    writer_options.setDictionaryKeySizeThreshold(0.8);
    std::unique_ptr<::orc::Writer> writer =
        ::orc::createWriter(*valid_orc_type, orc_output_stream.get(), writer_options);
    std::unique_ptr<::orc::ColumnVectorBatch> orc_batch = writer->createRowBatch(length);

    // Convert from arrow array to orc batch
    ASSERT_NOK(OrcAdapter::WriteBatch(array, orc_batch.get()));
}

TEST_P(OrcAdapterTest, TestEmptyBatch) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::binary())};
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
    ])")
            .ValueOrDie());
    auto [orc_reader_holder, read_batch] = GenerateOrcReadBatch(src_array);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> target_array,
        OrcAdapter::AppendBatch(arrow_type, read_batch.get(), arrow::default_memory_pool()));
    ASSERT_TRUE(target_array);
    ASSERT_OK_AND_ASSIGN(auto converted_array, paimon::test::DictArrayConverter::ConvertDictArray(
                                                   target_array, arrow::default_memory_pool()));
    ASSERT_TRUE(converted_array);
    ASSERT_TRUE(converted_array->Equals(src_array)) << converted_array->ToString();
    ASSERT_TRUE(src_array->Equals(converted_array));
}

TEST_P(OrcAdapterTest, TestDictionary) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),    arrow::field("f1", arrow::float32()),
        arrow::field("f2", arrow::float64()), arrow::field("f3", arrow::int8()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32()),
        arrow::field("f6", arrow::int64()),   arrow::field("f7", arrow::boolean()),
        arrow::field("f8", arrow::utf8()),    arrow::field("f9", arrow::boolean())};
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["data", 4.0, 5.1, 10, 100, 1000, 10000, true, "data1", true],
        ["data", 4.1, 6.2, null, 200, 2000, 20000, null, "data2", false],
        ["data", 4.2, null, 30, 300, null, 30000, true, "data3", true],
        ["data", 4.3, 8.4, 40, 400, 40000, 40000, false, "data4", false],
        ["data", 4.5, 8.4, 40, 400, 40000, 40000, false, "data5", true],
        ["data", 4.0, 8.0, 70, 500, 50000, 40000, true, "data6", true]
    ])")
            .ValueOrDie());

    // test with dict (f0), without dict (f1) orthogonal with enable_lazy_decoding = {true, false}
    // all 3 conditions are touched
    auto [orc_reader_holder, read_batch] = GenerateOrcReadBatch(src_array);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> target_array,
        OrcAdapter::AppendBatch(arrow_type, read_batch.get(), arrow::default_memory_pool()));
    ASSERT_OK_AND_ASSIGN(auto converted_array, paimon::test::DictArrayConverter::ConvertDictArray(
                                                   target_array, arrow::default_memory_pool()));
    ASSERT_TRUE(converted_array);
    ASSERT_TRUE(converted_array->Equals(src_array)) << converted_array->ToString();
}

TEST_P(OrcAdapterTest, TestShadowCopyWithBlob) {
    // when enable_dict & enable_lazy_encoding, meet condition2 in MakeOrcBackedBinaryBuilder,
    // return dict array
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),    arrow::field("f1", arrow::float32()),
        arrow::field("f2", arrow::float64()), arrow::field("f3", arrow::int8()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32()),
        arrow::field("f6", arrow::int64()),   arrow::field("f7", arrow::boolean()),
        arrow::field("f8", arrow::utf8()),    arrow::field("f9", arrow::boolean())};
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["apple", 4.0, 5.1, 10, 100, 1000, 10000, true, "apple", true],
        ["banana", 4.1, 6.2, null, 200, 2000, 20000, null, "banana", false],
        [null, 4.2, null, 30, 300, null, 30000, true, "apple", true],
        ["data", 4.3, 8.4, 40, 400, 40000, 40000, false, "data", false],
        ["data", 4.5, 8.4, 40, 400, 40000, 40000, false, "data", true],
        ["data", 4.0, 8.0, 70, 500, 50000, 40000, true, "data", true]
    ])")
            .ValueOrDie());
    auto [orc_reader_holder, read_batch] = GenerateOrcReadBatch(src_array);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> target_array,
        OrcAdapter::AppendBatch(arrow_type, read_batch.get(), arrow::default_memory_pool()));
    ASSERT_OK_AND_ASSIGN(auto converted_array, paimon::test::DictArrayConverter::ConvertDictArray(
                                                   target_array, arrow::default_memory_pool()));
    ASSERT_TRUE(converted_array);
    ASSERT_TRUE(converted_array->Equals(src_array)) << converted_array->ToString();
    ASSERT_TRUE(src_array->Equals(converted_array));
}

TEST_P(OrcAdapterTest, TestDeepCopyWithString) {
    // when enable_dict & disable_lazy encoding, f0 and f1 meet condition3,
    // degrade to deep copy
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::utf8())};
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["apple", "apple"],
        ["banana", "banana"],
        [null, "apple"],
        ["data", "data"],
        ["data", "data"],
        ["data", "data"]
    ])")
            .ValueOrDie());
    auto [orc_reader_holder, read_batch] = GenerateOrcReadBatch(src_array);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> target_array,
        OrcAdapter::AppendBatch(arrow_type, read_batch.get(), arrow::default_memory_pool()));
    ASSERT_TRUE(target_array);
    ASSERT_OK_AND_ASSIGN(auto converted_array, paimon::test::DictArrayConverter::ConvertDictArray(
                                                   target_array, arrow::default_memory_pool()));
    ASSERT_TRUE(converted_array);
    ASSERT_TRUE(converted_array->Equals(src_array)) << converted_array->ToString();
    ASSERT_TRUE(src_array->Equals(converted_array));
}

TEST_P(OrcAdapterTest, TestComplexTypeShallowCopyWithBlob) {
    // when disable_dict, meet condition1 in MakeOrcBackedBinaryBuilder, use shallow copy with valid
    // blob
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::list(arrow::int32())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("f2",
                     arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())})),
    };
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        null,
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false]],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true]],
        [[6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true]],
        [[7],          [["giraffe", 9]],                       [null, 40.1, true]],
        [null,         [["horse", 10], ["Panda", 11]],        [50, 50.1, null]],
        [[9],          null,                                   [60, 60.1, false]],
        [[10, 11, 12], [["rabbit", null], ["tiger", 13]],      null]
    ])")
            .ValueOrDie());

    auto [orc_reader_holder, read_batch] = GenerateOrcReadBatch(src_array);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> target_array,
        OrcAdapter::AppendBatch(arrow_type, read_batch.get(), arrow::default_memory_pool()));
    ASSERT_TRUE(target_array->Equals(src_array));
    ASSERT_TRUE(src_array->Equals(target_array));
}

TEST_P(OrcAdapterTest, TestAppendBatchWithBinary) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::binary()),
                                 arrow::field("f1", arrow::binary())};
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["apple", "apple"],
        ["banana", "banana"],
        [null, "apple"],
        ["data", "data"],
        ["data", "data"],
        ["data", "data"]
    ])")
            .ValueOrDie());
    auto [orc_reader_holder, read_batch] = GenerateOrcReadBatch(src_array);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> target_array,
        OrcAdapter::AppendBatch(arrow_type, read_batch.get(), arrow::default_memory_pool()));
    ASSERT_OK_AND_ASSIGN(auto converted_array, paimon::test::DictArrayConverter::ConvertDictArray(
                                                   target_array, arrow::default_memory_pool()));
    ASSERT_TRUE(converted_array);
    ASSERT_TRUE(converted_array->Equals(src_array)) << converted_array->ToString();
}

TEST_P(OrcAdapterTest, TestAppendBatchWithBinaryForAllNull) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::binary())};
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [null],
        [null]
    ])")
            .ValueOrDie());
    auto [orc_reader_holder, read_batch] = GenerateOrcReadBatch(src_array);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> target_array,
        OrcAdapter::AppendBatch(arrow_type, read_batch.get(), arrow::default_memory_pool()));
    ASSERT_OK_AND_ASSIGN(auto converted_array, paimon::test::DictArrayConverter::ConvertDictArray(
                                                   target_array, arrow::default_memory_pool()));
    ASSERT_TRUE(converted_array);
    ASSERT_TRUE(converted_array->Equals(src_array)) << converted_array->ToString();
}

TEST_P(OrcAdapterTest, TestDecimalAndTimestamp) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()),
        arrow::field("f3", arrow::date32()),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f5", arrow::decimal128(23, 5)),
        arrow::field("f6", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f7", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("f8", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("f9", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("f10", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
        arrow::field("f11", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("f12", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("f13", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
    };
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["Alice", 10, 1, 1234,  "2033-05-18 03:33:20.0",         "123456789987654321.45678", "2033-05-18 03:33:20.0", "2033-05-18 03:33:20", "2033-05-18 03:33:20.001", "2033-05-18 03:33:20.001001",
         "2033-05-18 03:33:20.0", "2033-05-18 03:33:20", "2033-05-18 03:33:20.001", "2033-05-18 03:33:20.001001"],
        ["Bob",   10, 1, null,  "1899-01-01 00:59:20.001001001", "-123456789987654321.45678", "1899-01-01 00:59:20.001001001", "1899-01-01 00:59:20", "1899-01-01 00:59:20.001","1899-01-01 00:59:20.001001",
         "1899-01-01 00:59:20.001001001", "1899-01-01 00:59:20", "1899-01-01 00:59:20.001","1899-01-01 00:59:20.001001"],
        ["Cool",  10, 1, -1234, null,                            "123.45678", "2024-12-10 09:20:20.02", "2024-12-10 09:20:20", "2024-12-10 09:20:20.001", "2024-12-10 09:20:20.001001",
         "2024-12-10 09:20:20.02", "2024-12-10 09:20:20", "2024-12-10 09:20:20.001", "2024-12-10 09:20:20.001001"],
        ["Dad",   10, 1, 0,     "2008-12-28",                    null, "2008-12-28", "2008-12-28", "2008-12-28", "2008-12-28",
         "2008-12-28", "2008-12-28", "2008-12-28", "2008-12-28"]
    ])")
            .ValueOrDie());

    auto [orc_reader_holder, read_batch] = GenerateOrcReadBatch(src_array);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> target_array,
        OrcAdapter::AppendBatch(arrow_type, read_batch.get(), arrow::default_memory_pool()));
    ASSERT_TRUE(target_array->Equals(src_array)) << target_array->ToString();
    ASSERT_TRUE(src_array->Equals(target_array));
}

TEST_F(OrcAdapterTest, TestBridgeForMapType) {
    // map type will lose meta of key and value field after c <-> c++ bridge
    auto key_field = DataField::ConvertDataFieldToArrowField(
        DataField(100, arrow::field("key", arrow::int32())));
    auto value_field = DataField::ConvertDataFieldToArrowField(
        DataField(101, arrow::field("value", arrow::utf8())));
    auto map_type = std::make_shared<arrow::MapType>(key_field, value_field);
    auto map_field = arrow::field("f0", map_type);
    arrow::Schema arrow_schema({map_field});
    // from c++ to c
    auto c_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(arrow_schema, c_schema.get()).ok());

    // from c to c++
    auto new_arrow_schema = arrow::ImportSchema(c_schema.get()).ValueOrDie();
    ASSERT_TRUE(new_arrow_schema->Equals(arrow_schema, /*check_metadata=*/false));
    // check meta failed
    ASSERT_FALSE(new_arrow_schema->Equals(arrow_schema, /*check_metadata=*/true));
}

TEST_F(OrcAdapterTest, TestDataBufferSetData) {
    {
        // buffer1 !ownBuf
        OrcMemoryPool orc_pool(GetDefaultPool());
        ::orc::DataBuffer<int32_t> buffer1(orc_pool, /*size=*/0, /*ownBuf=*/false);
        std::vector<int32_t> data = {1, 2, 3, 1};
        buffer1.setData(data.data(), /*bufSize=*/4 * 4);

        ASSERT_EQ(buffer1.size(), 4);
        ASSERT_EQ(buffer1.capacity(), 4);
        ASSERT_EQ(buffer1[1], 2);

        ::orc::DataBuffer<int32_t> buffer2(std::move(buffer1));
        ASSERT_EQ(buffer2.size(), 4);
        ASSERT_EQ(buffer2.capacity(), 4);
        ASSERT_EQ(buffer2[1], 2);
    }
    {
        // buffer1 ownBuf
        OrcMemoryPool orc_pool(GetDefaultPool());
        ::orc::DataBuffer<int32_t> buffer1(orc_pool, /*size=*/4, /*ownBuf=*/true);
        buffer1[0] = 1;
        buffer1[1] = 2;
        buffer1[2] = 3;
        buffer1[3] = 2;
        ASSERT_EQ(buffer1.size(), 4);
        ASSERT_EQ(buffer1.capacity(), 4);
        ASSERT_EQ(buffer1[1], 2);

        ::orc::DataBuffer<int32_t> buffer2(std::move(buffer1));
        ASSERT_EQ(buffer2.size(), 4);
        ASSERT_EQ(buffer2.capacity(), 4);
        ASSERT_EQ(buffer2[1], 2);
    }
}

}  // namespace paimon::orc::test
