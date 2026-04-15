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

#include "paimon/format/orc/orc_stats_extractor.h"

#include <cstddef>
#include <map>
#include <tuple>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "orc/Int128.hh"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/Writer.hh"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/format/orc/orc_adapter.h"
#include "paimon/format/orc/orc_format_writer.h"
#include "paimon/format/orc/orc_output_stream_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::orc::test {

class OrcStatsExtractorTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        test_root_ = dir_->Str();
        file_system_ = std::make_shared<LocalFileSystem>();
        file_name_ = test_root_ + "/test.orc";
        pool_ = GetDefaultPool();
    }
    void TearDown() override {
        dir_.reset();
        file_system_.reset();
        pool_.reset();
    }

    void CheckStats(const arrow::FieldVector& fields, const std::string& input,
                    const std::vector<std::string>& expected_stats,
                    int64_t expect_row_count) const {
        auto schema = std::make_shared<arrow::Schema>(fields);
        auto struct_type = arrow::struct_(fields);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             file_system_->Create(file_name_, /*overwrite=*/true));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> out_stream,
                             OrcOutputStreamImpl::Create(out));
        ASSERT_OK_AND_ASSIGN(
            auto format_writer,
            OrcFormatWriter::Create(std::move(out_stream), *schema,
                                    {{"orc.timestamp-ltz.legacy.type", "false"}}, "zstd",
                                    /*batch_size=*/10, pool_));
        auto src_array = arrow::ipc::internal::json::ArrayFromJSON(struct_type, input).ValueOrDie();
        ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*src_array, &c_array).ok());
        ASSERT_OK(format_writer->AddBatch(&c_array));
        ASSERT_OK(format_writer->Flush());
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Close());

        auto extractor = std::make_shared<OrcStatsExtractor>(schema);
        ASSERT_OK_AND_ASSIGN(auto result,
                             extractor->ExtractWithFileInfo(file_system_, file_name_, pool_));
        auto& col_stats_vec = result.first;
        ASSERT_EQ(fields.size(), col_stats_vec.size());
        ASSERT_EQ(col_stats_vec.size(), expected_stats.size());
        for (size_t i = 0; i < expected_stats.size(); i++) {
            ASSERT_EQ(expected_stats[i], col_stats_vec[i]->ToString());
        }
        auto row_count = result.second.GetRowCount();
        ASSERT_EQ(row_count, expect_row_count);
    }

 private:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::string test_root_;
    std::string file_name_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(OrcStatsExtractorTest, TestSimpleProcess) {
    // generate orc file
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system_->Create(file_name_, /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> out_stream,
                         OrcOutputStreamImpl::Create(out));
    ::orc::WriterOptions writer_options;
    writer_options.setUseTightNumericVector(true);
    std::string orc_schema =
        "struct<col1:boolean,col2:tinyint,col3:smallint,col4:int,col5:bigint,col6:float,col7:"
        "double,col8:string>";
    std::unique_ptr<::orc::Type> type = ::orc::Type::buildTypeFromString(orc_schema);
    ASSERT_OK_AND_ASSIGN(auto arrow_type, OrcAdapter::GetArrowType(type.get()));
    std::unique_ptr<::orc::Writer> writer =
        ::orc::createWriter(*type, out_stream.get(), writer_options);
    size_t batch_size = 10;
    auto batch = writer->createRowBatch(batch_size);
    auto* struct_batch = dynamic_cast<::orc::StructVectorBatch*>(batch.get());
    ASSERT_TRUE(struct_batch);
    auto* bool_batch = dynamic_cast<::orc::ByteVectorBatch*>(struct_batch->fields[0]);
    ASSERT_TRUE(bool_batch);
    auto* tiny_int_batch = dynamic_cast<::orc::ByteVectorBatch*>(struct_batch->fields[1]);
    ASSERT_TRUE(tiny_int_batch);
    auto* small_int_batch = dynamic_cast<::orc::ShortVectorBatch*>(struct_batch->fields[2]);
    ASSERT_TRUE(small_int_batch);
    auto* int_batch = dynamic_cast<::orc::IntVectorBatch*>(struct_batch->fields[3]);
    ASSERT_TRUE(int_batch);
    auto* big_int_batch = dynamic_cast<::orc::LongVectorBatch*>(struct_batch->fields[4]);
    ASSERT_TRUE(big_int_batch);
    auto* float_batch = dynamic_cast<::orc::FloatVectorBatch*>(struct_batch->fields[5]);
    ASSERT_TRUE(float_batch);
    auto* double_batch = dynamic_cast<::orc::DoubleVectorBatch*>(struct_batch->fields[6]);
    ASSERT_TRUE(double_batch);
    auto* string_batch = dynamic_cast<::orc::StringVectorBatch*>(struct_batch->fields[7]);
    ASSERT_TRUE(string_batch);

    std::vector<std::tuple<bool, int16_t, int64_t, double>> raw_data0;
    std::vector<std::tuple<int8_t, int32_t, float, std::string>> raw_data1;
    raw_data0.reserve(batch_size);
    raw_data1.reserve(batch_size);
    for (size_t i = 0; i < batch_size; i++) {
        raw_data0.emplace_back(i != 0, static_cast<int16_t>(i), static_cast<int64_t>(i),
                               static_cast<double>(i + 0.1));
    }
    for (size_t i = 0; i < batch_size; i++) {
        raw_data1.emplace_back(
            static_cast<int8_t>(batch_size - i), static_cast<int32_t>(batch_size - i),
            static_cast<float>(batch_size - i + 0.1), "str_" + std::to_string(batch_size - i));
    }

    for (size_t i = 0; i < batch_size; i++) {
        const auto& [bvalue, int16_value, int64_value, dvalue] = raw_data0[i];
        bool_batch->data[i] = bvalue;
        small_int_batch->data[i] = int16_value;
        big_int_batch->data[i] = int64_value;
        double_batch->data[i] = dvalue;
    }

    for (size_t i = 0; i < batch_size; i++) {
        const auto& [int8_value, int32_value, fvalue, svalue] = raw_data1[i];
        tiny_int_batch->data[i] = int8_value;
        int_batch->data[i] = int32_value;
        float_batch->data[i] = fvalue;
        string_batch->data[i] = const_cast<char*>(svalue.c_str());
        string_batch->length[i] = static_cast<int32_t>(svalue.length());
    }
    struct_batch->numElements = batch_size;
    struct_batch->fields[0]->numElements = batch_size;
    struct_batch->fields[1]->numElements = batch_size;
    struct_batch->fields[2]->numElements = batch_size;
    struct_batch->fields[3]->numElements = batch_size;
    struct_batch->fields[4]->numElements = batch_size;
    struct_batch->fields[5]->numElements = batch_size;
    struct_batch->fields[6]->numElements = batch_size;
    struct_batch->fields[7]->numElements = batch_size;

    writer->add(*batch);
    writer->close();
    ASSERT_OK(out->Close());
    ASSERT_TRUE(file_system_->Exists(file_name_).value());

    auto extractor = std::make_shared<OrcStatsExtractor>(arrow::schema(arrow_type->fields()));
    ASSERT_OK_AND_ASSIGN(auto ret, extractor->ExtractWithFileInfo(file_system_, file_name_, pool_));

    auto column_stats = ret.first;
    auto file_info = ret.second;
    ASSERT_EQ(batch_size, file_info.GetRowCount());
    ASSERT_OK_AND_ASSIGN(auto simple_stats,
                         SimpleStatsConverter::ToBinary(column_stats, pool_.get()));
    // check min value
    auto min_values = simple_stats.MinValues();
    ASSERT_EQ(min_values.GetFieldCount(), 8);
    ASSERT_EQ(min_values.GetBoolean(0), false);
    ASSERT_EQ(min_values.GetByte(1), 1);
    ASSERT_EQ(min_values.GetShort(2), 0);
    ASSERT_EQ(min_values.GetInt(3), 1);
    ASSERT_EQ(min_values.GetLong(4), 0);
    ASSERT_NEAR(min_values.GetFloat(5), 1.1, 0.0001);
    ASSERT_NEAR(min_values.GetDouble(6), 0.1, 0.0001);
    ASSERT_EQ(min_values.GetString(7), BinaryString::FromString("str_1", pool_.get()));
    // check max value
    auto max_values = simple_stats.MaxValues();
    ASSERT_EQ(max_values.GetFieldCount(), 8);
    ASSERT_EQ(max_values.GetBoolean(0), true);
    ASSERT_EQ(max_values.GetByte(1), 10);
    ASSERT_EQ(max_values.GetShort(2), 9);
    ASSERT_EQ(max_values.GetInt(3), 10);
    ASSERT_EQ(max_values.GetLong(4), 9);
    ASSERT_NEAR(max_values.GetFloat(5), 10.1, 0.0001);
    ASSERT_NEAR(max_values.GetDouble(6), 9.1, 0.0001);
    ASSERT_EQ(max_values.GetString(7), BinaryString::FromString("str_9", pool_.get()));
    // check null count
    auto null_counts = simple_stats.NullCounts();
    ASSERT_EQ(8, null_counts.Size());
    for (int32_t i = 0; i < null_counts.Size(); i++) {
        ASSERT_EQ(0, null_counts.GetLong(i));
    }
}

TEST_F(OrcStatsExtractorTest, TestTimestampAndDecimal) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system_->Create(file_name_, /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> out_stream,
                         OrcOutputStreamImpl::Create(out));

    ::orc::WriterOptions writer_options;
    writer_options.setUseTightNumericVector(true);
    std::string orc_schema =
        "struct<col1:timestamp,col2:timestamp,col3:date,col4:decimal(7,2),"
        "col5:decimal(23,5),col6:decimal(30,5)>";
    std::unique_ptr<::orc::Type> type = ::orc::Type::buildTypeFromString(orc_schema);
    ASSERT_OK_AND_ASSIGN(auto arrow_type, OrcAdapter::GetArrowType(type.get()));
    std::unique_ptr<::orc::Writer> writer =
        ::orc::createWriter(*type, out_stream.get(), writer_options);
    size_t batch_size = 10;
    auto batch = writer->createRowBatch(batch_size);
    auto* struct_batch = dynamic_cast<::orc::StructVectorBatch*>(batch.get());
    ASSERT_TRUE(struct_batch);
    auto* timestamp_batch = dynamic_cast<::orc::TimestampVectorBatch*>(struct_batch->fields[0]);
    ASSERT_TRUE(timestamp_batch);
    auto* timestamp2_batch = dynamic_cast<::orc::TimestampVectorBatch*>(struct_batch->fields[1]);
    ASSERT_TRUE(timestamp2_batch);
    auto* date_batch = dynamic_cast<::orc::LongVectorBatch*>(struct_batch->fields[2]);
    ASSERT_TRUE(date_batch);
    auto* decimal_batch = dynamic_cast<::orc::Decimal64VectorBatch*>(struct_batch->fields[3]);
    ASSERT_TRUE(decimal_batch);
    auto* decimal128_batch = dynamic_cast<::orc::Decimal128VectorBatch*>(struct_batch->fields[4]);
    ASSERT_TRUE(decimal128_batch);
    auto* negative_decimal128_batch =
        dynamic_cast<::orc::Decimal128VectorBatch*>(struct_batch->fields[5]);
    ASSERT_TRUE(negative_decimal128_batch);

    Decimal::int128_t decimal = 1234500;
    ASSERT_OK_AND_ASSIGN(Decimal::int128_t big_integer,
                         DecimalUtils::StrToInt128("12345678998765432145678"));
    ASSERT_OK_AND_ASSIGN(Decimal::int128_t negative_big_integer,
                         DecimalUtils::StrToInt128("-325627385378534568734875523420"));
    Decimal negative_decimal(30, 5, negative_big_integer);
    for (size_t i = 0; i < batch_size; i++) {
        timestamp_batch->data[i] = 1723535710ll + i;
        timestamp_batch->nanoseconds[i] = i;
        timestamp2_batch->data[i] = 1723535710ll + 10 + i;
        timestamp2_batch->nanoseconds[i] = 10 + i;
        date_batch->data[i] = i;

        decimal_batch->values[i] = static_cast<int64_t>(1234500);
        decimal128_batch->values[i] = ::orc::Int128(static_cast<int64_t>(big_integer >> 64),
                                                    big_integer & 0xFFFFFFFFFFFFFFFF);
        negative_decimal128_batch->values[i] =
            ::orc::Int128(negative_decimal.HighBits(), negative_decimal.LowBits());
    }
    struct_batch->numElements = batch_size;
    struct_batch->fields[0]->numElements = batch_size;
    struct_batch->fields[1]->numElements = batch_size;
    struct_batch->fields[2]->numElements = batch_size;
    struct_batch->fields[3]->numElements = batch_size;
    struct_batch->fields[4]->numElements = batch_size;
    struct_batch->fields[5]->numElements = batch_size;
    writer->add(*batch);
    writer->close();
    ASSERT_OK(out->Close());
    ASSERT_TRUE(file_system_->Exists(file_name_).value());

    auto extractor = std::make_shared<OrcStatsExtractor>(arrow::schema(arrow_type->fields()));
    ASSERT_OK_AND_ASSIGN(auto ret, extractor->ExtractWithFileInfo(file_system_, file_name_, pool_));

    auto column_stats = ret.first;
    ASSERT_OK_AND_ASSIGN(auto simple_stats,
                         SimpleStatsConverter::ToBinary(column_stats, pool_.get()));

    // check min value
    auto min_values = simple_stats.MinValues();
    ASSERT_EQ(min_values.GetFieldCount(), 6);
    ASSERT_EQ(min_values.GetTimestamp(0, Timestamp::MAX_PRECISION), Timestamp(1723535710000ll, 0));
    ASSERT_EQ(min_values.GetTimestamp(1, Timestamp::MAX_PRECISION), Timestamp(1723535720000ll, 10));
    ASSERT_EQ(min_values.GetLong(2), 0);
    ASSERT_EQ(min_values.GetDecimal(3, 7, 2), Decimal(7, 2, decimal));
    ASSERT_EQ(min_values.GetDecimal(4, 30, 5), Decimal(30, 5, big_integer));
    ASSERT_EQ(min_values.GetDecimal(5, 30, 5), Decimal(30, 5, negative_big_integer));

    // check max value
    auto max_values = simple_stats.MaxValues();
    ASSERT_EQ(max_values.GetFieldCount(), 6);
    ASSERT_EQ(max_values.GetTimestamp(0, Timestamp::MAX_PRECISION),
              Timestamp(1723535719000ll, batch_size - 1));
    ASSERT_EQ(max_values.GetTimestamp(1, Timestamp::MAX_PRECISION),
              Timestamp(1723535729000ll, 10 + batch_size - 1));
    ASSERT_EQ(max_values.GetLong(2), batch_size - 1);
    ASSERT_EQ(max_values.GetDecimal(3, 7, 2), Decimal(7, 2, decimal));
    ASSERT_EQ(max_values.GetDecimal(4, 30, 5), Decimal(30, 5, big_integer));
    ASSERT_EQ(max_values.GetDecimal(5, 30, 5), Decimal(30, 5, negative_big_integer));

    // check null count
    auto null_counts = simple_stats.NullCounts();
    ASSERT_EQ(6, null_counts.Size());
    for (int32_t i = 0; i < null_counts.Size(); i++) {
        ASSERT_EQ(0, null_counts.GetLong(i));
    }
}

TEST_F(OrcStatsExtractorTest, TestEmptyStats) {
    // generate orc file
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system_->Create(file_name_, /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> out_stream,
                         OrcOutputStreamImpl::Create(out));
    ::orc::WriterOptions writer_options;
    writer_options.setUseTightNumericVector(true);
    std::string orc_schema = "struct<col1:int,col2:struct<inner_col:int>,col3:binary>";
    std::unique_ptr<::orc::Type> type = ::orc::Type::buildTypeFromString(orc_schema);
    ASSERT_OK_AND_ASSIGN(auto arrow_type, OrcAdapter::GetArrowType(type.get()));
    std::unique_ptr<::orc::Writer> writer =
        ::orc::createWriter(*type, out_stream.get(), writer_options);
    size_t batch_size = 10;
    auto batch = writer->createRowBatch(batch_size);
    auto* struct_batch = dynamic_cast<::orc::StructVectorBatch*>(batch.get());
    ASSERT_TRUE(struct_batch);
    auto* int_batch = dynamic_cast<::orc::IntVectorBatch*>(struct_batch->fields[0]);
    ASSERT_TRUE(int_batch);
    auto* nested_batch = dynamic_cast<::orc::StructVectorBatch*>(struct_batch->fields[1]);
    ASSERT_TRUE(nested_batch);
    auto* inner_batch = dynamic_cast<::orc::IntVectorBatch*>(nested_batch->fields[0]);
    ASSERT_TRUE(inner_batch);
    auto* binary_batch = dynamic_cast<::orc::StringVectorBatch*>(struct_batch->fields[2]);
    ASSERT_TRUE(binary_batch);

    int_batch->hasNulls = true;
    for (size_t i = 0; i < batch_size; i++) {
        int_batch->data[i] = i;
        int_batch->notNull[i] = false;
        inner_batch->data[i] = i + 100;
        binary_batch->data[i] = const_cast<char*>("hello");
        binary_batch->length[i] = static_cast<int32_t>(5);
    }

    struct_batch->numElements = batch_size;
    nested_batch->numElements = batch_size;
    struct_batch->fields[0]->numElements = batch_size;
    struct_batch->fields[1]->numElements = batch_size;
    struct_batch->fields[2]->numElements = batch_size;
    nested_batch->fields[0]->numElements = batch_size;

    writer->add(*batch);
    writer->close();
    ASSERT_OK(out->Close());
    ASSERT_TRUE(file_system_->Exists(file_name_).value());

    auto extractor = std::make_shared<OrcStatsExtractor>(arrow::schema(arrow_type->fields()));
    ASSERT_OK_AND_ASSIGN(auto ret, extractor->ExtractWithFileInfo(file_system_, file_name_, pool_));

    auto column_stats = ret.first;
    auto file_info = ret.second;
    ASSERT_EQ(batch_size, file_info.GetRowCount());
    ASSERT_OK_AND_ASSIGN(auto simple_stats,
                         SimpleStatsConverter::ToBinary(column_stats, pool_.get()));

    // check min value
    auto min_values = simple_stats.MinValues();
    ASSERT_EQ(min_values.GetFieldCount(), 3);
    ASSERT_TRUE(min_values.IsNullAt(0));
    ASSERT_TRUE(min_values.IsNullAt(1));
    ASSERT_TRUE(min_values.IsNullAt(2));
    // check max value
    auto max_values = simple_stats.MaxValues();
    ASSERT_EQ(max_values.GetFieldCount(), 3);
    ASSERT_TRUE(max_values.IsNullAt(0));
    ASSERT_TRUE(max_values.IsNullAt(1));
    ASSERT_TRUE(max_values.IsNullAt(2));
    // check null count
    auto null_counts = simple_stats.NullCounts();
    ASSERT_EQ(3, null_counts.Size());
    ASSERT_EQ(batch_size, null_counts.GetLong(0));
    ASSERT_EQ(0, null_counts.GetLong(1));
    ASSERT_EQ(0, null_counts.GetLong(2));
}

TEST_F(OrcStatsExtractorTest, TestNullForAllType) {
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
        arrow::field("f9", arrow::map(arrow::int8(), arrow::int16())),
        arrow::field("f10", arrow::list(arrow::float32())),
        arrow::field("f11", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                            arrow::field("f1", arrow::int64())})),
        arrow::field("f12", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f13", arrow::date32()),
        arrow::field("f14", arrow::decimal128(2, 2)),
        arrow::field("f15", arrow::decimal128(30, 2)),
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    auto schema = std::make_shared<arrow::Schema>(fields);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system_->Create(file_name_, /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> out_stream,
                         OrcOutputStreamImpl::Create(out));
    ASSERT_OK_AND_ASSIGN(
        auto format_writer,
        OrcFormatWriter::Create(std::move(out_stream), *schema,
                                {{"orc.timestamp-ltz.legacy.type", "false"}}, "zstd",
                                /*batch_size=*/10, pool_));
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]
    ])")
            .ValueOrDie());
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*src_array, &c_array).ok());
    ASSERT_OK(format_writer->AddBatch(&c_array));
    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Close());

    auto extractor = std::make_shared<OrcStatsExtractor>(schema);
    ASSERT_OK_AND_ASSIGN(auto ret, extractor->ExtractWithFileInfo(file_system_, file_name_, pool_));

    auto column_stats = ret.first;
    auto file_info = ret.second;
    ASSERT_EQ(src_array->length(), file_info.GetRowCount());
    ASSERT_OK_AND_ASSIGN(auto stats, SimpleStatsConverter::ToBinary(column_stats, pool_.get()));
    // test compatible with java
    ASSERT_EQ(stats.min_values_.HashCode(), 0xf890741a);
    ASSERT_EQ(stats.max_values_.HashCode(), 0xf890741a);
    ASSERT_EQ(stats.null_counts_.HashCode(), 0x9299256f);
}

TEST_F(OrcStatsExtractorTest, TestExtractStatsTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    {
        std::string data_str = R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
["1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null,                         "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
["1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null,                         null,                            "1970-01-01 00:00:06",                     null,  "1970-01-01 00:00:00.000006", null]
    ])";
        std::vector<std::string> expected_stats_str = {
            "min 1970-01-01 00:00:01.000000000, max 1970-01-01 00:00:05.000000000, null count 0",
            "min 1970-01-01 00:00:00.001000000, max 1970-01-01 00:00:00.005000000, null count 0",
            "min 1970-01-01 00:00:00.000001000, max 1970-01-01 00:00:00.000001000, null count 2",
            "min 1970-01-01 00:00:00.000000001, max 1970-01-01 00:00:00.000000003, null count 1",
            "min 1970-01-01 00:00:02.000000000, max 1970-01-01 00:00:06.000000000, null count 0",
            "min 1970-01-01 00:00:00.002000000, max 1970-01-01 00:00:00.004000000, null count 1",
            "min 1970-01-01 00:00:00.000002000, max 1970-01-01 00:00:00.000006000, null count 0",
            "min 1970-01-01 00:00:00.000000002, max 1970-01-01 00:00:00.000000004, null count 1",
        };
        CheckStats(fields, data_str, expected_stats_str, /*expect_row_count=*/3);
    }
    {
        std::string data_str = R"([
         [null,null,null,null,null,null,null,null]
    ])";
        std::vector<std::string> expected_stats_str = {
            "min null, max null, null count 1", "min null, max null, null count 1",
            "min null, max null, null count 1", "min null, max null, null count 1",
            "min null, max null, null count 1", "min null, max null, null count 1",
            "min null, max null, null count 1", "min null, max null, null count 1",
        };
        CheckStats(fields, data_str, expected_stats_str, /*expect_row_count=*/1);
    }
}

}  // namespace paimon::orc::test
