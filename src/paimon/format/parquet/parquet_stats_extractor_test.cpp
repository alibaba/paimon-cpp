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

#include "paimon/format/parquet/parquet_stats_extractor.h"

#include <cstddef>
#include <map>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/compare.h"
#include "arrow/io/file.h"
#include "arrow/ipc/api.h"
#include "arrow/memory_pool.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_format_writer.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "parquet/arrow/reader.h"
#include "parquet/properties.h"

namespace paimon::parquet::test {

class ParquetStatsExtractorTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
    }
    void TearDown() override {}

    void CheckStats(const arrow::FieldVector& fields, const std::string& input,
                    const std::vector<std::string>& expected_stats, int64_t expect_row_count) {
        auto arrow_schema = arrow::schema(fields);
        auto struct_type = arrow::struct_(fields);
        std::map<std::string, std::string> options;
        std::shared_ptr<arrow::MemoryPool> pool = GetArrowPool(GetDefaultPool());
        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        std::string file_name;
        ASSERT_TRUE(UUID::Generate(&file_name));
        std::string file_path = PathUtil::JoinPath(dir_->Str(), file_name);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs->Create(file_path, /*overwrite=*/false));
        ::parquet::WriterProperties::Builder builder;
        builder.enable_store_decimal_as_integer();
        ASSERT_OK_AND_ASSIGN(auto format_writer, ParquetFormatWriter::Create(
                                                     out, arrow_schema, builder.build(),
                                                     DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, pool));
        auto array = arrow::ipc::internal::json::ArrayFromJSON(struct_type, input).ValueOrDie();
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());
        ASSERT_OK(format_writer->AddBatch(arrow_array.get()));
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
        auto write_schema = std::make_shared<arrow::Schema>(fields);
        ParquetStatsExtractor stats_extractor(write_schema);
        ASSERT_OK_AND_ASSIGN(auto result,
                             stats_extractor.ExtractWithFileInfo(fs, file_path, GetDefaultPool()));
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
};

TEST_F(ParquetStatsExtractorTest, TestExtractStats) {
    arrow::FieldVector fields = {
        arrow::field("col0", arrow::struct_({arrow::field("col2", arrow::boolean()),
                                             arrow::field("col3", arrow::int64())})),
        arrow::field("col1", arrow::utf8()),
        arrow::field("col2", arrow::int32()),
        arrow::field("col3", arrow::boolean()),
        arrow::field("col4", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("col5", arrow::decimal128(23, 2)),
        arrow::field("col6", arrow::float32()),
        arrow::field("col7", arrow::float64()),
    };
    {
        std::string data_str = R"([
            [[true, 0], "str0", 100, true, "3970-01-01 00:00:00.000", "0.22", 1.1, 12.2],
            [[false, 1], "str1", 101, false, "3970-01-01 00:02:03.999", "0.28", 2.2, 12.2],
            [[false, 2], "str2", 102, true, "3455-01-01 00:02:03.000", "1234567890123456.00", 3.3, 13.2]
        ])";
        std::vector<std::string> expected_stats_str = {
            "min null, max null, null count null",
            "min str0, max str2, null count 0",
            "min 100, max 102, null count 0",
            "min false, max true, null count 0",
            "min null, max null, null count null",
            "min 0.22, max 1234567890123456.00, null count 0",
            "min 1.1, max 3.3, null count 0",
            "min 12.2, max 13.2, null count 0",
        };
        CheckStats(fields, data_str, expected_stats_str, /*expect_row_count=*/3);
    }
    {
        std::string data_str = R"([
            [[true, 0], "str0", 100, true, "3970-01-01 00:00:00.000", "0.22", 1.1, 12.2],
            [[false, 1], "str1", 101, true, "3970-01-01 00:02:03.999", "0.28", 2.2, 12.2],
            [[false, 2], "str2", 102, true, "3455-01-01 00:02:03.000", "1234567890123456.00", 3.3, 13.2]
        ])";
        std::vector<std::string> expected_stats_str = {
            "min null, max null, null count null",
            "min str0, max str2, null count 0",
            "min 100, max 102, null count 0",
            "min true, max true, null count 0",
            "min null, max null, null count null",
            "min 0.22, max 1234567890123456.00, null count 0",
            "min 1.1, max 3.3, null count 0",
            "min 12.2, max 13.2, null count 0",
        };
        CheckStats(fields, data_str, expected_stats_str, /*expect_row_count=*/3);
    }

    {
        std::string data_str = R"([
            [[true, 0], "str0", 100, true, "1970-01-01 00:00:00.000", "0.22", 1.1, 12.2],
            [[false, 1], "str1", 101, false, "1970-01-01 00:02:03.999", "0.28", 2.2, 12.2],
            [[false, 2], "str2", 102, true, "1985-01-01 00:02:03.000", "1.00", 3.3, 13.2]
        ])";
        std::vector<std::string> expected_stats_str = {
            "min null, max null, null count null", "min str0, max str2, null count 0",
            "min 100, max 102, null count 0",      "min false, max true, null count 0",
            "min null, max null, null count null", "min 0.22, max 1.00, null count 0",
            "min 1.1, max 3.3, null count 0",      "min 12.2, max 13.2, null count 0",
        };
        CheckStats(fields, data_str, expected_stats_str, /*expect_row_count=*/3);
    }
    {
        std::string data_str = R"([
            [[true, 0], null, 100, null, "3970-01-01 00:00:00.000", null, null, null],
            [[false, 1], "str1", 101, null, "3970-01-01 00:02:03.999", null, null, 2.2],
            [null, "str2", null, true, "3455-01-01 00:02:03.000", null, 1.1, 3.3]
        ])";
        std::vector<std::string> expected_stats_str = {
            "min null, max null, null count null", "min str1, max str2, null count 1",
            "min 100, max 101, null count 1",      "min true, max true, null count 2",
            "min null, max null, null count null", "min null, max null, null count 3",
            "min 1.1, max 1.1, null count 2",      "min 2.2, max 3.3, null count 1",
        };
        CheckStats(fields, data_str, expected_stats_str, /*expect_row_count=*/3);
    }
}

TEST_F(ParquetStatsExtractorTest, TestExtractStatsSimpleType) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),       arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),         arrow::field("f3", arrow::int32()),
        arrow::field("field_null", arrow::int32()), arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),       arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),          arrow::field("f8", arrow::binary())};

    std::string data_str = R"([
        [true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "20250327", "banana"],
        [false, 1, 32767, null, null, 4294967296, 1.0, 2.141592658, "20250327", "dog"],
        [null, 1, 32767, 2147483647, null, null, 2.1, 3.141592657, null, "lucy"],
        [true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "20250326", null]
    ])";
    std::vector<std::string> expected_stats_str = {
        "min false, max true, null count 1",
        "min -2, max 1, null count 0",
        "min -32768, max 32767, null count 0",
        "min -2147483648, max 2147483647, null count 1",
        "min null, max null, null count 4",
        "min -4294967298, max 4294967296, null count 1",
        "min 0.5, max 2.1, null count 0",
        "min 1.141592659, max 3.141592657, null count 0",
        "min 20250326, max 20250327, null count 1",
        "min null, max null, null count 1",
    };
    CheckStats(fields, data_str, expected_stats_str, /*expect_row_count=*/4);
}

TEST_F(ParquetStatsExtractorTest, TestExtractStatsComplexType) {
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::map(arrow::int8(), arrow::int16())),
        arrow::field("f2", arrow::list(arrow::float32())),
        arrow::field("f3", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                           arrow::field("f1", arrow::int64())})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f5", arrow::date32()),
        arrow::field("f6", arrow::decimal128(2, 2))};

    std::string data_str = R"([
        [[[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999999", 24, "0.28"],
        [[[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123123", 245, "0.12"],
        [[[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [[[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123123", 24, "0.78"]
    ])";
    std::vector<std::string> expected_stats_str = {
        "min null, max null, null count null", "min null, max null, null count null",
        "min null, max null, null count null", "min null, max null, null count null",
        "min 24, max 2456, null count 0",      "min 0.12, max 0.78, null count 0",
    };
    CheckStats(fields, data_str, expected_stats_str, /*expect_row_count=*/6);
}

TEST_F(ParquetStatsExtractorTest, TestNullForAllType) {
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
    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    std::string file_name = dir_->Str() + "/test.parquet";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs->Create(file_name, /*overwrite=*/false));
    auto pool = GetDefaultPool();
    std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(pool);
    ::parquet::WriterProperties::Builder builder;
    builder.enable_store_decimal_as_integer();
    ASSERT_OK_AND_ASSIGN(
        auto format_writer,
        ParquetFormatWriter::Create(out, schema, builder.build(),
                                    DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool));
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]
    ])")
            .ValueOrDie());
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*src_array, &c_array).ok());
    ASSERT_OK(format_writer->AddBatch(&c_array));
    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    auto extractor = std::make_shared<ParquetStatsExtractor>(schema);
    ASSERT_OK_AND_ASSIGN(auto ret, extractor->ExtractWithFileInfo(fs, file_name, pool));

    auto column_stats = ret.first;
    auto file_info = ret.second;
    ASSERT_EQ(src_array->length(), file_info.GetRowCount());
    ASSERT_OK_AND_ASSIGN(auto stats, SimpleStatsConverter::ToBinary(column_stats, pool.get()));
    // test compatible with java
    ASSERT_EQ(stats.min_values_.HashCode(), 0xf890741a);
    ASSERT_EQ(stats.max_values_.HashCode(), 0xf890741a);
}

TEST_F(ParquetStatsExtractorTest, TestExtractStatsTimestampType) {
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
            "min null, max null, null count null",
            "min 1970-01-01 00:00:02.000000000, max 1970-01-01 00:00:06.000000000, null count 0",
            "min 1970-01-01 00:00:00.002000000, max 1970-01-01 00:00:00.004000000, null count 1",
            "min 1970-01-01 00:00:00.000002000, max 1970-01-01 00:00:00.000006000, null count 0",
            "min null, max null, null count null",
        };
        CheckStats(fields, data_str, expected_stats_str, /*expect_row_count=*/3);
    }
    {
        std::string data_str = R"([
         [null,null,null,null,null,null,null,null]
    ])";
        std::vector<std::string> expected_stats_str = {
            "min null, max null, null count 1", "min null, max null, null count 1",
            "min null, max null, null count 1", "min null, max null, null count null",
            "min null, max null, null count 1", "min null, max null, null count 1",
            "min null, max null, null count 1", "min null, max null, null count null",
        };
        CheckStats(fields, data_str, expected_stats_str, /*expect_row_count=*/1);
    }
}
}  // namespace paimon::parquet::test
