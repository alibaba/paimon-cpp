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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "orc/OrcFile.hh"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/orc/orc_file_batch_reader.h"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/format/orc/orc_input_stream_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::orc::test {

// test predicate push down for decimal & timestamp & date
class ComplexPredicateTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        batch_size_ = 10;

        arrow::FieldVector fields = {
            arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::int32()),
            arrow::field("f3", arrow::date32()),
            arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("f5", arrow::decimal128(23, 5)),
        };
    }
    void TearDown() override {}

    std::unique_ptr<OrcFileBatchReader> PrepareOrcFileBatchReader(
        const std::string& file_name, const arrow::Schema* read_schema,
        const std::shared_ptr<Predicate>& predicate, int32_t batch_size) {
        std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system->Open(file_name));
        EXPECT_TRUE(input_stream);
        EXPECT_OK_AND_ASSIGN(auto in_stream,
                             OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
        EXPECT_TRUE(in_stream);
        EXPECT_OK_AND_ASSIGN(
            auto orc_batch_reader,
            OrcFileBatchReader::Create(std::move(in_stream), pool_,
                                       /*options=*/{{"orc.timestamp-ltz.legacy.type", "false"}},
                                       batch_size));
        EXPECT_TRUE(orc_batch_reader);
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
        EXPECT_TRUE(arrow_status.ok());
        EXPECT_OK(orc_batch_reader->SetReadSchema(c_schema.get(), predicate,
                                                  /*selection_bitmap=*/std::nullopt));
        return orc_batch_reader;
    }

    void CheckResult(const std::string& file_name,
                     const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::shared_ptr<Predicate>& predicate,
                     const std::shared_ptr<arrow::Array>& expected_array) {
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(file_name, read_schema.get(), predicate, batch_size_);
        ASSERT_OK_AND_ASSIGN(auto arrow_array, paimon::test::ReadResultCollector::CollectResult(
                                                   orc_batch_reader.get()));
        // check result
        if (expected_array) {
            ASSERT_TRUE(arrow_array);
            auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
            ASSERT_TRUE(expected_chunk_array->Equals(arrow_array));
        } else {
            ASSERT_FALSE(arrow_array);
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    int32_t batch_size_;
};

TEST_F(ComplexPredicateTest, TestSimple) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_complex_data.db/append_complex_data/f1=10/bucket-0/"
                            "data-14a30421-7650-486c-9876-66a1fa4356ff-0.orc";
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()),
        arrow::field("f3", arrow::date32()),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f5", arrow::decimal128(23, 5)),
    };
    auto read_schema = arrow::schema(fields);
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [10, 1, 1234,  "2033-05-18 03:33:20.0",         "123456789987654321.45678"],
        [10, 1, 19909, "2033-05-18 03:33:20.000001001", "12.30000"],
        [10, 1, 0,     "2008-12-28 00:00:00.000123456", null],
        [10, 1, 100,   "2008-12-28 00:00:00.00012345",  "-123.45000"],
        [10, 1, null,  "1899-01-01 00:59:20.001001001", "0.00000"],
        [10, 1, 20006, "2024-10-10 10:10:10.100100100", "1728551410100.10010"]
    ])")
            .ValueOrDie());

    //  date
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f3",
                                                 FieldType::DATE, Literal(FieldType::DATE, 4));
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f3",
                                                 FieldType::DATE, Literal(FieldType::DATE, -111));
        CheckResult(file_name, read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::LessThan(
            /*field_index=*/2, /*field_name=*/"f3", FieldType::DATE,
            Literal(FieldType::DATE, 20006));
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(
            /*field_index=*/2, /*field_name=*/"f3", FieldType::DATE,
            Literal(FieldType::DATE, 20006));
        CheckResult(file_name, read_schema, predicate, /*expected_array=*/nullptr);
    }

    // timestamp
    {
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                    Literal(Timestamp(1230422400000l, 999999)));
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                    Literal(Timestamp(2240521239999l, 0)));
        CheckResult(file_name, read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate =
            PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                       Literal(Timestamp(1230422400000l, 123460)));
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f4",
                                                       FieldType::TIMESTAMP,
                                                       Literal(Timestamp(2000000000000l, 1001)));
        CheckResult(file_name, read_schema, predicate, /*expected_array=*/nullptr);
    }

    // decimal
    {
        auto predicate = PredicateBuilder::Equal(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            Literal(Decimal(23, 5, 123456)));
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::Equal(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            Literal(Decimal(22, 3, -123456)));
        CheckResult(file_name, read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::LessThan(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            Literal(Decimal(23, 3, DecimalUtils::StrToInt128("123456789987654321567").value())));
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            Literal(Decimal(23, 3, DecimalUtils::StrToInt128("123456789987654321567").value())));
        CheckResult(file_name, read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::In(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            {Literal(Decimal(23, 5, DecimalUtils::StrToInt128("-12345678998765432134567").value())),
             Literal(Decimal(23, 5, 1234567))});
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::NotIn(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            {Literal(Decimal(23, 5, DecimalUtils::StrToInt128("-12345678998765432134567").value())),
             Literal(Decimal(23, 5, 1234567))});
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
}

TEST_F(ComplexPredicateTest, TestTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_with_multiple_ts_precision_and_timezone.db/"
                            "append_with_multiple_ts_precision_and_timezone/bucket-0/"
                            "data-3f58c403-1672-49a3-93c0-d90cfff9bd8a-0.orc";
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
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
["1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
["1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])")
            .ValueOrDie());
    auto read_schema = arrow::schema(fields);
    {
        auto predicate = PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"ts_sec",
                                                  FieldType::TIMESTAMP);
        CheckResult(file_name, read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"ts_micro",
                                                  FieldType::TIMESTAMP);
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::IsNotNull(/*field_index=*/2, /*field_name=*/"ts_micro",
                                                     FieldType::TIMESTAMP);
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"ts_sec",
                                                 FieldType::TIMESTAMP, Literal(Timestamp(1000, 0)));
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/6, /*field_name=*/"ts_tz_micro",
                                                 FieldType::TIMESTAMP, Literal(Timestamp(0, 2000)));
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::NotEqual(
            /*field_index=*/2, /*field_name=*/"ts_micro", FieldType::TIMESTAMP,
            Literal(Timestamp(0, 1000)));
        CheckResult(file_name, read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(
            /*field_index=*/5, /*field_name=*/"ts_tz_milli", FieldType::TIMESTAMP,
            Literal(Timestamp(3, 0)));
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(
            /*field_index=*/5, /*field_name=*/"ts_tz_milli", FieldType::TIMESTAMP,
            Literal(Timestamp(10, 0)));
        CheckResult(file_name, read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"ts_milli",
                                                    FieldType::TIMESTAMP, Literal(Timestamp(2, 0)));
        CheckResult(file_name, read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(
            /*field_index=*/1, /*field_name=*/"ts_milli", FieldType::TIMESTAMP,
            Literal(Timestamp(10, 0)));
        CheckResult(file_name, read_schema, predicate, /*expected_array=*/nullptr);
    }
}
}  // namespace paimon::orc::test
