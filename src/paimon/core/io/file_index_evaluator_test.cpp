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

#include "paimon/core/io/file_index_evaluator.h"

#include <cstdint>
#include <cstring>
#include <optional>
#include <utility>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/fs/external_path_provider.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::test {
class FileIndexEvaluatorTest : public ::testing::Test {
 public:
    void SetUp() override {
        std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                              DataField(1, arrow::field("f1", arrow::int32())),
                                              DataField(2, arrow::field("f2", arrow::int32())),
                                              DataField(3, arrow::field("f3", arrow::float64()))};
        data_schema_ = DataField::ConvertDataFieldsToArrowSchema(read_fields);

        pool_ = GetDefaultPool();
        fs_ = std::make_shared<LocalFileSystem>();
    }
    void TearDown() override {
        pool_.reset();
        fs_.reset();
        data_schema_.reset();
    }

    void CheckResult(const std::shared_ptr<FileIndexResult>& result,
                     const std::vector<int32_t>& expected) const {
        auto typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap32* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*(typed_result->GetBitmap().value()), RoaringBitmap32::From(expected))
            << "result:" << typed_result->GetBitmap().value()->ToString()
            << ", expected:" << RoaringBitmap32::From(expected).ToString();
    }

    void CheckResult(const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
                     const std::shared_ptr<DataFileMeta>& data_file_meta) const {
        {
            auto predicate =
                PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {7});
        }
        {
            auto predicate =
                PredicateBuilder::IsNotNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {0, 1, 2, 3, 4, 5, 6});
        }
        {
            auto predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {0, 7});
        }
        {
            auto predicate = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::STRING,
                                                        Literal(FieldType::STRING, "Alice", 5));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {1, 2, 3, 4, 5, 6});
        }
        {
            auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::INT, Literal(10));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
        {
            auto predicate = PredicateBuilder::GreaterOrEqual(
                /*field_index=*/1, /*field_name=*/"f1", FieldType::INT, Literal(10));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
        {
            auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(10));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
        {
            auto predicate = PredicateBuilder::LessOrEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::INT, Literal(10));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
        {
            auto predicate = PredicateBuilder::In(
                /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                {Literal(FieldType::STRING, "Alice", 5), Literal(FieldType::STRING, "Bob", 3),
                 Literal(FieldType::STRING, "Lucy", 4)});
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {0, 1, 4, 5, 7});
        }
        {
            auto predicate = PredicateBuilder::NotIn(
                /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                {Literal(FieldType::STRING, "Alice", 5), Literal(FieldType::STRING, "Bob", 3),
                 Literal(FieldType::STRING, "Lucy", 4)});
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {2, 3, 6});
        }
        {
            auto f0_predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(20));
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::And({f0_predicate, f1_predicate}));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {7});
        }
        {
            auto f0_predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(20));
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::Or({f0_predicate, f1_predicate}));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {0, 4, 6, 7});
        }
        {
            // test non result
            auto predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "unknown", 7));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_FALSE(file_index_result->IsRemain().value());
        }
        {
            // test early stop
            auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(10));
            auto f2_predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                        FieldType::INT, Literal(6));
            auto f0_predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));

            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::And({f1_predicate, f2_predicate, f0_predicate}));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, predicate, data_file_path_factory,
                                             data_file_meta, fs_, pool_));
            ASSERT_FALSE(file_index_result->IsRemain().value());
        }
        {
            // test no predicate
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, /*predicate=*/nullptr,
                                             data_file_path_factory, data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
        {
            // test predicate on f3 (do not have index)
            auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                     FieldType::DOUBLE, Literal(14.1));
            ASSERT_OK_AND_ASSIGN(
                auto file_index_result,
                FileIndexEvaluator::Evaluate(data_schema_, /*predicate=*/nullptr,
                                             data_file_path_factory, data_file_meta, fs_, pool_));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<arrow::Schema> data_schema_;
};

TEST_F(FileIndexEvaluatorTest, TestEvaluateEmbeddingIndex) {
    std::string path =
        paimon::test::GetDataDir() + "/orc/append_with_bitmap.db/append_with_bitmap/";
    std::vector<uint8_t> embedded_bytes = {
        0,  5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,   0,   0,   96,  0,   0,
        0,  3,   0,   2,   102, 48,  0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112,
        0,  0,   0,   96,  0,   0,   0,   176, 0,   2,   102, 49,  0,   0,   0,   1,   0,   6,
        98, 105, 116, 109, 97,  112, 0,   0,   1,   16,  0,   0,   0,   102, 0,   2,   102, 50,
        0,  0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,   0,   1,   118, 0,   0,
        0,  108, 0,   0,   0,   0,   2,   0,   0,   0,   8,   0,   0,   0,   5,   0,   0,   0,
        0,  1,   0,   0,   0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,   0,
        85, 0,   0,   0,   5,   0,   0,   0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,
        0,  0,   0,   20,  0,   0,   0,   3,   66,  111, 98,  0,   0,   0,   20,  0,   0,   0,
        20, 0,   0,   0,   5,   69,  109, 105, 108, 121, 255, 255, 255, 253, 255, 255, 255, 255,
        0,  0,   0,   4,   76,  117, 99,  121, 255, 255, 255, 251, 255, 255, 255, 255, 0,   0,
        0,  4,   84,  111, 110, 121, 0,   0,   0,   40,  0,   0,   0,   20,  58,  48,  0,   0,
        1,  0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   0,   0,   7,   0,   58,  48,
        0,  0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   1,   0,   5,   0,
        58, 48,  0,   0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   3,   0,
        6,  0,   2,   0,   0,   0,   8,   0,   0,   0,   2,   0,   0,   0,   0,   1,   0,   0,
        0,  10,  0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,   0,   10,
        0,  0,   0,   22,  0,   0,   0,   26,  0,   0,   0,   20,  0,   0,   0,   0,   0,   0,
        0,  22,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,   16,  0,   0,   0,
        4,  0,   6,   0,   7,   0,   58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   4,   0,
        16, 0,   0,   0,   0,   0,   1,   0,   2,   0,   3,   0,   5,   0,   2,   0,   0,   0,
        8,  0,   0,   0,   2,   1,   255, 255, 255, 248, 0,   0,   0,   18,  0,   0,   0,   1,
        0,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,
        0,  0,   0,   0,   0,   0,   0,   0,   0,   22,  0,   0,   0,   1,   0,   0,   0,   22,
        0,  0,   0,   24,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,   16,  0,
        0,  0,   2,   0,   3,   0,   6,   0,   58,  48,  0,   0,   1,   0,   0,   0,   0,   0,
        3,  0,   16,  0,   0,   0,   0,   0,   1,   0,   4,   0,   5,   0};
    auto embedded_index = std::make_shared<Bytes>(embedded_bytes.size(), pool_.get());
    memcpy(embedded_index->data(), reinterpret_cast<char*>(embedded_bytes.data()),
           embedded_bytes.size());
    auto data_file_meta = std::make_shared<DataFileMeta>(
        "data-62feb610-c83f-4217-9b50-bbad9cd08eb4-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/embedded_index, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    CheckResult(/*data_file_path_factory=*/nullptr, data_file_meta);
}

TEST_F(FileIndexEvaluatorTest, TestEvaluateNoEmbeddingIndex) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/append_with_bitmap_no_embedding.db/append_with_bitmap_no_embedding/";
    auto data_file_meta = std::make_shared<DataFileMeta>(
        "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>(
            {"data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc.index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto data_file_path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(data_file_path_factory->Init(path + "/bucket-0/", /*format_identifier=*/"orc",
                                           /*data_file_prefix=*/"data-", nullptr));
    CheckResult(data_file_path_factory, data_file_meta);
}

TEST_F(FileIndexEvaluatorTest, TestTimestampType) {
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
    auto data_schema = arrow::schema(fields);

    std::string path = paimon::test::GetDataDir() + "/orc/timestamp_index.db/timestamp_index/";
    auto data_file_meta = std::make_shared<DataFileMeta>(
        "data-18569866-0c37-45e9-9d88-2eaf6dd084b0-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>(
            {"data-18569866-0c37-45e9-9d88-2eaf6dd084b0-0.orc.index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto data_file_path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(data_file_path_factory->Init(path + "/bucket-0/", /*format_identifier=*/"orc",
                                           /*data_file_prefix=*/"data-", nullptr));
    // {0,2,3,6}
    auto in_predicate = PredicateBuilder::In(
        /*field_index=*/0, /*field_name=*/"ts_sec", FieldType::TIMESTAMP,
        {Literal(Timestamp(1745542802000l, 0)), Literal(Timestamp(-1745000l, 0)),
         Literal(Timestamp(1745542602000l, 0))});
    // {0, 6}
    auto greater_than = PredicateBuilder::GreaterThan(
        /*field_index=*/6, /*field_name=*/"ts_tz_micro", FieldType::TIMESTAMP,
        Literal(Timestamp(1745542602001l, 1000)));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({in_predicate, greater_than}));
    ASSERT_OK_AND_ASSIGN(auto file_index_result, FileIndexEvaluator::Evaluate(
                                                     data_schema, predicate, data_file_path_factory,
                                                     data_file_meta, /*file_system=*/fs_, pool_));
    CheckResult(file_index_result, {0, 6});
}

TEST_F(FileIndexEvaluatorTest, TestInvalidEvaluate) {
    auto data_file_meta = std::make_shared<DataFileMeta>(
        "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>(
            {"data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc.index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto predicate =
        PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
    ASSERT_NOK_WITH_MSG(
        FileIndexEvaluator::Evaluate(data_schema_, predicate, /*data_file_path_factory=*/nullptr,
                                     data_file_meta, /*file_system=*/nullptr, pool_),
        "read process for FileIndexEvaluator must have data_file_path_factory and file_system");
}

}  // namespace paimon::test
