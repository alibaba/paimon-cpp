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
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/util/string_builder.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index_factory.h"
#include "paimon/common/file_index/bloomfilter/bloom_filter_file_index_factory.h"
#include "paimon/common/file_index/bsi/bit_slice_index_bitmap_file_index_factory.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/source/table_read.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class DataSplit;
class Predicate;
}  // namespace paimon

namespace paimon::test {
class ReadInteWithIndexTest : public testing::Test,
                              public ::testing::WithParamInterface<std::pair<std::string, bool>> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void TearDown() override {}

    void CheckResult(const std::string& table_path,
                     const std::vector<std::shared_ptr<Split>> splits,
                     const std::shared_ptr<Predicate>& predicate,
                     const std::shared_ptr<arrow::ChunkedArray>& expected_array) const {
        auto [file_format, enable_prefetch] = GetParam();
        ReadContextBuilder context_builder(table_path);
        context_builder.AddOption("read.batch-size", "2")
            .AddOption("test.enable-adaptive-prefetch-strategy", "false")
            .SetPredicate(predicate);
        if (enable_prefetch) {
            context_builder.EnablePrefetch(true).SetPrefetchBatchCount(3);
        }
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(splits));

        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(batch_reader.get()));
        ::arrow::PrettyPrintOptions print_option;
        print_option.container_window = 100;
        if (expected_array) {
            ASSERT_TRUE(result_array->Equals(*expected_array))
                << "\nActual:" << ::arrow::PrettyPrint(*result_array, print_option, &std::cout)
                << "\nExpected:" << ::arrow::PrettyPrint(*expected_array, print_option, &std::cout);
        } else {
            ASSERT_FALSE(result_array);
        }
    }

    void CheckResultForBitmap(const std::string& path,
                              const std::shared_ptr<arrow::DataType>& arrow_data_type,
                              const std::shared_ptr<Split> split) const {
        {
            // test with non predicate
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 10, 1, 16.1],
[0, "Tony", 20, 0, 17.1],
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, /*predicate=*/nullptr, expected_array);
        }
        {
            // test equal predicate for f0
            auto predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 10, 1, 11.1],
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test not equal predicate for f0
            auto predicate = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::STRING,
                                                        Literal(FieldType::STRING, "Alice", 5));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 10, 1, 16.1],
[0, "Tony", 20, 0, 17.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test equal predicate for f1
            auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                     FieldType::INT, Literal(20));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Lucy", 20, 1, 15.1],
[0, "Tony", 20, 0, 17.1],
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test equal predicate for f2
            auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                     FieldType::INT, Literal(1));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 10, 1, 16.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test is null predicate
            auto predicate =
                PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test is not null predicate
            auto predicate =
                PredicateBuilder::IsNotNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 10, 1, 16.1],
[0, "Tony", 20, 0, 17.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test in predicate
            auto predicate = PredicateBuilder::In(
                /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                {Literal(FieldType::STRING, "Alice", 5), Literal(FieldType::STRING, "Bob", 3),
                 Literal(FieldType::STRING, "Lucy", 4)});
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 10, 1, 16.1],
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test not in predicate
            auto predicate = PredicateBuilder::NotIn(
                /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                {Literal(FieldType::STRING, "Alice", 5), Literal(FieldType::STRING, "Bob", 3),
                 Literal(FieldType::STRING, "Lucy", 4)});
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Tony", 20, 0, 17.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test and predicate
            auto f0_predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(20));
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::And({f0_predicate, f1_predicate}));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test or predicate
            auto f0_predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(20));
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::Or({f0_predicate, f1_predicate}));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 10, 1, 11.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Tony", 20, 0, 17.1],
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test predicate push down
            auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::INT, Literal(30));
            CheckResult(path, {split}, predicate, /*expected_array=*/nullptr);
        }
        {
            // test non-result
            auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                     FieldType::INT, Literal(30));
            CheckResult(path, {split}, predicate, /*expected_array=*/nullptr);
        }
        {
            // test early stopping
            auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(10));
            auto f2_predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                        FieldType::INT, Literal(6));
            auto f0_predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));

            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::And({f1_predicate, f2_predicate, f0_predicate}));
            CheckResult(path, {split}, predicate, /*expected_array=*/nullptr);
        }
    }

    void CheckResultForBitmapWithSingleRowGroup(
        const std::string& path, const std::shared_ptr<arrow::DataType>& arrow_data_type,
        const std::shared_ptr<Split> split) const {
        // test bitmap index takes effective
        CheckResultForBitmap(path, arrow_data_type, split);

        // test no index take effective
        {
            // test greater than predicate (take no effective on bitmap index)
            auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::INT, Literal(10));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 10, 1, 16.1],
[0, "Tony", 20, 0, 17.1],
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test predicate on f3 (do not have index)
            auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                     FieldType::DOUBLE, Literal(14.1));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 10, 1, 16.1],
[0, "Tony", 20, 0, 17.1],
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
    }

    void CheckResultForBsi(const std::string& path,
                           const std::shared_ptr<arrow::DataType>& arrow_data_type,
                           const std::shared_ptr<Split> split) const {
        {
            // test with non predicate
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 100, -2, 11.1, 1745542802000123000],
[0, "Bob", 200, -3, 12.1, 1745542902000123000],
[0, "Emily", 300, 1, 13.1, 1745542602000123000],
[0, "Tony", 50, 1, 14.1, -1744877000],
[0, "Lucy", 500, -1, 15.1, -1764877000],
[0, "Bob", 100, 2, 16.1, null],
[0, "Tony", null, -2, 17.1, 1745542802000123001],
[0, "Alice", 20, null, 18.1, -1724877000]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, /*predicate=*/nullptr, expected_array);
        }
        {
            // test is null predicate for f4
            auto predicate = PredicateBuilder::IsNull(/*field_index=*/4, /*field_name=*/"f4",
                                                      FieldType::TIMESTAMP);
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Bob", 100, 2, 16.1, null]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test equal predicate for f1
            auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                     FieldType::INT, Literal(100));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 100, -2, 11.1, 1745542802000123000],
[0, "Bob", 100, 2, 16.1, null]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test not equal predicate for f2
            auto predicate = PredicateBuilder::NotEqual(/*field_index=*/2, /*field_name=*/"f2",
                                                        FieldType::INT, Literal(-2));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Bob", 200, -3, 12.1, 1745542902000123000],
[0, "Emily", 300, 1, 13.1, 1745542602000123000],
[0, "Tony", 50, 1, 14.1, -1744877000],
[0, "Lucy", 500, -1, 15.1, -1764877000],
[0, "Bob", 100, 2, 16.1, null]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test greater than predicate for f1
            auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::INT, Literal(100));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Bob", 200, -3, 12.1, 1745542902000123000],
[0, "Emily", 300, 1, 13.1, 1745542602000123000],
[0, "Lucy", 500, -1, 15.1, -1764877000]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test greater or equal predicate for f2
            auto predicate = PredicateBuilder::GreaterOrEqual(
                /*field_index=*/2, /*field_name=*/"f2", FieldType::INT, Literal(-1));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Emily", 300, 1, 13.1, 1745542602000123000],
[0, "Tony", 50, 1, 14.1, -1744877000],
[0, "Lucy", 500, -1, 15.1, -1764877000],
[0, "Bob", 100, 2, 16.1, null]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test less than predicate for f4
            auto predicate = PredicateBuilder::LessThan(/*field_index=*/4, /*field_name=*/"f4",
                                                        FieldType::TIMESTAMP,
                                                        Literal(Timestamp(1745542802000l, 123000)));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Emily", 300, 1, 13.1, 1745542602000123000],
[0, "Tony", 50, 1, 14.1, -1744877000],
[0, "Lucy", 500, -1, 15.1, -1764877000],
[0, "Alice", 20, null, 18.1, -1724877000]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test less or equal predicate for f4, as timestamp is normalized to long (micros),
            // 123001 ns is approximated to 123 micro
            auto predicate = PredicateBuilder::LessOrEqual(
                /*field_index=*/4, /*field_name=*/"f4", FieldType::TIMESTAMP,
                Literal(Timestamp(1745542802000l, 123001)));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 100, -2, 11.1, 1745542802000123000],
[0, "Emily", 300, 1, 13.1, 1745542602000123000],
[0, "Tony", 50, 1, 14.1, -1744877000],
[0, "Lucy", 500, -1, 15.1, -1764877000],
[0, "Tony", null, -2, 17.1, 1745542802000123001],
[0, "Alice", 20, null, 18.1, -1724877000]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test in for f2
            auto predicate =
                PredicateBuilder::In(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT,
                                     {Literal(-1), Literal(2), Literal(-2)});
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 100, -2, 11.1, 1745542802000123000],
[0, "Lucy", 500, -1, 15.1, -1764877000],
[0, "Bob", 100, 2, 16.1, null],
[0, "Tony", null, -2, 17.1, 1745542802000123001]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test not in for f1
            auto predicate =
                PredicateBuilder::NotIn(/*field_index=*/1, /*field_name=*/"f1", FieldType::INT,
                                        {Literal(100), Literal(400), Literal(200)});
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Emily", 300, 1, 13.1, 1745542602000123000],
[0, "Tony", 50, 1, 14.1, -1744877000],
[0, "Lucy", 500, -1, 15.1, -1764877000],
[0, "Alice", 20, null, 18.1, -1724877000]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test and predicate
            auto f1_predicate = PredicateBuilder::GreaterThan(
                /*field_index=*/1, /*field_name=*/"f1", FieldType::INT, Literal(100));
            auto f2_predicate = PredicateBuilder::LessOrEqual(
                /*field_index=*/2, /*field_name=*/"f2", FieldType::INT, Literal(0));
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::And({f1_predicate, f2_predicate}));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Bob", 200, -3, 12.1, 1745542902000123000],
[0, "Lucy", 500, -1, 15.1, -1764877000]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test or predicate
            auto f2_predicate = PredicateBuilder::GreaterThan(
                /*field_index=*/2, /*field_name=*/"f2", FieldType::INT, Literal(0));
            auto f4_predicate = PredicateBuilder::LessOrEqual(
                /*field_index=*/4, /*field_name=*/"f4", FieldType::TIMESTAMP,
                Literal(Timestamp(1745542802000l, 123000)));
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::Or({f2_predicate, f4_predicate}));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 100, -2, 11.1, 1745542802000123000],
[0, "Emily", 300, 1, 13.1, 1745542602000123000],
[0, "Tony", 50, 1, 14.1, -1744877000],
[0, "Lucy", 500, -1, 15.1, -1764877000],
[0, "Bob", 100, 2, 16.1, null],
[0, "Tony", null, -2, 17.1, 1745542802000123001],
[0, "Alice", 20, null, 18.1, -1724877000]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test and predicate for is not null
            auto f1_predicate =
                PredicateBuilder::IsNotNull(/*field_index=*/1, /*field_name=*/"f1", FieldType::INT);
            auto f2_predicate =
                PredicateBuilder::IsNotNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
            auto f4_predicate = PredicateBuilder::IsNotNull(
                /*field_index=*/4, /*field_name=*/"f4", FieldType::TIMESTAMP);
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::And({f1_predicate, f2_predicate, f4_predicate}));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 100, -2, 11.1, 1745542802000123000],
[0, "Bob", 200, -3, 12.1, 1745542902000123000],
[0, "Emily", 300, 1, 13.1, 1745542602000123000],
[0, "Tony", 50, 1, 14.1, -1744877000],
[0, "Lucy", 500, -1, 15.1, -1764877000]
])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

std::vector<std::pair<std::string, bool>> GetTestValuesForReadInteWithIndexTest() {
    std::vector<std::pair<std::string, bool>> values = {{"parquet", false}, {"parquet", true}};
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc", false);
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormatAndEnablePaimonPrefetch, ReadInteWithIndexTest,
                         ::testing::ValuesIn(std::vector<std::pair<std::string, bool>>(
                             GetTestValuesForReadInteWithIndexTest())));

TEST_P(ReadInteWithIndexTest, TestSimple) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path = GetDataDir() + "/" + file_format +
                       "/append_with_bitmap_no_embedding.db/append_with_bitmap_no_embedding/";
    std::string file_name;
    if (file_format == "orc") {
        file_name = "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc";
    } else if (file_format == "parquet") {
        file_name = "data-783929b2-49d4-4006-a898-194a62e3278d-0.parquet";
    }
    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({file_name + ".index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, "Alice", 5));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 10, 1, 11.1],
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());

    ReadContextBuilder context_builder(path);
    context_builder.AddOption("read.batch-size", "2")
        .AddOption("test.enable-adaptive-prefetch-strategy", "false")
        .AddOption("orc.read.enable-metrics", "true")
        .SetPredicate(predicate);
    if (enable_prefetch) {
        context_builder.EnablePrefetch(true).SetPrefetchBatchCount(3);
    }
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(split));
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(batch_reader.get()));
    ASSERT_TRUE(result_array);
    ASSERT_TRUE(result_array->Equals(*expected_array));

    // test metrics
    if (file_format == "orc") {
        auto read_metrics = batch_reader->GetReaderMetrics();
        ASSERT_OK_AND_ASSIGN(uint64_t io_count, read_metrics->GetCounter("orc.read.io.count"));
        ASSERT_TRUE(io_count > 0);
        ASSERT_OK_AND_ASSIGN(uint64_t latency,
                             read_metrics->GetCounter("orc.read.inclusive.latency.us"));
        ASSERT_TRUE(latency > 0);
    }
    batch_reader->Close();
}

TEST_P(ReadInteWithIndexTest, TestReadWithLimits) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path = GetDataDir() + "/" + file_format +
                       "/append_with_bitmap_no_embedding.db/append_with_bitmap_no_embedding/";
    std::string file_name;
    if (file_format == "orc") {
        file_name = "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc";
    } else if (file_format == "parquet") {
        file_name = "data-783929b2-49d4-4006-a898-194a62e3278d-0.parquet";
    }

    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);
    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({file_name + ".index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

    auto predicate =
        PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "Alice", 5));
    ReadContextBuilder context_builder(path);
    context_builder.AddOption(Options::FILE_FORMAT, "orc")
        .AddOption("test.enable-adaptive-prefetch-strategy", "false")
        .AddOption(Options::READ_BATCH_SIZE, "1")
        .AddOption("orc.read.enable-metrics", "true")
        .SetPredicate(predicate);
    if (enable_prefetch) {
        context_builder.EnablePrefetch(true).SetPrefetchBatchCount(3);
    }
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(split));
    // simulate read limits, only read 3 batches
    for (int32_t i = 0; i < 3; i++) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, batch_reader->NextBatch());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array,
                             ReadResultCollector::GetArray(std::move(batch)));
        ASSERT_TRUE(array);
        ASSERT_EQ(array->length(), 1);
    }
    batch_reader->Close();

    // test metrics
    if (file_format == "orc") {
        auto read_metrics = batch_reader->GetReaderMetrics();
        ASSERT_TRUE(read_metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t io_count, read_metrics->GetCounter("orc.read.io.count"));
        ASSERT_TRUE(io_count > 0);
        ASSERT_OK_AND_ASSIGN(uint64_t latency,
                             read_metrics->GetCounter("orc.read.inclusive.latency.us"));
        ASSERT_TRUE(latency > 0);
    }
}

TEST_P(ReadInteWithIndexTest, TestEmbeddingBitmapIndex) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path =
        GetDataDir() + "/" + file_format + "/append_with_bitmap.db/append_with_bitmap/";
    std::string file_name;
    if (file_format == "orc") {
        file_name = "data-62feb610-c83f-4217-9b50-bbad9cd08eb4-0.orc";
    } else if (file_format == "parquet") {
        file_name = "data-54b396b6-df2d-4c6a-a0ae-b6a5afb612d7-0.parquet";
    }

    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

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
    memcpy(embedded_index->data(), reinterpret_cast<const void*>(embedded_bytes.data()),
           embedded_bytes.size());
    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/689,
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
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());
    CheckResultForBitmapWithSingleRowGroup(path, arrow_data_type, split);
}

TEST_P(ReadInteWithIndexTest, TestBitmapWithV1) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path =
        GetDataDir() + "/" + file_format + "/append_with_bitmap_v1.db/append_with_bitmap_v1/";
    std::string file_name;
    if (file_format == "orc") {
        file_name = "data-c29cf741-80ce-4b74-9cf3-e42efef557b3-0.orc";
    } else if (file_format == "parquet") {
        file_name = "data-6b38a150-c99c-4add-a449-a48a5d34e36c-0.parquet";
    }

    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    std::vector<uint8_t> embedded_bytes = {
        0,   5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,  0,   0,   96,  0,   0,
        0,   3,   0,   2,   102, 48,  0,   0,   0,   1,   0,   6,   98, 105, 116, 109, 97,  112,
        0,   0,   0,   96,  0,   0,   0,   131, 0,   2,   102, 49,  0,  0,   0,   1,   0,   6,
        98,  105, 116, 109, 97,  112, 0,   0,   0,   227, 0,   0,   0,  74,  0,   2,   102, 50,
        0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,  0,   1,   45,  0,   0,
        0,   76,  0,   0,   0,   0,   1,   0,   0,   0,   8,   0,   0,  0,   5,   0,   0,   0,
        0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,  0,   4,   76,  117, 99,
        121, 255, 255, 255, 251, 0,   0,   0,   3,   66,  111, 98,  0,  0,   0,   20,  0,   0,
        0,   5,   69,  109, 105, 108, 121, 255, 255, 255, 253, 0,   0,  0,   4,   84,  111, 110,
        121, 0,   0,   0,   40,  58,  48,  0,   0,   1,   0,   0,   0,  0,   0,   1,   0,   16,
        0,   0,   0,   0,   0,   7,   0,   58,  48,  0,   0,   1,   0,  0,   0,   0,   0,   1,
        0,   16,  0,   0,   0,   1,   0,   5,   0,   58,  48,  0,   0,  1,   0,   0,   0,   0,
        0,   1,   0,   16,  0,   0,   0,   3,   0,   6,   0,   1,   0,  0,   0,   8,   0,   0,
        0,   2,   0,   0,   0,   0,   20,  0,   0,   0,   0,   0,   0,  0,   10,  0,   0,   0,
        22,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,  16,  0,   0,   0,   4,
        0,   6,   0,   7,   0,   58,  48,  0,   0,   1,   0,   0,   0,  0,   0,   4,   0,   16,
        0,   0,   0,   0,   0,   1,   0,   2,   0,   3,   0,   5,   0,  1,   0,   0,   0,   8,
        0,   0,   0,   2,   1,   255, 255, 255, 248, 0,   0,   0,   0,  0,   0,   0,   0,   0,
        0,   0,   1,   0,   0,   0,   22,  58,  48,  0,   0,   1,   0,  0,   0,   0,   0,   2,
        0,   16,  0,   0,   0,   2,   0,   3,   0,   6,   0,   58,  48, 0,   0,   1,   0,   0,
        0,   0,   0,   3,   0,   16,  0,   0,   0,   0,   0,   1,   0,  4,   0,   5,   0};
    auto embedded_index = std::make_shared<Bytes>(embedded_bytes.size(), pool_.get());
    memcpy(embedded_index->data(), reinterpret_cast<const void*>(embedded_bytes.data()),
           embedded_bytes.size());
    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/689,
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
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());
    CheckResultForBitmapWithSingleRowGroup(path, arrow_data_type, split);
}

TEST_P(ReadInteWithIndexTest, TestNoEmbeddingBitmapIndex) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path = GetDataDir() + "/" + file_format +
                       "/append_with_bitmap_no_embedding.db/append_with_bitmap_no_embedding/";
    std::string file_name;
    if (file_format == "orc") {
        file_name = "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc";
    } else if (file_format == "parquet") {
        file_name = "data-783929b2-49d4-4006-a898-194a62e3278d-0.parquet";
    }

    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({file_name + ".index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());
    CheckResultForBitmapWithSingleRowGroup(path, arrow_data_type, split);
}

TEST_P(ReadInteWithIndexTest, TestNoEmbeddingBitmapIndexWithExternalPath) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path = GetDataDir() + "/" + file_format +
                       "/append_with_bitmap_no_embedding.db/append_with_bitmap_no_embedding/";
    std::string file_name, external_file_path;
    if (file_format == "orc") {
        file_name = "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc";
        external_file_path = GetDataDir() + "/" + file_format +
                             "/append_with_bitmap_no_embedding.db/external-path/bucket-0/" +
                             file_name;
    } else if (file_format == "parquet") {
        file_name = "data-783929b2-49d4-4006-a898-194a62e3278d-0.parquet";
        external_file_path = GetDataDir() + "/" + file_format +
                             "/append_with_bitmap_no_embedding.db/external-path/bucket-0/" +
                             file_name;
    }

    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({file_name + ".index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/external_file_path, /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());
    CheckResultForBitmapWithSingleRowGroup(path, arrow_data_type, split);
}

TEST_P(ReadInteWithIndexTest, TestBitmapIndexWithDv) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path =
        GetDataDir() + "/" + file_format + "/pk_with_bitmap_dv.db/pk_with_bitmap_dv/";
    std::string file_name, deletion_file_path;
    if (file_format == "orc") {
        file_name = "data-0da0dc51-797c-4c82-9b08-269b2ce400dc-0.orc";
        deletion_file_path = path + "/index/index-73ec72e9-489e-49d9-a96c-f6db9b56fa07-0";
    } else if (file_format == "parquet") {
        file_name = "data-bfa36b28-40e7-4026-893b-d23116973174-0.parquet";
        deletion_file_path = path + "/index/index-daed25d2-8e68-45ba-aaad-ff342666ef27-0";
    }

    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("key", arrow::int32())),
                                          DataField(1, arrow::field("f0", arrow::utf8())),
                                          DataField(2, arrow::field("f1", arrow::int32())),
                                          DataField(3, arrow::field("f2", arrow::int32())),
                                          DataField(4, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/1001,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/5,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({file_name + ".index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DeletionFile deletion_file(deletion_file_path,
                               /*offset=*/1, /*length=*/24, /*cardinality=*/2);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split, builder.WithSnapshot(4)
                                         .WithDataDeletionFiles({deletion_file})
                                         .IsStreaming(false)
                                         .RawConvertible(true)
                                         .Build());
    {
        // test with non predicate
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 1, "Bob", 10, 1, 12.1],
[0, 2, "Emily", 10, 0, 13.1],
[0, 3, "Tony", 10, 0, 14.1],
[0, 5, "Bob", 10, 1, 16.1],
[0, 6, "Tony", 20, 0, 17.1],
[0, 7, "Alice", 20, null, 18.1]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, /*predicate=*/nullptr, expected_array);
    }
    {
        // test equal, Alice with key 0 is removed by dv
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, 7, "Alice", 20, null, 18.1]
        ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test equal, Lucy is removed by dv
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Lucy", 4));
        CheckResult(path, {split}, predicate, /*expected_array=*/nullptr);
    }
    {
        // test or predicate
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({f0_predicate, f1_predicate}));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 1, "Bob", 10, 1, 12.1],
[0, 2, "Emily", 10, 0, 13.1],
[0, 3, "Tony", 10, 0, 14.1],
[0, 5, "Bob", 10, 1, 16.1],
[0, 7, "Alice", 20, null, 18.1]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
}

TEST_P(ReadInteWithIndexTest, TestWithAlterTable) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path = GetDataDir() + "/" + file_format +
                       "/append_with_bitmap_alter_table.db/append_with_bitmap_alter_table/";
    std::string file_name1, file_name2;
    if (file_format == "orc") {
        file_name1 = "data-68014988-5451-478f-a18a-a1668214cf3d-0.orc";
        file_name2 = "data-a29b7235-760d-4838-881c-39cbef585dd2-0.orc";
    } else if (file_format == "parquet") {
        file_name1 = "data-da667947-0604-4459-973b-9d364c3953a9-0.parquet";
        file_name2 = "data-2efd8477-9fb4-44a6-9a39-901dd5a83b28-0.parquet";
    }

    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(1, arrow::field("f1", arrow::int64())),
                                          DataField(0, arrow::field("f4", arrow::utf8())),
                                          DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(4, arrow::field("f5", arrow::int32()))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    auto create_data_file_meta = [&](const std::string& file_name, int32_t schema_id,
                                     const std::vector<uint8_t>& embedded_bytes) {
        auto embedded_index = std::make_shared<Bytes>(embedded_bytes.size(), pool_.get());
        memcpy(embedded_index->data(), reinterpret_cast<const void*>(embedded_bytes.data()),
               embedded_bytes.size());
        return std::make_shared<DataFileMeta>(
            file_name, /*file_size=*/10,
            /*row_count=*/10, /*min_key=*/BinaryRow::EmptyRow(),
            /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
            /*max_sequence_number=*/10, schema_id,
            /*level=*/0,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
            /*embedded_index=*/embedded_index, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    };

    std::vector<uint8_t> embedded_bytes1 = {
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
    std::vector<uint8_t> embedded_bytes2 = {
        0,   5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,   0,   0,   96,  0,   0,
        0,   3,   0,   2,   102, 49,  0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112,
        0,   0,   0,   96,  0,   0,   0,   102, 0,   2,   102, 52,  0,   0,   0,   1,   0,   6,
        98,  105, 116, 109, 97,  112, 0,   0,   0,   198, 0,   0,   0,   101, 0,   2,   102, 53,
        0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,   0,   1,   43,  0,   0,
        0,   82,  0,   0,   0,   0,   2,   0,   0,   0,   4,   0,   0,   0,   3,   0,   0,   0,
        0,   1,   0,   0,   0,   0,   0,   0,   0,   10,  0,   0,   0,   0,   0,   0,   0,   52,
        0,   0,   0,   3,   0,   0,   0,   0,   0,   0,   0,   10,  255, 255, 255, 253, 255, 255,
        255, 255, 0,   0,   0,   0,   0,   0,   0,   20,  255, 255, 255, 254, 255, 255, 255, 255,
        0,   0,   0,   0,   0,   0,   0,   30,  0,   0,   0,   0,   0,   0,   0,   20,  58,  48,
        0,   0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   0,   0,   3,   0,
        2,   0,   0,   0,   4,   0,   0,   0,   4,   0,   0,   0,   0,   1,   0,   0,   0,   5,
        65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,   0,   70,  0,   0,   0,   4,   0,
        0,   0,   5,   65,  108, 105, 99,  101, 255, 255, 255, 255, 255, 255, 255, 255, 0,   0,
        0,   3,   66,  111, 98,  255, 255, 255, 253, 255, 255, 255, 255, 0,   0,   0,   5,   68,
        97,  118, 105, 100, 255, 255, 255, 252, 255, 255, 255, 255, 0,   0,   0,   5,   69,  109,
        105, 108, 121, 255, 255, 255, 254, 255, 255, 255, 255, 2,   0,   0,   0,   4,   0,   0,
        0,   2,   1,   255, 255, 255, 252, 0,   0,   0,   18,  0,   0,   0,   1,   0,   0,   0,
        100, 0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,   0,   100, 0,
        0,   0,   0,   0,   0,   0,   20,  0,   0,   0,   101, 255, 255, 255, 254, 255, 255, 255,
        255, 58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   0,
        0,   2,   0};
    auto data_file_meta1 = create_data_file_meta(file_name1, 0, embedded_bytes1);
    auto data_file_meta2 = create_data_file_meta(file_name2, 1, embedded_bytes2);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "/bucket-0/",
                                   {data_file_meta1, data_file_meta2});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(2).IsStreaming(false).RawConvertible(true).Build());
    {
        // test with non predicate
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Alice", 11.1, null],
[0, 10, "Bob", 12.1, null],
[0, 10, "Emily", 13.1, null],
[0, 10, "Tony", 14.1, null],
[0, 20, "Lucy", 15.1, null],
[0, 10, "Bob", 16.1, null],
[0, 20, "Tony", 17.1, null],
[0, 20, "Alice", 18.1, null],
[0, 30, "Alice", 21.1, 100],
[0, 20, "Emily", 22.1, 101],
[0, 10, "Bob", 23.1, 100],
[0, 30, "David", 24.1, null]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, /*predicate=*/nullptr, expected_array);
    }
    {
        // test equal predicate for f1
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f1",
                                                 FieldType::BIGINT, Literal(10l));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Alice", 11.1, null],
[0, 10, "Bob", 12.1, null],
[0, 10, "Emily", 13.1, null],
[0, 10, "Tony", 14.1, null],
[0, 10, "Bob", 16.1, null],
[0, 10, "Bob", 23.1, 100]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test not equal predicate for f1
        auto predicate = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(10l));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 20, "Lucy", 15.1, null],
[0, 20, "Tony", 17.1, null],
[0, 20, "Alice", 18.1, null],
[0, 30, "Alice", 21.1, 100],
[0, 20, "Emily", 22.1, 101],
[0, 30, "David", 24.1, null]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test equal predicate for f4
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f4", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Alice", 11.1, null],
[0, 20, "Alice", 18.1, null],
[0, 30, "Alice", 21.1, 100]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test not equal predicate for f4
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f4", FieldType::STRING,
                                       Literal(FieldType::STRING, "Alice", 5));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Bob", 12.1, null],
[0, 10, "Emily", 13.1, null],
[0, 10, "Tony", 14.1, null],
[0, 20, "Lucy", 15.1, null],
[0, 10, "Bob", 16.1, null],
[0, 20, "Tony", 17.1, null],
[0, 20, "Emily", 22.1, 101],
[0, 10, "Bob", 23.1, 100],
[0, 30, "David", 24.1, null]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test equal predicate for f3, only do predicate push down
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(14.1));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Alice", 11.1, null],
[0, 10, "Bob", 12.1, null],
[0, 10, "Emily", 13.1, null],
[0, 10, "Tony", 14.1, null],
[0, 20, "Lucy", 15.1, null],
[0, 10, "Bob", 16.1, null],
[0, 20, "Tony", 17.1, null],
[0, 20, "Alice", 18.1, null]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test is null predicate for f5
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f5", FieldType::INT);
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Alice", 11.1, null],
[0, 10, "Bob", 12.1, null],
[0, 10, "Emily", 13.1, null],
[0, 10, "Tony", 14.1, null],
[0, 20, "Lucy", 15.1, null],
[0, 10, "Bob", 16.1, null],
[0, 20, "Tony", 17.1, null],
[0, 20, "Alice", 18.1, null],
[0, 30, "David", 24.1, null]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test is_not_null predicate for f5
        // recall result contains the whole data of the first file, as f5 does not exist in the
        // first data file (the predicate is removed when reading that file)
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/3, /*field_name=*/"f5", FieldType::INT);
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Alice", 11.1, null],
[0, 10, "Bob", 12.1, null],
[0, 10, "Emily", 13.1, null],
[0, 10, "Tony", 14.1, null],
[0, 20, "Lucy", 15.1, null],
[0, 10, "Bob", 16.1, null],
[0, 20, "Tony", 17.1, null],
[0, 20, "Alice", 18.1, null],
[0, 30, "Alice", 21.1, 100],
[0, 20, "Emily", 22.1, 101],
[0, 10, "Bob", 23.1, 100]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test greater than predicate for f1, do not take effective in bitmap index
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f1",
                                                       FieldType::BIGINT, Literal(10l));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Alice", 11.1, null],
[0, 10, "Bob", 12.1, null],
[0, 10, "Emily", 13.1, null],
[0, 10, "Tony", 14.1, null],
[0, 20, "Lucy", 15.1, null],
[0, 10, "Bob", 16.1, null],
[0, 20, "Tony", 17.1, null],
[0, 20, "Alice", 18.1, null],
[0, 30, "Alice", 21.1, 100],
[0, 20, "Emily", 22.1, 101],
[0, 10, "Bob", 23.1, 100],
[0, 30, "David", 24.1, null]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test in predicate
        auto predicate =
            PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f1", FieldType::BIGINT,
                                 {Literal(10l), Literal(30l), Literal(50l)});
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Alice", 11.1, null],
[0, 10, "Bob", 12.1, null],
[0, 10, "Emily", 13.1, null],
[0, 10, "Tony", 14.1, null],
[0, 10, "Bob", 16.1, null],
[0, 30, "Alice", 21.1, 100],
[0, 10, "Bob", 23.1, 100],
[0, 30, "David", 24.1, null]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test not in predicate
        auto predicate =
            PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f1", FieldType::BIGINT,
                                    {Literal(10l), Literal(30l), Literal(50l)});
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 20, "Lucy", 15.1, null],
[0, 20, "Tony", 17.1, null],
[0, 20, "Alice", 18.1, null],
[0, 20, "Emily", 22.1, 101]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test and predicate
        auto f4_predicate =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f4", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(30l));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({f4_predicate, f1_predicate}));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 30, "Alice", 21.1, 100]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test or predicate
        auto f4_predicate =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f4", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(30l));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({f4_predicate, f1_predicate}));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Alice", 11.1, null],
[0, 20, "Alice", 18.1, null],
[0, 30, "Alice", 21.1, 100],
[0, 30, "David", 24.1, null]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test early stop
        auto f4_predicate =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f4", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(40l));
        auto f5_predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/3, /*field_name=*/"f5", FieldType::INT);
        ASSERT_OK_AND_ASSIGN(auto predicate,
                             PredicateBuilder::And({f4_predicate, f1_predicate, f5_predicate}));
        CheckResult(path, {split}, predicate, /*expected_array=*/nullptr);
    }
    {
        // test non result
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f1",
                                                 FieldType::BIGINT, Literal(40l));
        CheckResult(path, {split}, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(40l));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, 10, "Alice", 11.1, null],
[0, 10, "Bob", 12.1, null],
[0, 10, "Emily", 13.1, null],
[0, 10, "Tony", 14.1, null],
[0, 20, "Lucy", 15.1, null],
[0, 10, "Bob", 16.1, null],
[0, 20, "Tony", 17.1, null],
[0, 20, "Alice", 18.1, null],
[0, 30, "Alice", 21.1, 100],
[0, 20, "Emily", 22.1, 101],
[0, 10, "Bob", 23.1, 100],
[0, 30, "David", 24.1, null]
])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
}

TEST_P(ReadInteWithIndexTest, TestWithBsiIndex) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path = GetDataDir() + "/" + file_format + "/append_with_bsi.db/append_with_bsi/";
    std::string file_name;
    if (file_format == "orc") {
        file_name = "data-0befce14-587d-48e5-ad08-efeecceb0f09-0.orc";
    } else if (file_format == "parquet") {
        file_name = "data-af9e31b7-b44d-4a0e-85ae-5877d224ec9f-0.parquet";
    }

    std::vector<DataField> read_fields = {
        SpecialFields::ValueKind(),
        DataField(0, arrow::field("f0", arrow::utf8())),
        DataField(1, arrow::field("f1", arrow::int32())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::float64())),
        DataField(4, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);
    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/932,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({file_name + ".index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());
    {
        // test equal predicate for f0, take no effective as bsi does not support string
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Alice", 100, -2, 11.1, 1745542802000123000],
    [0, "Bob", 200, -3, 12.1, 1745542902000123000],
    [0, "Emily", 300, 1, 13.1, 1745542602000123000],
    [0, "Tony", 50, 1, 14.1, -1744877000],
    [0, "Lucy", 500, -1, 15.1, -1764877000],
    [0, "Bob", 100, 2, 16.1, null],
    [0, "Tony", null, -2, 17.1, 1745542802000123001],
    [0, "Alice", 20, null, 18.1, -1724877000]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    CheckResultForBsi(path, arrow_data_type, {split});
}

TEST_P(ReadInteWithIndexTest, TestWithBloomFilterIndex) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path =
        GetDataDir() + "/" + file_format + "/append_with_bloomfilter.db/append_with_bloomfilter/";
    std::string file_name;
    if (file_format == "orc") {
        file_name = "data-34e8acb2-110b-4c32-9dc6-d6435178d0ad-0.orc";
    } else if (file_format == "parquet") {
        file_name = "data-79a27bc0-bcec-4062-9915-83ec8eb1622d-0.parquet";
    }

    std::vector<DataField> read_fields = {
        SpecialFields::ValueKind(),
        DataField(0, arrow::field("f0", arrow::utf8())),
        DataField(1, arrow::field("f1", arrow::int32())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::float64())),
        DataField(4, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);
    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/932,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({file_name + ".index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

    std::shared_ptr<arrow::ChunkedArray> all_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 100, -2, 11.1, 1745542802000123000],
[0, "Bob", 200, -3, 12.1, 1745542902000123000],
[0, "Emily", 300, 1, 13.1, 1745542602000123000],
[0, "Tony", 50, 1, 14.1, -1744877000],
[0, "Lucy", 500, -1, 15.1, -1764877000],
[0, "Bob", 100, 2, 16.1, null],
[0, "Tony", null, -2, 17.1, 1745542802000123001],
[0, "Alice", 20, null, 18.1, -1724877000]
])"},
                                                                         &all_array);
    ASSERT_TRUE(array_status.ok());
    {
        // test with non predicate
        CheckResult(path, {split}, /*predicate=*/nullptr, all_array);
    }
    {
        // test equal predicate for f0
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test equal predicate for f0, where literal does not exist
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice2", 6));
        CheckResult(path, {split}, predicate, nullptr);
    }
    {
        // test equal predicate for f1
        auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::INT, Literal(200));
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test equal predicate for f1, where literal does not exist
        auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::INT, Literal(201));
        CheckResult(path, {split}, predicate, nullptr);
    }
    {
        // test equal predicate for f2
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(-1));
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test equal predicate for f2, where literal does not exist
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(0));
        CheckResult(path, {split}, predicate, nullptr);
    }
    {
        // test equal predicate for f3
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(13.1));
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test equal predicate for f3, where literal does not exist
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(13.2));
        CheckResult(path, {split}, predicate, nullptr);
    }
    {
        // test equal predicate for f4
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                    Literal(Timestamp(1745542902000l, 123000)));
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test equal predicate for f4, where literal does not exist
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                    Literal(Timestamp(1745542502000l, 123000)));
        CheckResult(path, {split}, predicate, nullptr);
    }
    {
        // test not equal predicate for f1
        auto predicate = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(200));
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test is null predicate for f2
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test is not null predicate for f2
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test greater than predicate for f1
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(200));
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test in for f2
        auto predicate =
            PredicateBuilder::In(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT,
                                 {Literal(-1), Literal(2), Literal(100)});
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test in for f2, where literals do not exist
        auto predicate =
            PredicateBuilder::In(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT,
                                 {Literal(-1000), Literal(0), Literal(1000)});
        CheckResult(path, {split}, predicate, nullptr);
    }
    {
        // test not in for f3
        auto predicate =
            PredicateBuilder::NotIn(/*field_index=*/3, /*field_name=*/"f3", FieldType::DOUBLE,
                                    {Literal(11.1), Literal(12.1), Literal(13.1)});
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test not in for f3, where literals do not exist
        auto predicate =
            PredicateBuilder::NotIn(/*field_index=*/3, /*field_name=*/"f3", FieldType::DOUBLE,
                                    {Literal(11.12), Literal(12.12), Literal(13.12)});
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test and predicate
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(100));
        auto f4_predicate =
            PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                    Literal(Timestamp(1745542902000l, 123000)));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({f1_predicate, f4_predicate}));
        CheckResult(path, {split}, predicate, all_array);
    }
    {
        // test and predicate
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(100));
        auto f4_predicate =
            PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                    Literal(Timestamp(-1728l, 123000)));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({f1_predicate, f4_predicate}));
        CheckResult(path, {split}, predicate, nullptr);
    }
    {
        // test or predicate
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(100));
        auto f4_predicate =
            PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                    Literal(Timestamp(-1728l, 123000)));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({f1_predicate, f4_predicate}));
        CheckResult(path, {split}, predicate, all_array);
    }
}

TEST_P(ReadInteWithIndexTest, TestBitmapPushDownWithMultiStripes) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path = paimon::test::GetDataDir() +
                       "/append_with_bitmap_multi_stripes.db/append_with_bitmap_multi_stripes/";
    std::string file_name, index_file_name;
    if (file_format == "orc") {
        // each record is a stripe
        file_name = "data-55e21ed3-d118-4d75-a0fd-cd52039cb634-0.orc";
        index_file_name = "data-55e21ed3-d118-4d75-a0fd-cd52039cb634-0.orc.index";
    } else if (file_format == "parquet") {
        // each record is a row group
        file_name = "data-multi-row-groups.parquet";
        index_file_name = "data-55e21ed3-d118-4d75-a0fd-cd52039cb634-0.orc.index";
    }
    // each record is a stripe
    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/2637,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({index_file_name}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

    // test bitmap index takes effective
    CheckResultForBitmap(path, arrow_data_type, split);

    // test predicate push down takes effective
    {
        // test greater than predicate (take no effective on bitmap index, but predicates can
        // be pushdown)
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/
                                                       "f1", FieldType::INT, Literal(10));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Lucy", 20, 1, 15.1],
    [0, "Tony", 20, 0, 17.1],
    [0, "Alice", 20, null, 18.1]
        ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test predicate on f3 (do not have index), but predicates can be pushdown
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(14.1));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Tony", 10, 0, 14.1]
        ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test and predicate, although the bitmap index cannot handle the LessThan
        // predicate, the result is still correct due to predicate pushdown
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/
                                                       "f1", FieldType::INT, Literal(15));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({f0_predicate, f1_predicate}));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Alice", 10, 1, 11.1]
        ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test or predicate, although the bitmap index cannot handle the LessOrEqual
        // predicate, the result is still correct due to predicate pushdown
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::LessOrEqual(/*field_index=*/1, /*field_name=*/
                                                          "f1", FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({f0_predicate, f1_predicate}));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Alice", 10, 1, 11.1],
    [0, "Bob", 10, 1, 12.1],
    [0, "Emily", 10, 0, 13.1],
    [0, "Tony", 10, 0, 14.1],
    [0, "Bob", 10, 1, 16.1],
    [0, "Alice", 20, null, 18.1]
        ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
}

TEST_P(ReadInteWithIndexTest, TestWithBitmapAndBsiAndBloomFilterIndex) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path = GetDataDir() + "/" + file_format +
                       "/append_with_bsi_bitmap_bloomfilter.db/append_with_bsi_bitmap_bloomfilter/";
    std::string file_name;
    if (file_format == "orc") {
        file_name = "data-d77394c5-8f82-4fca-93ca-d4da892e7f4f-0.orc";
    } else if (file_format == "parquet") {
        file_name = "data-15f89029-c10f-437f-b819-338c3e8ab2ee-0.parquet";
    }

    std::vector<DataField> read_fields = {
        SpecialFields::ValueKind(),
        DataField(0, arrow::field("f0", arrow::utf8())),
        DataField(1, arrow::field("f1", arrow::int32())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::float64())),
        DataField(4, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);
    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/932,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({file_name + ".index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

    CheckResultForBsi(path, arrow_data_type, split);
    {
        // test equal predicate for f3, only bloom filter take effective
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(14.1));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Alice", 100, -2, 11.1, 1745542802000123000],
    [0, "Bob", 200, -3, 12.1, 1745542902000123000],
    [0, "Emily", 300, 1, 13.1, 1745542602000123000],
    [0, "Tony", 50, 1, 14.1, -1744877000],
    [0, "Lucy", 500, -1, 15.1, -1764877000],
    [0, "Bob", 100, 2, 16.1, null],
    [0, "Tony", null, -2, 17.1, 1745542802000123001],
    [0, "Alice", 20, null, 18.1, -1724877000]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
    {
        // test equal predicate for f3, only bloom filter take effective
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(14.13));
        CheckResult(path, {split}, predicate, /*expected_array=*/nullptr);
    }
    {
        // test equal predicate for f0, bitmap index takes effective
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Alice", 100, -2, 11.1, 1745542802000123000],
    [0, "Alice", 20, null, 18.1, -1724877000]
    ])"},
                                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(path, {split}, predicate, expected_array);
    }
}

TEST_P(ReadInteWithIndexTest, TestWithIndexWithoutRegistered) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path = GetDataDir() + "/" + file_format +
                       "/append_with_bsi_bitmap_bloomfilter.db/append_with_bsi_bitmap_bloomfilter/";
    std::string file_name;
    if (file_format == "orc") {
        file_name = "data-d77394c5-8f82-4fca-93ca-d4da892e7f4f-0.orc";
    } else if (file_format == "parquet") {
        file_name = "data-15f89029-c10f-437f-b819-338c3e8ab2ee-0.parquet";
    }

    auto factory_creator = FactoryCreator::GetInstance();
    std::vector<DataField> read_fields = {
        SpecialFields::ValueKind(),
        DataField(0, arrow::field("f0", arrow::utf8())),
        DataField(1, arrow::field("f1", arrow::int32())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::float64())),
        DataField(4, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);
    // index file contains bitmap & bsi & bloomfilter
    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/932,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({file_name + ".index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());
    {
        // only bitmap is registered
        factory_creator->TEST_Unregister("bsi");
        factory_creator->TEST_Unregister("bloom-filter");
        ScopeGuard guard([&factory_creator]() {
            factory_creator->Register("bsi", (new BitSliceIndexBitmapFileIndexFactory));
            factory_creator->Register("bloom-filter", (new BloomFilterFileIndexFactory));
        });
        {
            // test greater than predicate for f1, as only bitmap is registered, predicate takes
            // no effective
            auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/
                                                           "f1", FieldType::INT, Literal(100));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Alice", 100, -2, 11.1, 1745542802000123000],
    [0, "Bob", 200, -3, 12.1, 1745542902000123000],
    [0, "Emily", 300, 1, 13.1, 1745542602000123000],
    [0, "Tony", 50, 1, 14.1, -1744877000],
    [0, "Lucy", 500, -1, 15.1, -1764877000],
    [0, "Bob", 100, 2, 16.1, null],
    [0, "Tony", null, -2, 17.1, 1745542802000123001],
    [0, "Alice", 20, null, 18.1, -1724877000]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test and predicate, only f1_equals takes effective
            auto f1_equals = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                     FieldType::INT, Literal(100));
            auto f2_greater_than = PredicateBuilder::GreaterThan(
                /*field_index=*/2, /*field_name=*/"f2", FieldType::INT, Literal(0));
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::And({f1_equals, f2_greater_than}));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Alice", 100, -2, 11.1, 1745542802000123000],
    [0, "Bob", 100, 2, 16.1, null]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
    }
    {
        // only bsi is registered
        factory_creator->TEST_Unregister("bitmap");
        factory_creator->TEST_Unregister("bloom-filter");
        ScopeGuard guard([&factory_creator]() {
            factory_creator->Register("bitmap", (new BitmapFileIndexFactory));
            factory_creator->Register("bloom-filter", (new BloomFilterFileIndexFactory));
        });
        {
            // test equal predicate for f0, as bitmap is unregistered, predicate takes no
            // effective
            auto predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Alice", 100, -2, 11.1, 1745542802000123000],
    [0, "Bob", 200, -3, 12.1, 1745542902000123000],
    [0, "Emily", 300, 1, 13.1, 1745542602000123000],
    [0, "Tony", 50, 1, 14.1, -1744877000],
    [0, "Lucy", 500, -1, 15.1, -1764877000],
    [0, "Bob", 100, 2, 16.1, null],
    [0, "Tony", null, -2, 17.1, 1745542802000123001],
    [0, "Alice", 20, null, 18.1, -1724877000]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
        {
            // test and predicate, as bsi is registered f1_equals and f2_greater_than all take
            // effective
            auto f1_equals = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                     FieldType::INT, Literal(100));
            auto f2_greater_than = PredicateBuilder::GreaterThan(
                /*field_index=*/2, /*field_name=*/"f2", FieldType::INT, Literal(0));
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::And({f1_equals, f2_greater_than}));
            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto array_status =
                arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
    [0, "Bob", 100, 2, 16.1, null]
    ])"},
                                                                 &expected_array);
            ASSERT_TRUE(array_status.ok());
            CheckResult(path, {split}, predicate, expected_array);
        }
    }
}

TEST_P(ReadInteWithIndexTest, TestWithIOException) {
    auto [file_format, enable_prefetch] = GetParam();
    std::string path = GetDataDir() + "/" + file_format +
                       "/append_with_bitmap_no_embedding.db/append_with_bitmap_no_embedding/";
    std::string file_name;
    if (file_format == "orc") {
        file_name = "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc";
    } else if (file_format == "parquet") {
        file_name = "data-783929b2-49d4-4006-a898-194a62e3278d-0.parquet";
    }

    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    auto data_file_meta = std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>({file_name + ".index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                   /*bucket_path=*/path + "bucket-0/", {data_file_meta});
    ASSERT_OK_AND_ASSIGN(auto split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, "Alice", 5));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
[0, "Alice", 10, 1, 11.1],
[0, "Alice", 20, null, 18.1]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 200; i++) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        ReadContextBuilder context_builder(path);
        context_builder.AddOption("read.batch-size", "2")
            .AddOption("test.enable-adaptive-prefetch-strategy", "false")
            .SetPredicate(predicate);
        if (enable_prefetch) {
            context_builder.EnablePrefetch(true).SetPrefetchBatchCount(3);
        }
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        Result<std::unique_ptr<TableRead>> table_read = TableRead::Create(std::move(read_context));
        CHECK_HOOK_STATUS(table_read.status(), i);
        Result<std::unique_ptr<BatchReader>> batch_reader = table_read.value()->CreateReader(split);
        CHECK_HOOK_STATUS(batch_reader.status(), i);
        auto result = ReadResultCollector::CollectResult(batch_reader.value().get());
        CHECK_HOOK_STATUS(result.status(), i);
        auto result_array = result.value();
        ASSERT_TRUE(result_array);
        ASSERT_TRUE(result_array->Equals(*expected_array));
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}
}  // namespace paimon::test
