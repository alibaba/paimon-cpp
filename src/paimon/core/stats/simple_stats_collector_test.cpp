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

#include "paimon/core/stats/simple_stats_collector.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <variant>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(SimpleStatsCollectorTest, TestSimple) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),   arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),   arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()), arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::date32()),
    };

    auto schema = arrow::schema(fields);
    SimpleStatsCollector collector(schema);
    auto pool = GetDefaultPool();
    ASSERT_OK(collector.Collect(BinaryRowGenerator::GenerateRow(
        {true, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(1),
         static_cast<int64_t>(1), static_cast<float>(3.0), static_cast<double>(3.0),
         std::string("abc"), 2025},
        pool.get())));
    ASSERT_OK(collector.Collect(BinaryRowGenerator::GenerateRow(
        {false, static_cast<int8_t>(2), static_cast<int16_t>(2), static_cast<int32_t>(2),
         static_cast<int64_t>(2), static_cast<float>(6.0), static_cast<double>(6.0),
         std::string("bcd"), 2026},
        pool.get())));
    ASSERT_OK_AND_ASSIGN(auto col_stats, collector.GetResult());
    ASSERT_OK_AND_ASSIGN(SimpleStats stats, SimpleStatsConverter::ToBinary(col_stats, pool.get()));

    auto expected_stats = BinaryRowGenerator::GenerateStats(
        {false, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(1),
         static_cast<int64_t>(1), static_cast<float>(3.0), static_cast<double>(3.0),
         std::string("abc"), 2025},
        {true, static_cast<int8_t>(2), static_cast<int16_t>(2), static_cast<int32_t>(2),
         static_cast<int64_t>(2), static_cast<float>(6.0), static_cast<double>(6.0),
         std::string("bcd"), 2026},
        std::vector<int64_t>({0, 0, 0, 0, 0, 0, 0, 0, 0}), GetDefaultPool().get());

    ASSERT_EQ(stats, expected_stats);
}

TEST(SimpleStatsCollectorTest, TestNull) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),   arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),   arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()), arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::date32()),  arrow::field("key", arrow::int32()),
    };

    auto schema = arrow::schema(fields);
    SimpleStatsCollector collector(schema);
    auto pool = GetDefaultPool();
    ASSERT_OK(collector.Collect(
        BinaryRowGenerator::GenerateRow({NullType(), NullType(), NullType(), NullType(), NullType(),
                                         NullType(), NullType(), NullType(), NullType(), 100},
                                        pool.get())));
    ASSERT_OK_AND_ASSIGN(auto col_stats, collector.GetResult());
    ASSERT_EQ(10, col_stats.size());
    ASSERT_OK_AND_ASSIGN(SimpleStats stats, SimpleStatsConverter::ToBinary(col_stats, pool.get()));

    ASSERT_EQ(stats.MinValues(), stats.MaxValues());
    for (size_t i = 0; i < 9; ++i) {
        ASSERT_TRUE(stats.MinValues().IsNullAt(i));
    }
    ASSERT_EQ(stats.MinValues().GetInt(9), 100);
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> expected, stats.NullCounts().ToLongArray());
    ASSERT_EQ(expected, std::vector<int64_t>({1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 0l}));
}

TEST(SimpleStatsCollectorTest, TestInvalidPartition) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::boolean())};

    auto schema = arrow::schema(fields);
    SimpleStatsCollector collector(schema);
    auto pool = GetDefaultPool();
    ASSERT_NOK_WITH_MSG(collector.Collect(BinaryRow::EmptyRow()), "partition schema not equal");
}

}  // namespace paimon::test
