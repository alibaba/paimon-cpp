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

#include "paimon/core/stats/simple_stats.h"

#include <vector>

#include "gtest/gtest.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
TEST(SimpleStatsTest, TestToFromRow) {
    auto pool = GetDefaultPool();
    std::vector<SimpleStats> simple_stats_vec = {
        BinaryRowGenerator::GenerateStats(
            {false, static_cast<int8_t>(-2), static_cast<int16_t>(-32768),
             static_cast<int32_t>(-2147483648), NullType(), static_cast<int64_t>(-4294967298),
             static_cast<float>(0.5), 1.141592659, "20250326", NullType()},
            {true, static_cast<int8_t>(1), static_cast<int16_t>(32767),
             static_cast<int32_t>(2147483647), NullType(), static_cast<int64_t>(4294967296),
             static_cast<float>(2.0), 3.141592657, "20250327", NullType()},
            std::vector<int64_t>({1, 0, 0, 1, 4, 1, 0, 0, 1, 0}), pool.get()),
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, {1}, pool.get()),
        SimpleStats::EmptyStats(),
    };
    for (const auto& stats : simple_stats_vec) {
        auto row = stats.ToRow();
        ASSERT_OK_AND_ASSIGN(auto result_stats, SimpleStats::FromRow(&row, pool.get()));
        ASSERT_EQ(result_stats, stats);
        ASSERT_EQ(result_stats.ToString(), stats.ToString());
    }
}

}  // namespace paimon::test
