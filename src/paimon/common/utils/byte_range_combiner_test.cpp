/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Adapted from Apache ORC
// https://github.com/apache/orc/blob/main/c%2B%2B/test/TestCache.cc

#include "paimon/common/utils/byte_range_combiner.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/read_ahead_cache.h"

namespace paimon::test {

TEST(ByteRangeCombinerTest, TestBasics) {
    auto check = [](std::vector<ByteRange> ranges, std::vector<ByteRange> expected) -> void {
        const uint64_t hole_size_limit = 9;
        const uint64_t range_size_limit = 99;
        auto actual = ranges;
        ASSERT_OK_AND_ASSIGN(auto coalesced,
                             ByteRangeCombiner::CoalesceByteRanges(
                                 std::move(actual), hole_size_limit, range_size_limit));
        ASSERT_EQ(coalesced, expected);
    };

    check({}, {});
    // Zero sized range that ends up in empty list
    check({{110, 0}}, {});
    // Combination on 1 zero sized range and 1 non-zero sized range
    check({{110, 10}, {120, 0}}, {{110, 10}});
    // 1 non-zero sized range
    check({{110, 10}}, {{110, 10}});
    // No holes + unordered ranges
    check({{130, 10}, {110, 10}, {120, 10}}, {{110, 30}});
    // No holes
    check({{110, 10}, {120, 10}, {130, 10}}, {{110, 30}});
    // Small holes only
    check({{110, 11}, {130, 11}, {150, 11}}, {{110, 51}});
    // Large holes
    check({{110, 10}, {130, 10}}, {{110, 10}, {130, 10}});
    check({{110, 11}, {130, 11}, {150, 10}, {170, 11}, {190, 11}}, {{110, 50}, {170, 31}});

    // With zero-sized ranges
    check({{110, 11}, {130, 0}, {130, 11}, {145, 0}, {150, 11}, {200, 0}}, {{110, 51}});

    // No holes but large ranges
    check({{110, 100}, {210, 100}}, {{110, 100}, {210, 100}});
    // Small holes and large range in the middle (*)
    check({{110, 10}, {120, 11}, {140, 100}, {240, 11}, {260, 11}},
          {{110, 21}, {140, 100}, {240, 31}});
    // Mid-size ranges that would turn large after coalescing
    check({{100, 50}, {150, 50}}, {{100, 50}, {150, 50}});
    check({{100, 30}, {130, 30}, {160, 30}, {190, 30}, {220, 30}}, {{100, 90}, {190, 60}});

    // Same as (*) but unsorted
    check({{140, 100}, {120, 11}, {240, 11}, {110, 10}, {260, 11}},
          {{110, 21}, {140, 100}, {240, 31}});

    // Completely overlapping ranges should be eliminated
    check({{20, 5}, {20, 5}, {21, 2}}, {{20, 5}});
}

}  // namespace paimon::test
