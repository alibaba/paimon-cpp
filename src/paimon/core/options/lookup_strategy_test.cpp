/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/core/options/lookup_strategy.h"

#include "gtest/gtest.h"

namespace paimon::test {

TEST(LookupStrategyTest, TestFrom) {
    auto strategy = LookupStrategy::From(
        /*is_first_row=*/true,
        /*produce_changelog=*/false,
        /*deletion_vector=*/false,
        /*force_lookup=*/false);

    ASSERT_TRUE(strategy.need_lookup);
    ASSERT_TRUE(strategy.is_first_row);
    ASSERT_FALSE(strategy.produce_changelog);
    ASSERT_FALSE(strategy.deletion_vector);
}

TEST(LookupStrategyTest, TestNeedLookupCombinations) {
    ASSERT_FALSE(LookupStrategy::From(false, false, false, false).need_lookup);
    ASSERT_TRUE(LookupStrategy::From(false, true, false, false).need_lookup);
    ASSERT_TRUE(LookupStrategy::From(false, false, true, false).need_lookup);
    ASSERT_TRUE(LookupStrategy::From(false, false, false, true).need_lookup);
}

}  // namespace paimon::test
