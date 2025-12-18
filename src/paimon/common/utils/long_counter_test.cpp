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

#include "paimon/common/utils/long_counter.h"

#include "gtest/gtest.h"

namespace paimon::test {

// Test case: Test default constructor and initial value
TEST(LongCounterTest, DefaultConstructor) {
    LongCounter counter;
    EXPECT_EQ(counter.GetValue(), 0);
}

// Test case: Test constructor with initial value
TEST(LongCounterTest, ConstructorWithInitialValue) {
    LongCounter counter(10);
    EXPECT_EQ(counter.GetValue(), 10);
}

// Test case: Test Add method
TEST(LongCounterTest, Add) {
    LongCounter counter(10);
    counter.Add(5);
    EXPECT_EQ(counter.GetValue(), 15);

    counter.Add(-3);
    EXPECT_EQ(counter.GetValue(), 12);
}

// Test case: Test Reset method
TEST(LongCounterTest, Reset) {
    LongCounter counter(10);
    counter.Add(5);
    EXPECT_EQ(counter.GetValue(), 15);

    counter.Reset();
    EXPECT_EQ(counter.GetValue(), 0);
}

// Test case: Test Merge method
TEST(LongCounterTest, Merge) {
    LongCounter counter1(10);
    LongCounter counter2(20);

    counter1.Merge(counter2);
    EXPECT_EQ(counter1.GetValue(), 30);
}

// Test case: Test ToString method
TEST(LongCounterTest, ToString) {
    LongCounter counter(42);
    EXPECT_EQ(counter.ToString(), "LongCounter 42");

    counter.Reset();
    EXPECT_EQ(counter.ToString(), "LongCounter 0");
}

}  // namespace paimon::test
