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

#include "paimon/common/utils/threadsafe_queue.h"

#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(ThreadsafeQueueTest, BasicPushPop) {
    ThreadsafeQueue<int> q;
    q.push(1);
    q.push(2);

    auto val1 = q.try_pop();
    ASSERT_TRUE(val1);
    EXPECT_EQ(*val1, 1);

    auto val2 = q.try_pop();
    ASSERT_TRUE(val2);
    EXPECT_EQ(*val2, 2);

    auto val3 = q.try_pop();
    EXPECT_FALSE(val3);
}

TEST(ThreadsafeQueueTest, PushRValue) {
    ThreadsafeQueue<std::string> q;
    std::string s1 = "hello";
    q.push(std::move(s1));

    auto val = q.try_pop();
    ASSERT_TRUE(val);
    EXPECT_EQ(*val, "hello");
    EXPECT_TRUE(s1.empty());  // NOLINT(bugprone-use-after-move)
}

TEST(ThreadsafeQueueTest, EmptyMethod) {
    ThreadsafeQueue<int> q;
    EXPECT_TRUE(q.empty());

    q.push(1);
    EXPECT_FALSE(q.empty());

    q.try_pop();
    EXPECT_TRUE(q.empty());
}

TEST(ThreadsafeQueueTest, SizeMethod) {
    ThreadsafeQueue<int> q;
    EXPECT_EQ(q.size(), 0);

    q.push(1);
    EXPECT_EQ(q.size(), 1);

    q.push(2);
    EXPECT_EQ(q.size(), 2);

    q.try_pop();
    EXPECT_EQ(q.size(), 1);

    q.try_pop();
    EXPECT_EQ(q.size(), 0);
}

TEST(ThreadsafeQueueTest, TryFront) {
    ThreadsafeQueue<int> q;
    EXPECT_EQ(q.try_front(), nullptr);

    q.push(10);
    const int* front_val = q.try_front();
    ASSERT_NE(front_val, nullptr);
    EXPECT_EQ(*front_val, 10);
    EXPECT_EQ(q.size(), 1);

    q.push(20);
    front_val = q.try_front();
    ASSERT_NE(front_val, nullptr);
    EXPECT_EQ(*front_val, 10);

    q.try_pop();
    front_val = q.try_front();
    ASSERT_NE(front_val, nullptr);
    EXPECT_EQ(*front_val, 20);
}

}  // namespace paimon::test
