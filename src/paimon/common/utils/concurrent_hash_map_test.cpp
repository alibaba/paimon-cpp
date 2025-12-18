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

#include "paimon/common/utils/concurrent_hash_map.h"

#include <unistd.h>

#include <cstdlib>
#include <functional>
#include <thread>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(ConcurrentHashMapTest, TestSimple) {
    ConcurrentHashMap<int32_t, std::string> hash_map;
    ASSERT_EQ(hash_map.Find(10), std::nullopt);
    hash_map.Insert(1, "a");
    hash_map.Insert(2, "b");
    hash_map.Insert(3, "c");

    ASSERT_EQ(hash_map.Find(1).value(), "a");
    ASSERT_EQ(hash_map.Find(2).value(), "b");
    ASSERT_EQ(hash_map.Find(3).value(), "c");
    ASSERT_EQ(hash_map.Find(10), std::nullopt);
    ASSERT_EQ(hash_map.Size(), 3);

    hash_map.Erase(2);
    ASSERT_EQ(hash_map.Find(2), std::nullopt);
    ASSERT_EQ(hash_map.Size(), 2);

    // non-exist key
    hash_map.Erase(4);
    ASSERT_EQ(hash_map.Find(1).value(), "a");
    ASSERT_EQ(hash_map.Find(3).value(), "c");
    ASSERT_EQ(hash_map.Size(), 2);
}

TEST(ConcurrentHashMapTest, TestVectorStringHashCompare) {
    ConcurrentHashMap<std::vector<std::string>, int32_t, VectorStringHashCompare> hash_map;
    ASSERT_EQ(hash_map.Find({}), std::nullopt);

    hash_map.Insert({"a", "b"}, 1);
    hash_map.Insert({"a", "c"}, 2);
    hash_map.Insert({"b", "c"}, 3);
    hash_map.Insert({}, 4);

    ASSERT_EQ(hash_map.Find({"a", "b"}).value(), 1);
    ASSERT_EQ(hash_map.Find({"a", "c"}).value(), 2);
    ASSERT_EQ(hash_map.Find({"b", "c"}).value(), 3);
    ASSERT_EQ(hash_map.Find({}), 4);
    ASSERT_EQ(hash_map.Find({"non"}), std::nullopt);
    ASSERT_EQ(hash_map.Size(), 4);
}

TEST(ConcurrentHashMapTest, TestMultiThreadInsertAndFindAndDelete) {
    int32_t map_size = 1000;
    auto insert_task = [&](ConcurrentHashMap<int32_t, std::string>& hash_map) {
        for (int32_t i = 0; i < map_size; i++) {
            usleep(paimon::test::RandomNumber(0, 9));
            hash_map.Insert(i, std::to_string(i + 1));
        }
    };
    auto find_task = [&](ConcurrentHashMap<int32_t, std::string>& hash_map) {
        int32_t found = 0, not_found = 0;
        for (int32_t i = 0; i < map_size; i++) {
            usleep(paimon::test::RandomNumber(0, 9));
            auto value = hash_map.Find(i);
            if (value) {
                ASSERT_EQ(value.value(), std::to_string(i + 1));
                found++;
            } else {
                not_found++;
            }
        }
        ASSERT_EQ(found + not_found, map_size);
    };

    auto delete_task = [&](ConcurrentHashMap<int32_t, std::string>& hash_map) {
        for (int32_t i = 0; i < map_size; i++) {
            usleep(paimon::test::RandomNumber(0, 9));
            hash_map.Erase(i);
        }
    };

    {
        ConcurrentHashMap<int32_t, std::string> hash_map;
        // insert
        insert_task(hash_map);
        // multi-thread find and delete
        std::thread thread1(find_task, std::ref(hash_map));
        std::thread thread2(delete_task, std::ref(hash_map));

        thread1.join();
        thread2.join();

        // check final states
        ASSERT_EQ(hash_map.Size(), 0);
    }
    {
        ConcurrentHashMap<int32_t, std::string> hash_map;
        // multi-thread insert and find
        std::thread thread1(insert_task, std::ref(hash_map));
        std::thread thread2(find_task, std::ref(hash_map));

        thread1.join();
        thread2.join();

        // check final states
        ASSERT_EQ(hash_map.Size(), map_size);
        for (int32_t i = 0; i < map_size; i++) {
            auto value = hash_map.Find(i);
            ASSERT_TRUE(value);
            ASSERT_EQ(value.value(), std::to_string(i + 1));
        }
    }
    {
        ConcurrentHashMap<int32_t, std::string> hash_map;
        // multi-thread insert and find and delete
        std::thread thread1(insert_task, std::ref(hash_map));
        std::thread thread2(find_task, std::ref(hash_map));
        std::thread thread3(delete_task, std::ref(hash_map));

        thread1.join();
        thread2.join();
        thread3.join();

        // check final states
        ASSERT_TRUE(hash_map.Size() >= 0 && hash_map.Size() <= static_cast<size_t>(map_size));
        for (int32_t i = 0; i < map_size; i++) {
            auto value = hash_map.Find(i);
            if (value) {
                ASSERT_EQ(value.value(), std::to_string(i + 1));
            }
        }
    }
}

}  // namespace paimon::test
