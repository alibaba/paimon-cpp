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

#include "paimon/common/data/binary_map.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(BinaryMapTest, TestSimple) {
    auto pool = GetDefaultPool();
    auto key = BinaryArray::FromIntArray({1, 2, 3, 5}, pool.get());
    auto value = BinaryArray::FromLongArray({100ll, 200ll, 300ll, 500ll}, pool.get());

    auto binary_map = BinaryMap::ValueOf(key, value, pool.get());

    ASSERT_EQ(binary_map->Size(), 4);
    auto key_in_map = std::dynamic_pointer_cast<BinaryArray>(binary_map->KeyArray());
    auto value_in_map = std::dynamic_pointer_cast<BinaryArray>(binary_map->ValueArray());
    ASSERT_EQ(key_in_map->HashCode(), key.HashCode());
    ASSERT_EQ(value_in_map->HashCode(), value.HashCode());
    ASSERT_EQ(key_in_map->ToIntArray().value(), std::vector<int32_t>({1, 2, 3, 5}));
    ASSERT_EQ(value_in_map->ToLongArray().value(),
              std::vector<int64_t>({100ll, 200ll, 300ll, 500ll}));
}
}  // namespace paimon::test
