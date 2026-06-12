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

#include "paimon/common/data/shredding/map_shared_shredding_field_dict.h"

#include "gtest/gtest.h"

namespace paimon {

TEST(MapSharedShreddingFieldDictTest, AssignMonotonicallyIncreasingIds) {
    MapSharedShreddingFieldDict dict;
    ASSERT_EQ(0, dict.GetOrAssign("cpu_usage"));
    ASSERT_EQ(1, dict.GetOrAssign("mem_load"));
    ASSERT_EQ(2, dict.GetOrAssign("disk_io"));
    ASSERT_EQ(3, dict.Size());
}

TEST(MapSharedShreddingFieldDictTest, LookupReturnsExistingId) {
    MapSharedShreddingFieldDict dict;
    int32_t id = dict.GetOrAssign("alpha");
    ASSERT_EQ(id, dict.GetOrAssign("alpha"));
    ASSERT_EQ(1, dict.Size());
}

TEST(MapSharedShreddingFieldDictTest, GetNameToId) {
    MapSharedShreddingFieldDict dict;
    dict.GetOrAssign("b_field");
    dict.GetOrAssign("a_field");

    auto name_to_id = dict.GetNameToId();
    ASSERT_EQ(2u, name_to_id.size());
    ASSERT_EQ(1, name_to_id.at("a_field"));
    ASSERT_EQ(0, name_to_id.at("b_field"));
}

TEST(MapSharedShreddingFieldDictTest, ResetClearsState) {
    MapSharedShreddingFieldDict dict;
    dict.GetOrAssign("foo");
    dict.GetOrAssign("bar");
    ASSERT_EQ(2, dict.Size());

    dict.Reset();
    ASSERT_EQ(0, dict.Size());
    ASSERT_TRUE(dict.GetNameToId().empty());

    // Ids restart from 0 after reset
    ASSERT_EQ(0, dict.GetOrAssign("baz"));
    ASSERT_EQ(1, dict.GetOrAssign("foo"));
}

TEST(MapSharedShreddingFieldDictTest, EmptyDict) {
    MapSharedShreddingFieldDict dict;
    ASSERT_EQ(0, dict.Size());
    ASSERT_TRUE(dict.GetNameToId().empty());
}

}  // namespace paimon
