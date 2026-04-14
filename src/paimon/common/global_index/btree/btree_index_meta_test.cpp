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

#include "paimon/common/global_index/btree/btree_index_meta.h"

#include <gtest/gtest.h>

#include "paimon/memory/memory_pool.h"

namespace paimon::test {

class BTreeIndexMetaTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BTreeIndexMetaTest, SerializeDeserializeNormalKeys) {
    // Create a BTreeIndexMeta with normal keys
    // Use std::make_shared<Bytes> to create shared_ptr with proper memory management
    // Bytes constructor uses pool->Malloc() for internal data, and destructor uses pool->Free()
    auto first_key = std::make_shared<Bytes>("first_key_data", pool_.get());
    auto last_key = std::make_shared<Bytes>("last_key_data", pool_.get());
    auto meta = std::make_shared<BTreeIndexMeta>(first_key, last_key, true);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_NE(serialized, nullptr);
    ASSERT_GT(serialized->size(), 0u);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_NE(deserialized, nullptr);

    // Verify first_key
    auto deserialized_first = deserialized->FirstKey();
    ASSERT_NE(deserialized_first, nullptr);
    EXPECT_EQ(std::string(deserialized_first->data(), deserialized_first->size()),
              "first_key_data");

    // Verify last_key
    auto deserialized_last = deserialized->LastKey();
    ASSERT_NE(deserialized_last, nullptr);
    EXPECT_EQ(std::string(deserialized_last->data(), deserialized_last->size()), "last_key_data");

    // Verify has_nulls
    ASSERT_TRUE(deserialized->HasNulls());
}

TEST_F(BTreeIndexMetaTest, SerializeDeserializeEmptyKeys) {
    // Create a BTreeIndexMeta with empty keys (OnlyNulls case)
    auto meta = std::make_shared<BTreeIndexMeta>(nullptr, nullptr, true);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_NE(serialized, nullptr);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_NE(deserialized, nullptr);

    // Verify keys are null
    ASSERT_EQ(deserialized->FirstKey(), nullptr);
    ASSERT_EQ(deserialized->LastKey(), nullptr);

    // Verify has_nulls
    ASSERT_TRUE(deserialized->HasNulls());

    // Verify OnlyNulls
    ASSERT_TRUE(deserialized->OnlyNulls());
}

TEST_F(BTreeIndexMetaTest, HasNullsAndOnlyNulls) {
    // Case 1: Has nulls with keys
    auto meta1 =
        std::make_shared<BTreeIndexMeta>(std::make_shared<Bytes>("key", pool_.get()),
                                         std::make_shared<Bytes>("key", pool_.get()), true);
    EXPECT_TRUE(meta1->HasNulls());
    EXPECT_FALSE(meta1->OnlyNulls());

    // Case 2: No nulls with keys
    auto meta2 =
        std::make_shared<BTreeIndexMeta>(std::make_shared<Bytes>("key", pool_.get()),
                                         std::make_shared<Bytes>("key", pool_.get()), false);
    EXPECT_FALSE(meta2->HasNulls());
    EXPECT_FALSE(meta2->OnlyNulls());

    // Case 3: Only nulls (no keys)
    auto meta3 = std::make_shared<BTreeIndexMeta>(nullptr, nullptr, true);
    EXPECT_TRUE(meta3->HasNulls());
    EXPECT_TRUE(meta3->OnlyNulls());

    // Case 4: No nulls and no keys (edge case)
    auto meta4 = std::make_shared<BTreeIndexMeta>(nullptr, nullptr, false);
    EXPECT_FALSE(meta4->HasNulls());
    EXPECT_TRUE(meta4->OnlyNulls());
}

TEST_F(BTreeIndexMetaTest, SerializeDeserializeNoNulls) {
    // Create a BTreeIndexMeta without nulls
    auto first_key = std::make_shared<Bytes>("abc", pool_.get());
    auto last_key = std::make_shared<Bytes>("xyz", pool_.get());
    auto meta = std::make_shared<BTreeIndexMeta>(first_key, last_key, false);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_NE(serialized, nullptr);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_NE(deserialized, nullptr);

    // Verify has_nulls is false
    EXPECT_FALSE(deserialized->HasNulls());
}

TEST_F(BTreeIndexMetaTest, SerializeDeserializeWithOnlyFirstKey) {
    // Create a BTreeIndexMeta with only first_key (edge case)
    auto first_key = std::make_shared<Bytes>("first", pool_.get());
    auto meta = std::make_shared<BTreeIndexMeta>(first_key, nullptr, false);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_NE(serialized, nullptr);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_NE(deserialized, nullptr);

    // Verify first_key
    auto deserialized_first = deserialized->FirstKey();
    ASSERT_NE(deserialized_first, nullptr);
    EXPECT_EQ(std::string(deserialized_first->data(), deserialized_first->size()), "first");

    // Verify last_key is null
    EXPECT_EQ(deserialized->LastKey(), nullptr);
}

TEST_F(BTreeIndexMetaTest, SerializeDeserializeWithOnlyLastKey) {
    // Create a BTreeIndexMeta with only last_key (edge case)
    auto last_key = std::make_shared<Bytes>("last", pool_.get());
    auto meta = std::make_shared<BTreeIndexMeta>(nullptr, last_key, false);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_NE(serialized, nullptr);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_NE(deserialized, nullptr);

    // Verify first_key is null
    EXPECT_EQ(deserialized->FirstKey(), nullptr);

    // Verify last_key
    auto deserialized_last = deserialized->LastKey();
    ASSERT_NE(deserialized_last, nullptr);
    EXPECT_EQ(std::string(deserialized_last->data(), deserialized_last->size()), "last");
}

TEST_F(BTreeIndexMetaTest, SerializeDeserializeBinaryKeys) {
    // Create a BTreeIndexMeta with binary keys containing null bytes
    std::string binary_first = std::string("key\0with\0nulls", 14);
    std::string binary_last = std::string("last\0key", 8);
    auto first_key = std::make_shared<Bytes>(binary_first, pool_.get());
    auto last_key = std::make_shared<Bytes>(binary_last, pool_.get());
    auto meta = std::make_shared<BTreeIndexMeta>(first_key, last_key, true);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_NE(serialized, nullptr);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_NE(deserialized, nullptr);

    // Verify first_key
    auto deserialized_first = deserialized->FirstKey();
    ASSERT_NE(deserialized_first, nullptr);
    EXPECT_EQ(std::string(deserialized_first->data(), deserialized_first->size()), binary_first);

    // Verify last_key
    auto deserialized_last = deserialized->LastKey();
    ASSERT_NE(deserialized_last, nullptr);
    EXPECT_EQ(std::string(deserialized_last->data(), deserialized_last->size()), binary_last);
}

}  // namespace paimon::test
