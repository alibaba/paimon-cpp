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

#include "gtest/gtest.h"
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
    auto first_key = std::make_shared<Bytes>("first_key_data", pool_.get());
    auto last_key = std::make_shared<Bytes>("last_key_data", pool_.get());
    auto meta = std::make_shared<BTreeIndexMeta>(first_key, last_key, true);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_TRUE(serialized);
    ASSERT_GT(serialized->size(), 0u);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_TRUE(deserialized);

    // Verify first_key
    auto deserialized_first = deserialized->FirstKey();
    ASSERT_TRUE(deserialized_first);
    ASSERT_EQ(std::string(deserialized_first->data(), deserialized_first->size()),
              "first_key_data");

    // Verify last_key
    auto deserialized_last = deserialized->LastKey();
    ASSERT_TRUE(deserialized_last);
    ASSERT_EQ(std::string(deserialized_last->data(), deserialized_last->size()), "last_key_data");

    // Verify has_nulls
    ASSERT_TRUE(deserialized->HasNulls());
}

TEST_F(BTreeIndexMetaTest, SerializeDeserializeEmptyKeys) {
    // Create a BTreeIndexMeta with empty keys (OnlyNulls case)
    auto meta = std::make_shared<BTreeIndexMeta>(nullptr, nullptr, true);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_TRUE(serialized);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_TRUE(deserialized);

    // Verify keys are null
    ASSERT_FALSE(deserialized->FirstKey());
    ASSERT_FALSE(deserialized->LastKey());

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
    ASSERT_TRUE(meta1->HasNulls());
    ASSERT_FALSE(meta1->OnlyNulls());

    // Case 2: No nulls with keys
    auto meta2 =
        std::make_shared<BTreeIndexMeta>(std::make_shared<Bytes>("key", pool_.get()),
                                         std::make_shared<Bytes>("key", pool_.get()), false);
    ASSERT_FALSE(meta2->HasNulls());
    ASSERT_FALSE(meta2->OnlyNulls());

    // Case 3: Only nulls (no keys)
    auto meta3 = std::make_shared<BTreeIndexMeta>(nullptr, nullptr, true);
    ASSERT_TRUE(meta3->HasNulls());
    ASSERT_TRUE(meta3->OnlyNulls());

    // Case 4: No nulls and no keys (edge case)
    auto meta4 = std::make_shared<BTreeIndexMeta>(nullptr, nullptr, false);
    ASSERT_FALSE(meta4->HasNulls());
    ASSERT_TRUE(meta4->OnlyNulls());
}

TEST_F(BTreeIndexMetaTest, SerializeDeserializeNoNulls) {
    // Create a BTreeIndexMeta without nulls
    auto first_key = std::make_shared<Bytes>("abc", pool_.get());
    auto last_key = std::make_shared<Bytes>("xyz", pool_.get());
    auto meta = std::make_shared<BTreeIndexMeta>(first_key, last_key, false);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_TRUE(serialized);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_TRUE(deserialized);

    // Verify has_nulls is false
    ASSERT_FALSE(deserialized->HasNulls());
}

TEST_F(BTreeIndexMetaTest, SerializeDeserializeWithOnlyFirstKey) {
    // Create a BTreeIndexMeta with only first_key (edge case)
    auto first_key = std::make_shared<Bytes>("first", pool_.get());
    auto meta = std::make_shared<BTreeIndexMeta>(first_key, nullptr, false);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_TRUE(serialized);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_TRUE(deserialized);

    // Verify first_key
    auto deserialized_first = deserialized->FirstKey();
    ASSERT_TRUE(deserialized_first);
    ASSERT_EQ(std::string(deserialized_first->data(), deserialized_first->size()), "first");

    // Verify last_key is null
    ASSERT_FALSE(deserialized->LastKey());
}

TEST_F(BTreeIndexMetaTest, SerializeDeserializeWithOnlyLastKey) {
    // Create a BTreeIndexMeta with only last_key (edge case)
    auto last_key = std::make_shared<Bytes>("last", pool_.get());
    auto meta = std::make_shared<BTreeIndexMeta>(nullptr, last_key, false);

    // Serialize
    auto serialized = meta->Serialize(pool_.get());
    ASSERT_TRUE(serialized);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_TRUE(deserialized);

    // Verify first_key is null
    ASSERT_FALSE(deserialized->FirstKey());

    // Verify last_key
    auto deserialized_last = deserialized->LastKey();
    ASSERT_TRUE(deserialized_last);
    ASSERT_EQ(std::string(deserialized_last->data(), deserialized_last->size()), "last");
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
    ASSERT_TRUE(serialized);

    // Deserialize
    auto deserialized = BTreeIndexMeta::Deserialize(serialized, pool_.get());
    ASSERT_TRUE(deserialized);

    // Verify first_key
    auto deserialized_first = deserialized->FirstKey();
    ASSERT_TRUE(deserialized_first);
    ASSERT_EQ(std::string(deserialized_first->data(), deserialized_first->size()), binary_first);

    // Verify last_key
    auto deserialized_last = deserialized->LastKey();
    ASSERT_TRUE(deserialized_last);
    ASSERT_EQ(std::string(deserialized_last->data(), deserialized_last->size()), binary_last);
}

}  // namespace paimon::test
