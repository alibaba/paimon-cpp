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
#include "paimon/common/data/blob_view_struct.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BlobViewStructTest : public testing::Test {
 public:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    Identifier identifier_ = Identifier("test_db", "test_table");
    BlobViewStruct view_struct_ = BlobViewStruct(identifier_, /*field_id=*/7, /*row_id=*/1024);
};

TEST_F(BlobViewStructTest, TestConstructorAndGetters) {
    ASSERT_EQ(view_struct_.GetIdentifier().GetDatabaseName(), "test_db");
    ASSERT_EQ(view_struct_.GetIdentifier().GetTableName(), "test_table");
    ASSERT_EQ(view_struct_.FieldId(), 7);
    ASSERT_EQ(view_struct_.RowId(), 1024);
}

TEST_F(BlobViewStructTest, TestSerializeDeserializeRoundTrip) {
    auto serialized = view_struct_.Serialize(pool_);
    ASSERT_NE(serialized, nullptr);
    ASSERT_GT(serialized->size(), 0u);

    ASSERT_OK_AND_ASSIGN(auto restored,
                         BlobViewStruct::Deserialize(serialized->data(), serialized->size()));
    ASSERT_EQ(restored->GetIdentifier().GetDatabaseName(), "test_db");
    ASSERT_EQ(restored->GetIdentifier().GetTableName(), "test_table");
    ASSERT_EQ(restored->FieldId(), 7);
    ASSERT_EQ(restored->RowId(), 1024);
}

TEST_F(BlobViewStructTest, TestDeserializeWithInvalidVersion) {
    auto serialized = view_struct_.Serialize(pool_);
    (*serialized)[0] = '\x02';  // invalid version (current is 1).
    ASSERT_NOK_WITH_MSG(BlobViewStruct::Deserialize(serialized->data(), serialized->size()),
                        "Expecting BlobViewStruct version to be 1, but found 2");
}

TEST_F(BlobViewStructTest, TestDeserializeWithInvalidMagic) {
    auto serialized = view_struct_.Serialize(pool_);
    (*serialized)[1] = '\x00';
    ASSERT_NOK_WITH_MSG(BlobViewStruct::Deserialize(serialized->data(), serialized->size()),
                        "missing magic header");
}

TEST_F(BlobViewStructTest, TestToString) {
    std::string debug_str = view_struct_.ToString();
    ASSERT_EQ(debug_str, "BlobViewStruct{identifier=test_db.test_table, fieldId=7, rowId=1024}");
}

TEST_F(BlobViewStructTest, TestEqual) {
    {
        // test equal itself
        ASSERT_EQ(view_struct_, view_struct_);
    }
    {
        // test equal
        BlobViewStruct other_view_struct =
            BlobViewStruct(identifier_, /*field_id=*/7, /*row_id=*/1024);
        ASSERT_EQ(view_struct_, other_view_struct);
    }
    {
        // test wrong identifier
        Identifier wrong_identifier = Identifier("db", "table");
        BlobViewStruct wrong_view_struct =
            BlobViewStruct(wrong_identifier, /*field_id=*/7, /*row_id=*/1024);
        ASSERT_NE(view_struct_, wrong_view_struct);
    }
    {
        // test wrong field_id
        BlobViewStruct wrong_view_struct =
            BlobViewStruct(identifier_, /*field_id=*/8, /*row_id=*/1024);
        ASSERT_NE(view_struct_, wrong_view_struct);
    }
    {
        // test wrong row_id
        BlobViewStruct wrong_view_struct =
            BlobViewStruct(identifier_, /*field_id=*/7, /*row_id=*/1000);
        ASSERT_NE(view_struct_, wrong_view_struct);
    }
}

TEST_F(BlobViewStructTest, TestIsBlobViewStructValid) {
    auto serialized = view_struct_.Serialize(pool_);
    ASSERT_OK_AND_ASSIGN(bool result,
                         BlobViewStruct::IsBlobViewStruct(serialized->data(), serialized->size()));
    ASSERT_TRUE(result);
}

TEST_F(BlobViewStructTest, TestIsBlobViewStructWithTooShortBuffer) {
    // Buffer shorter than 9 bytes should return false
    std::vector<char> short_buffer = {0x02, 0x43, 0x53, 0x45, 0x44, 0x42, 0x4F, 0x4C};
    ASSERT_OK_AND_ASSIGN(
        bool result, BlobViewStruct::IsBlobViewStruct(short_buffer.data(), short_buffer.size()));
    ASSERT_FALSE(result);

    // Empty buffer
    ASSERT_OK_AND_ASSIGN(bool empty_result, BlobViewStruct::IsBlobViewStruct(nullptr, 0));
    ASSERT_FALSE(empty_result);
}

TEST_F(BlobViewStructTest, TestIsBlobViewStructWithFutureVersion) {
    // Version > CURRENT_VERSION should return false (not an error)
    auto serialized = view_struct_.Serialize(pool_);
    (*serialized)[0] = '\x02';  // set version to 2 (> CURRENT_VERSION)
    ASSERT_OK_AND_ASSIGN(bool result,
                         BlobViewStruct::IsBlobViewStruct(serialized->data(), serialized->size()));
    ASSERT_FALSE(result);
}

TEST_F(BlobViewStructTest, TestIsBlobViewStructWithWrongMagic) {
    // Wrong magic number should return false
    auto serialized = view_struct_.Serialize(pool_);
    // Corrupt the magic bytes (bytes 1-8)
    (*serialized)[1] = '\x00';
    (*serialized)[2] = '\x00';
    ASSERT_OK_AND_ASSIGN(bool result,
                         BlobViewStruct::IsBlobViewStruct(serialized->data(), serialized->size()));
    ASSERT_FALSE(result);
}

TEST_F(BlobViewStructTest, TestIsBlobViewStructWithRandomData) {
    // Random data that doesn't match format
    std::vector<char> random_data = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    ASSERT_OK_AND_ASSIGN(bool result,
                         BlobViewStruct::IsBlobViewStruct(random_data.data(), random_data.size()));
    ASSERT_FALSE(result);
}
}  // namespace paimon::test
