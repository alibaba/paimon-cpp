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

#include "paimon/common/file_index/rangebitmap/bit_slice_index_bitmap.h"

#include <algorithm>
#include <map>
#include <optional>
#include <random>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {

class BitSliceIndexBitmapTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void TearDown() override {
        pool_.reset();
    }

    // Helper to build BitSliceIndexBitmap: serialize data then deserialize it
    Result<std::unique_ptr<BitSliceIndexBitmap>> BuildBitmap(
        const std::map<int32_t, int32_t>& data, int32_t min_val, int32_t max_val,
        PAIMON_UNIQUE_PTR<Bytes>* serialized) const {
        PAIMON_ASSIGN_OR_RAISE(auto appender,
                               BitSliceIndexBitmap::Appender::Create(min_val, max_val, pool_));
        for (const auto& [key, value] : data) {
            PAIMON_RETURN_NOT_OK(appender->Append(key, value));
        }
        PAIMON_ASSIGN_OR_RAISE(*serialized, appender->Serialize());
        auto input_stream =
            std::make_shared<ByteArrayInputStream>((*serialized)->data(), (*serialized)->size());
        return BitSliceIndexBitmap::Create(input_stream, 0, pool_);
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BitSliceIndexBitmapTest, TestIsNotNull) {
    std::map<int32_t, int32_t> data = {{0, 0}, {1, 2}, {5, 7}, {10, 3}};
    PAIMON_UNIQUE_PTR<Bytes> serialized;
    ASSERT_OK_AND_ASSIGN(auto bitmap, BuildBitmap(data, 0, 10, &serialized));
    // IsNotNull without found_set should return all rows with values
    ASSERT_OK_AND_ASSIGN(auto result, bitmap->IsNotNull({}));
    EXPECT_EQ(result.ToString(), "{0,1,5,10}");
    // IsNotNull with found_set should filter elements not in the found set
    RoaringBitmap32 found_set = RoaringBitmap32::From({0, 1, 2, 3});
    ASSERT_OK_AND_ASSIGN(result, bitmap->IsNotNull(found_set));
    EXPECT_EQ(result.ToString(), "{0,1}");
    ASSERT_OK_AND_ASSIGN(result, bitmap->Gt(0));
    EXPECT_EQ(result.ToString(), "{1,5,10}");
    ASSERT_OK_AND_ASSIGN(result, bitmap->Gte(-1));
    ASSERT_EQ(result.ToString(), "{0,1,5,10}");
}

TEST_F(BitSliceIndexBitmapTest, TestGetExistenceBitmap) {
    std::map<int32_t, int32_t> data = {{0, 1}, {1, 2}, {2, 3}};
    PAIMON_UNIQUE_PTR<Bytes> serialized;
    ASSERT_OK_AND_ASSIGN(auto bitmap, BuildBitmap(data, 0, 10, &serialized));
    ASSERT_OK_AND_ASSIGN(const auto* ebm, bitmap->GetExistenceBitmap());
    EXPECT_NE(ebm, nullptr);
    // EBM should contain all row ids with values
    EXPECT_EQ(ebm->ToString(), "{0,1,2}");
}

// Test empty data
TEST_F(BitSliceIndexBitmapTest, TestEmptyData) {
    std::map<int32_t, int32_t> data = {};
    PAIMON_UNIQUE_PTR<Bytes> serialized;
    ASSERT_OK_AND_ASSIGN(auto bitmap, BuildBitmap(data, 0, 0, &serialized));
    ASSERT_OK_AND_ASSIGN(auto result, bitmap->Eq(5));
    EXPECT_EQ(result.ToString(), "{}");
    ASSERT_OK_AND_ASSIGN(result, bitmap->Gt(-1));
    EXPECT_EQ(result.ToString(), "{}");
    ASSERT_OK_AND_ASSIGN(result, bitmap->Gte(5));
    EXPECT_EQ(result.ToString(), "{}");
}

TEST_F(BitSliceIndexBitmapTest, TestOnlyZero) {
    std::map<int32_t, int32_t> data = {{0, 0}};
    PAIMON_UNIQUE_PTR<Bytes> serialized;
    ASSERT_OK_AND_ASSIGN(auto bitmap, BuildBitmap(data, 0, 0, &serialized));
    ASSERT_OK_AND_ASSIGN(auto result, bitmap->Eq(0));
    EXPECT_EQ(result.ToString(), "{0}");
}

// Test Int MAX
TEST_F(BitSliceIndexBitmapTest, TestIntMax) {
    int32_t max = std::numeric_limits<int32_t>::max();
    std::map<int32_t, int32_t> data = {{1, max}};
    PAIMON_UNIQUE_PTR<Bytes> serialized;
    ASSERT_OK_AND_ASSIGN(auto bitmap, BuildBitmap(data, 0, 2147483647, &serialized));
    ASSERT_OK_AND_ASSIGN(auto result, bitmap->Gt(max));
    EXPECT_EQ(result.ToString(), "{}");
    ASSERT_OK_AND_ASSIGN(result, bitmap->Eq(max));
    EXPECT_EQ(result.ToString(), "{1}");
    ASSERT_OK_AND_ASSIGN(result, bitmap->Gte(max));
    EXPECT_EQ(result.ToString(), "{1}");
}

// Test invalid append (value out of range)
TEST_F(BitSliceIndexBitmapTest, TestInvalidAppender) {
    {
        ASSERT_OK_AND_ASSIGN(auto appender, BitSliceIndexBitmap::Appender::Create(0, 10, pool_));
        // Valid append
        ASSERT_OK(appender->Append(0, 5));
        // Invalid append: value out of range
        ASSERT_NOK_WITH_MSG(appender->Append(1, 15), "Invalid: value: 15 not in range [0, 10]");
        // Invalid append: negative key
        ASSERT_NOK_WITH_MSG(appender->Append(-1, 5), "Invalid: key: -1 cannot be negative");
        // Invalid append: negative value
        ASSERT_NOK_WITH_MSG(appender->Append(1, -3), "Invalid: value: -3 cannot be negative");
    }
    {
        ASSERT_NOK_WITH_MSG(BitSliceIndexBitmap::Appender::Create(-1, 19, pool_),
                            "Invalid: min -1 cannot be negative");
        ASSERT_NOK_WITH_MSG(BitSliceIndexBitmap::Appender::Create(10, 5, pool_),
                            "Invalid: min 10 > max 5");
    }
}

// Uses random data and verifies results against expected values computed from source data
TEST_F(BitSliceIndexBitmapTest, TestRandomData) {
    static constexpr int32_t kCardinality = 100;
    static constexpr int32_t kRowCount = 10000;
    static constexpr int32_t kQueryCount = 10;  // Number of random queries per type

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> value_dist(0, kCardinality - 1);
    std::uniform_int_distribution<> bool_dist(0, 1);

    // Generate random data: each row has 50% chance to be null, 50% chance random value
    std::vector<std::optional<int32_t>> pairs;
    pairs.reserve(kRowCount);
    ASSERT_OK_AND_ASSIGN(auto appender,
                         BitSliceIndexBitmap::Appender::Create(0, kCardinality, pool_));
    for (int32_t i = 0; i < kRowCount; i++) {
        if (bool_dist(gen)) {
            pairs.push_back(std::nullopt);
        } else {
            int32_t value = value_dist(gen);
            pairs.push_back(value);
            ASSERT_OK(appender->Append(i, value));
        }
    }
    // Serialize and deserialize
    ASSERT_OK_AND_ASSIGN(auto serialized, appender->Serialize());
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(serialized->data(), serialized->size());
    ASSERT_OK_AND_ASSIGN(auto bitmap, BitSliceIndexBitmap::Create(input_stream, 0, pool_));
    // Test eq
    for (int i = 0; i < kQueryCount; i++) {
        int32_t code = value_dist(gen);
        // Compute expected result from source data
        RoaringBitmap32 expected;
        for (int32_t row = 0; row < kRowCount; row++) {
            if (pairs[row].has_value() && pairs[row].value() == code) {
                expected.Add(row);
            }
        }
        ASSERT_OK_AND_ASSIGN(auto result, bitmap->Eq(code));
        EXPECT_EQ(result.ToString(), expected.ToString());
    }
    // Test gt
    for (int i = 0; i < kQueryCount; i++) {
        int32_t code = value_dist(gen);
        // Compute expected result from source data
        RoaringBitmap32 expected;
        for (int32_t row = 0; row < kRowCount; row++) {
            if (pairs[row].has_value() && pairs[row].value() > code) {
                expected.Add(row);
            }
        }
        ASSERT_OK_AND_ASSIGN(auto result, bitmap->Gt(code));
        EXPECT_EQ(result.ToString(), expected.ToString());
    }
    // Test gte
    for (int i = 0; i < kQueryCount; i++) {
        int32_t code = value_dist(gen);
        // Compute expected result from source data
        RoaringBitmap32 expected;
        for (int32_t row = 0; row < kRowCount; row++) {
            if (pairs[row].has_value() && pairs[row].value() >= code) {
                expected.Add(row);
            }
        }
        ASSERT_OK_AND_ASSIGN(auto result, bitmap->Gte(code));
        EXPECT_EQ(result.ToString(), expected.ToString());
    }
    // Test isNotNull
    {
        RoaringBitmap32 expected;
        for (int32_t row = 0; row < kRowCount; row++) {
            if (pairs[row].has_value()) {
                expected.Add(row);
            }
        }
        ASSERT_OK_AND_ASSIGN(auto result, bitmap->IsNotNull({}));
        EXPECT_EQ(result.ToString(), expected.ToString());
    }
}

}  // namespace paimon::test
