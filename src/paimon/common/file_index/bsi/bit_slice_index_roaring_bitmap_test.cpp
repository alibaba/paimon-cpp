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

#include "paimon/common/file_index/bsi/bit_slice_index_roaring_bitmap.h"

#include <algorithm>
#include <cstdlib>
#include <map>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class BitSliceIndexRoaringBitmapTest : public ::testing::Test {
 public:
    void SetUp() override {
        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        srand(seed);
        pool_ = GetDefaultPool();
        int64_t min = 0, max = 0;
        // force add min 1, max VALUE_BOUND
        std::vector<int64_t> bounds = {1, VALUE_BOUND};
        for (int32_t i = 0; i < static_cast<int32_t>(bounds.size()); i++) {
            int64_t next = bounds[i];
            min = std::min(min == 0 ? next : min, next);
            max = std::max(max == 0 ? next : max, next);
            expected_map_[next].push_back(i);
        }
        // add random value
        for (int32_t i = bounds.size(); i < NUM_OF_ROWS; i++) {
            if (i % 5 == 0) {
                // for null
                continue;
            }
            int64_t next = GenerateNextValue();
            min = std::min(min == 0 ? next : min, next);
            max = std::max(max == 0 ? next : max, next);
            expected_map_[next].push_back(i);
        }
        ASSERT_OK_AND_ASSIGN(auto appender, BitSliceIndexRoaringBitmap::Appender::Create(min, max));
        for (const auto& [value, rids] : expected_map_) {
            for (const auto& rid : rids) {
                ASSERT_OK(appender->Append(rid, value));
            }
        }
        bsi_ = appender->Build();
    }
    void TearDown() override {
        pool_.reset();
        bsi_.reset();
        expected_map_.clear();
    }

    int64_t GenerateNextValue() const {
        // return a value in the range [1, VALUE_BOUND]
        return rand() % VALUE_BOUND + 1;
    }

    int64_t GenerateNextValueExceptMinMax() const {
        // return a value in the range (1, VALUE_BOUND)
        return 2 + paimon::test::RandomNumber(0, (VALUE_BOUND - 2) - 1);
    }

    static constexpr int32_t NUM_OF_ROWS = 100000;
    static constexpr int32_t VALUE_BOUND = 1000;
    static constexpr int32_t VALUE_LT_MIN = 0;
    static constexpr int32_t VALUE_GT_MAX = VALUE_BOUND + 100;

 private:
    std::shared_ptr<BitSliceIndexRoaringBitmap> bsi_;
    // value : [rid]
    std::map<int64_t, std::vector<int32_t>> expected_map_;
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BitSliceIndexRoaringBitmapTest, TestSerializeAndDeserialize) {
    ASSERT_OK_AND_ASSIGN(auto appender,
                         BitSliceIndexRoaringBitmap::Appender::Create(/*min=*/0, /*max=*/10));
    ASSERT_FALSE(appender->IsNotEmpty());
    ASSERT_OK(appender->Append(0, 0));
    ASSERT_OK(appender->Append(1, 1));
    ASSERT_OK(appender->Append(2, 2));
    ASSERT_OK(appender->Append(10, 6));

    // test invalid append
    ASSERT_NOK_WITH_MSG(appender->Append(11, 20),
                        "value 20 is too large for append to BitSliceIndexRoaringBitmap");
    ASSERT_NOK_WITH_MSG(appender->Append(10, 6),
                        "rid 10 is already exists for append to BitSliceIndexRoaringBitmap");

    ASSERT_TRUE(appender->IsNotEmpty());
    auto bsi = appender->Build();

    auto serialized_bytes = appender->Serialize(pool_);
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(serialized_bytes->data(), serialized_bytes->size());
    ASSERT_OK_AND_ASSIGN(auto de_bsi, BitSliceIndexRoaringBitmap::Create(input_stream));
    ASSERT_EQ(*bsi, *de_bsi);
}

TEST_F(BitSliceIndexRoaringBitmapTest, TestEqual) {
    // test predicate in the value bound
    for (int32_t i = 0; i < 10; i++) {
        int64_t literal = GenerateNextValue();
        ASSERT_OK_AND_ASSIGN(auto result, bsi_->Equal(literal));
        ASSERT_EQ(result, RoaringBitmap32::From(expected_map_[literal]));
    }
    // test predicate out of the value bound
    ASSERT_TRUE(bsi_->Equal(VALUE_LT_MIN).value().IsEmpty());
    ASSERT_TRUE(bsi_->Equal(VALUE_GT_MAX).value().IsEmpty());
}

TEST_F(BitSliceIndexRoaringBitmapTest, TestNotEqualInONeilCompare) {
    // test predicate in the value bound
    for (int32_t i = 0; i < 10; i++) {
        int64_t literal = GenerateNextValue();
        ASSERT_OK_AND_ASSIGN(auto result,
                             bsi_->ONeilCompare(Function::Type::NOT_EQUAL, literal - bsi_->min_));
        ASSERT_EQ(result, RoaringBitmap32::AndNot(bsi_->ebm_,
                                                  RoaringBitmap32::From(expected_map_[literal])));
    }
    ASSERT_NOK_WITH_MSG(bsi_->ONeilCompare(Function::Type::IN, bsi_->min_),
                        "Invalid Function::Type in ONeilCompare of BitSliceIndex, only support "
                        "EQUAL/NOT_EQUAL/GREATER_OR_EQUAL/GREATER_THAN/LESS_OR_EQUAL/LESS_THAN");
}

TEST_F(BitSliceIndexRoaringBitmapTest, TestLT) {
    // test predicate in the value bound
    for (int32_t i = 0; i < 10; i++) {
        int64_t literal = GenerateNextValue();
        RoaringBitmap32 expected;
        for (const auto& [value, rids] : expected_map_) {
            if (value < literal) {
                expected |= RoaringBitmap32::From(rids);
            }
        }
        ASSERT_OK_AND_ASSIGN(auto result, bsi_->LessThan(literal));
        ASSERT_EQ(result, expected);
    }
    ASSERT_TRUE(bsi_->LessThan(VALUE_LT_MIN).value().IsEmpty());
    ASSERT_EQ(bsi_->LessThan(VALUE_GT_MAX).value(), bsi_->IsNotNull());
}

TEST_F(BitSliceIndexRoaringBitmapTest, TestLTE) {
    // test predicate in the value bound
    for (int32_t i = 0; i < 10; i++) {
        int64_t literal = GenerateNextValue();
        RoaringBitmap32 expected;
        for (const auto& [value, rids] : expected_map_) {
            if (value <= literal) {
                expected |= RoaringBitmap32::From(rids);
            }
        }
        ASSERT_OK_AND_ASSIGN(auto result, bsi_->LessOrEqual(literal));
        ASSERT_EQ(result, expected);
    }
    ASSERT_TRUE(bsi_->LessOrEqual(VALUE_LT_MIN).value().IsEmpty());
    ASSERT_EQ(bsi_->LessOrEqual(VALUE_GT_MAX).value(), bsi_->IsNotNull());
}

TEST_F(BitSliceIndexRoaringBitmapTest, TestGT) {
    // test predicate in the value bound
    for (int32_t i = 0; i < 10; i++) {
        int64_t literal = GenerateNextValue();
        RoaringBitmap32 expected;
        for (const auto& [value, rids] : expected_map_) {
            if (value > literal) {
                expected |= RoaringBitmap32::From(rids);
            }
        }
        ASSERT_OK_AND_ASSIGN(auto result, bsi_->GreaterThan(literal));
        ASSERT_EQ(result, expected);
    }
    ASSERT_TRUE(bsi_->GreaterThan(VALUE_GT_MAX).value().IsEmpty());
    ASSERT_EQ(bsi_->GreaterThan(VALUE_LT_MIN).value(), bsi_->IsNotNull());
}

TEST_F(BitSliceIndexRoaringBitmapTest, TestGTE) {
    // test predicate in the value bound
    for (int32_t i = 0; i < 10; i++) {
        int64_t literal = GenerateNextValue();
        RoaringBitmap32 expected;
        for (const auto& [value, rids] : expected_map_) {
            if (value >= literal) {
                expected |= RoaringBitmap32::From(rids);
            }
        }
        ASSERT_OK_AND_ASSIGN(auto result, bsi_->GreaterOrEqual(literal));
        ASSERT_EQ(result, expected);
    }
    ASSERT_TRUE(bsi_->GreaterOrEqual(VALUE_GT_MAX).value().IsEmpty());
    ASSERT_EQ(bsi_->GreaterOrEqual(VALUE_LT_MIN).value(), bsi_->IsNotNull());
}

TEST_F(BitSliceIndexRoaringBitmapTest, TestIsNotNull) {
    RoaringBitmap32 expected;
    for (const auto& [value, rids] : expected_map_) {
        expected |= RoaringBitmap32::From(rids);
    }
    ASSERT_EQ(bsi_->IsNotNull(), expected);
}

TEST_F(BitSliceIndexRoaringBitmapTest, TestCompareUsingMinMax) {
    // a predicate in the value bound
    int64_t literal = GenerateNextValue();
    int64_t literal_except_min_max = GenerateNextValueExceptMinMax();
    std::optional<RoaringBitmap32> empty = RoaringBitmap32();
    std::optional<RoaringBitmap32> not_null = bsi_->IsNotNull();
    std::optional<RoaringBitmap32> in_bound = std::nullopt;

    // test eq & neq
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::EQUAL, literal).value(), in_bound);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::EQUAL, VALUE_LT_MIN).value(), empty);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::EQUAL, VALUE_GT_MAX).value(), empty);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::NOT_EQUAL, literal).value(), in_bound);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::NOT_EQUAL, VALUE_LT_MIN).value(), not_null);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::NOT_EQUAL, VALUE_GT_MAX).value(), not_null);

    // test lt & lte
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::LESS_THAN, literal_except_min_max).value(),
              in_bound);
    ASSERT_EQ(
        bsi_->CompareUsingMinMax(Function::Type::LESS_OR_EQUAL, literal_except_min_max).value(),
        in_bound);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::LESS_THAN, VALUE_LT_MIN).value(), empty);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::LESS_OR_EQUAL, VALUE_LT_MIN).value(), empty);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::LESS_THAN, VALUE_GT_MAX).value(), not_null);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::LESS_OR_EQUAL, VALUE_GT_MAX).value(),
              not_null);

    // test gt & gte
    ASSERT_EQ(
        bsi_->CompareUsingMinMax(Function::Type::GREATER_THAN, literal_except_min_max).value(),
        in_bound);
    ASSERT_EQ(
        bsi_->CompareUsingMinMax(Function::Type::GREATER_OR_EQUAL, literal_except_min_max).value(),
        in_bound);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::GREATER_THAN, VALUE_LT_MIN).value(),
              not_null);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::GREATER_OR_EQUAL, VALUE_LT_MIN).value(),
              not_null);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::GREATER_THAN, VALUE_GT_MAX).value(), empty);
    ASSERT_EQ(bsi_->CompareUsingMinMax(Function::Type::GREATER_OR_EQUAL, VALUE_GT_MAX).value(),
              empty);

    // test invalid case
    ASSERT_NOK_WITH_MSG(
        bsi_->CompareUsingMinMax(Function::Type::IN, 10),
        "Invalid Function::Type in CompareUsingMinMax of BitSliceIndex, only support "
        "EQUAL/NOT_EQUAL/GREATER_OR_EQUAL/GREATER_THAN/LESS_OR_EQUAL/LESS_THAN");
}

TEST_F(BitSliceIndexRoaringBitmapTest, TestSingleValue) {
    ASSERT_OK_AND_ASSIGN(auto appender,
                         BitSliceIndexRoaringBitmap::Appender::Create(/*min=*/1, /*max=*/1));
    ASSERT_OK(appender->Append(0, 1));
    ASSERT_OK(appender->Append(1, 1));
    ASSERT_OK(appender->Append(3, 1));
    auto bsi = appender->Build();
    ASSERT_EQ(bsi->Equal(1).value(), RoaringBitmap32::From({0, 1, 3}));
    ASSERT_EQ(bsi->CompareUsingMinMax(Function::Type::NOT_EQUAL, 1).value(),
              RoaringBitmap32::From({}));
}

}  // namespace paimon::test
