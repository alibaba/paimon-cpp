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

#include "paimon/global_index/global_index_result.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_vector_search_global_index_result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class GlobalIndexResultTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    class FakeGlobalIndexResult : public GlobalIndexResult {
     public:
        explicit FakeGlobalIndexResult(const std::vector<int64_t>& values) : values_(values) {}
        class Iterator : public GlobalIndexResult::Iterator {
         public:
            Iterator(const std::vector<int64_t>* values,
                     const std::vector<int64_t>::const_iterator& iter)
                : values_(values), iter_(iter) {}
            bool HasNext() const override {
                return iter_ != values_->end();
            }
            int64_t Next() override {
                int64_t value = *iter_;
                iter_++;
                return value;
            }
            const std::vector<int64_t>* values_;
            std::vector<int64_t>::const_iterator iter_;
        };

        Result<std::unique_ptr<GlobalIndexResult::Iterator>> CreateIterator() const override {
            auto iter = values_.begin();
            return std::make_unique<Iterator>(&values_, iter);
        }

        Result<bool> IsEmpty() const override {
            return values_.empty();
        }

        std::string ToString() const override {
            return "fake";
        }

        Result<std::shared_ptr<GlobalIndexResult>> AddOffset(int64_t offset) override {
            std::vector<int64_t> values = values_;
            for (auto& value : values) {
                value += offset;
            }
            return std::make_shared<FakeGlobalIndexResult>(values);
        }

     private:
        std::vector<int64_t> values_;
    };
};

TEST_F(GlobalIndexResultTest, TestSimple) {
    auto result1 = std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({1, 3, 5, 100}));
    auto result2 =
        std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({100, 5, 4, 3, 200}));
    ASSERT_OK_AND_ASSIGN(auto and_result, result1->And(result2));
    ASSERT_EQ(and_result->ToString(), "{3,5,100}");
    ASSERT_OK_AND_ASSIGN(auto and_ranges, and_result->ToRanges());
    std::vector<Range> expect_and_ranges = {Range(3, 3), Range(5, 5), Range(100, 100)};
    ASSERT_EQ(and_ranges, expect_and_ranges);

    ASSERT_OK_AND_ASSIGN(auto or_result, result1->Or(result2));
    ASSERT_EQ(or_result->ToString(), "{1,3,4,5,100,200}");
    ASSERT_OK_AND_ASSIGN(auto or_ranges, or_result->ToRanges());
    std::vector<Range> expect_or_ranges = {Range(1, 1), Range(3, 5), Range(100, 100),
                                           Range(200, 200)};
    ASSERT_EQ(or_ranges, expect_or_ranges);
}

TEST_F(GlobalIndexResultTest, TestSerializeAndDeserializeSimple) {
    auto pool = GetDefaultPool();
    std::vector<uint8_t> byte_buffer = {
        0,  0, 0, 1,  0,   0,   0, 69, 1,   0,   0, 0,  0,   0,   0, 0, 0,   0,   0, 0, 59,
        48, 3, 0, 5,  0,   0,   5, 0,  255, 127, 0, 0,  0,   128, 2, 0, 245, 133, 0, 0, 37,
        0,  0, 0, 47, 0,   0,   0, 49, 0,   0,   0, 55, 0,   0,   0, 2, 0,   1,   0, 4, 0,
        10, 0, 0, 0,  255, 255, 1, 0,  0,   0,   2, 0,  255, 224, 0, 0, 0,   0};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> index_result,
                         GlobalIndexResult::Deserialize(reinterpret_cast<char*>(byte_buffer.data()),
                                                        byte_buffer.size(), pool));
    auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(index_result);
    ASSERT_TRUE(typed_result);

    auto bitmap = RoaringBitmap64::From(
        {1l, 2l, 3l, 4l, 5l, 10l, 2247483647l, 2147483647l, 2147483648l, 2147483649l, 2147483650l});
    auto expected_result = std::make_shared<BitmapGlobalIndexResult>(
        [bitmap]() -> Result<RoaringBitmap64> { return bitmap; });
    ASSERT_EQ(expected_result->ToString(), typed_result->ToString());
    ASSERT_OK_AND_ASSIGN(auto serialize_bytes, GlobalIndexResult::Serialize(index_result, pool));
    ASSERT_EQ(byte_buffer, std::vector<uint8_t>(serialize_bytes->data(),
                                                serialize_bytes->data() + serialize_bytes->size()));
}

TEST_F(GlobalIndexResultTest, TestSerializeAndDeserializeWithScore) {
    auto pool = GetDefaultPool();
    std::vector<uint8_t> byte_buffer = {
        0,   0,   0,   1,   0,  0,   0,  64, 1,  0,  0,   0,   0,   0,   0,   0, 0,  0,   0,   0,
        58,  48,  0,   0,   4,  0,   0,  0,  0,  0,  0,   0,   255, 127, 0,   0, 0,  128, 2,   0,
        245, 133, 0,   0,   40, 0,   0,  0,  42, 0,  0,   0,   44,  0,   0,   0, 50, 0,   0,   0,
        10,  0,   255, 255, 1,  0,   3,  0,  5,  0,  255, 224, 0,   0,   0,   6, 63, 129, 71,  174,
        191, 168, 245, 195, 64, 135, 92, 41, 66, 74, 245, 195, 194, 200, 128, 0, 64, 6,   102, 102};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> index_result,
                         GlobalIndexResult::Deserialize(reinterpret_cast<char*>(byte_buffer.data()),
                                                        byte_buffer.size(), pool));
    auto typed_result =
        std::dynamic_pointer_cast<BitmapVectorSearchGlobalIndexResult>(index_result);
    ASSERT_TRUE(typed_result);

    auto bitmap = RoaringBitmap64::From(
        {10l, 2147483647l, 2147483649l, 2147483651l, 2147483653l, 2247483647l});
    std::vector<float> scores = {1.01f, -1.32f, 4.23f, 50.74f, -100.25f, 2.10f};
    auto expected_result =
        std::make_shared<BitmapVectorSearchGlobalIndexResult>(std::move(bitmap), std::move(scores));
    ASSERT_EQ(expected_result->ToString(), typed_result->ToString());
    ASSERT_OK_AND_ASSIGN(auto serialize_bytes, GlobalIndexResult::Serialize(index_result, pool));
    ASSERT_EQ(byte_buffer, std::vector<uint8_t>(serialize_bytes->data(),
                                                serialize_bytes->data() + serialize_bytes->size()));
}

TEST_F(GlobalIndexResultTest, TestInvalidSerialize) {
    auto pool = GetDefaultPool();
    auto result = std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({1, 3, 5, 100}));
    ASSERT_NOK_WITH_MSG(GlobalIndexResult::Serialize(result, pool),
                        "invalid GlobalIndexResult, must be BitmapGlobalIndexResult or "
                        "BitmapVectorSearchGlobalIndexResult");
}
}  // namespace paimon::test
