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
#include "paimon/global_index/bitmap_vector_search_global_index_result.h"

#include "gtest/gtest.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"
namespace paimon::test {
class BitmapVectorSearchGlobalIndexResultTest : public ::testing::Test {
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

        std::string ToString() const override {
            return "fake";
        }

        Result<bool> IsEmpty() const override {
            return values_.empty();
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
TEST_F(BitmapVectorSearchGlobalIndexResultTest, TestIterator) {
    auto check_iterator = [](const std::vector<int64_t>& expected_ids,
                             const std::vector<float>& expected_scores) {
        ASSERT_EQ(expected_ids.size(), expected_scores.size());
        auto tmp_scores = expected_scores;

        auto index_result = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
            RoaringBitmap64::From(expected_ids), std::move(tmp_scores));
        if (expected_ids.empty()) {
            ASSERT_TRUE(index_result->IsEmpty().value());
        }
        // check iterator
        ASSERT_OK_AND_ASSIGN(auto iter, index_result->CreateIterator());
        for (auto expected_id : expected_ids) {
            ASSERT_TRUE(iter->HasNext());
            ASSERT_EQ(iter->Next(), expected_id);
        }
        ASSERT_FALSE(iter->HasNext());

        // check vector search iterator
        ASSERT_OK_AND_ASSIGN(auto vector_search_iter, index_result->CreateVectorSearchIterator());
        for (size_t i = 0; i < expected_ids.size(); i++) {
            ASSERT_TRUE(vector_search_iter->HasNext());
            auto [id, score] = vector_search_iter->NextWithScore();
            ASSERT_EQ(id, expected_ids[i]);
            ASSERT_NEAR(score, expected_scores[i], 0.01);
        }
        ASSERT_FALSE(vector_search_iter->HasNext());
    };

    check_iterator({}, {});
    check_iterator({1, 4, 7, RoaringBitmap32::MAX_VALUE, RoaringBitmap64::MAX_VALUE},
                   {1.0f, 2.1f, 3.2f, 4.5f, 6.7f});
    check_iterator({100, 101, 102, 103}, {100.1f, 200.2f, 0.12f, 0.34f});
}

TEST_F(BitmapVectorSearchGlobalIndexResultTest, TestAnd) {
    auto check_and_result = [](const std::vector<int64_t>& left_ids,
                               const std::vector<int64_t>& right_ids,
                               const std::string& expected_str) {
        std::vector<float> left_scores(left_ids.size(), 1.1f);
        auto index_result1 = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
            RoaringBitmap64::From(left_ids), std::move(left_scores));
        std::vector<float> right_scores(right_ids.size(), 1.2f);
        auto index_result2 = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
            RoaringBitmap64::From(right_ids), std::move(right_scores));
        ASSERT_OK_AND_ASSIGN(auto result, index_result1->And(index_result2));
        ASSERT_EQ(result->ToString(), expected_str);
    };
    check_and_result({1, 2, 3}, {1, 2, 7}, "{1,2}");
    check_and_result({1, 2, 3}, {1, 2, 3}, "{1,2,3}");
    check_and_result({1, 2, 3}, {100, 200, 300}, "{}");
    check_and_result({1, 2, 3}, {}, "{}");
    check_and_result({}, {}, "{}");
    check_and_result({1, 2, 3, RoaringBitmap64::MAX_VALUE}, {1, RoaringBitmap64::MAX_VALUE},
                     "{1,9223372036854775807}");
}

TEST_F(BitmapVectorSearchGlobalIndexResultTest, TestAndBitmapResult) {
    auto check_and_result =
        [](const std::vector<int64_t>& left_ids, std::vector<float>&& left_scores,
           const std::vector<int64_t>& right_ids, const std::string& expected_str) {
            auto index_result1 = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
                RoaringBitmap64::From(left_ids), std::move(left_scores));

            auto bitmap_supplier2 = [&]() -> Result<RoaringBitmap64> {
                return RoaringBitmap64::From(right_ids);
            };
            auto index_result2 = std::make_shared<BitmapGlobalIndexResult>(bitmap_supplier2);

            ASSERT_OK_AND_ASSIGN(auto result, index_result1->And(index_result2));
            ASSERT_EQ(result->ToString(), expected_str);
        };
    check_and_result({1, 2, 3}, {1.1f, 1.2f, 1.3f}, {1, 2, 7},
                     "row ids: {1,2}, scores: {1.10,1.20}");
    check_and_result({1, 2, 3}, {100.1f, 100.2f, 100.3f}, {1, 2, 3},
                     "row ids: {1,2,3}, scores: {100.10,100.20,100.30}");
    check_and_result({1, 2, 3}, {1.1f, 1.2f, 1.3f}, {100, 200, 300}, "row ids: {}, scores: {}");
    check_and_result({1, 2, 3}, {1.1f, 1.2f, 1.3f}, {}, "row ids: {}, scores: {}");
    check_and_result({}, {}, {}, "row ids: {}, scores: {}");
    check_and_result({1, 2, 3, RoaringBitmap64::MAX_VALUE}, {0.12f, 0.13f, 0.14f, 0.15f},
                     {1, RoaringBitmap64::MAX_VALUE},
                     "row ids: {1,9223372036854775807}, scores: {0.12,0.15}");
}

TEST_F(BitmapVectorSearchGlobalIndexResultTest, TestAndOtherResult) {
    auto index_result1 = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
        RoaringBitmap64::From({1, 2, 3}), std::vector<float>({1.1f, 1.2f, 1.3f}));

    auto fake_result = std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({1l, 2l, 7l}));

    ASSERT_OK_AND_ASSIGN(auto result, index_result1->And(fake_result));
    ASSERT_EQ(result->ToString(), "{1,2}");
}

TEST_F(BitmapVectorSearchGlobalIndexResultTest, TestOr) {
    auto check_or_result = [](const std::vector<int64_t>& left_ids,
                              std::vector<float>&& left_scores,
                              const std::vector<int64_t>& right_ids,
                              std::vector<float>&& right_scores, const std::string& expected_str) {
        auto index_result1 = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
            RoaringBitmap64::From(left_ids), std::move(left_scores));
        auto index_result2 = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
            RoaringBitmap64::From(right_ids), std::move(right_scores));
        ASSERT_OK_AND_ASSIGN(auto result, index_result1->Or(index_result2));
        ASSERT_EQ(result->ToString(), expected_str);
    };
    check_or_result({1, 2, 3}, {1.1f, 1.2f, 1.3f}, {100, 200, 300}, {100.1f, 200.1f, 300.1f},
                    "row ids: {1,2,3,100,200,300}, scores: {1.10,1.20,1.30,100.10,200.10,300.10}");
    check_or_result({1, 2, 3}, {1.1f, 1.2f, 1.3f}, {}, {},
                    "row ids: {1,2,3}, scores: {1.10,1.20,1.30}");
    check_or_result({}, {}, {}, {}, "row ids: {}, scores: {}");
    check_or_result(
        {1, 2, 3, RoaringBitmap64::MAX_VALUE}, {1.1f, 1.2f, 1.3f, 1.4f},
        {RoaringBitmap32::MAX_VALUE}, {0.12f},
        "row ids: {1,2,3,2147483647,9223372036854775807}, scores: {1.10,1.20,1.30,0.12,1.40}");
}

TEST_F(BitmapVectorSearchGlobalIndexResultTest, TestOrBitmapResult) {
    auto check_or_result = [](const std::vector<int64_t>& left_ids,
                              const std::vector<int64_t>& right_ids,
                              const std::string& expected_str) {
        std::vector<float> left_scores(left_ids.size(), 1.1f);
        auto index_result1 = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
            RoaringBitmap64::From(left_ids), std::move(left_scores));

        auto bitmap_supplier2 = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(right_ids);
        };
        auto index_result2 = std::make_shared<BitmapGlobalIndexResult>(bitmap_supplier2);

        ASSERT_OK_AND_ASSIGN(auto result, index_result1->Or(index_result2));
        ASSERT_EQ(result->ToString(), expected_str);
    };

    check_or_result({1, 2, 3}, {1, 2, 7}, "{1,2,3,7}");
    check_or_result({1, 2, 3}, {1, 2, 3}, "{1,2,3}");
    check_or_result({1, 2, 3}, {100, 200, 300}, "{1,2,3,100,200,300}");
    check_or_result({1, 2, 3}, {}, "{1,2,3}");
    check_or_result({}, {}, "{}");
    check_or_result({1, 2, 3, RoaringBitmap64::MAX_VALUE}, {1, RoaringBitmap64::MAX_VALUE},
                    "{1,2,3,9223372036854775807}");
}

TEST_F(BitmapVectorSearchGlobalIndexResultTest, TestOrOtherResult) {
    auto index_result1 = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
        RoaringBitmap64::From({1, 2, 3}), std::vector<float>({1.1f, 1.2f, 1.3f}));

    auto fake_result = std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({1l, 2l, 7l}));

    ASSERT_OK_AND_ASSIGN(auto result, index_result1->Or(fake_result));
    ASSERT_EQ(result->ToString(), "{1,2,3,7}");
}

TEST_F(BitmapVectorSearchGlobalIndexResultTest, TestInvalidOr) {
    std::vector<int64_t> left_ids = {1, 2, 3};
    std::vector<float> left_scores = {1.1f, 1.2f, 1.3f};
    auto index_result1 = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
        RoaringBitmap64::From(left_ids), std::move(left_scores));
    std::vector<int64_t> right_ids = {1, 2, 7};
    std::vector<float> right_scores = {2.1f, 2.2f, 2.3f};
    auto index_result2 = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
        RoaringBitmap64::From(right_ids), std::move(right_scores));
    ASSERT_NOK_WITH_MSG(index_result1->Or(index_result2),
                        "not support two BitmapVectorSearchGlobalIndexResult or with same row id");
}

TEST_F(BitmapVectorSearchGlobalIndexResultTest, TestAddOffset) {
    {
        std::vector<int64_t> ids = {1, 2, 3};
        std::vector<float> scores = {1.1f, 1.2f, 1.3f};
        auto index_result = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
            RoaringBitmap64::From(ids), std::move(scores));
        ASSERT_OK_AND_ASSIGN(auto result_with_offset, index_result->AddOffset(10));
        ASSERT_EQ(result_with_offset->ToString(), "row ids: {11,12,13}, scores: {1.10,1.20,1.30}");
    }
    {
        std::vector<int64_t> ids = {};
        std::vector<float> scores = {};
        auto index_result = std::make_shared<BitmapVectorSearchGlobalIndexResult>(
            RoaringBitmap64::From(ids), std::move(scores));
        ASSERT_OK_AND_ASSIGN(auto result_with_offset, index_result->AddOffset(10));
        ASSERT_EQ(result_with_offset->ToString(), "row ids: {}, scores: {}");
    }
}
}  // namespace paimon::test
