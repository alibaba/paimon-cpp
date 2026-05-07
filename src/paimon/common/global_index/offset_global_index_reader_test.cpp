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

#include "paimon/common/global_index/offset_global_index_reader.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::test {
class FakeGlobalIndexReader : public GlobalIndexReader {
 public:
    void SetDefaultResult(const std::vector<int64_t>& row_ids) {
        default_result_ = row_ids;
    }

    void SetVectorSearchResult(const std::vector<int64_t>& row_ids,
                               const std::vector<float>& scores) {
        vector_search_row_ids_ = row_ids;
        vector_search_scores_ = scores;
        has_vector_search_result_ = true;
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(
        const Literal& literal) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override {
        return MakeResult(default_result_);
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override {
        if (!has_vector_search_result_) {
            return Status::Invalid("FakeGlobalIndexReader does not support vector search");
        }
        auto bitmap = RoaringBitmap64::From(vector_search_row_ids_);
        auto scores = vector_search_scores_;
        return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap),
                                                               std::move(scores));
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override {
        return MakeResult(default_result_);
    }

    bool IsThreadSafe() const override {
        return true;
    }

    std::string GetIndexType() const override {
        return "fake";
    }

 private:
    static Result<std::shared_ptr<GlobalIndexResult>> MakeResult(
        const std::vector<int64_t>& row_ids) {
        auto ids = row_ids;
        return std::make_shared<BitmapGlobalIndexResult>(
            [ids]() { return RoaringBitmap64::From(ids); });
    }

 private:
    std::vector<int64_t> default_result_;
    std::vector<int64_t> vector_search_row_ids_;
    std::vector<float> vector_search_scores_;
    bool has_vector_search_result_ = false;
};

class OffsetGlobalIndexReaderTest : public ::testing::Test {
 public:
    void CheckResult(const std::shared_ptr<GlobalIndexResult>& result,
                     const std::vector<int64_t>& expected) const {
        ASSERT_TRUE(result);
        auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected).ToString();
    }

    static void CheckScoredResult(const std::shared_ptr<ScoredGlobalIndexResult>& result,
                                  const std::vector<int64_t>& expected_row_ids,
                                  const std::vector<float>& expected_scores) {
        ASSERT_TRUE(result);
        auto typed_result = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected_row_ids))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected_row_ids).ToString();
        ASSERT_EQ(typed_result->GetScores(), expected_scores);
    }
};

TEST_F(OffsetGlobalIndexReaderTest, TestVisitEqualWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 1, 3});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 100);

    Literal literal_5(5);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitEqual(literal_5));
    // row ids {0, 1, 3} + offset 100 -> {100, 101, 103}
    CheckResult(result, {100, 101, 103});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitIsNotNullWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 2, 4, 6});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 50);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitIsNotNull());
    // row ids {0, 2, 4, 6} + offset 50 -> {50, 52, 54, 56}
    CheckResult(result, {50, 52, 54, 56});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitIsNullWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({1, 3});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 200);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitIsNull());
    // row ids {1, 3} + offset 200 -> {201, 203}
    CheckResult(result, {201, 203});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitLessThanWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 1});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 10);

    Literal literal_5(5);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitLessThan(literal_5));
    // row ids {0, 1} + offset 10 -> {10, 11}
    CheckResult(result, {10, 11});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitGreaterOrEqualWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({3, 4, 5});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 1000);

    Literal literal_10(10);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitGreaterOrEqual(literal_10));
    // row ids {3, 4, 5} + offset 1000 -> {1003, 1004, 1005}
    CheckResult(result, {1003, 1004, 1005});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitInWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 2});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 5);

    std::vector<Literal> literals = {Literal(1), Literal(3)};
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitIn(literals));
    // row ids {0, 2} + offset 5 -> {5, 7}
    CheckResult(result, {5, 7});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitNotInWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({1, 3, 5});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 20);

    std::vector<Literal> literals = {Literal(2)};
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitNotIn(literals));
    // row ids {1, 3, 5} + offset 20 -> {21, 23, 25}
    CheckResult(result, {21, 23, 25});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitNotEqualWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 2, 4});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 7);

    Literal literal_1(1);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitNotEqual(literal_1));
    // row ids {0, 2, 4} + offset 7 -> {7, 9, 11}
    CheckResult(result, {7, 9, 11});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitLessOrEqualWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 1, 2});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 30);

    Literal literal_3(3);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitLessOrEqual(literal_3));
    // row ids {0, 1, 2} + offset 30 -> {30, 31, 32}
    CheckResult(result, {30, 31, 32});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitGreaterThanWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({4, 5});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 15);

    Literal literal_3(3);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitGreaterThan(literal_3));
    // row ids {4, 5} + offset 15 -> {19, 20}
    CheckResult(result, {19, 20});
}

TEST_F(OffsetGlobalIndexReaderTest, TestZeroOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 1, 2});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 0);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitIsNotNull());
    // row ids {0, 1, 2} + offset 0 -> {0, 1, 2}
    CheckResult(result, {0, 1, 2});
}

TEST_F(OffsetGlobalIndexReaderTest, TestEmptyResultWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 100);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitIsNotNull());
    // empty result + offset 100 -> still empty
    CheckResult(result, {});
}

TEST_F(OffsetGlobalIndexReaderTest, TestIsThreadSafeDelegated) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 100);
    // FakeGlobalIndexReader returns true for IsThreadSafe
    ASSERT_TRUE(offset_reader->IsThreadSafe());
}

TEST_F(OffsetGlobalIndexReaderTest, TestGetIndexTypeDelegated) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 100);
    ASSERT_EQ(offset_reader->GetIndexType(), "fake");
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitFullTextSearchWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 3, 5});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 10);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitFullTextSearch(nullptr));
    // row ids {0, 3, 5} + offset 10 -> {10, 13, 15}
    CheckResult(result, {10, 13, 15});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitVectorSearchWithOffset) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    fake_reader->SetVectorSearchResult({0, 2, 5}, {0.9f, 0.7f, 0.3f});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 100);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitVectorSearch(nullptr));
    // row ids {0, 2, 5} + offset 100 -> {100, 102, 105}, scores unchanged
    CheckScoredResult(result, {100, 102, 105}, {0.9f, 0.7f, 0.3f});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitVectorSearchNotSupported) {
    auto fake_reader = std::make_shared<FakeGlobalIndexReader>();
    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 10);
    // FakeGlobalIndexReader without SetVectorSearchResult returns error for VectorSearch
    ASSERT_NOK_WITH_MSG(offset_reader->VisitVectorSearch(nullptr),
                        "FakeGlobalIndexReader does not support vector search");
}

}  // namespace paimon::test
